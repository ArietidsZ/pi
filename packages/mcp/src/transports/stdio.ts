import { type ChildProcess, spawn } from "node:child_process";
import process from "node:process";
import crossSpawn from "cross-spawn";
import {
	type JsonRpcMessage,
	McpAbortError,
	McpConnectionClosedError,
	parseJsonRpcMessage,
	toError,
} from "../protocol/jsonrpc.ts";
import {
	DEFAULT_MAX_MESSAGE_BYTES,
	type McpTransport,
	type McpTransportSendOptions,
	TransportEvents,
} from "./transport.ts";

const DEFAULT_MAX_STDERR_BYTES = 64 * 1024;
const DEFAULT_CLOSE_TIMEOUT_MS = 2_000;
/** How long a server gets to exit on its own after stdin closes, before it is sent SIGTERM. */
const STDIN_CLOSE_GRACE_MS = 500;
const USE_PROCESS_GROUPS = process.platform !== "win32";

/** Process groups of running servers, killed if the host exits without closing them. */
const liveProcessGroups = new Set<number>();
let exitHookInstalled = false;

function processGroupExists(pid: number): boolean {
	if (!liveProcessGroups.has(pid)) return false;
	try {
		process.kill(-pid, 0);
		return true;
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "ESRCH") return true;
		// Never use this numeric PGID again after observing that our group is gone.
		liveProcessGroups.delete(pid);
		return false;
	}
}

function killProcessTree(child: ChildProcess, signal: NodeJS.Signals): Error | undefined {
	const pid = child.pid;
	if (process.platform === "win32" && pid !== undefined) {
		if (child.exitCode !== null) return;
		// Windows has no graceful signals, and cross-spawn runs `.cmd` shims through cmd.exe; killing only
		// that would leave the server running.
		spawn("taskkill", ["/pid", String(pid), "/T", "/F"], { stdio: "ignore", windowsHide: true }).on(
			"error",
			() => {},
		);
		return;
	}
	if (USE_PROCESS_GROUPS && pid !== undefined) {
		if (!liveProcessGroups.has(pid)) return;
		try {
			// Negative pid: the whole group, so wrappers like `npx` or `uvx` do not leave the server behind.
			process.kill(-pid, signal);
			return;
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code === "ESRCH") {
				liveProcessGroups.delete(pid);
				return;
			}
			// A direct-child fallback is still useful, but cannot prove that the
			// owned group was signalled. Preserve that failure for final escalation.
			try {
				child.kill(signal);
			} catch {}
			return toError(error);
		}
	}
	try {
		if (!child.kill(signal) && child.exitCode === null && child.signalCode === null) {
			return new Error(`Could not send ${signal} to MCP stdio server`);
		}
	} catch (error) {
		return toError(error);
	}
}

function installExitHook(): void {
	if (exitHookInstalled) return;
	exitHookInstalled = true;
	process.once("exit", () => {
		for (const pid of liveProcessGroups) {
			try {
				// Host exit cannot wait for a graceful escalation timer.
				process.kill(-pid, "SIGKILL");
			} catch {}
		}
	});
}

export interface StdioTransportOptions {
	command: string;
	args?: readonly string[];
	cwd?: string;
	env?: Record<string, string>;
	inheritEnv?: boolean;
	stderr?: "pipe" | "inherit";
	onStderr?: (chunk: string) => void;
	maxMessageBytes?: number;
	maxStderrBytes?: number;
	/** Time to wait for the server to exit after SIGTERM before sending SIGKILL. Default: 2000. */
	closeTimeoutMs?: number;
}

export class StdioTransport extends TransportEvents implements McpTransport {
	readonly options: Readonly<StdioTransportOptions>;
	private child: ChildProcess | undefined;
	private stdoutBuffer = Buffer.alloc(0);
	private stderrBuffer = Buffer.alloc(0);
	private started = false;
	private closed = false;
	private childClosed = false;
	private closing: Promise<void> | undefined;

	constructor(options: StdioTransportOptions) {
		super();
		this.options = Object.freeze({ ...options, args: options.args ? [...options.args] : undefined });
	}

	get pid(): number | undefined {
		return this.child?.pid;
	}

	get stderr(): string {
		return this.stderrBuffer.toString("utf8");
	}

	async start(): Promise<void> {
		if (this.started) throw new Error("MCP stdio transport already started");
		if (this.closed) throw new McpConnectionClosedError();
		this.started = true;
		const env = this.options.inheritEnv === false ? { ...this.options.env } : { ...process.env, ...this.options.env };
		const child = crossSpawn(this.options.command, this.options.args ?? [], {
			cwd: this.options.cwd,
			env,
			stdio: ["pipe", "pipe", this.options.stderr === "inherit" ? "inherit" : "pipe"],
			windowsHide: true,
			// Own process group, so closing the transport can terminate the server's children too.
			detached: USE_PROCESS_GROUPS,
		});
		this.child = child;
		const pid = child.pid;
		if (USE_PROCESS_GROUPS && pid !== undefined) {
			installExitHook();
			liveProcessGroups.add(pid);
		}
		child.once("exit", () => {
			// The leader exiting does not establish that its descendants exited.
			if (!this.closing) void this.close().catch((error) => this.emitError(error));
		});
		child.stdout?.on("data", (chunk: Buffer | string) => this.handleStdout(chunk));
		child.stdout?.on("error", (error) => this.emitError(error));
		child.stdin?.on("error", (error) => {
			if (!this.closed) this.emitError(error);
		});
		child.stderr?.on("data", (chunk: Buffer | string) => this.handleStderr(chunk));
		child.stderr?.on("error", (error) => this.emitError(error));
		child.on("close", () => {
			this.childClosed = true;
			if (!this.closing) void this.close().catch((error) => this.emitError(error));
			if (this.stdoutBuffer.toString("utf8").trim()) {
				this.emitError(new Error("MCP stdio server closed with an incomplete JSON-RPC message"));
			}
			this.stdoutBuffer = Buffer.alloc(0);
			this.emitClose();
		});

		await new Promise<void>((resolve, reject) => {
			const onSpawn = () => {
				child.off("error", onError);
				resolve();
			};
			const onError = (error: Error) => {
				child.off("spawn", onSpawn);
				reject(error);
			};
			child.once("spawn", onSpawn);
			child.once("error", onError);
		});
		child.on("error", (error) => {
			if (!this.closed) this.emitError(error);
		});
	}

	async send(message: JsonRpcMessage, options: McpTransportSendOptions = {}): Promise<void> {
		const signal = options.signal;
		const stdin = this.child?.stdin;
		if (!this.started || this.closed || !stdin?.writable) throw new McpConnectionClosedError();
		if (signal?.aborted) throw new McpAbortError();
		const payload = `${JSON.stringify(message)}\n`;
		// Serialization may run user code; check again at the dispatch boundary.
		if (signal?.aborted) throw new McpAbortError();
		await new Promise<void>((resolve, reject) => {
			stdin.write(payload, (error) => (error ? reject(error) : resolve()));
		});
	}

	close(): Promise<void> {
		if (this.closing) return this.closing;
		this.closed = true;
		// Publish shared completion before callbacks can re-enter close().
		this.closing = Promise.resolve().then(() => {
			const child = this.child;
			if (!child) {
				this.emitClose();
				return;
			}
			const closeTimeoutMs = this.options.closeTimeoutMs ?? DEFAULT_CLOSE_TIMEOUT_MS;
			return new Promise<void>((resolve, reject) => {
				let forced = false;
				let graceTimer: ReturnType<typeof setTimeout> | undefined;
				let killTimer: ReturnType<typeof setTimeout> | undefined;
				const finish = (): boolean => {
					// The first internal listener sets this before invoking observers.
					// An observer may throw before our later close listener runs.
					if (!this.childClosed) return false;
					if (!forced && USE_PROCESS_GROUPS && child.pid !== undefined && processGroupExists(child.pid))
						return false;
					clearTimeout(graceTimer);
					clearTimeout(killTimer);
					child.off("close", onClose);
					if (child.pid !== undefined) liveProcessGroups.delete(child.pid);
					this.child = undefined;
					resolve();
					return true;
				};
				const terminate = () => {
					clearTimeout(graceTimer);
					if (killTimer) return;
					killProcessTree(child, "SIGTERM");
					if (finish()) return;
					killTimer = setTimeout(() => {
						const failure = killProcessTree(child, "SIGKILL");
						// Final signal responsibility is bounded. Do not retain a numeric
						// PGID for a later host-exit hook after this final attempt.
						if (child.pid !== undefined) liveProcessGroups.delete(child.pid);
						forced = true;
						// Escaped descendants are outside group ownership, but must not
						// keep our pipe handles (and close promise) open indefinitely.
						child.stdin?.destroy();
						child.stdout?.destroy();
						child.stderr?.destroy();
						if (failure) {
							child.off("close", onClose);
							this.child = undefined;
							reject(failure);
							return;
						}
						finish();
					}, closeTimeoutMs);
				};
				const onClose = () => {
					if (!finish()) terminate();
				};
				child.once("close", onClose);
				if (this.childClosed || child.exitCode !== null || child.signalCode !== null) {
					if (!finish()) terminate();
				} else {
					// Close stdin first, then terminate and escalate the owned process group.
					graceTimer = setTimeout(terminate, Math.min(STDIN_CLOSE_GRACE_MS, closeTimeoutMs));
					child.stdin?.end();
				}
			});
		});
		return this.closing;
	}

	private handleStdout(chunk: Buffer | string): void {
		this.stdoutBuffer = Buffer.concat([this.stdoutBuffer, Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)]);
		const maxMessageBytes = this.options.maxMessageBytes ?? DEFAULT_MAX_MESSAGE_BYTES;
		while (true) {
			const newline = this.stdoutBuffer.indexOf(0x0a);
			if (newline < 0) {
				if (this.stdoutBuffer.length > maxMessageBytes) {
					this.stdoutBuffer = Buffer.alloc(0);
					this.emitError(new Error(`MCP stdio message exceeds ${maxMessageBytes} bytes`));
				}
				return;
			}
			const line = this.stdoutBuffer.subarray(0, newline);
			this.stdoutBuffer = this.stdoutBuffer.subarray(newline + 1);
			if (line.length > maxMessageBytes) {
				this.emitError(new Error(`MCP stdio message exceeds ${maxMessageBytes} bytes`));
				continue;
			}
			const text = line.toString("utf8").replace(/\r$/, "");
			if (!text.trim()) continue;
			try {
				this.emitMessage(parseJsonRpcMessage(JSON.parse(text)));
			} catch (error) {
				this.emitError(error);
			}
		}
	}

	private handleStderr(chunk: Buffer | string): void {
		const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
		const maxStderrBytes = this.options.maxStderrBytes ?? DEFAULT_MAX_STDERR_BYTES;
		this.stderrBuffer = Buffer.concat([this.stderrBuffer, buffer]);
		if (this.stderrBuffer.length > maxStderrBytes) this.stderrBuffer = this.stderrBuffer.subarray(-maxStderrBytes);
		this.options.onStderr?.(buffer.toString("utf8"));
	}
}
