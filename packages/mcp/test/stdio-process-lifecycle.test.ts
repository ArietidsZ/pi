import type { ChildProcess } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { describe, expect, it, vi } from "vitest";
import { StdioTransport } from "../src/transports/stdio.ts";

const fixture = fileURLToPath(new URL("./fixtures/process-lifecycle-server.mjs", import.meta.url));
type Identity = { pid: number; startTime?: string };

function linuxState(pid: number): { state: string; startTime: string } | undefined {
	try {
		const stat = readFileSync(`/proc/${pid}/stat`, "utf8");
		const fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
		return { state: fields[0], startTime: fields[19] };
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
		throw error;
	}
}

function executing(identity: Identity): boolean {
	if (process.platform === "linux") {
		const state = linuxState(identity.pid);
		return state !== undefined && identity.startTime === state.startTime && !["Z", "X"].includes(state.state);
	}
	try {
		process.kill(identity.pid, 0);
		return true;
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ESRCH") return false;
		throw error;
	}
}

async function bounded<T>(promise: Promise<T>, label: string): Promise<T> {
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		return await Promise.race([
			promise,
			new Promise<never>((_, reject) => {
				timer = setTimeout(() => reject(new Error(`${label} timed out`)), 3_000);
			}),
		]);
	} finally {
		clearTimeout(timer);
	}
}

async function waitStopped(identities: Identity[]): Promise<void> {
	const deadline = Date.now() + 3_000;
	while (identities.some(executing)) {
		if (Date.now() >= deadline) throw new Error("fixture processes stopping timed out");
		await delay(10);
	}
}

async function cleanupFixture(
	transport: StdioTransport,
	identities: Identity[],
	manifest: string,
	directory: string,
	expectedCloseFailure?: Error,
): Promise<unknown[]> {
	const failures: unknown[] = [];
	// Check original Linux identities even if readiness failed. Never kill a process group here.
	if (identities.length === 0) {
		try {
			identities.push(...(JSON.parse(readFileSync(manifest, "utf8")) as Identity[]));
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ENOENT") failures.push(error);
		}
	}
	for (const identity of identities) {
		try {
			if (executing(identity)) process.kill(identity.pid, "SIGKILL");
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ESRCH") failures.push(error);
		}
	}
	try {
		await bounded(transport.close(), "final transport close");
	} catch (error) {
		if (error !== expectedCloseFailure) failures.push(error);
	}
	try {
		await waitStopped(identities);
	} catch (error) {
		failures.push(error);
	}
	try {
		rmSync(directory, { recursive: true, force: true });
	} catch (error) {
		failures.push(error);
	}
	return failures;
}

async function withFixture(
	run: (transport: StdioTransport, identities: Identity[], reportedErrors: Error[]) => Promise<void>,
	options: { expectedCloseFailure?: Error; checkReportedErrors?: boolean } = {},
): Promise<void> {
	const directory = mkdtempSync(join(tmpdir(), "mcp-process-lifecycle-"));
	const manifest = join(directory, "pids.json");
	const transport = new StdioTransport({
		command: process.execPath,
		args: [fixture, manifest],
		inheritEnv: false,
		env: {},
		closeTimeoutMs: 100,
	});
	const identities: Identity[] = [];
	const errors: Error[] = [];
	const failures: unknown[] = [];
	transport.onError((error) => errors.push(error));
	const ready = new Promise<void>((resolve) => {
		transport.onMessage((message) => {
			if ("method" in message && message.method === "fixture/ready") resolve();
		});
	});
	try {
		await bounded(transport.start(), "spawn");
		await bounded(ready, "descendant IPC readiness");
		identities.push(...(JSON.parse(readFileSync(manifest, "utf8")) as Identity[]));
		expect(identities[0].pid).toBe(transport.pid);
		expect(identities.every(executing)).toBe(true);
		await run(transport, identities, errors);
		if (options.checkReportedErrors !== false) {
			if (options.expectedCloseFailure)
				expect(errors.every((error) => error === options.expectedCloseFailure)).toBe(true);
			else expect(errors).toEqual([]);
		}
	} catch (error) {
		failures.push(error);
	} finally {
		failures.push(
			...(await cleanupFixture(transport, identities, manifest, directory, options.expectedCloseFailure)),
		);
	}
	if (failures.length === 1) throw failures[0];
	if (failures.length > 1) throw new AggregateError(failures, "fixture assertion and cleanup failures");
}

describe.skipIf(process.platform === "win32")("stdio real same-group process lifecycle", () => {
	it("rejects shared close when final group SIGKILL fails", async () => {
		const failure = Object.assign(new Error("fixture group SIGKILL denied"), { code: "EPERM" });
		await withFixture(
			async (transport, identities) => {
				const originalKill = process.kill.bind(process);
				let injected = 0;
				const kill = vi.spyOn(process, "kill").mockImplementation((pid, signal) => {
					if (pid === -identities[0].pid && signal === "SIGKILL") {
						injected++;
						throw failure;
					}
					return originalKill(pid, signal);
				});
				try {
					const first = transport.close();
					expect(transport.close()).toBe(first);
					await expect(bounded(first, "failed group close")).rejects.toBe(failure);
					expect(injected).toBe(1);
					expect(executing(identities[1])).toBe(true);
				} finally {
					kill.mockRestore();
				}
			},
			{ expectedCloseFailure: failure },
		);
	}, 12_000);

	it.each(["close", "error"] as const)(
		"finishes internal cleanup despite a throwing %s observer",
		async (kind) => {
			await withFixture(
				async (transport, identities, reportedErrors) => {
					const fields = transport as unknown as { child: ChildProcess; stdoutBuffer: Buffer };
					const child = fields.child;
					const failure = new Error(`fixture ${kind} observer failed`);
					const caught: unknown[] = [];
					const originalEmit = child.emit.bind(child) as (event: string | symbol, ...args: unknown[]) => boolean;
					// Catch only the intentional synchronous close-dispatch exception at its emitter boundary.
					// This preserves EventEmitter's skipped-later-listeners behavior without crashing Vitest.
					const emit = vi.spyOn(child, "emit").mockImplementation((event: string | symbol, ...args: unknown[]) => {
						try {
							return originalEmit(event, ...args);
						} catch (error) {
							if (event !== "close" || error !== failure) throw error;
							caught.push(error);
							return true;
						}
					});
					const dispose =
						kind === "close"
							? transport.onClose(() => {
									throw failure;
								})
							: transport.onError(() => {
									throw failure;
								});
					if (kind === "error") fields.stdoutBuffer = Buffer.from("{");
					try {
						await bounded(transport.close(), "observer-safe close");
						await waitStopped(identities);
						expect(caught).toEqual([failure]);
						if (kind === "error") {
							expect(reportedErrors).toHaveLength(1);
							expect(reportedErrors[0].message).toContain("incomplete JSON-RPC message");
						} else expect(reportedErrors).toEqual([]);
					} finally {
						dispose();
						emit.mockRestore();
					}
				},
				{ checkReportedErrors: false },
			);
		},
		12_000,
	);

	it("escalates a TERM-ignoring descendant after its leader exits on stdin EOF", async () => {
		await withFixture(async (transport, identities) => {
			await bounded(transport.close(), "close");
			await waitStopped(identities);
		});
	}, 12_000);

	it("shares completion for concurrent close calls", async () => {
		await withFixture(async (transport, identities) => {
			const first = transport.close();
			const second = transport.close();
			expect(second).toBe(first);
			await bounded(Promise.all([first, second]), "concurrent close");
			await waitStopped(identities);
		});
	}, 12_000);

	it("cleans descendants after unexpected leader exit without an explicit close", async () => {
		await withFixture(async (transport, identities) => {
			const closed = new Promise<void>((resolve) => transport.onClose(resolve));
			await transport.send({ jsonrpc: "2.0", method: "fixture/exit" });
			await bounded(closed, "unexpected close event");
			await waitStopped(identities);
		});
	}, 12_000);
});
