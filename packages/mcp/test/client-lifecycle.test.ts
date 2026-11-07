// Candidate for packages/mcp/test/client-lifecycle.test.ts. NOT EXECUTED.
import { describe, expect, it } from "vitest";
import { McpClient } from "../src/client.ts";
import type { JsonRpcMessage, JsonRpcResponse } from "../src/protocol/jsonrpc.ts";
import { LATEST_PROTOCOL_VERSION } from "../src/protocol/types.ts";
import { type McpTransportSendOptions, TransportEvents } from "../src/transports/transport.ts";

function deferred<T>() {
	let resolve!: (value: T) => void;
	let reject!: (error: unknown) => void;
	const promise = new Promise<T>((yes, no) => {
		resolve = yes;
		reject = no;
	});
	return { promise, resolve, reject };
}

class ControlledTransport extends TransportEvents {
	replies: JsonRpcResponse[] = [];
	closes = 0;
	initialized: () => Promise<void> = async () => {};
	reply: (message: JsonRpcResponse, options?: McpTransportSendOptions) => Promise<void> = async () => {};
	async start() {}
	async close() {
		this.closes++;
	}
	receive(message: JsonRpcMessage) {
		this.emitMessage(message);
	}
	send(message: JsonRpcMessage, options?: McpTransportSendOptions): Promise<void> {
		if ("method" in message) {
			if (message.method === "notifications/initialized") return this.initialized();
			if (message.method === "initialize" && "id" in message) {
				queueMicrotask(() =>
					this.receive({
						jsonrpc: "2.0",
						id: message.id,
						result: {
							protocolVersion: LATEST_PROTOCOL_VERSION,
							capabilities: {},
							serverInfo: { name: "controlled", version: "1" },
						},
					}),
				);
			}
			return Promise.resolve();
		}
		this.replies.push(message);
		return this.reply(message, options);
	}
}

async function setup() {
	const client = new McpClient({ name: "test", version: "1" });
	const transport = new ControlledTransport();
	await client.connect(transport);
	return { client, transport };
}

// Only drain the known Promise continuations in these controlled fixtures.
async function drain() {
	for (let i = 0; i < 8; i++) await Promise.resolve();
}
const request = { jsonrpc: "2.0", id: 42, method: "work" } as const;

describe("MCP lifecycle cancellation candidate", () => {
	it("does not reply to a handler resolving after close", async () => {
		const { client, transport } = await setup();
		const result = deferred<unknown>();
		client.setRequestHandler("work", () => result.promise);
		transport.receive(request);
		await client.close();
		result.resolve({ done: true });
		await drain();
		expect(transport.replies).toEqual([]);
	});

	it("does not send an error response after cancellation", async () => {
		const { client, transport } = await setup();
		const result = deferred<unknown>();
		client.setRequestHandler("work", () => result.promise);
		transport.receive(request);
		transport.receive({ jsonrpc: "2.0", method: "notifications/cancelled", params: { requestId: 42 } });
		result.reject(new Error("cancelled"));
		await drain();
		expect(transport.replies).toEqual([]);
		await client.close();
	});

	it("does not send a successful handler result after cancellation", async () => {
		const { client, transport } = await setup();
		const result = deferred<unknown>();
		client.setRequestHandler("work", () => result.promise);
		transport.receive(request);
		transport.receive({ jsonrpc: "2.0", method: "notifications/cancelled", params: { requestId: 42 } });
		result.resolve({ done: true });
		await drain();
		expect(transport.replies).toEqual([]);
		await client.close();
	});

	it("tracks method-not-found replies until send settlement", async () => {
		const { client, transport } = await setup();
		const pending = deferred<void>();
		let signal: AbortSignal | undefined;
		transport.reply = (_message, options) => {
			signal = options?.signal;
			return pending.promise;
		};
		transport.receive({ ...request, method: "missing" });
		await drain();
		expect(signal?.aborted).toBe(false);
		await client.close();
		expect(signal?.aborted).toBe(true);
		pending.reject(new Error("aborted"));
		await drain();
		expect(transport.replies).toHaveLength(1);
	});

	it("serves ping while initialization notification is still pending", async () => {
		const client = new McpClient({ name: "test", version: "1" });
		const transport = new ControlledTransport();
		const initialized = deferred<void>();
		const entered = deferred<void>();
		transport.initialized = () => {
			entered.resolve();
			return initialized.promise;
		};
		const connected = client.connect(transport);
		await entered.promise;
		expect(client.connectionState).toBe("connecting");
		transport.receive({ ...request, method: "ping" });
		await drain();
		expect(transport.replies).toEqual([{ jsonrpc: "2.0", id: 42, result: {} }]);
		initialized.resolve();
		await connected;
		await client.close();
	});

	it("aborts a queued reply and retains the original controller on duplicate id", async () => {
		const { client, transport } = await setup();
		const pending = deferred<void>();
		let signal: AbortSignal | undefined;
		let calls = 0;
		const errors: Error[] = [];
		client.onError((error) => errors.push(error));
		client.setRequestHandler("work", () => {
			calls++;
			return {};
		});
		transport.reply = (_message, options) => {
			signal = options?.signal;
			return pending.promise;
		};
		transport.receive(request);
		await drain();
		transport.receive(request);
		await drain();
		expect(calls).toBe(1);
		expect(errors).toHaveLength(1);
		expect(signal?.aborted).toBe(false);
		await client.close();
		expect(signal?.aborted).toBe(true);
		pending.reject(new Error("aborted"));
		await drain();
		expect(transport.replies).toHaveLength(1);
		expect(errors).toHaveLength(1);
	});

	it.each(["sync", "async"])("never sends a second reply after %s send failure", async (kind) => {
		const { client, transport } = await setup();
		const errors: Error[] = [];
		client.onError((error) => errors.push(error));
		client.setRequestHandler("work", () => ({}));
		transport.reply = () => {
			if (kind === "sync") throw new Error("send failed");
			return Promise.reject(new Error("send failed"));
		};
		transport.receive(request);
		await drain();
		expect(transport.replies).toHaveLength(1);
		expect(errors).toHaveLength(1);
		await client.close();
	});

	it("still replies once to live handler and method-not-found errors", async () => {
		const { client, transport } = await setup();
		client.setRequestHandler("work", () => {
			throw new Error("handler failed");
		});
		transport.receive(request);
		await drain();
		transport.receive({ ...request, id: 43, method: "missing" });
		await drain();
		expect(transport.replies).toMatchObject([
			{ id: 42, error: { code: -32603 } },
			{ id: 43, error: { code: -32601 } },
		]);
		await client.close();
	});

	it("delivers later close observers despite an error-observer exception", async () => {
		const { client, transport } = await setup();
		const events: string[] = [];
		const failure = new Error("reporting failed");
		client.onClose(() => {
			events.push("first");
			throw new Error("observer failed");
		});
		client.onClose(() => {
			events.push("second");
		});
		client.onError(() => {
			throw failure;
		});
		const closing = client.close();
		expect(client.close()).toBe(closing);
		await expect(closing).rejects.toBe(failure);
		expect(events).toEqual(["first", "second"]);
		expect(transport.closes).toBe(1);
	});
});
