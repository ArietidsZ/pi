// Candidate for packages/mcp/test/client-memory-cancellation.test.ts. NOT EXECUTED.
// Real in-memory cloning/queue, with a synchronous fault-injection seam at enqueue.
import { describe, expect, it } from "vitest";
import { McpClient } from "../src/client.ts";
import {
	isJsonRpcRequest,
	isJsonRpcResponse,
	type JsonRpcMessage,
	type JsonRpcResponse,
} from "../src/protocol/jsonrpc.ts";
import { LATEST_PROTOCOL_VERSION } from "../src/protocol/types.ts";
import { InMemoryTransport } from "../src/transports/in-memory.ts";
import type { McpTransportSendOptions } from "../src/transports/transport.ts";

class EnqueueProbe extends InMemoryTransport {
	afterEnqueue?: (message: JsonRpcResponse, options?: McpTransportSendOptions) => void;
	override send(message: JsonRpcMessage, options?: McpTransportSendOptions): Promise<void> {
		const pending = super.send(message, options);
		if (isJsonRpcResponse(message)) this.afterEnqueue?.(message, options);
		return pending;
	}
	injectCancellation(id: number) {
		this.emitMessage({ jsonrpc: "2.0", method: "notifications/cancelled", params: { requestId: id } });
	}
}

async function setup() {
	const transport = new EnqueueProbe();
	const server = new InMemoryTransport();
	transport.connectPeer(server);
	server.connectPeer(transport);
	const replies: JsonRpcResponse[] = [];
	const handshakeErrors: unknown[] = [];
	server.onMessage((message) => {
		if (isJsonRpcRequest(message) && message.method === "initialize") {
			void server
				.send({
					jsonrpc: "2.0",
					id: message.id,
					result: {
						protocolVersion: LATEST_PROTOCOL_VERSION,
						capabilities: {},
						serverInfo: { name: "memory-peer", version: "1" },
					},
				})
				.catch((error) => handshakeErrors.push(error));
		} else if (isJsonRpcResponse(message)) replies.push(message);
	});
	await server.start();
	const client = new McpClient({ name: "integration-test", version: "1" });
	await client.connect(transport);
	expect(handshakeErrors).toEqual([]);
	return { client, server, transport, replies };
}

// Known microtask queue barrier, not a timeout or an end-to-end completion proof.
function queueBarrier(): Promise<void> {
	return new Promise((resolve) => queueMicrotask(resolve));
}

describe("client cancellation reaches the in-memory dispatch boundary", () => {
	it.each(["work", "missing"])("suppresses an enqueued %s response after synchronous cancellation", async (method) => {
		const { client, server, transport, replies } = await setup();
		try {
			client.setRequestHandler("work", () => ({ done: true }));
			let signal: AbortSignal | undefined;
			let attempts = 0;
			let observedId: unknown;
			let initiallyAborted: boolean | undefined;
			let enqueued!: () => void;
			const reached = new Promise<void>((resolve) => {
				enqueued = resolve;
			});
			transport.afterEnqueue = (message, options) => {
				observedId = message.id;
				attempts++;
				signal = options?.signal;
				initiallyAborted = signal?.aborted;
				transport.injectCancellation(42);
				enqueued();
			};
			await server.send({ jsonrpc: "2.0", id: 42, method });
			await reached;
			await queueBarrier();
			expect(observedId).toBe(42);
			expect(initiallyAborted).toBe(false);
			expect(attempts).toBe(1);
			expect(signal?.aborted).toBe(true);
			expect(replies).toEqual([]);
			expect(client.connectionState).toBe("connected");
		} finally {
			await client.close();
		}
	});

	it("delivers a live reply without cancellation", async () => {
		const { client, server, transport, replies } = await setup();
		try {
			client.setRequestHandler("work", () => ({ done: true }));
			let enqueued!: () => void;
			const reached = new Promise<void>((resolve) => {
				enqueued = resolve;
			});
			let initiallyAborted: boolean | undefined;
			transport.afterEnqueue = (_message, options) => {
				initiallyAborted = options?.signal?.aborted;
				enqueued();
			};
			await server.send({ jsonrpc: "2.0", id: 42, method: "work" });
			await reached;
			await queueBarrier();
			expect(initiallyAborted).toBe(false);
			expect(replies).toEqual([{ jsonrpc: "2.0", id: 42, result: { done: true } }]);
		} finally {
			await client.close();
		}
	});
});
