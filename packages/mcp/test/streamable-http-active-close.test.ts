import { getEventListeners } from "node:events";
import { setImmediate } from "node:timers/promises";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { JsonRpcMessage } from "../src/protocol/jsonrpc.ts";
import { consumeSseStream, StreamableHttpTransport } from "../src/transports/streamable-http.ts";
import { closeServers, listen } from "./helpers.ts";

afterEach(closeServers);

describe("HTTP close while a custom response body is being read", () => {
	it.each([
		["POST", "application/json", 200],
		["POST", "text/plain", 503],
		["POST", "text/event-stream", 200],
		["GET", "text/event-stream", 200],
		["GET", "text/plain", 503],
	] as const)("cancels the active %s %s reader (status %i)", async (method, type, status) => {
		const reading = Promise.withResolvers<void>();
		const cleanup = Promise.withResolvers<void>();
		let controller!: ReadableStreamDefaultController<Uint8Array>;
		const cancel = vi.fn(() => cleanup.promise);
		const body = new ReadableStream<Uint8Array>(
			{
				start(value) {
					controller = value;
				},
				pull() {
					reading.resolve();
				},
				cancel,
			},
			{ highWaterMark: 0 },
		);
		let lifecycleSignal: AbortSignal | null | undefined;
		const methods: string[] = [];
		const transport = new StreamableHttpTransport({
			url: "http://localhost/mcp",
			openGetStream: method === "GET",
			fetch: async (_url, init) => {
				methods.push(init?.method ?? "");
				lifecycleSignal = init?.signal;
				// Deliberately return an independent stream, as a custom fetch adapter may do.
				return init?.method === method
					? new Response(body, { status, headers: { "content-type": type } })
					: new Response(null, { status: 202 });
			},
		});
		const messages: JsonRpcMessage[] = [];
		const errors: Error[] = [];
		const closed = vi.fn();
		transport.onMessage((message) => messages.push(message));
		transport.onError((error) => errors.push(error));
		transport.onClose(closed);
		await transport.start();
		let settled = false;
		let failure: unknown;
		const sending = transport
			.send(
				method === "GET"
					? { jsonrpc: "2.0", method: "notifications/initialized" }
					: { jsonrpc: "2.0", id: 1, method: "ping" },
			)
			.catch((error: unknown) => {
				failure = error;
			})
			.finally(() => {
				settled = true;
			});
		try {
			await reading.promise;
			expect(body.locked).toBe(true);
			await transport.close();
			await setImmediate();
			expect(lifecycleSignal?.aborted).toBe(true);
			expect(cancel).toHaveBeenCalledOnce();
			expect(body.locked).toBe(false);
			expect(settled).toBe(true);
			if (method === "POST" && type !== "text/event-stream")
				expect(failure).toMatchObject({ name: "McpConnectionClosedError" });
			else expect(failure).toBeUndefined();
			expect(messages).toEqual([]);
			expect(errors).toEqual([]);
			expect(closed).toHaveBeenCalledOnce();
			expect(methods).toEqual(method === "GET" ? ["POST", "GET"] : ["POST"]);
			expect(getEventListeners(lifecycleSignal!, "abort")).toHaveLength(0);
		} finally {
			cleanup.resolve();
			if (cancel.mock.calls.length === 0) controller.close();
			await sending;
			await transport.close();
			await setImmediate();
		}
	});
});

describe("HTTP reader cancellation ownership", () => {
	it("stops within a buffered SSE chunk when the event observer aborts", async () => {
		const owner = new AbortController();
		const reason = new Error("owner stopped");
		const body = new ReadableStream<Uint8Array>({
			start(controller) {
				controller.enqueue(new TextEncoder().encode("data: first\n\ndata: second\n\n"));
			},
		});
		const event = vi.fn(() => owner.abort(reason));
		await expect(consumeSseStream(body, { signal: owner.signal, onEvent: event })).rejects.toBe(reason);
		expect(event).toHaveBeenCalledOnce();
		expect(body.locked).toBe(false);
		expect(getEventListeners(owner.signal, "abort")).toHaveLength(0);
	});

	it.each(["application/json", "text/event-stream"])("removes the abort listener after %s EOF", async (type) => {
		let signal: AbortSignal | null | undefined;
		const transport = new StreamableHttpTransport({
			url: "http://localhost/mcp",
			openGetStream: false,
			fetch: async (_url, init) => {
				signal = init?.signal;
				const json = '{"jsonrpc":"2.0","id":1,"result":{}}';
				return new Response(type === "application/json" ? json : `data: ${json}\n\n`, {
					headers: { "content-type": type },
				});
			},
		});
		const received = vi.fn();
		transport.onMessage(received);
		try {
			await transport.start();
			await transport.send({ jsonrpc: "2.0", id: 1, method: "ping" });
			await setImmediate();
			expect(received).toHaveBeenCalledOnce();
			expect(getEventListeners(signal!, "abort")).toHaveLength(0);
		} finally {
			await transport.close();
		}
	});

	it("drops a buffered SSE event and releases the reader even if producer cancellation rejects", async () => {
		const owner = new AbortController();
		const reason = new Error("owner stopped");
		const pendingRead = Promise.withResolvers<void>();
		const event = vi.fn();
		const cancel = vi.fn(async () => {
			throw new Error("producer cleanup failed");
		});
		const body = new ReadableStream<Uint8Array>(
			{
				start(controller) {
					controller.enqueue(new TextEncoder().encode("data: incomplete-event\n"));
				},
				pull() {
					pendingRead.resolve();
				},
				cancel,
			},
			{ highWaterMark: 0 },
		);
		const consumed = consumeSseStream(body, { signal: owner.signal, onEvent: event }).catch(
			(error: unknown) => error,
		);
		try {
			await pendingRead.promise;
			owner.abort(reason);
			expect(await consumed).toBe(reason);
			expect(event).not.toHaveBeenCalled();
			expect(cancel).toHaveBeenCalledOnce();
			expect(body.locked).toBe(false);
			expect(getEventListeners(owner.signal, "abort")).toHaveLength(0);
		} finally {
			owner.abort(reason);
			await consumed;
		}
	});

	it.each(["application/json", "text/event-stream"])("closes an active native %s response socket", async (type) => {
		const socketClosed = Promise.withResolvers<void>();
		const origin = await listen(async (request, response) => {
			request.resume();
			response.on("close", () => socketClosed.resolve());
			response.writeHead(200, { "content-type": type });
			response.flushHeaders();
		});
		const captured = Promise.withResolvers<Response>();
		const transport = new StreamableHttpTransport({
			url: origin,
			openGetStream: false,
			fetch: async (input, init) => {
				const response = await globalThis.fetch(input, init);
				captured.resolve(response);
				return response;
			},
		});
		const message = vi.fn();
		const error = vi.fn();
		transport.onMessage(message);
		transport.onError(error);
		await transport.start();
		const sending = transport.send({ jsonrpc: "2.0", id: 1, method: "ping" }).catch((failure: unknown) => failure);
		try {
			const response = await captured.promise;
			await vi.waitFor(() => expect(response.body!.locked).toBe(true));
			await transport.close();
			const outcome = await sending;
			await socketClosed.promise;
			expect(response.body!.locked).toBe(false);
			if (type === "application/json") expect(outcome).toMatchObject({ name: "McpConnectionClosedError" });
			else expect(outcome).toBeUndefined();
			expect(message).not.toHaveBeenCalled();
			expect(error).not.toHaveBeenCalled();
		} finally {
			await transport.close();
			await closeServers();
			await sending;
		}
	});
});
