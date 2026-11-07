import { setImmediate } from "node:timers/promises";
import { describe, expect, it, vi } from "vitest";
import type { JsonRpcMessage } from "../src/protocol/jsonrpc.ts";
import { consumeSseStream, StreamableHttpTransport } from "../src/transports/streamable-http.ts";

const request = { jsonrpc: "2.0", id: 1, method: "ping" } as const;
const initialized = { jsonrpc: "2.0", method: "notifications/initialized" } as const;
const encoder = new TextEncoder();

function heldBody(chunk?: Uint8Array) {
	const cleanup = Promise.withResolvers<void>();
	let controller!: ReadableStreamDefaultController<Uint8Array>;
	let sent = false;
	const cancel = vi.fn(() => cleanup.promise);
	const pull = vi.fn((c: ReadableStreamDefaultController<Uint8Array>) => {
		if (chunk && !sent) {
			sent = true;
			c.enqueue(chunk);
		}
	});
	const body = new ReadableStream<Uint8Array>(
		{
			start: (c) => {
				controller = c;
			},
			pull,
			cancel,
		},
		{ highWaterMark: 0 },
	);
	return {
		body,
		cancel,
		pull,
		finish() {
			cleanup.resolve();
			if (cancel.mock.calls.length === 0) controller.close();
		},
	};
}

describe("HTTP cleanup without waiting for the producer", () => {
	it.each(["notification", "unsupported", "accepted", "get405", "getUnsupported", "delete", "auth"] as const)(
		"settles %s while body cancellation is pending",
		async (kind) => {
			const held = heldBody();
			let calls = 0;
			const errors: Error[] = [];
			const response = new Response(held.body, {
				status: kind === "get405" ? 405 : kind === "accepted" ? 202 : kind === "auth" ? 401 : 200,
				headers: { "content-type": "text/plain" },
			});
			const transport = new StreamableHttpTransport({
				url: "http://localhost/mcp",
				openGetStream: kind.startsWith("get"),
				authProvider:
					kind === "auth" ? { token: async () => undefined, onUnauthorized: async () => {} } : undefined,
				fetch: async (_url, init) => {
					calls++;
					if (kind === "delete" && init?.method !== "DELETE")
						return new Response(null, { status: 202, headers: { "mcp-session-id": "session" } });
					if (kind.startsWith("get") && init?.method !== "GET") return new Response(null, { status: 202 });
					if (kind === "auth" && calls > 1) return new Response(null, { status: 202 });
					return response;
				},
			});
			transport.onError((e) => errors.push(e));
			await transport.start();
			if (kind === "delete") await transport.send(initialized);
			let settled = false;
			let error: unknown;
			const operation = (
				kind === "delete"
					? transport.close()
					: transport.send(kind === "unsupported" || kind === "accepted" ? request : initialized)
			)
				.catch((e: unknown) => {
					error = e;
				})
				.finally(() => {
					settled = true;
				});
			try {
				await setImmediate();
				expect(held.cancel).toHaveBeenCalledOnce();
				expect(held.body.locked).toBe(false);
				expect(settled).toBe(true);
				if (kind === "unsupported" || kind === "accepted") expect(error).toBeInstanceOf(Error);
				else expect(error).toBeUndefined();
				if (kind === "getUnsupported") expect(errors).toHaveLength(1);
				if (kind === "auth") expect(calls).toBe(2);
			} finally {
				held.finish();
				await operation;
				await transport.close();
			}
		},
	);

	it.each(["jsonOverflow", "errorExactCap", "sseOverflow", "sseCallback"] as const)(
		"releases the reader for %s before cancellation settles",
		async (kind) => {
			const held = heldBody(encoder.encode(kind === "errorExactCap" ? "x".repeat(8192) : "data: payload\n\n"));
			const transport = new StreamableHttpTransport({
				url: "http://localhost/mcp",
				openGetStream: false,
				maxMessageBytes: 4,
				fetch: async () =>
					new Response(held.body, {
						status: kind === "errorExactCap" ? 503 : 200,
						headers: { "content-type": "application/json" },
					}),
			});
			await transport.start();
			let error: unknown;
			let settled = false;
			const callbackError = new Error("callback failed");
			const operation = (
				kind.startsWith("sse")
					? consumeSseStream(held.body, {
							maxEventBytes: kind === "sseOverflow" ? 4 : 100,
							onEvent() {
								throw callbackError;
							},
						})
					: transport.send(request)
			)
				.catch((e: unknown) => {
					error = e;
				})
				.finally(() => {
					settled = true;
				});
			try {
				await setImmediate();
				expect(settled).toBe(true);
				expect(held.cancel).toHaveBeenCalledOnce();
				expect(held.body.locked).toBe(false);
				if (kind === "sseCallback") expect(error).toBe(callbackError);
				else expect(error).toBeInstanceOf(Error);
				if (kind === "errorExactCap") expect(error).toMatchObject({ status: 503, body: "x".repeat(8192) });
			} finally {
				held.finish();
				await operation;
				await transport.close();
			}
		},
	);

	it.each(["eof", "extra"] as const)("waits beyond an exact JSON cap for %s", async (ending) => {
		const text = JSON.stringify({ jsonrpc: "2.0", id: 1, result: "é" });
		const bytes = encoder.encode(text);
		let controller!: ReadableStreamDefaultController<Uint8Array>;
		const cancel = vi.fn();
		const body = new ReadableStream<Uint8Array>({
			start(c) {
				controller = c;
				c.enqueue(bytes);
			},
			cancel,
		});
		const transport = new StreamableHttpTransport({
			url: "http://localhost/mcp",
			openGetStream: false,
			maxMessageBytes: bytes.length,
			fetch: async () => new Response(body, { headers: { "content-type": "application/json" } }),
		});
		const messages: JsonRpcMessage[] = [];
		transport.onMessage((m) => messages.push(m));
		await transport.start();
		let settled = false;
		let error: unknown;
		const sending = transport
			.send(request)
			.catch((e: unknown) => {
				error = e;
			})
			.finally(() => {
				settled = true;
			});
		try {
			await setImmediate();
			expect(settled).toBe(false);
			expect(messages).toEqual([]);
			if (ending === "extra") controller.enqueue(encoder.encode(" "));
			else controller.close();
			await sending;
			if (ending === "extra") {
				expect(error).toBeInstanceOf(Error);
				expect(cancel).toHaveBeenCalledOnce();
			} else {
				expect(error).toBeUndefined();
				expect(messages).toEqual([JSON.parse(text)]);
				expect(cancel).not.toHaveBeenCalled();
			}
			expect(body.locked).toBe(false);
		} finally {
			await transport.close();
		}
	});
});

describe("HTTP close before response consumption", () => {
	it.each([
		["POST", "application/json", 200, 0, 5],
		["POST", "application/json", 200, 1, 5],
		["POST", "application/json", 200, 2, 5],
		["POST", "text/event-stream", 200, 1, 5],
		["POST", "text/event-stream", 200, 2, 5],
		["GET", "text/event-stream", 200, 1, 5],
		["GET", "text/event-stream", 200, 2, 5],
		["GET", "text/event-stream", 200, 3, 5],
		["GET", "text/event-stream", 200, 3, 0],
		["POST", "text/plain", 503, 1, 5],
		["GET", "text/plain", 503, 1, 5],
	] as const)(
		"does not read %s %s status %i after close at depth %i (retries %i)",
		async (method, type, status, depth, retries) => {
			const ready = Promise.withResolvers<Response>();
			const held = heldBody();
			const transport = new StreamableHttpTransport({
				url: "http://localhost/mcp",
				openGetStream: method === "GET",
				reconnect: { maxRetries: retries },
				fetch: async (_url, init) =>
					init?.method === method ? ready.promise : new Response(null, { status: 202 }),
			});
			const errors: Error[] = [];
			const messages: JsonRpcMessage[] = [];
			const closed = vi.fn();
			transport.onError((e) => errors.push(e));
			transport.onMessage((m) => messages.push(m));
			transport.onClose(closed);
			await transport.start();
			let settled = false;
			let error: unknown;
			const sending = transport
				.send(method === "POST" ? request : initialized)
				.catch((e: unknown) => {
					error = e;
				})
				.finally(() => {
					settled = true;
				});
			await setImmediate();
			ready.resolve(
				new Response(held.body, { status, headers: { "content-type": type, "mcp-session-id": "late-session" } }),
			);
			let closing = Promise.resolve();
			for (let i = 0; i < depth; i++) closing = closing.then(() => {});
			let previousSession: string | undefined;
			closing = closing.then(async () => {
				expect(held.pull).not.toHaveBeenCalled();
				expect(held.body.locked).toBe(false);
				previousSession = transport.sessionId;
				await transport.close();
			});
			try {
				await closing;
				await setImmediate();
				expect(held.pull).not.toHaveBeenCalled();
				expect(held.cancel).toHaveBeenCalledOnce();
				expect(held.body.locked).toBe(false);
				expect(transport.sessionId).toBe(previousSession);
				expect(closed).toHaveBeenCalledOnce();
				expect(errors).toEqual([]);
				expect(messages).toEqual([]);
				expect(settled).toBe(true);
				if (method === "POST") expect(error).toMatchObject({ name: "McpConnectionClosedError" });
			} finally {
				held.finish();
				await sending;
				await transport.close();
			}
		},
	);
});
