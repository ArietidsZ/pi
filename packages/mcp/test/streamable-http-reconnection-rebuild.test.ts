import { afterEach, describe, expect, it, vi } from "vitest";
import type { JsonRpcMessage } from "../src/protocol/jsonrpc.ts";
import { StreamableHttpTransport } from "../src/transports/streamable-http.ts";

const request = { jsonrpc: "2.0", id: 1, method: "ping" } as const;
const initialized = { jsonrpc: "2.0", method: "notifications/initialized" } as const;
const notification = { jsonrpc: "2.0", method: "notifications/tools/list_changed" } as const;
const encoder = new TextEncoder();

afterEach(() => vi.useRealTimers());

describe("HTTP consecutive reconnection failures", () => {
	it.each([true, false])("resets the response retry budget only after progress: %s", async (progress) => {
		vi.useFakeTimers();
		const ids: (string | null)[] = [];
		const messages: JsonRpcMessage[] = [];
		const transport = new StreamableHttpTransport({
			url: "http://localhost/mcp",
			openGetStream: false,
			reconnect: { maxRetries: 1, initialDelayMs: 10 },
			fetch: async (_url, init) => {
				let text = "id: start\n\n";
				if (init?.method === "GET") {
					ids.push(new Headers(init.headers).get("last-event-id"));
					text =
						ids.length === 1
							? progress
								? `id: next\ndata: ${JSON.stringify(notification)}\n\n`
								: ": idle\n\n"
							: 'data: {"jsonrpc":"2.0","id":1,"result":{}}\n\n';
				}
				return new Response(text, { headers: { "content-type": "text/event-stream" } });
			},
		});
		transport.onMessage((m) => messages.push(m));
		await transport.start();
		try {
			await transport.send(request);
			await vi.advanceTimersByTimeAsync(30);
			if (progress) {
				expect(ids).toEqual(["start", "next"]);
				expect(messages).toEqual([notification, { jsonrpc: "2.0", id: 1, result: {} }]);
			} else {
				expect(ids).toEqual(["start"]);
				expect(messages).toHaveLength(1);
				expect(messages[0]).toHaveProperty("error");
			}
		} finally {
			await transport.close();
		}
	});

	it.each(["message", "idle", "failure", "protocol"] as const)(
		"accounts for GET %s before a broken body",
		async (mode) => {
			vi.useFakeTimers();
			let controller!: ReadableStreamDefaultController<Uint8Array>;
			const ids: (string | null)[] = [];
			const errors: Error[] = [];
			const transport = new StreamableHttpTransport({
				url: "http://localhost/mcp",
				reconnect: { maxRetries: 1, initialDelayMs: 10, maxDelayMs: 100 },
				maxMessageBytes: 128,
				fetch: async (_url, init) => {
					if (init?.method !== "GET") return new Response(null, { status: 202 });
					ids.push(new Headers(init.headers).get("last-event-id"));
					if (ids.length === 1 || mode === "failure") throw new TypeError("network failure");
					if (ids.length > 2) return new Response(null, { status: 405 });
					return new Response(
						new ReadableStream<Uint8Array>({
							start(c) {
								controller = c;
							},
						}),
						{ headers: { "content-type": "text/event-stream" } },
					);
				},
			});
			transport.onError((e) => errors.push(e));
			await transport.start();
			try {
				await transport.send(initialized);
				await vi.advanceTimersByTimeAsync(10);
				expect(ids).toHaveLength(2);
				if (mode !== "failure") {
					if (mode === "idle") await vi.advanceTimersByTimeAsync(101);
					else {
						controller.enqueue(encoder.encode(`id: delivered\ndata: ${JSON.stringify(notification)}\n\n`));
						await vi.advanceTimersByTimeAsync(0);
					}
					if (mode === "protocol") controller.enqueue(encoder.encode(`data: ${"x".repeat(256)}\n\n`));
					else controller.error(new TypeError("connection dropped"));
					await vi.advanceTimersByTimeAsync(10);
				}
				if (mode === "message" || mode === "idle") {
					expect(ids).toEqual([null, null, mode === "message" ? "delivered" : null]);
					expect(errors).toEqual([]);
				} else {
					expect(ids).toHaveLength(2);
					expect(errors).toHaveLength(1);
				}
			} finally {
				await transport.close();
			}
		},
	);
});

describe("HTTP buffered delivery after close", () => {
	it.each(["json", "sse", "malformed"] as const)("does not dispatch a second %s item after closing", async (kind) => {
		vi.useFakeTimers();
		const first = { jsonrpc: "2.0", id: 1, result: {} };
		const second = kind === "malformed" ? "not JSON" : JSON.stringify(notification);
		const body =
			kind === "json"
				? JSON.stringify([first, notification])
				: `data: ${JSON.stringify(first)}\n\ndata: ${second}\n\n`;
		const transport = new StreamableHttpTransport({
			url: "http://localhost/mcp",
			openGetStream: false,
			fetch: async () =>
				new Response(body, {
					headers: { "content-type": kind === "json" ? "application/json" : "text/event-stream" },
				}),
		});
		const messages: JsonRpcMessage[] = [];
		const errors: Error[] = [];
		transport.onMessage((m) => {
			messages.push(m);
			void transport.close();
		});
		transport.onError((e) => errors.push(e));
		await transport.start();
		try {
			await transport.send(request);
			await vi.advanceTimersByTimeAsync(0);
			expect(messages).toEqual([first]);
			expect(errors).toEqual([]);
		} finally {
			await transport.close();
		}
	});
});
