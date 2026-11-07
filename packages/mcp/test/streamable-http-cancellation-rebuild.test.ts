import { getEventListeners } from "node:events";
import { setTimeout as delay, setImmediate as nextTurn } from "node:timers/promises";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { AuthProvider, McpFetch } from "../src/auth-provider.ts";
import { McpClient } from "../src/client.ts";
import {
	isJsonRpcRequest,
	type JsonRpcMessage,
	McpAbortError,
	McpConnectionClosedError,
	parseJsonRpcMessage,
} from "../src/protocol/jsonrpc.ts";
import { LATEST_PROTOCOL_VERSION } from "../src/protocol/types.ts";
import { StreamableHttpTransport } from "../src/transports/streamable-http.ts";

function deferred<T>() {
	let resolve!: (value: T) => void;
	let reject!: (error: unknown) => void;
	const promise = new Promise<T>((resolvePromise, rejectPromise) => {
		resolve = resolvePromise;
		reject = rejectPromise;
	});
	return { promise, resolve, reject };
}

// A stalled regression must enter each test's finally block, not wait for Vitest to abandon the test.
async function bounded<T>(promise: Promise<T>): Promise<T> {
	const controller = new AbortController();
	try {
		return await Promise.race([
			promise,
			delay(1_000, undefined, { signal: controller.signal }).then(() => {
				throw new Error("Fixture operation did not settle within 1 second");
			}),
		]);
	} finally {
		controller.abort();
	}
}

function responseStream() {
	let controller!: ReadableStreamDefaultController<Uint8Array>;
	let closed = false;
	const cancel = vi.fn(() => {
		closed = true;
	});
	const body = new ReadableStream<Uint8Array>({
		start(value) {
			controller = value;
		},
		cancel,
	});
	return {
		body,
		cancel,
		write(text: string) {
			controller.enqueue(new TextEncoder().encode(text));
		},
		close() {
			if (closed) return;
			closed = true;
			controller.close();
		},
		response() {
			return new Response(body, { headers: { "content-type": "text/event-stream" } });
		},
	};
}

function jsonResponse(id: string | number, result: unknown = {}, sessionId?: string) {
	return new Response(JSON.stringify({ jsonrpc: "2.0", id, result }), {
		headers: {
			"content-type": "application/json",
			...(sessionId ? { "mcp-session-id": sessionId } : {}),
		},
	});
}

function postedMessage(init?: RequestInit): JsonRpcMessage {
	return parseJsonRpcMessage(JSON.parse(String(init?.body)));
}

afterEach(() => {
	vi.restoreAllMocks();
	vi.useRealTimers();
});

describe("Streamable HTTP request cancellation", () => {
	it("does not synthesize a late error when a cancelled response stream ends", async () => {
		const stream = responseStream();
		const request = new AbortController();
		const messages: JsonRpcMessage[] = [];
		const errors: Error[] = [];
		const transport = new StreamableHttpTransport({
			url: "https://mcp.example/endpoint",
			openGetStream: false,
			fetch: async () => stream.response(),
		});
		transport.onMessage((message) => messages.push(message));
		transport.onError((error) => errors.push(error));
		try {
			await transport.start();
			await transport.send({ jsonrpc: "2.0", id: 1, method: "tools/call" }, { signal: request.signal });
			request.abort();
			stream.close();
			await nextTurn();
			expect(messages).toEqual([]);
			expect(errors).toEqual([]);
		} finally {
			await transport.close();
			stream.close();
			await nextTurn();
		}
	});

	it("does not resume a cancelled request after an event-ID stream ends", async () => {
		vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
		const stream = responseStream();
		const request = new AbortController();
		const methods: string[] = [];
		const messages: JsonRpcMessage[] = [];
		const transport = new StreamableHttpTransport({
			url: "https://mcp.example/endpoint",
			openGetStream: false,
			reconnect: { initialDelayMs: 25, maxRetries: 1 },
			fetch: async (_url, init) => {
				methods.push(init?.method ?? "");
				return init?.method === "POST" ? stream.response() : new Response(null, { status: 405 });
			},
		});
		transport.onMessage((message) => messages.push(message));
		try {
			await transport.start();
			await transport.send({ jsonrpc: "2.0", id: 1, method: "tools/call" }, { signal: request.signal });
			stream.write("id: before-cancellation\n\n");
			await nextTurn();
			request.abort();
			stream.close();
			await nextTurn();
			await vi.advanceTimersByTimeAsync(100);
			expect(methods).toEqual(["POST"]);
			expect(messages).toEqual([]);
			expect(vi.getTimerCount()).toBe(0);
		} finally {
			await transport.close();
			stream.close();
			await nextTurn();
		}
	});

	it("cancels reconnect sleep and removes its timer and abort listeners", async () => {
		vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
		const stream = responseStream();
		const request = new AbortController();
		const methods: string[] = [];
		let lifecycleSignal: AbortSignal | undefined;
		const transport = new StreamableHttpTransport({
			url: "https://mcp.example/endpoint",
			openGetStream: false,
			reconnect: { initialDelayMs: 1_000, maxRetries: 1 },
			fetch: async (_url, init) => {
				methods.push(init?.method ?? "");
				lifecycleSignal = init?.signal ?? undefined;
				return init?.method === "POST" ? stream.response() : new Response(null, { status: 405 });
			},
		});
		try {
			await transport.start();
			await transport.send({ jsonrpc: "2.0", id: 1, method: "tools/call" }, { signal: request.signal });
			stream.write("id: sleeping\n\n");
			stream.close();
			await nextTurn();
			expect(vi.getTimerCount()).toBe(1);
			expect(lifecycleSignal).toBeDefined();
			request.abort();
			await nextTurn();
			expect(vi.getTimerCount()).toBe(0);
			expect(getEventListeners(request.signal, "abort")).toHaveLength(0);
			expect(getEventListeners(lifecycleSignal!, "abort")).toHaveLength(0);
			await vi.advanceTimersByTimeAsync(2_000);
			expect(methods).toEqual(["POST"]);
		} finally {
			await transport.close();
			stream.close();
			await nextTurn();
		}
	});

	it("removes reconnect sleep listeners when the delay completes normally", async () => {
		vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
		const initial = responseStream();
		const resumed = responseStream();
		const request = new AbortController();
		const messages: JsonRpcMessage[] = [];
		let lifecycleSignal: AbortSignal | undefined;
		const transport = new StreamableHttpTransport({
			url: "https://mcp.example/endpoint",
			openGetStream: false,
			reconnect: { initialDelayMs: 25, maxRetries: 1 },
			fetch: async (_url, init) => {
				lifecycleSignal = init?.signal ?? undefined;
				return init?.method === "POST" ? initial.response() : resumed.response();
			},
		});
		transport.onMessage((message) => messages.push(message));
		try {
			await transport.start();
			await transport.send({ jsonrpc: "2.0", id: 1, method: "tools/call" }, { signal: request.signal });
			initial.write("id: sleeping\n\n");
			initial.close();
			await nextTurn();
			expect(vi.getTimerCount()).toBe(1);
			await vi.advanceTimersByTimeAsync(25);
			resumed.write('data: {"jsonrpc":"2.0","id":1,"result":{"ok":true}}\n\n');
			resumed.close();
			await nextTurn();
			expect(messages).toEqual([{ jsonrpc: "2.0", id: 1, result: { ok: true } }]);
			expect(vi.getTimerCount()).toBe(0);
			expect(getEventListeners(request.signal, "abort")).toHaveLength(0);
			expect(getEventListeners(lifecycleSignal!, "abort")).toHaveLength(0);
		} finally {
			await transport.close();
			initial.close();
			resumed.close();
			await nextTurn();
		}
	});

	it("does not dispatch a resume GET after cancellation while its token is pending", async () => {
		vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
		const stream = responseStream();
		const request = new AbortController();
		const token = deferred<string>();
		const tokenStarted = deferred<void>();
		const methods: string[] = [];
		const messages: JsonRpcMessage[] = [];
		let tokenCalls = 0;
		const transport = new StreamableHttpTransport({
			url: "https://mcp.example/endpoint",
			openGetStream: false,
			reconnect: { initialDelayMs: 25, maxRetries: 1 },
			authProvider: {
				async token() {
					if (++tokenCalls === 1) return "sent-token";
					tokenStarted.resolve();
					return token.promise;
				},
			},
			fetch: async (_url, init) => {
				methods.push(init?.method ?? "");
				return init?.method === "POST" ? stream.response() : new Response(null, { status: 405 });
			},
		});
		transport.onMessage((message) => messages.push(message));
		try {
			await transport.start();
			await transport.send({ jsonrpc: "2.0", id: 1, method: "tools/call" }, { signal: request.signal });
			stream.write("id: resume-token\n\n");
			stream.close();
			await nextTurn();
			await vi.advanceTimersByTimeAsync(25);
			await bounded(tokenStarted.promise);
			request.abort();
			token.resolve("unused-token");
			await nextTurn();
			expect(methods).toEqual(["POST"]);
			expect(messages).toEqual([]);
			expect(vi.getTimerCount()).toBe(0);
		} finally {
			await transport.close();
			token.resolve("cleanup-token");
			stream.close();
			await nextTurn();
		}
	});

	it("does not synthesize a late error when an in-flight resume GET fails after cancellation", async () => {
		vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
		const stream = responseStream();
		const request = new AbortController();
		const resumed = deferred<Response>();
		const resumeStarted = deferred<void>();
		const messages: JsonRpcMessage[] = [];
		const errors: Error[] = [];
		const transport = new StreamableHttpTransport({
			url: "https://mcp.example/endpoint",
			openGetStream: false,
			reconnect: { initialDelayMs: 25, maxRetries: 1 },
			fetch: async (_url, init) => {
				if (init?.method === "POST") return stream.response();
				resumeStarted.resolve();
				return resumed.promise;
			},
		});
		transport.onMessage((message) => messages.push(message));
		transport.onError((error) => errors.push(error));
		try {
			await transport.start();
			await transport.send({ jsonrpc: "2.0", id: 1, method: "tools/call" }, { signal: request.signal });
			stream.write("id: resume-failure\n\n");
			stream.close();
			await nextTurn();
			await vi.advanceTimersByTimeAsync(25);
			await bounded(resumeStarted.promise);
			request.abort();
			resumed.reject(new Error("terminal resume failure"));
			await nextTurn();
			expect(messages).toEqual([]);
			expect(errors).toEqual([]);
			expect(vi.getTimerCount()).toBe(0);
		} finally {
			await transport.close();
			resumed.resolve(new Response(null, { status: 405 }));
			stream.close();
			await nextTurn();
		}
	});
});

describe("Streamable HTTP authorization dispatch guards", () => {
	it("does not dispatch a POST whose token resolves after close", async () => {
		const pendingToken = deferred<string>();
		const tokenStarted = deferred<void>();
		const requests: { method: string; authorization: string | null }[] = [];
		let calls = 0;
		let pending: Promise<unknown> | undefined;
		const transport = new StreamableHttpTransport({
			url: "https://mcp.example/endpoint",
			openGetStream: false,
			authProvider: {
				async token() {
					if (++calls === 1) return "sent-token";
					tokenStarted.resolve();
					return pendingToken.promise;
				},
			},
			fetch: async (_url, init) => {
				requests.push({
					method: init?.method ?? "",
					authorization: new Headers(init?.headers).get("authorization"),
				});
				if (init?.method === "DELETE") return new Response(null, { status: 204 });
				const message = postedMessage(init);
				if (!isJsonRpcRequest(message)) throw new Error("Expected a request");
				return jsonResponse(message.id, {}, "session-1");
			},
		});
		try {
			await transport.start();
			await transport.send({ jsonrpc: "2.0", id: 1, method: "initialize" });
			pending = transport.send({ jsonrpc: "2.0", id: 2, method: "tools/call" }).catch((error: unknown) => error);
			await bounded(tokenStarted.promise);
			await transport.close();
			pendingToken.resolve("unused-token");
			expect(await bounded(pending)).toBeInstanceOf(McpConnectionClosedError);
			expect(requests).toEqual([
				{ method: "POST", authorization: "Bearer sent-token" },
				{ method: "DELETE", authorization: "Bearer sent-token" },
			]);
			expect(calls).toBe(2);
		} finally {
			await transport.close();
			pendingToken.resolve("cleanup-token");
			await pending;
		}
	});

	it("keeps the last dispatched token for DELETE when a pending POST is aborted", async () => {
		const pendingToken = deferred<string>();
		const tokenStarted = deferred<void>();
		const request = new AbortController();
		const requests: { method: string; authorization: string | null }[] = [];
		let calls = 0;
		let pending: Promise<unknown> | undefined;
		const transport = new StreamableHttpTransport({
			url: "https://mcp.example/endpoint",
			openGetStream: false,
			authProvider: {
				async token() {
					if (++calls === 1) return "sent-token";
					tokenStarted.resolve();
					return pendingToken.promise;
				},
			},
			fetch: async (_url, init) => {
				requests.push({
					method: init?.method ?? "",
					authorization: new Headers(init?.headers).get("authorization"),
				});
				return init?.method === "DELETE" ? new Response(null, { status: 204 }) : jsonResponse(1, {}, "session-1");
			},
		});
		try {
			await transport.start();
			await transport.send({ jsonrpc: "2.0", id: 1, method: "initialize" });
			pending = transport
				.send({ jsonrpc: "2.0", id: 2, method: "tools/call" }, { signal: request.signal })
				.catch((error: unknown) => error);
			await bounded(tokenStarted.promise);
			request.abort();
			pendingToken.resolve("unused-token");
			expect(await bounded(pending)).toBeInstanceOf(McpAbortError);
			await transport.close();
			expect(requests).toEqual([
				{ method: "POST", authorization: "Bearer sent-token" },
				{ method: "DELETE", authorization: "Bearer sent-token" },
			]);
			expect(calls).toBe(2);
		} finally {
			await transport.close();
			pendingToken.resolve("cleanup-token");
			await pending;
		}
	});

	// #10598: cancellation must suppress only that request's retry, not the provider's shared recovery.
	it("lets a surviving McpClient request finish a shared authorization recovery", async () => {
		const refreshGate = deferred<void>();
		const bothWaiting = deferred<void>();
		const executed: string[] = [];
		const errors: Error[] = [];
		let token = "old-token";
		let refreshes = 0;
		let waiters = 0;
		let sharedRefresh: Promise<void> | undefined;
		let refreshSignal: AbortSignal | null | undefined;
		const fetch: McpFetch = async (url, init) => {
			if (String(url) === "https://mcp.example/token") {
				refreshes++;
				refreshSignal = init?.signal;
				await refreshGate.promise;
				return new Response(null, { status: 204 });
			}
			const message = postedMessage(init);
			if (!isJsonRpcRequest(message)) return new Response(null, { status: 202 });
			if (message.method === "initialize") {
				return jsonResponse(message.id, {
					protocolVersion: LATEST_PROTOCOL_VERSION,
					capabilities: {},
					serverInfo: { name: "fixture", version: "1" },
				});
			}
			if (new Headers(init?.headers).get("authorization") === "Bearer old-token") {
				return new Response(null, { status: 401, headers: { "www-authenticate": "Bearer" } });
			}
			executed.push(message.method);
			return jsonResponse(message.id, { ok: message.method });
		};
		const authProvider: AuthProvider = {
			token: async () => token,
			async onUnauthorized(context) {
				if (!sharedRefresh) {
					sharedRefresh = context.fetch("https://mcp.example/token").then(async (response) => {
						await response.body?.cancel();
						token = "fresh-token";
					});
				}
				if (++waiters === 2) bothWaiting.resolve();
				await sharedRefresh;
			},
		};
		const transport = new StreamableHttpTransport({
			url: "https://mcp.example/endpoint",
			openGetStream: false,
			authProvider,
			fetch,
		});
		const client = new McpClient({ name: "cancellation-fixture", version: "1", requestTimeoutMs: 0 });
		client.onError((error) => errors.push(error));
		const controller = new AbortController();
		const pending: Promise<unknown>[] = [];
		try {
			await bounded(client.connect(transport));
			const cancelled = client
				.request("cancelled", undefined, { signal: controller.signal })
				.catch((error: unknown) => error);
			const kept = client.request("kept").catch((error: unknown) => error);
			pending.push(cancelled, kept);
			await bounded(bothWaiting.promise);
			controller.abort();
			expect(await bounded(cancelled)).toBeInstanceOf(McpAbortError);
			expect(refreshSignal?.aborted ?? false).toBe(false);
			refreshGate.resolve();
			expect(await bounded(kept)).toEqual({ ok: "kept" });
			await nextTurn();
			expect(refreshes).toBe(1);
			expect(executed).toEqual(["kept"]);
			expect(errors).toEqual([]);
		} finally {
			await client.close();
			refreshGate.resolve();
			await Promise.all(pending);
			await sharedRefresh;
			await nextTurn();
		}
	});
});

describe("Streamable HTTP with real McpClient completion signals", () => {
	it("continues reading other messages after the owning request succeeds and aborts its send signal", async () => {
		const ownerStream = responseStream();
		const otherStream = responseStream();
		const requests = new Map<string, string | number>();
		const replies: JsonRpcMessage[] = [];
		const errors: Error[] = [];
		const otherDispatched = deferred<void>();
		const serverReplied = deferred<void>();
		const transport = new StreamableHttpTransport({
			url: "https://mcp.example/endpoint",
			openGetStream: false,
			fetch: async (_url, init) => {
				const message = postedMessage(init);
				if (!isJsonRpcRequest(message)) {
					if ("id" in message) {
						replies.push(message);
						serverReplied.resolve();
					}
					return new Response(null, { status: 202 });
				}
				if (message.method === "initialize") {
					return jsonResponse(message.id, {
						protocolVersion: LATEST_PROTOCOL_VERSION,
						capabilities: {},
						serverInfo: { name: "fixture", version: "1" },
					});
				}
				requests.set(message.method, message.id);
				if (message.method === "owner") return ownerStream.response();
				otherDispatched.resolve();
				return otherStream.response();
			},
		});
		const send = vi.spyOn(transport, "send");
		const client = new McpClient({ name: "completion-fixture", version: "1", requestTimeoutMs: 0 });
		const changed = vi.fn();
		client.onNotification("notifications/tools/list_changed", changed);
		client.onError((error) => errors.push(error));
		const pending: Promise<unknown>[] = [];
		try {
			await bounded(client.connect(transport));
			const owner = client.request("owner").catch((error: unknown) => error);
			const other = client.request("other").catch((error: unknown) => error);
			pending.push(owner, other);
			await bounded(otherDispatched.promise);
			await nextTurn();
			ownerStream.write(
				`data: ${JSON.stringify({ jsonrpc: "2.0", id: requests.get("owner"), result: "owner-result" })}\n\n`,
			);
			expect(await bounded(owner)).toBe("owner-result");
			const ownerSignal = send.mock.calls.find(([message]) => "method" in message && message.method === "owner")?.[1]
				?.signal;
			expect(ownerSignal?.aborted).toBe(true);
			expect(ownerStream.cancel).not.toHaveBeenCalled();
			ownerStream.write(
				`data: ${JSON.stringify({ jsonrpc: "2.0", id: requests.get("other"), result: "other-result" })}\n\n` +
					'data: {"jsonrpc":"2.0","method":"notifications/tools/list_changed"}\n\n' +
					'data: {"jsonrpc":"2.0","id":"server-ping","method":"ping"}\n\n',
			);
			expect(await bounded(other)).toBe("other-result");
			await bounded(serverReplied.promise);
			expect(changed).toHaveBeenCalledTimes(1);
			expect(replies).toEqual([{ jsonrpc: "2.0", id: "server-ping", result: {} }]);
			ownerStream.close();
			// The second request's response arrived on the first stream. Its own EOF must not invent a failure.
			otherStream.close();
			await nextTurn();
			expect(errors).toEqual([]);
		} finally {
			await client.close();
			ownerStream.close();
			otherStream.close();
			await Promise.all(pending);
			await nextTurn();
		}
	});
});
