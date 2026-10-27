import { afterEach, describe, expect, it } from "vitest";
import type { JsonRpcMessage } from "../src/protocol/jsonrpc.ts";
import {
	McpAuthRequiredError,
	McpHttpError,
	McpSessionExpiredError,
	StreamableHttpTransport,
} from "../src/transports/streamable-http.ts";

const transports: StreamableHttpTransport[] = [];
const encoder = new TextEncoder();
const request = { jsonrpc: "2.0", id: 1, method: "tools/list" } as const;

afterEach(async () => {
	await Promise.all(transports.splice(0).map((transport) => transport.close()));
});

function trackedBody(chunks: Uint8Array[]) {
	let pulls = 0;
	let cancellations = 0;
	const body = new ReadableStream<Uint8Array>(
		{
			pull(controller) {
				const chunk = chunks[pulls++];
				if (chunk) controller.enqueue(chunk);
				else controller.close();
			},
			cancel() {
				cancellations++;
			},
		},
		{ highWaterMark: 0 },
	);
	return { body, pulls: () => pulls, cancellations: () => cancellations };
}

async function startTransport(response: Response, maxMessageBytes = 128) {
	const messages: JsonRpcMessage[] = [];
	const transport = new StreamableHttpTransport({
		url: "http://127.0.0.1:9200/mcp",
		maxMessageBytes,
		openGetStream: false,
		fetch: async (_url, init) => (init?.method === "DELETE" ? new Response(null, { status: 204 }) : response),
	});
	transports.push(transport);
	transport.onMessage((message) => messages.push(message));
	await transport.start();
	return { transport, messages };
}

describe("StreamableHttpTransport response body limits", () => {
	it("rejects an oversized single JSON chunk and cancels the unread body", async () => {
		const wire = encoder.encode(JSON.stringify({ jsonrpc: "2.0", id: 1, result: "x".repeat(256) }));
		const source = trackedBody([wire, encoder.encode(" ".repeat(256))]);
		const { transport, messages } = await startTransport(
			new Response(source.body, { headers: { "content-type": "application/json" } }),
		);

		await expect(transport.send(request)).rejects.toThrow(/exceeds.*128.*bytes/i);
		expect(messages).toEqual([]);
		expect(source.pulls()).toBe(1);
		expect(source.cancellations()).toBe(1);
		expect(source.body.locked).toBe(false);
	});

	it("counts JSON bytes across chunks without Content-Length", async () => {
		const wire = encoder.encode(JSON.stringify({ jsonrpc: "2.0", id: 1, result: "x".repeat(256) }));
		const source = trackedBody([wire.slice(0, 64), wire.slice(64, 129), wire.slice(129)]);
		const { transport, messages } = await startTransport(
			new Response(source.body, { headers: { "content-type": "application/json" } }),
		);

		await expect(transport.send(request)).rejects.toThrow(/exceeds.*128.*bytes/i);
		expect(messages).toEqual([]);
		expect(source.pulls()).toBe(2);
		expect(source.cancellations()).toBe(1);
	});

	it("accepts valid JSON exactly at the configured byte limit", async () => {
		const message = { jsonrpc: "2.0", id: 1, result: "exact" };
		const wire = encoder.encode(JSON.stringify(message));
		const source = trackedBody([wire.slice(0, 7), wire.slice(7)]);
		const { transport, messages } = await startTransport(
			new Response(source.body, { headers: { "content-type": "application/json; charset=utf-8" } }),
			wire.byteLength,
		);

		await transport.send(request);
		expect(messages).toEqual([message]);
		expect(source.cancellations()).toBe(0);
		expect(source.body.locked).toBe(false);
	});

	it("decodes split UTF-8 code points at the exact byte limit", async () => {
		const message = { jsonrpc: "2.0", id: 1, result: "汉🙂" };
		const wire = encoder.encode(JSON.stringify(message));
		const source = trackedBody(Array.from(wire, (byte) => Uint8Array.of(byte)));
		const { transport, messages } = await startTransport(
			new Response(source.body, { headers: { "content-type": "application/json" } }),
			wire.byteLength,
		);

		await transport.send(request);
		expect(messages).toEqual([message]);
	});

	it("rejects multibyte JSON one byte over the limit rather than counting UTF-16 characters", async () => {
		const text = JSON.stringify({ jsonrpc: "2.0", id: 1, result: "汉🙂" });
		const wire = encoder.encode(text);
		const limit = wire.byteLength - 1;
		expect(text.length).toBeLessThan(limit);
		const source = trackedBody([wire.slice(0, limit), wire.slice(limit)]);
		const { transport, messages } = await startTransport(
			new Response(source.body, { headers: { "content-type": "application/json" } }),
			limit,
		);

		await expect(transport.send(request)).rejects.toThrow(/exceeds.*bytes/i);
		expect(messages).toEqual([]);
		expect(source.cancellations()).toBe(1);
	});

	it.each(["1", "128", "not-a-number"])(
		"does not trust Content-Length %s instead of counting received bytes",
		async (contentLength) => {
			const wire = encoder.encode(JSON.stringify({ jsonrpc: "2.0", id: 1, result: "x".repeat(256) }));
			const source = trackedBody([wire.slice(0, 64), wire.slice(64)]);
			const { transport, messages } = await startTransport(
				new Response(source.body, {
					headers: { "content-type": "application/json", "content-length": contentLength },
				}),
			);

			await expect(transport.send(request)).rejects.toThrow(/exceeds.*128.*bytes/i);
			expect(messages).toEqual([]);
			expect(source.cancellations()).toBe(1);
		},
	);

	it("stops and cancels an unending error body after a bounded diagnostic prefix", async () => {
		let pulls = 0;
		let cancelled = false;
		const body = new ReadableStream<Uint8Array>(
			{
				pull(controller) {
					// An unbounded consumer hits this safety fuse instead of allocating forever.
					if (++pulls > 32) controller.error(new Error("unbounded diagnostic read"));
					else controller.enqueue(encoder.encode("x".repeat(1024)));
				},
				cancel() {
					cancelled = true;
				},
			},
			{ highWaterMark: 0 },
		);
		const { transport } = await startTransport(new Response(body, { status: 503 }));

		const error = await transport.send(request).catch((error: unknown) => error);
		expect(error).toBeInstanceOf(McpHttpError);
		expect(error).toMatchObject({ name: "McpHttpError", status: 503 });
		if (!(error instanceof McpHttpError)) throw new Error("expected an HTTP error");
		expect(error.body).toBe("x".repeat(8 * 1024));
		expect(pulls).toBeLessThanOrEqual(9);
		expect(cancelled).toBe(true);
		expect(body.locked).toBe(false);
	});

	it("bounds a single large error chunk even when Content-Length understates it", async () => {
		const source = trackedBody([encoder.encode("y".repeat(16 * 1024)), encoder.encode("unread")]);
		const { transport } = await startTransport(
			new Response(source.body, { status: 429, headers: { "content-length": "1" } }),
		);

		await expect(transport.send(request)).rejects.toMatchObject({
			name: "McpHttpError",
			status: 429,
			body: "y".repeat(8 * 1024),
		});
		expect(source.pulls()).toBe(1);
		expect(source.cancellations()).toBe(1);
	});

	it("retains the authentication error type and challenge after truncation", async () => {
		const challenge = 'Bearer resource_metadata="https://example.invalid/meta"';
		const source = trackedBody([encoder.encode("login ".repeat(2000))]);
		const { transport } = await startTransport(
			new Response(source.body, { status: 401, headers: { "www-authenticate": challenge } }),
		);

		const error = await transport.send(request).catch((error: unknown) => error);
		expect(error).toBeInstanceOf(McpAuthRequiredError);
		expect(error).toMatchObject({ name: "McpAuthRequiredError", status: 401, wwwAuthenticate: challenge });
		if (!(error instanceof McpAuthRequiredError)) throw new Error("expected an authentication error");
		expect(error.body).toBe("login ".repeat(2000).slice(0, 8 * 1024));
		expect(source.cancellations()).toBe(1);
	});

	it("retains the expired-session error type after truncation", async () => {
		const source = trackedBody([encoder.encode("gone".repeat(3000))]);
		let posts = 0;
		const transport = new StreamableHttpTransport({
			url: "http://127.0.0.1:9200/mcp",
			openGetStream: false,
			fetch: async (_url, init) => {
				if (init?.method === "DELETE") return new Response(null, { status: 204 });
				if (posts++ === 0) {
					return new Response(JSON.stringify({ jsonrpc: "2.0", id: 1, result: {} }), {
						headers: { "content-type": "application/json", "mcp-session-id": "session-fixture" },
					});
				}
				return new Response(source.body, { status: 404 });
			},
		});
		transports.push(transport);
		await transport.start();
		await transport.send(request);

		const error = await transport.send({ ...request, id: 2 }).catch((error: unknown) => error);
		expect(error).toBeInstanceOf(McpSessionExpiredError);
		expect(error).toMatchObject({ name: "McpSessionExpiredError", status: 404 });
		if (!(error instanceof McpSessionExpiredError)) throw new Error("expected an expired-session error");
		expect(error.body).toBe("gone".repeat(3000).slice(0, 8 * 1024));
		expect(source.cancellations()).toBe(1);
	});

	it("preserves short HTTP error diagnostics unchanged", async () => {
		const { transport } = await startTransport(new Response("Invalid Accept header", { status: 400 }));
		await expect(transport.send(request)).rejects.toMatchObject({
			name: "McpHttpError",
			status: 400,
			body: "Invalid Accept header",
			message: "MCP HTTP request failed with status 400: Invalid Accept header",
		});
	});
});
