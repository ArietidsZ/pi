import { afterEach, describe, expect, it, vi } from "vitest";
import { StreamableHttpTransport } from "../src/transports/streamable-http.ts";
import { closeServers, listen } from "./helpers.ts";

afterEach(closeServers);

describe("HTTP close with native fetch", () => {
	it.each([1, 2])("does not acquire a native response reader after close at continuation %i", async (depth) => {
		const socketClosed = Promise.withResolvers<void>();
		const origin = await listen(async (request, response) => {
			request.resume();
			response.on("close", () => socketClosed.resolve());
			response.writeHead(200, { "content-type": "application/json" });
			response.flushHeaders();
		});
		const captured = Promise.withResolvers<Response>();
		const release = Promise.withResolvers<Response>();
		let actualSignal: AbortSignal | null | undefined;
		const transport = new StreamableHttpTransport({
			url: origin,
			openGetStream: false,
			fetch: async (input, init) => {
				actualSignal = init?.signal;
				captured.resolve(await globalThis.fetch(input, init));
				return release.promise;
			},
		});
		await transport.start();
		const sending = transport.send({ jsonrpc: "2.0", id: 1, method: "ping" }).catch((error: unknown) => error);
		const response = await captured.promise;
		const body = response.body!;
		const getReader = vi.spyOn(body, "getReader");
		release.resolve(response);
		let closing = Promise.resolve();
		for (let step = 0; step < depth; step++) closing = closing.then(() => {});
		closing = closing.then(async () => {
			expect(getReader).not.toHaveBeenCalled();
			await transport.close();
		});
		try {
			await closing;
			const outcome = await sending;
			expect(getReader).not.toHaveBeenCalled();
			expect(outcome).toMatchObject({ name: "McpConnectionClosedError" });
			expect(body.locked).toBe(false);
			expect(actualSignal?.aborted).toBe(true);
			await socketClosed.promise;
		} finally {
			await transport.close();
		}
	});
});
