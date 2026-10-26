// Candidate destination: packages/mcp/test/in-memory-abort.test.ts. NOT EXECUTED.
import { describe, expect, it } from "vitest";
import { McpAbortError, type JsonRpcMessage } from "../src/protocol/jsonrpc.ts";
import { createInMemoryTransportPair } from "../src/transports/in-memory.ts";
import type { McpTransportSendOptions } from "../src/transports/transport.ts";

async function setup() {
  const pair = createInMemoryTransportPair();
  await pair.client.start(); await pair.server.start();
  const received: JsonRpcMessage[] = [];
  pair.server.onMessage((message) => received.push(message));
  return { ...pair, received };
}

describe("in-memory send cancellation boundary", () => {
  it("rejects pre-aborted sends before cloning", async () => {
    const { client, received } = await setup();
    const controller = new AbortController(); controller.abort();
    let reads = 0;
    const message = { jsonrpc: "2.0", method: "notify", get params() { reads++; return {}; } } as const;
    await expect(client.send(message, { signal: controller.signal })).rejects.toBeInstanceOf(McpAbortError);
    expect(reads).toBe(0); expect(received).toEqual([]);
    await client.close();
  });

  it("suppresses delivery when aborted after enqueue but before the microtask", async () => {
    const { client, received } = await setup();
    const controller = new AbortController();
    const sent = client.send({ jsonrpc: "2.0", method: "notify" }, { signal: controller.signal });
    controller.abort();
    await sent; await Promise.resolve();
    expect(received).toEqual([]);
    await client.close();
  });

  it("keeps the captured signal when the options object is changed", async () => {
    const { client, received } = await setup();
    const controller = new AbortController();
    const options: McpTransportSendOptions = { signal: controller.signal };
    const sent = client.send({ jsonrpc: "2.0", method: "notify" }, options);
    options.signal = new AbortController().signal;
    controller.abort(); await sent;
    expect(received).toEqual([]);
    await client.close();
  });

  it("suppresses delivery if cloning aborts the captured signal", async () => {
    const { client, received } = await setup();
    const controller = new AbortController();
    const message = { jsonrpc: "2.0", method: "notify", get params() { controller.abort(); return {}; } } as const;
    await client.send(message, { signal: controller.signal });
    expect(received).toEqual([]);
    await client.close();
  });

  it("preserves normal cloned delivery", async () => {
    const { client, received } = await setup();
    const message = { jsonrpc: "2.0", method: "notify", params: { value: 1 } } as const;
    await client.send(message);
    expect(received).toEqual([message]);
    expect(received[0]).not.toBe(message);
    await client.close();
  });

  it("suppresses queued delivery when the source closes synchronously", async () => {
    const { client, received } = await setup();
    const sent = client.send({ jsonrpc: "2.0", method: "notify" });
    const closing = client.close();
    await sent; await closing;
    expect(received).toEqual([]);
  });

  it("cannot retract a message already delivered", async () => {
    const { client, received } = await setup();
    const controller = new AbortController();
    await client.send({ jsonrpc: "2.0", method: "notify" }, { signal: controller.signal });
    controller.abort();
    expect(received).toHaveLength(1);
    await client.close();
  });
});
