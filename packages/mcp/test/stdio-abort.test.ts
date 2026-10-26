// Candidate destination: packages/mcp/test/stdio-abort.test.ts. NOT EXECUTED.
// White-box dispatch-boundary fixture only: no process is spawned, no real pipe.
import { describe, expect, it, vi } from "vitest";
import { McpAbortError } from "../src/protocol/jsonrpc.ts";
import { StdioTransport } from "../src/transports/stdio.ts";

function fixture(write = vi.fn((_payload: string, callback: (error?: Error | null) => void) => callback())) {
  const transport = new StdioTransport({ command: "not-spawned" });
  const fields = transport as unknown as {
    started: boolean;
    child: { stdin: { writable: boolean; write: typeof write } };
  };
  fields.started = true;
  fields.child = { stdin: { writable: true, write } };
  return { transport, write };
}

describe("stdio send abort boundary, fake writable", () => {
  it("rejects pre-abort before serialization or write", async () => {
    const { transport, write } = fixture();
    const controller = new AbortController(); controller.abort();
    let serialized = false;
    const message = { jsonrpc: "2.0", method: "notify", params: { toJSON() { serialized = true; return {}; } } } as const;
    await expect(transport.send(message, { signal: controller.signal })).rejects.toBeInstanceOf(McpAbortError);
    expect(serialized).toBe(false); expect(write).not.toHaveBeenCalled();
  });

  it("does not write if serialization aborts the signal", async () => {
    const { transport, write } = fixture();
    const controller = new AbortController();
    const message = { jsonrpc: "2.0", method: "notify", params: { toJSON() { controller.abort(); return {}; } } } as const;
    await expect(transport.send(message, { signal: controller.signal })).rejects.toBeInstanceOf(McpAbortError);
    expect(write).not.toHaveBeenCalled();
  });

  it("writes normal messages once with a newline", async () => {
    const { transport, write } = fixture();
    const message = { jsonrpc: "2.0", method: "notify" } as const;
    await transport.send(message);
    expect(write).toHaveBeenCalledTimes(1);
    expect(write.mock.calls[0][0]).toBe(`${JSON.stringify(message)}\n`);
  });

  it("does not pretend to undo a write already dispatched", async () => {
    let finish!: (error?: Error | null) => void;
    const write = vi.fn((_payload: string, callback: (error?: Error | null) => void) => { finish = callback; });
    const { transport } = fixture(write);
    const controller = new AbortController();
    const sent = transport.send({ jsonrpc: "2.0", method: "notify" }, { signal: controller.signal });
    controller.abort(); finish(); await sent;
    expect(write).toHaveBeenCalledTimes(1);
  });
});
