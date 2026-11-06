import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

const packageRoot = fileURLToPath(new URL("..", import.meta.url));
const modelIds = ["deepseek-v4-flash", "deepseek-v4.1-flash"];
let temporaryRoot: string | undefined;
let models: unknown;

afterAll(() => {
	if (temporaryRoot) rmSync(temporaryRoot, { force: true, recursive: true });
});

beforeAll(() => {
	temporaryRoot = mkdtempSync(join(tmpdir(), "pi-opencode-generation-"));
	const preloadPath = join(temporaryRoot, "mock-catalog.mjs");
	const outputPath = join(temporaryRoot, "catalog");
	const catalog = {
		opencode: {
			models: Object.fromEntries(modelIds.map((id) => [id, { id, tool_call: true, reasoning: true }])),
		},
	};
	writeFileSync(
		preloadPath,
		`const catalog = ${JSON.stringify(catalog)};\n` +
			`globalThis.fetch = async (input) => {\n` +
			`  const url = String(input);\n` +
			`  if (url === "https://models.dev/api.json") return Response.json(catalog);\n` +
			`  if (url === "https://models.dev/models.json?type=decision") return Response.json({ "typesafe/jev-latest": { name: "Jev", type: "decision", limit: { context: 64000, output: 0 } } });\n` +
			`  if (url.startsWith("https://openrouter.ai/api/v1/models") || url === "https://ai-gateway.vercel.sh/v1/models") return Response.json({ data: [] });\n` +
			`  if (url === "https://radius.pi.dev/v1/config") return Response.json({ baseUrl: "https://radius.pi.dev", models: [{ id: "test", name: "Test", reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 4096, maxTokens: 4096 }] });\n` +
			`  throw new Error(\`Unexpected fetch: \${url}\`);\n` +
			`};\n`,
	);
	const result = spawnSync(
		process.execPath,
		[
			"--import",
			pathToFileURL(preloadPath).href,
			"scripts/generate-models.ts",
			"--json-only",
			"--json-output",
			outputPath,
		],
		{ cwd: packageRoot, encoding: "utf8", timeout: 10_000 },
	);
	expect(result.status, `${result.stdout}\n${result.stderr}`).toBe(0);
	expect(result.stderr).toBe("");
	models = JSON.parse(readFileSync(join(outputPath, "providers/opencode.json"), "utf8"));
});

describe("OpenCode model generation", () => {
	// Regression for #5702: preserve the cache-field override when the Flash catalog ID changes.
	it.each(modelIds)("disables long cache retention for opencode/%s", (id) => {
		expect(models).toMatchObject({
			[id]: {
				id,
				provider: "opencode",
				api: "openai-completions",
				compat: { supportsLongCacheRetention: false },
			},
		});
	});
});
