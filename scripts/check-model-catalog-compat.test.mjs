import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { copyFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

const scripts = fileURLToPath(new URL(".", import.meta.url));
const repositoryEnvironment = [
	"GIT_DIR", "GIT_COMMON_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_OBJECT_DIRECTORY",
	"GIT_ALTERNATE_OBJECT_DIRECTORIES", "GIT_NAMESPACE", "GIT_SHALLOW_FILE", "GIT_CONFIG_PARAMETERS", "GIT_CONFIG_COUNT",
	"GIT_CONFIG", "GIT_IMPLICIT_WORK_TREE", "GIT_GRAFT_FILE", "GIT_NO_REPLACE_OBJECTS", "GIT_REPLACE_REF_BASE", "GIT_PREFIX",
];

function fixture(t, { sourceVersion = "1.5.0", packVersion = "1.5.0", sourceName = "@earendil-works/pi-ai", packName = "@earendil-works/pi-ai", release = "1.5.0", tag = true, annotated = false, localTag = false, fetchError = false, typeError = false } = {}) {
	const root = mkdtempSync(join(tmpdir(), "pi-release-source-test-"));
	t.after(() => rmSync(root, { recursive: true, force: true }));
	const checkout = join(root, "checkout");
	const official = join(root, "official");
	const temporary = join(root, "tmp");
	const log = join(root, "commands.jsonl");
	for (const directory of [checkout, official, temporary]) mkdirSync(directory);
	const env = { ...process.env };
	for (const name of repositoryEnvironment) delete env[name];
	const git = (cwd, ...args) => {
		const result = spawnSync("git", args, { cwd, env, encoding: "utf8" });
		assert.equal(result.status, 0, result.stderr);
		return result.stdout;
	};
	const write = (path, value) => {
		mkdirSync(join(path, ".."), { recursive: true });
		writeFileSync(path, value);
	};
	git(official, "init");
	write(join(official, "packages/ai/package.json"), JSON.stringify({ name: sourceName, version: sourceVersion, dependencies: { "@earendil-works/pi-telemetry": sourceVersion } }));
	write(join(official, "source-marker"), "official");
	git(official, "add", "packages/ai/package.json", "source-marker");
	git(official, "-c", "user.name=Test", "-c", "user.email=test@example.test", "commit", "-m", "Release fixture");
	if (tag) git(official, "-c", "user.name=Test", "-c", "user.email=test@example.test", "tag", ...(annotated ? ["-a", "-m", "Release"] : []), "v1.5.0");
	git(checkout, "init");
	write(join(checkout, "source-marker"), "fork");
	git(checkout, "add", "source-marker");
	git(checkout, "-c", "user.name=Test", "-c", "user.email=test@example.test", "commit", "-m", "Fork fixture");
	if (localTag) git(checkout, "tag", "v1.5.0");
	const refsBefore = git(checkout, "show-ref");
	const headBefore = readFileSync(join(checkout, ".git/HEAD"));
	const indexBefore = readFileSync(join(checkout, ".git/index"));
	write(join(checkout, "package.json"), '{"type":"module"}');
	mkdirSync(join(checkout, "scripts"));
	for (const name of ["check-model-catalog-compat.mjs", "model-catalog-minimum-version.mjs", "model-catalog-protocol.ts"]) copyFileSync(join(scripts, name), join(checkout, "scripts", name));
	const model = { type: "chat", id: "kept", api: "openai-completions", provider: "test" };
	write(join(root, "package/dist/providers/data/test.json"), JSON.stringify({ "openai-completions": { "chat:kept": model } }));
	const packed = join(root, "fixture.tgz");
	const tar = spawnSync("tar", ["czf", packed, "-C", root, "package"], { encoding: "utf8" });
	assert.equal(tar.status, 0, tar.stderr);
	write(join(checkout, "catalog.json"), JSON.stringify({ test: [{ ...model, id: "added" }], ignored: [{ ...model, provider: "ignored" }] }));
	const preload = join(root, "mock-processes.mjs");
	write(preload, `
import assert from "node:assert/strict";
import childProcess from "node:child_process";
import { appendFileSync, copyFileSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { syncBuiltinESMExports } from "node:module";
const settings = ${JSON.stringify({ official, packed, log, packVersion, packName, release, fetchError, typeError, repositoryEnvironment })};
const spawn = childProcess.spawnSync;
const ok = (stdout = "") => ({ status: 0, stdout, stderr: "" });
childProcess.spawnSync = (command, args, options) => {
  appendFileSync(settings.log, JSON.stringify({ command, args, cwd: options.cwd }) + "\\n");
  if (command === "git") {
    for (const name of settings.repositoryEnvironment) assert.equal(options.env?.[name], undefined, name);
    assert.equal(args[0], "--git-dir=" + join(options.cwd, "source.git"));
    if (args.includes("fetch")) {
      const url = args.indexOf("https://github.com/earendil-works/pi.git");
      assert.ok(url >= 0, "fetch must name the official repository");
      assert.ok(args.includes("--depth=1"));
      assert.ok(args.includes("--no-tags"));
      assert.equal(args.at(-1), "refs/tags/v1.5.0");
      if (settings.fetchError) return { status: 128, stdout: "", stderr: "Official remote unavailable" };
      args = args.map((value, index) => index === url ? settings.official : value);
      const result = spawn(command, args, options);
      if (result.status === 0) {
        const refs = spawn("git", [args[0], "for-each-ref", "--format=%(refname)", "refs/tags"], options);
        assert.equal(refs.status, 0, refs.stderr);
        assert.equal(refs.stdout, "", "fetch must not create tag refs");
      }
      return result;
    }
    return spawn(command, args, options);
  }
  if (command === "tar") return spawn(command, args, options);
  if (command === "npm") {
    if (args[0] === "view") return ok(settings.release + "\\n");
    if (args[0] === "pack") {
      assert.ok(args.includes("--ignore-scripts"));
      copyFileSync(settings.packed, join(args[args.indexOf("--pack-destination") + 1], "fixture.tgz"));
      return ok(JSON.stringify([{ filename: "fixture.tgz", name: settings.packName, version: settings.packVersion }]));
    }
    assert.equal(readFileSync(join(options.cwd, "source-marker"), "utf8"), "official");
    if (args[0] === "ci") { assert.ok(args.includes("--ignore-scripts")); return ok(); }
    if (args.join(" ") === "run build --workspace @earendil-works/pi-telemetry") return ok();
  }
  if (command === process.execPath) {
    if (args[0] === "packages/ai/scripts/hydrate-model-catalog.ts") {
      const merged = JSON.parse(readFileSync(args[1], "utf8"));
      assert.deepEqual(Object.keys(merged), ["test"]);
      assert.deepEqual(merged.test.map(model => model.id), ["kept", "added"]);
      return ok();
    }
    if (args.join(" ") === "node_modules/typescript/bin/tsc -p packages/ai/tsconfig.build.json --noEmit") {
      return settings.typeError ? { status: 2, stdout: "error TS2322: incompatible model API", stderr: "" } : ok();
    }
  }
  throw new Error("Unexpected command: " + command + " " + args.join(" "));
};
syncBuiltinESMExports();
`);
	const inherited = { ...env, ...Object.fromEntries(repositoryEnvironment.map(name => [name, "inherited-value"])), TMPDIR: temporary, TMP: temporary, TEMP: temporary, GIT_DIR: join(checkout, ".git"), GIT_COMMON_DIR: join(checkout, ".git"), GIT_WORK_TREE: checkout, GIT_INDEX_FILE: join(checkout, ".git/index"), GIT_OBJECT_DIRECTORY: join(checkout, ".git/objects"), GIT_ALTERNATE_OBJECT_DIRECTORIES: join(official, ".git/objects"), GIT_NAMESPACE: "hostile", GIT_SHALLOW_FILE: join(root, "missing-shallow"), GIT_CONFIG_PARAMETERS: "'core.bare=false'", GIT_CONFIG_COUNT: "0" };
	const result = spawnSync(process.execPath, ["--import", preload, "scripts/check-model-catalog-compat.mjs", "--catalog", "catalog.json"], { cwd: checkout, env: inherited, encoding: "utf8", timeout: 15_000 });
	assert.equal(git(checkout, "show-ref"), refsBefore, "checkout refs must remain unchanged");
	assert.deepEqual(readFileSync(join(checkout, ".git/HEAD")), headBefore);
	assert.deepEqual(readFileSync(join(checkout, ".git/index")), indexBefore);
	assert.equal(readFileSync(join(checkout, "source-marker"), "utf8"), "fork");
	assert.deepEqual(readdirSync(temporary), [], "temporary release repository must be cleaned up");
	const commands = existsSync(log) ? readFileSync(log, "utf8").trim().split("\n").map(line => JSON.parse(line)) : [];
	return { result, commands };
}

for (const localTag of [false, true]) {
	test(`checks the official release without trusting fork tags (local tag: ${localTag})`, (t) => {
		const { result, commands } = fixture(t, { localTag });
		assert.equal(result.status, 0, result.stderr);
		assert.match(result.stdout, /The model catalog is compatible with pi 1\.5\.0/);
		assert.ok(commands.some(({ args }) => args.includes("fetch")));
		assert.ok(commands.some(({ args }) => args[0] === "node_modules/typescript/bin/tsc"));
	});
}

test("fails closed when the official release tag is absent even if the fork has it", (t) => {
	const { result, commands } = fixture(t, { tag: false, localTag: true });
	assert.equal(result.status, 1);
	assert.match(result.stderr, /fetch[\s\S]*refs\/tags\/v1\.5\.0/);
	assert.equal(commands.some(({ args }) => args[0] === "ci"), false);
});

test("peels annotated official tags to a commit", (t) => {
	const { result } = fixture(t, { annotated: true });
	assert.equal(result.status, 0, result.stderr);
});

test("fails closed on an official fetch error", (t) => {
	const { result, commands } = fixture(t, { fetchError: true });
	assert.equal(result.status, 1);
	assert.match(result.stderr, /Official remote unavailable/);
	assert.equal(commands.some(({ args }) => args[0] === "pack"), false);
});

for (const mismatch of ["sourceVersion", "packVersion", "sourceName", "packName"]) {
	test(`rejects a mismatched ${mismatch} before installing release dependencies`, (t) => {
		const { result, commands } = fixture(t, { [mismatch]: "1.4.0" });
		assert.equal(result.status, 1);
		assert.match(result.stderr, /does not match.*1\.5\.0/);
		assert.equal(commands.some(({ args }) => args[0] === "ci"), false);
	});
}

test("preserves the full compatibility failure instead of skipping a released client", (t) => {
	const { result, commands } = fixture(t, { typeError: true });
	assert.equal(result.status, 1);
	assert.match(result.stderr, /error TS2322: incompatible model API/);
	assert.match(result.stderr, /The model catalog does not type-check against pi 1\.5\.0/);
	assert.ok(commands.some(({ args }) => args[0] === "ci"));
	assert.ok(commands.some(({ args }) => args[0] === "packages/ai/scripts/hydrate-model-catalog.ts"));
});

test("keeps the existing minimum-version skip without fetching source", (t) => {
	const { result, commands } = fixture(t, { release: "1.0.0" });
	assert.equal(result.status, 0, result.stderr);
	assert.match(result.stdout, /Skipped: the latest release 1\.0\.0 is older/);
	assert.equal(commands.length, 1);
});

test("rejects malformed npm versions before fetching source", (t) => {
	const { result, commands } = fixture(t, { release: "not-a-version" });
	assert.equal(result.status, 1);
	assert.match(result.stderr, /Invalid Pi version/);
	assert.equal(commands.length, 1);
});
