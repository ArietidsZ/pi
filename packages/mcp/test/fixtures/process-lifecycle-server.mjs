import { spawn } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { createInterface } from "node:readline";

// A finite watchdog is deliberately independent of the transport under test.
const watchdog = setTimeout(() => process.exit(90), 8_000);
if (process.argv[2] === "descendant") {
	process.on("SIGTERM", () => {});
	process.send({ ready: true, pid: process.pid });
	process.disconnect();
} else {
	const child = spawn(process.execPath, [process.argv[1], "descendant"], {
		stdio: ["ignore", "ignore", "ignore", "ipc"],
		// Inherit the transport-owned group; do not create an escaped session.
		detached: false,
		env: {},
	});
	const identities = [process.pid, child.pid].map((pid) => {
		const stat = process.platform === "linux" ? readFileSync(`/proc/${pid}/stat`, "utf8") : undefined;
		return { pid, startTime: stat?.slice(stat.lastIndexOf(")") + 2).split(" ")[19] };
	});
	writeFileSync(process.argv[2], JSON.stringify(identities));
	child.on("error", () => process.exit(91));
	child.once("message", (message) => {
		if (!message || message.ready !== true || message.pid !== child.pid) process.exit(92);
		process.stdout.write(`${JSON.stringify({ jsonrpc: "2.0", method: "fixture/ready", params: {
			leader: process.pid, descendant: child.pid,
		} })}\n`);
	});
	const lines = createInterface({ input: process.stdin });
	lines.on("line", (line) => {
		if (JSON.parse(line).method === "fixture/exit") process.exit(0);
	});
	lines.once("close", () => process.exit(0));
}
// Keep the watchdog referenced even when the IPC channel has disconnected.
watchdog.ref();
