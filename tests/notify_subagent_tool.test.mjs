import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const bundle = process.argv[2];
if (!bundle) throw new Error("bundle required");
const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
process.env.PI_AGENT_NOTIFY_SENDER = path.join(repo, "scripts/notify_agent.py");
delete process.env.PI_SUBAGENT_TASK_ID;

const registered = new Map();
const calls = [];
const pi = {
  on() {},
  registerTool(tool) { registered.set(tool.name, tool); },
  async exec(command, args, options) {
    calls.push({ command, args, options });
    return { code: 0, stdout: JSON.stringify({ ok: true, event: { targetId: "task-live" } }), stderr: "" };
  },
};

const { default: activate } = await import(`${pathToFileURL(bundle).href}?notify-tool=${Date.now()}`);
activate(pi);
const tool = registered.get("notify_subagent");
assert.ok(tool, "main sessions must expose notify_subagent");
assert.match(tool.promptGuidelines.join("\n"), /instead of subagent_reload/);

const result = await tool.execute("call-1", { taskId: "task-live", message: "continue" }, undefined);
assert.match(result.content[0].text, /task-live/);
assert.equal(calls[0].command, "python3");
assert.ok(fs.existsSync(calls[0].args[0]));
assert.deepEqual(calls[0].args.slice(1, 4), ["send", "continue", "--item"]);
assert.equal(calls[0].args[calls[0].args.indexOf("--item") + 1], "worker:task-live");
assert.equal(calls[0].args[calls[0].args.indexOf("--to") + 1], "task-live");

await tool.execute("call-2", {
  taskId: "task-live",
  message: "wake after terminal",
  itemKey: "mission:test",
  requireLease: true,
  level: "red",
}, undefined);
assert.equal(calls[1].args[calls[1].args.indexOf("--item") + 1], "mission:test");
assert.ok(calls[1].args.includes("--require-lease"));
assert.equal(calls[1].args[calls[1].args.indexOf("--level") + 1], "red");

console.log("notify_subagent tool defaults to the worker control key and supports domain leases");
