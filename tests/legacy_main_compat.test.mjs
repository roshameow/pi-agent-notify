import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const bundle = process.argv[2];
if (!bundle) throw new Error("bundle required");
const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-agent-notify-main-"));
const cwd = path.join(root, "project"); fs.mkdirSync(cwd);
process.env.PI_AGENT_NOTIFY_DIR = path.join(root, "notify");
process.env.PI_AGENT_NOTIFY_STATE_DIR = path.join(root, "state");
delete process.env.PI_SUBAGENT_TASK_ID;
process.env.PI_AGENT_NOTIFY_SCAN_MS = "30";
process.env.PI_AGENT_NOTIFY_BATCH_MS = "5";
const { default: activate } = await import(`${pathToFileURL(bundle).href}?main=${Date.now()}`);
const handlers = new Map(); const sent = [];
const pi = { on(name, fn) { if (!handlers.has(name)) handlers.set(name, []); handlers.get(name).push(fn); }, registerTool() {}, sendUserMessage(text, options) { sent.push({ text, options }); }, appendEntry() {} };
const sessionId = "legacy-main-session";
const ctx = { cwd, hasUI: false, isIdle: () => false, sessionManager: { getSessionId: () => sessionId, getSessionFile: () => null, getEntries: () => [] } };
async function emit(name, event = {}) { for (const fn of handlers.get(name) || []) await fn(event, ctx); }
async function waitUntil(predicate) { const start = Date.now(); while (!predicate()) { if (Date.now() - start > 2500) throw new Error("timeout"); await new Promise(r => setTimeout(r, 10)); } }

try {
  activate(pi); await emit("session_start");
  const registry = JSON.parse(fs.readFileSync(path.join(process.env.PI_AGENT_NOTIFY_DIR, ".main-sessions", `${sessionId}.json`), "utf8"));
  assert.equal(typeof registry.heartbeat, "number", "v1 sender heartbeat alias must exist");
  assert.ok(registry.heartbeatAt && registry.runId && registry.nonce);

  // Existing project senders write protocol-v1 files directly to the exact inbox.
  fs.writeFileSync(path.join(registry.inbox, "evt-legacy-main.json"), JSON.stringify({ level: "yellow", source: "legacy-main", message: "legacy exact inbox", itemId: "168373", ts: Math.floor(Date.now() / 1000) }));
  await waitUntil(() => sent.length === 1); assert.match(sent[0].text, /legacy exact inbox/);
  await emit("agent_start");

  // Offline v1 senders use main-pending/<cwdHash>; a resumed main session must drain it.
  const scope = createHash("sha256").update(path.resolve(cwd)).digest("hex").slice(0, 16);
  const pending = path.join(process.env.PI_AGENT_NOTIFY_DIR, "main-pending", scope);
  fs.writeFileSync(path.join(pending, "evt-legacy-pending.json"), JSON.stringify({ level: "yellow", source: "legacy-pending", message: "legacy pending inbox", ts: Math.floor(Date.now() / 1000) }));
  await waitUntil(() => sent.length === 2); assert.match(sent[1].text, /legacy pending inbox/);
  await emit("session_shutdown", { reason: "test" });
  console.log("legacy main compatibility tests passed");
} finally { fs.rmSync(root, { recursive: true, force: true }); }
