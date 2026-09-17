import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

// Regression: the worker inbox holds extension bookkeeping dotfiles next to the
// inbound envelopes. `.receiver-identity.json` is valid JSON, so a naive
// `endsWith(".json")` filter collected it, renamed it to `*.processing` and
// quarantined it as an "invalid inbox envelope". Because that file is rewritten
// on every heartbeat (15s) and is also READ BY THE GENERIC SENDER for busy-worker
// delivery, the loop never ended: the sender intermittently found no identity
// file and the quarantine directory grew without bound (71k entries / 279 MB).

const bundle = process.argv[2];
if (!bundle) throw new Error("bundle required");
const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-agent-notify-dotfile-"));
const cwd = path.join(root, "project"); fs.mkdirSync(cwd);
process.env.PI_AGENT_NOTIFY_DIR = path.join(root, "notify");
process.env.PI_AGENT_NOTIFY_STATE_DIR = path.join(root, "state");
delete process.env.PI_SUBAGENT_TASK_ID;
process.env.PI_AGENT_NOTIFY_SCAN_MS = "30";
process.env.PI_AGENT_NOTIFY_BATCH_MS = "5";
const { default: activate } = await import(`${pathToFileURL(bundle).href}?dotfile=${Date.now()}`);
const handlers = new Map(); const sent = [];
const pi = { on(name, fn) { if (!handlers.has(name)) handlers.set(name, []); handlers.get(name).push(fn); }, registerTool() {}, sendUserMessage(text, options) { sent.push({ text, options }); }, appendEntry() {} };
const sessionId = "dotfile-main-session";
const ctx = { cwd, hasUI: false, isIdle: () => false, sessionManager: { getSessionId: () => sessionId, getSessionFile: () => null, getEntries: () => [] } };
async function emit(name, event = {}) { for (const fn of handlers.get(name) || []) await fn(event, ctx); }
async function waitUntil(predicate) { const start = Date.now(); while (!predicate()) { if (Date.now() - start > 2500) throw new Error("timeout"); await new Promise(r => setTimeout(r, 10)); } }
const settle = () => new Promise(r => setTimeout(r, 300));
const quarantineFiles = () => { try { return fs.readdirSync(path.join(process.env.PI_AGENT_NOTIFY_STATE_DIR, "quarantine")); } catch { return []; } };

try {
  activate(pi); await emit("session_start");
  const registry = JSON.parse(fs.readFileSync(path.join(process.env.PI_AGENT_NOTIFY_DIR, ".main-sessions", `${sessionId}.json`), "utf8"));

  // Extension bookkeeping that a sender relies on must survive inbox collection.
  const identity = path.join(registry.inbox, ".receiver-identity.json");
  const identityBody = JSON.stringify({ version: 2, targetKind: "worker", targetId: "task-fixture", nonce: "a".repeat(32) });
  fs.writeFileSync(identity, identityBody);

  // A real envelope written right after it must still be delivered.
  fs.writeFileSync(path.join(registry.inbox, "evt-dotfile.json"), JSON.stringify({ level: "yellow", source: "dotfile", message: "dotfile regression envelope", itemId: "260918", ts: Math.floor(Date.now() / 1000) }));

  await waitUntil(() => sent.length === 1); assert.match(sent[0].text, /dotfile regression envelope/);
  await settle(); await settle();

  assert.equal(fs.existsSync(identity), true, "bookkeeping dotfile must not be consumed by inbox collection");
  assert.equal(fs.readFileSync(identity, "utf8"), identityBody, "bookkeeping dotfile must keep its contents");
  assert.equal(fs.readdirSync(registry.inbox).filter(n => n.startsWith(".")).length, 1, "no dotfile should be renamed to *.processing");
  assert.deepEqual(quarantineFiles(), [], "collecting a bookkeeping dotfile must never quarantine anything");
  assert.equal(sent.length, 1, "bookkeeping dotfiles must not be delivered as events");

  await emit("session_shutdown", { reason: "test" });
  console.log("dotfile inbox tests passed");
} finally { fs.rmSync(root, { recursive: true, force: true }); }
