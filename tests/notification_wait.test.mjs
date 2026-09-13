import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath, pathToFileURL } from "node:url";

const bundle = process.argv[2];
if (!bundle) throw new Error("bundle path argument is required");
const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-agent-notify-test-"));
const stateRoot = path.join(root, "state");
const taskId = "task-notification-wait-test";
const itemKey = "mission:hkg_super_v13";
const legacyItemId = "168373";
const cwd = path.join(root, "workspace");
fs.mkdirSync(path.join(root, taskId), { recursive: true });
fs.mkdirSync(cwd, { recursive: true });
const leasePath = path.join(root, taskId, ".notification-wait-lease");
const marker = path.join(cwd, "watcher.launch.json");
fs.writeFileSync(marker, JSON.stringify({ pid: process.pid, itemKey, notifyTo: taskId }));
fs.writeFileSync(leasePath, JSON.stringify({ version: 2, taskId, itemKey, runId: "run-stale", nonce: "0123456789abcdef", producerPid: 1, producerMarkerPath: marker, cwd, expiresAt: 2 }));
fs.writeFileSync(path.join(root, ".active-workers.json"), JSON.stringify({ version: 2, workers: { [taskId]: { taskId, ownerPid: process.pid, ownerToken: "token", cwd, itemKeys: [itemKey, legacyItemId], itemIds: [legacyItemId], startedAt: new Date().toISOString(), heartbeatAt: new Date().toISOString() } } }));
const staleRegistryTmp = path.join(root, ".active-workers.json.123.tmp");
fs.writeFileSync(staleRegistryTmp, "stale");
fs.utimesSync(staleRegistryTmp, new Date(Date.now() - 600000), new Date(Date.now() - 600000));
process.env.PI_AGENT_NOTIFY_DIR = root;
process.env.PI_AGENT_NOTIFY_STATE_DIR = stateRoot;
process.env.PI_SUBAGENT_TASK_ID = taskId;
process.env.PI_AGENT_NOTIFY_SCAN_MS = "30";
process.env.PI_AGENT_NOTIFY_BATCH_MS = "5";

const { default: activate } = await import(`${pathToFileURL(bundle).href}?test=${Date.now()}`);
const handlers = new Map(); let registeredTool; const sent = [];
const pi = { on(name, fn) { if (!handlers.has(name)) handlers.set(name, []); handlers.get(name).push(fn); }, registerTool(tool) { registeredTool = tool; }, sendUserMessage(text, options) { sent.push({ text, options }); }, appendEntry() {} };
let pendingMessages = false;
const ctx = { cwd, hasUI: false, isIdle: () => false, hasPendingMessages: () => pendingMessages, sessionManager: { getSessionId: () => "test-session" } };
async function emit(name, event = {}) { for (const fn of handlers.get(name) || []) await fn(event, ctx); }
async function waitUntil(predicate, timeout = 2500) { const start = Date.now(); while (!predicate()) { if (Date.now() - start > timeout) throw new Error("condition timed out"); await new Promise(r => setTimeout(r, 10)); } }
function event(lease, overrides = {}) { return { version: 2, eventId: `evt-${Math.random().toString(16).slice(2)}`, runId: lease.runId, itemKey, targetKind: "worker", targetId: taskId, state: "quantnight.mission.terminal", sequence: 1, occurredAt: new Date().toISOString(), nonce: lease.nonce, level: "yellow", source: "quantnight-watcher", message: "mission terminal; re-read MongoDB", actionable: true, terminal: true, expiresAt: new Date(Date.now() + 60000).toISOString(), ...overrides }; }

activate(pi);
assert.equal(registeredTool.name, "arm_notification_wait");

try {
  await emit("session_start");
  assert.equal(fs.existsSync(leasePath), false, "expired/dead-producer lease must be removed");
  assert.equal(fs.existsSync(staleRegistryTmp), false, "stale registry temp files must be collected");

  const refused = await registeredTool.execute("bad-item", { itemKey: "mission:other", reason: "bad", wakeCondition: "bad", producerPid: process.pid, producerMarkerPath: marker }, undefined, undefined, ctx);
  assert.equal(refused.details.armed, false);

  const arm = await registeredTool.execute("arm", { itemKey, reason: "wait for terminal", wakeCondition: "terminal/stalled", leaseSeconds: 60, checkpointPath: "checkpoint.json", producerPid: process.pid, producerMarkerPath: marker }, undefined, undefined, ctx);
  assert.equal(arm.details.armed, true);
  const duplicateArm = await registeredTool.execute("arm2", { itemKey, reason: "same", wakeCondition: "same", leaseSeconds: 60, producerPid: process.pid, producerMarkerPath: marker }, undefined, undefined, ctx);
  assert.equal(duplicateArm.details.alreadyArmed, true);

  // Wrong nonce is fail-closed and must not release agent_end.
  fs.writeFileSync(path.join(root, taskId, "evt-wrong.json"), JSON.stringify(event(arm.details, { eventId: "evt-wrong", nonce: "fedcba9876543210" })));
  await new Promise(r => setTimeout(r, 100));
  assert.equal(sent.length, 0);

  let ended = false; const endPromise = emit("agent_end").then(() => { ended = true; });
  await new Promise(r => setTimeout(r, 80)); assert.equal(ended, false);
  const rawEvent = path.join(cwd, "quantnight-event.json");
  fs.writeFileSync(rawEvent, JSON.stringify({ schemaVersion: 1, eventId: "evt-valid", producer: "quantnight-watcher", occurredAt: new Date().toISOString(), expiresAt: new Date(Date.now() + 60000).toISOString(), itemKey, target: { taskId }, eventType: "quantnight.mission.terminal", payload: { terminal: true } }));
  const sender = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../scripts/notify_agent.py");
  const sentResult = spawnSync("python3", [sender, "send", "--event-file", rawEvent, "--lease-wait-seconds", "0"], { env: process.env, encoding: "utf8" });
  assert.equal(sentResult.status, 0, sentResult.stderr);
  await waitUntil(() => ended); await endPromise;
  assert.equal(sent.length, 1); assert.match(sent[0].text, /quantnight\.mission\.terminal/); assert.equal(sent[0].options.deliverAs, "followUp");
  assert.equal(fs.existsSync(leasePath), false);

  // Preserve the mac-mini regression fix: if the event resumes a turn while
  // the original producer remains alive and the model does not explicitly
  // re-arm, agent_end auto-rearms once within the original expiry.
  await emit("agent_start");
  let autoEnded = false;
  const autoEndPromise = emit("agent_end").then(() => { autoEnded = true; });
  await new Promise(r => setTimeout(r, 80));
  assert.equal(autoEnded, false);
  assert.equal(fs.existsSync(leasePath), true, "released live-producer lease must auto-rearm once");
  fs.writeFileSync(path.join(root, taskId, "evt-auto-wake.json"), JSON.stringify(event(arm.details, { eventId: "evt-auto-wake", sequence: 2, message: "auto-rearm wake", level: "green", state: "progress", actionable: false, terminal: false })));
  await waitUntil(() => autoEnded); await autoEndPromise;
  assert.equal(sent.length, 2); assert.match(sent[1].text, /auto-rearm wake/);
  await emit("agent_start");

  // Replay is consumed/deduped and cannot inject twice.
  fs.writeFileSync(path.join(root, taskId, "evt-replay.json"), JSON.stringify(event(arm.details, { eventId: "evt-valid", sequence: 1 })));
  await new Promise(r => setTimeout(r, 120)); assert.equal(sent.length, 2);

  // Protocol-v1 directed events remain compatible for a busy live worker,
  // while ownership is still checked against the durable registry.
  fs.writeFileSync(path.join(root, taskId, "evt-legacy.json"), JSON.stringify({
    level: "yellow", source: "legacy-watcher", message: "legacy busy notification",
    itemId: legacyItemId, to: taskId, ts: Math.floor(Date.now() / 1000),
  }));
  await waitUntil(() => sent.length === 3);
  assert.match(sent[2].text, /legacy busy notification/);
  await emit("agent_start");
  fs.writeFileSync(path.join(root, taskId, "evt-legacy-unowned.json"), JSON.stringify({
    level: "yellow", source: "legacy-watcher", message: "must not deliver",
    itemId: "999999", to: taskId, ts: Math.floor(Date.now() / 1000),
  }));
  await new Promise(r => setTimeout(r, 100));
  assert.equal(sent.length, 3, "legacy compatibility must still enforce ownership");

  // Expired events are ignored even with otherwise valid identity.
  const rearm = await registeredTool.execute("rearm", { itemKey, reason: "expiry", wakeCondition: "fresh only", leaseSeconds: 60, producerPid: process.pid, producerMarkerPath: marker }, undefined, undefined, ctx);
  fs.writeFileSync(path.join(root, taskId, "evt-expired.json"), JSON.stringify(event(rearm.details, { eventId: "evt-expired", expiresAt: new Date(Date.now() - 1000).toISOString() })));
  await new Promise(r => setTimeout(r, 120)); assert.equal(sent.length, 3);
  await emit("session_shutdown", { reason: "test" });
  console.log("notification wait tests passed");
} finally { fs.rmSync(root, { recursive: true, force: true }); }
