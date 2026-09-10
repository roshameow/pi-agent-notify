import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const bundle = process.argv[2];
if (!bundle) throw new Error("bundle path argument is required");

const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-agent-notify-test-"));
const taskId = "task-notification-wait-test";
const itemId = "168373";
const cwd = path.join(root, "workspace");
fs.mkdirSync(path.join(cwd, "docs"), { recursive: true });
fs.writeFileSync(path.join(cwd, "docs", "current_tasks.json"), JSON.stringify({ items: { [itemId]: {} } }));
fs.mkdirSync(path.join(root, taskId), { recursive: true });
const leasePath = path.join(root, taskId, ".notification-wait-lease");
const producerMarkerPath = path.join(cwd, "watcher.launch.json");
fs.writeFileSync(producerMarkerPath, JSON.stringify({ pid: process.pid, itemId, notifyTo: taskId }));
const producer = { producerPid: process.pid, producerMarkerPath };
fs.writeFileSync(leasePath, JSON.stringify({
  version: 1, leaseId: "stale", taskId, itemId, cwd, pid: 1,
  reason: "stale", wakeCondition: "never", armedAt: 1, expiresAt: 2,
}));
fs.writeFileSync(path.join(root, ".active-workers.json"), JSON.stringify({
  version: 1,
  workers: {
    [taskId]: { taskId, pid: process.pid, cwd, itemIds: [itemId], startedAt: new Date().toISOString() },
  },
}));

process.env.PI_AGENT_NOTIFY_DIR = root;
process.env.PI_SUBAGENT_TASK_ID = taskId;
process.env.PI_AGENT_NOTIFY_SCAN_MS = "50";
process.env.PI_AGENT_NOTIFY_BATCH_MS = "5";

const { default: activate } = await import(`${pathToFileURL(bundle).href}?test=${Date.now()}`);
const handlers = new Map();
let registeredTool = null;
const sent = [];
const pi = {
  on(name, handler) {
    if (!handlers.has(name)) handlers.set(name, []);
    handlers.get(name).push(handler);
  },
  registerTool(tool) { registeredTool = tool; },
  sendUserMessage(text, options) { sent.push({ text, options }); },
};
let pendingMessages = false;
const ctx = {
  cwd,
  hasUI: false,
  isIdle: () => false,
  hasPendingMessages: () => pendingMessages,
  sessionManager: { getSessionId: () => "test-session", getEntries: () => [] },
};
async function emit(name, event = {}) {
  for (const handler of handlers.get(name) || []) await handler(event, ctx);
}
async function waitUntil(predicate, timeoutMs = 2000) {
  const started = Date.now();
  while (!predicate()) {
    if (Date.now() - started > timeoutMs) throw new Error("condition timed out");
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

try {
  activate(pi);
  assert.equal(registeredTool?.name, "arm_notification_wait");
  await emit("session_start");
  assert.equal(fs.existsSync(leasePath), false, "session start must remove an expired crash lease");

  const arm = await registeredTool.execute("call-1", {
    itemId,
    reason: "unit test external wait",
    wakeCondition: "directed event",
    leaseSeconds: 60,
    checkpointPath: "checkpoint/test.json",
    ...producer,
  }, undefined, undefined, ctx);
  assert.equal(arm.details.armed, true);
  assert.equal(fs.existsSync(leasePath), true);

  const duplicate = await registeredTool.execute("call-duplicate", {
    itemId, reason: "duplicate", wakeCondition: "duplicate", leaseSeconds: 60, ...producer,
  }, undefined, undefined, ctx);
  assert.equal(duplicate.details.armed, true);
  assert.equal(duplicate.details.alreadyArmed, true);
  assert.match(duplicate.content[0].text, /ALREADY_ARMED/);

  let endResolved = false;
  const endPromise = emit("agent_end").then(() => { endResolved = true; });
  await new Promise((resolve) => setTimeout(resolve, 80));
  assert.equal(endResolved, false, "armed agent_end must hold the durable worker");

  fs.writeFileSync(path.join(root, taskId, "evt-wake.json"), JSON.stringify({
    level: "yellow", source: "unit", message: "wake-one", itemId, ts: Date.now(),
  }));
  await waitUntil(() => endResolved);
  await endPromise;
  assert.equal(sent.length, 1);
  assert.equal(sent[0].options?.deliverAs, "followUp");
  assert.match(sent[0].text, /wake-one/);
  assert.equal(fs.existsSync(leasePath), false, "successful injection must consume the lease");

  await emit("agent_start");
  fs.writeFileSync(path.join(root, taskId, "evt-second.json"), JSON.stringify({
    level: "yellow", source: "unit", message: "wake-two", itemId, ts: Date.now(),
  }));
  await waitUntil(() => sent.length === 2);
  assert.equal(sent[1].options?.deliverAs, "followUp");
  assert.match(sent[1].text, /wake-two/);

  // Regression: a follow-up injected while busy can leave the outstanding flag
  // set even after the host has consumed it. An armed lease must still hold when
  // ctx reports no pending messages, instead of falling through to agent_settled.
  const staleFollowUpArm = await registeredTool.execute("call-stale-followup", {
    itemId, reason: "stale followup regression", wakeCondition: "wake-after-stale", leaseSeconds: 60, ...producer,
  }, undefined, undefined, ctx);
  assert.equal(staleFollowUpArm.details.armed, true);
  pendingMessages = false;
  let staleEndResolved = false;
  const staleEndPromise = emit("agent_end").then(() => { staleEndResolved = true; });
  await new Promise((resolve) => setTimeout(resolve, 80));
  assert.equal(staleEndResolved, false, "stale followUp flag must not bypass an armed lease");
  fs.writeFileSync(path.join(root, taskId, "evt-stale-wake.json"), JSON.stringify({
    level: "yellow", source: "unit", message: "wake-after-stale", itemId, ts: Date.now(),
  }));
  await waitUntil(() => staleEndResolved);
  await staleEndPromise;
  assert.equal(sent.length, 3);
  assert.match(sent[2].text, /wake-after-stale/);
  await emit("agent_start");

  // Regression 2026-09-11 (multiple items): an orchestrator notification wakes a worker out of its
  // armed wait (consuming the lease). If that resumed turn ends WITHOUT re-arming, the extension
  // used to exit, orphaning the still-running watcher and silently losing its later wakeup. It must
  // instead auto re-arm the released lease once while the producer is still alive.
  await emit("agent_start");
  const rearmArm = await registeredTool.execute("call-rearm", {
    itemId, reason: "auto rearm regression", wakeCondition: "auto-rearm-wake", leaseSeconds: 60, ...producer,
  }, undefined, undefined, ctx);
  assert.equal(rearmArm.details.armed, true);
  let rearmEndResolved = false;
  const rearmEnd = emit("agent_end").then(() => { rearmEndResolved = true; });
  fs.writeFileSync(path.join(root, taskId, "evt-rearm-wake.json"), JSON.stringify({
    level: "yellow", source: "unit", message: "wake-rearm", itemId, ts: Date.now(),
  }));
  await waitUntil(() => rearmEndResolved);
  await rearmEnd;
  assert.equal(fs.existsSync(leasePath), false, "injection must consume the lease");
  await emit("agent_start");
  // This turn ends without re-arming: the watcher is still alive, so hold instead of exiting.
  let resolvedAfterConsume = false;
  const heldPromise = emit("agent_end").then(() => { resolvedAfterConsume = true; });
  await new Promise((resolve) => setTimeout(resolve, 80));
  assert.equal(resolvedAfterConsume, false, "released lease with a live producer must be auto re-armed");
  assert.equal(fs.existsSync(leasePath), true, "auto re-arm must restore the lease file");
  fs.writeFileSync(path.join(root, taskId, "evt-rearm-release.json"), JSON.stringify({
    level: "yellow", source: "unit", message: "wake-release", itemId, ts: Date.now(),
  }));
  await waitUntil(() => resolvedAfterConsume);
  await heldPromise;
  await emit("agent_start");

  const mismatch = await registeredTool.execute("call-2", {
    itemId: "999999", reason: "bad", wakeCondition: "bad", leaseSeconds: 60, ...producer,
  }, undefined, undefined, ctx);
  assert.equal(mismatch.details.armed, false);
  assert.match(mismatch.content[0].text, /^REFUSED:/);

  const outside = await registeredTool.execute("call-3", {
    itemId, reason: "bad path", wakeCondition: "bad", leaseSeconds: 60,
    checkpointPath: path.join(root, "outside.json"), ...producer,
  }, undefined, undefined, ctx);
  assert.equal(outside.details.armed, false);
  assert.match(outside.content[0].text, /checkpointPath must stay under/);

  const missingProducer = await registeredTool.execute("call-missing-producer", {
    itemId, reason: "missing producer", wakeCondition: "never", leaseSeconds: 60,
  }, undefined, undefined, ctx);
  assert.equal(missingProducer.details.armed, false);
  assert.match(missingProducer.content[0].text, /live detached watcher PID/);

  const deadMarkerPath = path.join(cwd, "dead-watcher.launch.json");
  fs.writeFileSync(deadMarkerPath, JSON.stringify({ pid: 2147483647, itemId, notifyTo: taskId }));
  const deadProducer = await registeredTool.execute("call-dead-producer", {
    itemId, reason: "dead producer", wakeCondition: "never", leaseSeconds: 60,
    producerPid: 2147483647, producerMarkerPath: deadMarkerPath,
  }, undefined, undefined, ctx);
  assert.equal(deadProducer.details.armed, false);
  assert.match(deadProducer.content[0].text, /is not alive/);

  await emit("agent_start");
  const shutdownArm = await registeredTool.execute("call-4", {
    itemId, reason: "shutdown cleanup", wakeCondition: "shutdown", leaseSeconds: 60,
    checkpointPath: "docs/current_tasks.json", ...producer,
  }, undefined, undefined, ctx);
  assert.equal(shutdownArm.details.armed, true);
  assert.equal(fs.existsSync(leasePath), true);
  await emit("session_shutdown");
  assert.equal(fs.existsSync(leasePath), false, "session shutdown must remove its active lease");
  console.log("OK: notification wait lease holds, wakes, resets follow-up, rejects invalid waits, and cleans stale leases");
} finally {
  fs.rmSync(root, { recursive: true, force: true });
}
