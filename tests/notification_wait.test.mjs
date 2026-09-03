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
const ctx = {
  cwd,
  hasUI: false,
  isIdle: () => false,
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
  }, undefined, undefined, ctx);
  assert.equal(arm.details.armed, true);
  assert.equal(fs.existsSync(leasePath), true);

  const duplicate = await registeredTool.execute("call-duplicate", {
    itemId, reason: "duplicate", wakeCondition: "duplicate", leaseSeconds: 60,
  }, undefined, undefined, ctx);
  assert.equal(duplicate.details.armed, false);
  assert.match(duplicate.content[0].text, /already has active wait lease/);

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

  const mismatch = await registeredTool.execute("call-2", {
    itemId: "999999", reason: "bad", wakeCondition: "bad", leaseSeconds: 60,
  }, undefined, undefined, ctx);
  assert.equal(mismatch.details.armed, false);
  assert.match(mismatch.content[0].text, /^REFUSED:/);

  const outside = await registeredTool.execute("call-3", {
    itemId, reason: "bad path", wakeCondition: "bad", leaseSeconds: 60,
    checkpointPath: path.join(root, "outside.json"),
  }, undefined, undefined, ctx);
  assert.equal(outside.details.armed, false);
  assert.match(outside.content[0].text, /checkpointPath must stay under/);

  await emit("agent_start");
  const shutdownArm = await registeredTool.execute("call-4", {
    itemId, reason: "shutdown cleanup", wakeCondition: "shutdown", leaseSeconds: 60,
    checkpointPath: "docs/current_tasks.json",
  }, undefined, undefined, ctx);
  assert.equal(shutdownArm.details.armed, true);
  assert.equal(fs.existsSync(leasePath), true);
  await emit("session_shutdown");
  assert.equal(fs.existsSync(leasePath), false, "session shutdown must remove its active lease");
  console.log("OK: notification wait lease holds, wakes, resets follow-up, rejects invalid waits, and cleans stale leases");
} finally {
  fs.rmSync(root, { recursive: true, force: true });
}
