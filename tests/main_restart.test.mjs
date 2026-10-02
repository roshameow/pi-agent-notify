import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fork, spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const bundle = path.resolve(process.argv[2]);
const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-notify-cold-start-"));
const cwd = path.join(root, "project"); fs.mkdirSync(cwd);
const inbox = path.join(root, "inbox"), state = path.join(root, "state");
const sessionId = "cold-main-session", sessionFile = path.join(root, `${sessionId}.jsonl`);
fs.writeFileSync(sessionFile, JSON.stringify({ type: "session", version: 3, id: sessionId, cwd }) + "\n");
const env = { ...process.env, PI_AGENT_NOTIFY_DIR: inbox, PI_AGENT_NOTIFY_STATE_DIR: state, PI_AGENT_NOTIFY_SCAN_MS: "50", PI_AGENT_NOTIFY_BATCH_MS: "5", PI_AGENT_NOTIFY_SENDER: path.join(repo, "scripts/notify_agent.py") };
delete env.PI_SUBAGENT_TASK_ID;
const children = new Set();
function start(options = {}) {
  const child = fork(path.join(repo, "tests/fixtures/session-process.mjs"), [], {
    env: { ...env, ...options.env, NOTIFY_FIXTURE: JSON.stringify({ bundle, cwd, sessionId, sessionFile, ...options }) },
    stdio: ["ignore", "pipe", "pipe", "ipc"],
  });
  children.add(child); child.events = []; child.errors = "";
  child.on("message", (message) => child.events.push(message));
  child.stderr.on("data", (text) => { child.errors += text; });
  child.on("exit", () => children.delete(child));
  return child;
}
async function until(predicate, label, timeout = 5000) {
  const deadline = Date.now() + timeout;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`timeout: ${label}`);
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}
async function ready(child) {
  await until(() => child.events.some((event) => ["ready", "error"].includes(event.type)), "child startup");
  assert.equal(child.events.find((event) => event.type === "error"), undefined, child.errors + JSON.stringify(child.events));
}
async function shutdown(child) {
  if (child.exitCode !== null || child.signalCode) return;
  child.send({ type: "shutdown" });
  await until(() => child.exitCode !== null, "graceful shutdown");
}
function send(id, extra = [], check = true) {
  const result = spawnSync("python3", [path.join(repo, "scripts/notify_agent.py"), "send", `notification ${id}`, "--item", "ci:cold-start", "--session-id", sessionId, "--event-id", id, ...extra], { env, encoding: "utf8" });
  if (check) assert.equal(result.status, 0, result.stderr);
  return check ? JSON.parse(result.stdout) : result;
}
const registry = () => JSON.parse(fs.readFileSync(path.join(inbox, ".main-sessions", `${sessionId}.json`)));
const outboxes = () => fs.readdirSync(path.join(state, "outbox")).filter((name) => name.endsWith(".json"));
async function ack(child) { const n = child.events.filter((e) => e.type === "acked").length; child.send({ type: "ack" }); await until(() => child.events.filter((e) => e.type === "acked").length > n, "ack user message"); }

try {
  // Before the first migration an old live Pi has a registry but no flock.
  // It must still exclude another controller for the same exact session.
  fs.mkdirSync(path.join(inbox, ".main-sessions"), { recursive: true });
  const legacyRegistration = path.join(inbox, ".main-sessions", `${sessionId}.json`);
  fs.writeFileSync(legacyRegistration, JSON.stringify({ sessionId, pid: process.pid, cwd, sessionFile, heartbeat: Date.now() }));
  const oldConflict = start();
  await until(() => oldConflict.events.some((event) => event.type === "error"), "legacy live PID exclusion");
  assert.match(oldConflict.events.find((event) => event.type === "error").message, /active controller/);
  assert.ok(oldConflict.events.some((event) => event.type === "shutdown-requested"), "excluded legacy controller requests Pi shutdown, not merely an extension error");
  assert.equal(JSON.parse(fs.readFileSync(legacyRegistration)).pid, process.pid);
  fs.rmSync(legacyRegistration);
  const migration = { runId: "run-existing-global", nonce: "existing0123456789abcdef" };
  let main = start({ firstMigration: migration }); await ready(main);
  const original = registry();
  assert.equal(original.runId, migration.runId, "first reload migration preserves live global identity");
  const loser = start();
  await until(() => loser.events.some((event) => event.type === "error"), "same-session controller exclusion");
  assert.match(loser.events.find((event) => event.type === "error").message, /active controller/);
  assert.ok(loser.events.some((event) => event.type === "shutdown-requested"), "duplicate main writer must be asked to shut down");
  assert.equal(registry().pid, main.pid, "loser cannot overwrite winner registration");

  send("evt-live-unacked");
  await until(() => main.events.some((event) => event.type === "queued"), "live queue");
  assert.equal(outboxes().length, 1, "queue acceptance is NOT a persisted ACK");
  main.send({ type: "memoryAck" }); await until(() => main.events.some((e) => e.type === "memory-acked"), "in-memory message_end");
  assert.equal(outboxes().length, 1, "unpersisted message_end is NOT an ACK");
  await new Promise((resolve) => setTimeout(resolve, 220));
  assert.equal(main.events.filter((event) => event.type === "queued").length, 1, "scan does not repeatedly queue inflight");
  main.send({ type: "reload" }); await until(() => main.events.some((e) => e.type === "reloaded"), "warm reload");
  assert.equal(main.events.filter((event) => event.type === "queued").length, 1, "same process reload does not duplicate host queue");
  main.kill("SIGKILL"); await until(() => main.signalCode === "SIGKILL", "abrupt main exit");
  await new Promise((resolve) => setTimeout(resolve, 100)); // guardian EOF, bounded fixture-only wait

  const offline = send("evt-offline"); assert.equal(offline.event.runId, original.runId);
  assert.notEqual(send("evt-offline-too-long", ["--ttl", "7200"], false).status, 0);
  const ambiguous = spawnSync("python3", [path.join(repo, "scripts/notify_agent.py"), "send", "must not guess offline", "--cwd", cwd, "--item", "ci:cold-start"], { env, encoding: "utf8" });
  assert.notEqual(ambiguous.status, 0, "offline cwd routing must fail closed");
  assert.equal(outboxes().length, 2);

  main = start(); await ready(main);
  assert.equal(registry().runId, original.runId); assert.equal(registry().nonce, original.nonce);
  await until(() => main.events.some((event) => event.type === "queued"), "cold replay");
  const text = main.events.find((event) => event.type === "queued").text;
  assert.match(text, /evt-live-unacked/); assert.match(text, /evt-offline/);
  assert.equal(outboxes().length, 2);
  await ack(main); await until(() => outboxes().length === 0, "session persistence ACK removes outbox");
  // Crash after durable receipt but before dedup/outbox ACK: the receipt itself
  // completes ACK on restart, without a second user injection.
  send("evt-receipt-before-crash");
  await until(() => main.events.filter((e) => e.type === "queued").length >= 2, "receipt crash queue");
  main.send({ type: "persistAndCrash" });
  await until(() => main.signalCode === "SIGKILL", "receipt crash");
  await new Promise((resolve) => setTimeout(resolve, 100));
  main = start(); await ready(main);
  await until(() => outboxes().length === 0, "reconstruct ACK from persisted receipt");
  assert.equal(main.events.filter((event) => event.type === "queued").length, 0);
  // Same eventId cannot be re-injected after acknowledged restart.
  await shutdown(main); main = start(); await ready(main);
  await new Promise((resolve) => setTimeout(resolve, 130));
  assert.equal(main.events.filter((event) => event.type === "queued").length, 0);
  assert.equal(send("evt-live-unacked").alreadySent, true);

  const stale = { ...offline.event, eventId: "evt-wrong-nonce", nonce: "wrong0123456789abcdef", message: "wrong nonce", sequence: 99 };
  fs.writeFileSync(path.join(state, "outbox", "evt-wrong-nonce.json"), JSON.stringify(stale));
  await until(() => fs.readdirSync(path.join(state, "quarantine")).some((name) => name.endsWith(".reason") && fs.readFileSync(path.join(state, "quarantine", name), "utf8").includes("stale main run identity")), "old nonce quarantine");
  assert.equal(main.events.filter((event) => event.type === "queued").length, 0);

  // A live receiver-owned worker survives loss of its main controller. Its
  // legacy-format identity file remains sufficient; registry heartbeat is stale.
  const workerSession = "receiver-worker-session", workerFile = path.join(root, "worker.jsonl");
  fs.writeFileSync(workerFile, JSON.stringify({ type: "session", version: 3, id: workerSession, cwd }) + "\n");
  const taskId = "task-receiver-owned";
  const worker = start({ sessionId: workerSession, sessionFile: workerFile, worker: { taskId, itemKeys: ["ci:worker", `worker:${taskId}`], parentSessionId: sessionId }, env: { PI_SUBAGENT_TASK_ID: taskId } });
  await ready(worker); await shutdown(main);
  const result = spawnSync("python3", [path.join(repo, "scripts/notify_agent.py"), "send", "receiver survives main exit", "--item", "ci:worker", "--to", taskId, "--event-id", "evt-worker-main-dead"], { env, encoding: "utf8" });
  assert.equal(result.status, 0, result.stderr);
  await until(() => worker.events.some((event) => event.type === "queued"), "worker delivery after main exit");
  await ack(worker); await until(() => outboxes().length === 0, "worker ACK");
  await shutdown(worker);

  // New/fork session IDs never inherit the original identity.
  const forkId = "forked-session", forkFile = path.join(root, "forked.jsonl");
  fs.writeFileSync(forkFile, JSON.stringify({ type: "session", version: 3, id: forkId, cwd, parentSession: sessionFile }) + "\n");
  const forked = start({ sessionId: forkId, sessionFile: forkFile }); await ready(forked);
  const forkReg = JSON.parse(fs.readFileSync(path.join(inbox, ".main-sessions", `${forkId}.json`)));
  assert.notEqual(forkReg.runId, original.runId); await shutdown(forked);
  console.log("real subprocess cold restart / offline sender / controller / persisted ACK / receiver ownership tests passed");
} finally {
  for (const child of children) child.kill("SIGKILL");
  await new Promise((resolve) => setTimeout(resolve, 100));
  fs.rmSync(root, { recursive: true, force: true });
}
