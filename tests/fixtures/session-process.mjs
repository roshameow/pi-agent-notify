// Real OS process, real JSONL persistence, controllable host queue. No model or
// user configuration is loaded; these fixtures never touch the live notify roots.
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";

const { bundle, cwd, sessionId, sessionFile, worker, firstMigration } = JSON.parse(process.env.NOTIFY_FIXTURE);
const handlers = new Map();
const queued = [];
const entries = () => fs.readFileSync(sessionFile, "utf8").trim().split("\n").slice(1).map(JSON.parse);
const emit = async (name, event = {}) => { for (const fn of handlers.get(name) || []) await fn(event, ctx); };
const append = (entry) => {
  fs.appendFileSync(sessionFile, JSON.stringify(entry) + "\n");
  const fd = fs.openSync(sessionFile, "r"); try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
};
const ctx = { cwd, hasUI: false, isIdle: () => true, shutdown: () => process.send({ type: "shutdown-requested" }), sessionManager: { getSessionId: () => sessionId, getSessionFile: () => sessionFile, getEntries: entries } };
const pi = {
  on(name, fn) { if (!handlers.has(name)) handlers.set(name, []); handlers.get(name).push(fn); },
  registerTool() {},
  sendUserMessage(text) { queued.push(text); process.send({ type: "queued", text }); },
  appendEntry(customType, data) { append({ type: "custom", id: String(Date.now()), customType, data }); },
};

if (firstMigration) globalThis.__pi_agent_notify_main_identities__ = new Map([[sessionId, firstMigration]]);
if (worker) {
  const inbox = path.join(process.env.PI_AGENT_NOTIFY_DIR, worker.taskId);
  fs.mkdirSync(inbox, { recursive: true });
  fs.writeFileSync(path.join(inbox, ".receiver-identity.json"), JSON.stringify({ version: 2, targetKind: "worker", targetId: worker.taskId, taskId: worker.taskId, pid: process.pid, cwd, itemKeys: worker.itemKeys, runId: "run-bootstrap", nonce: "bootstrap0123456789", heartbeatAt: new Date().toISOString() }));
  fs.writeFileSync(path.join(process.env.PI_AGENT_NOTIFY_DIR, ".active-workers.json"), JSON.stringify({ version: 2, workers: { [worker.taskId]: { ownershipMode: "receiver", taskId: worker.taskId, workerPid: process.pid, ownerPid: process.pid, parentSessionId: worker.parentSessionId, receiverIdentityPath: path.join(inbox, ".receiver-identity.json"), cwd, itemKeys: worker.itemKeys, heartbeatAt: "2000-01-01T00:00:00Z" } } }));
}

const { default: activate } = await import(pathToFileURL(bundle).href);
activate(pi);
let chain = Promise.resolve();
process.on("message", (command) => {
  chain = chain.then(async () => {
    if (command.type === "memoryAck") {
      await emit("message_end", { message: { role: "user", content: queued[0], timestamp: Date.now() } });
      process.send({ type: "memory-acked" }); // in-memory event is not disk persistence
    } else if (command.type === "persistAndCrash") {
      const text = queued.shift();
      append({ type: "message", id: String(Date.now()), parentId: null, message: { role: "user", content: text, timestamp: Date.now() } });
      process.kill(process.pid, "SIGKILL"); // crash after receipt, before extension ACK
    } else if (command.type === "ack") {
      const text = queued.shift();
      if (text) {
        const message = { role: "user", content: text, timestamp: Date.now() };
        await emit("message_end", { message }); // actual Pi emits before appendMessage
        append({ type: "message", id: String(Date.now()), parentId: null, message });
      }
      process.send({ type: "acked" });
    } else if (command.type === "reload") {
      await emit("session_shutdown", { reason: "reload" });
      await emit("session_start", { reason: "reload" });
      process.send({ type: "reloaded" });
    } else if (command.type === "shutdown") {
      await emit("session_shutdown", { reason: "quit" });
      process.exit(0);
    }
  }).catch((error) => { process.send({ type: "error", message: String(error) }); process.exit(1); });
});
try {
  await emit("session_start", { reason: firstMigration ? "reload" : "startup" });
  process.send({ type: "ready", pid: process.pid });
} catch (error) {
  process.send({ type: "error", message: String(error) });
  await emit("session_shutdown", { reason: "quit" });
  process.exit(2);
}
