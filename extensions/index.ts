import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { createHash, randomUUID } from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const INBOX_ROOT = path.resolve(process.env.PI_AGENT_NOTIFY_DIR || "/tmp/pi-agent-notify");
const STATE_ROOT = path.resolve(process.env.PI_AGENT_NOTIFY_STATE_DIR || path.join(os.homedir(), ".pi", "agent", "agent-notify"));
const OUTBOX_DIR = path.join(STATE_ROOT, "outbox");
const QUARANTINE_DIR = path.join(STATE_ROOT, "quarantine");
const EVENT_LOG = process.env.PI_AGENT_NOTIFY_LOG || path.join(STATE_ROOT, "events.log");
const MAIN_REGISTRY_DIR = path.join(INBOX_ROOT, ".main-sessions");
const MAIN_PENDING_DIR = path.join(INBOX_ROOT, "main-pending");
const WORKER_REGISTRY = path.join(INBOX_ROOT, ".active-workers.json");
const MAIN_IDENTITY_DIR = path.join(STATE_ROOT, "main-identities");
const OFFLINE_IDENTITY_MS = 24 * 60 * 60 * 1000;
const WAIT_LEASE_FILE = ".notification-wait-lease";
const RECEIVER_IDENTITY_FILE = ".receiver-identity.json";
const SUBAGENT_TASK_ID = process.env.PI_SUBAGENT_TASK_ID || "";
const SCAN_INTERVAL_MS = Math.max(50, Number(process.env.PI_AGENT_NOTIFY_SCAN_MS || 2000));
const BATCH_WINDOW_MS = Math.max(0, Number(process.env.PI_AGENT_NOTIFY_BATCH_MS || 15000));
const DEDUP_COOLDOWN_MS = Math.max(1000, Number(process.env.PI_AGENT_NOTIFY_DEDUP_MS || 30 * 60 * 1000));
const MAIN_HEARTBEAT_MS = 15000;
const LIVE_HEARTBEAT_MS = 90000;
const MIN_WAIT_LEASE_SECONDS = 30;
const MAX_WAIT_LEASE_SECONDS = 24 * 60 * 60;
const FUTURE_SKEW_MS = 5 * 60 * 1000;
const MAX_AGE_BY_LEVEL: Record<string, number> = {
	green: 6 * 60 * 60 * 1000,
	yellow: 48 * 60 * 60 * 1000,
	red: 7 * 24 * 60 * 60 * 1000,
};
const SAFE_ITEM_KEY = /^[A-Za-z0-9][A-Za-z0-9._:@+/-]{0,199}$/;
const SAFE_TARGET_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$/;
const SENDER_SCRIPT = path.resolve(
	process.env.PI_AGENT_NOTIFY_SENDER || path.join(path.dirname(fileURLToPath(import.meta.url)), "../scripts/notify_agent.py"),
);

const CONTROLLER_SCRIPT = process.env.PI_AGENT_NOTIFY_CONTROLLER || path.join(path.dirname(SENDER_SCRIPT), "session_controller.py");

interface NotifyEvent {
	version: 2;
	eventId: string;
	runId: string;
	itemKey: string;
	targetKind: "main" | "worker";
	targetId: string;
	state: string;
	sequence: number;
	checkpointPath?: string;
	occurredAt: string;
	nonce: string;
	level: "green" | "yellow" | "red";
	source: string;
	message: string;
	actionable?: boolean;
	terminal?: boolean;
	expiresAt?: string;
	legacyItemId?: string;
	_outboxPath?: string;
}

interface WaitLease {
	version: 2;
	leaseId: string;
	taskId: string;
	itemKey: string;
	itemId?: string;
	runId: string;
	nonce: string;
	reason: string;
	wakeCondition: string;
	checkpointPath?: string;
	producerPid: number;
	producerMarkerPath: string;
	cwd: string;
	pid: number;
	armedAt: number;
	expiresAt: number;
}

interface DedupState {
	version: 1;
	seen: Record<string, number>;
	semantic: Record<string, number>;
	streams: Record<string, { sequence: number; occurredAt: number }>;
}

let currentCtx: any = null;
let receiverEnabled = false;
let controller: ChildProcess | null = null;
let controllerToken = "";
let mainSessionFile = "";
let inflight = new Map<string, { event: NotifyEvent; processId: string }>();
const processIdentityKey = "__pi_agent_notify_process_identity__";
const processIdentity = ((globalThis as any)[processIdentityKey] ||= randomUUID());
let currentSessionId = "";
let currentRunId = "";
let currentNonce = "";
let inboxDir = SUBAGENT_TASK_ID ? path.join(INBOX_ROOT, SUBAGENT_TASK_ID) : "";
let sessionRegistryFile = "";
let pendingScopeDir = "";
let pendingEvents: NotifyEvent[] = [];
let pendingEventIds = new Set<string>();
let followUpOutstanding = false;
let activeWaitLease: WaitLease | null = null;
let lastEventReleasedLease: WaitLease | null = null;
let lastEventReleasedAutoRearmed = false;
let lastReleasedReason = "";
let waitLeaseResolve: (() => void) | null = null;
let waitLeaseTimer: ReturnType<typeof setTimeout> | null = null;
let scanTimer: ReturnType<typeof setInterval> | null = null;
let batchTimer: ReturnType<typeof setTimeout> | null = null;
let heartbeatTimer: ReturnType<typeof setInterval> | null = null;
let watchers: fs.FSWatcher[] = [];
let dedupState: DedupState = { version: 1, seen: {}, semantic: {}, streams: {} };

function isSafeItemKey(value: unknown): value is string {
	return typeof value === "string" && SAFE_ITEM_KEY.test(value) && value !== "." && value !== ".." && !value.includes("//");
}

function isSafeTargetId(value: unknown): value is string {
	return typeof value === "string" && SAFE_TARGET_ID.test(value);
}

function ensurePrivateDir(dir: string): void {
	fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
	try { fs.chmodSync(dir, 0o700); } catch {}
}

function fsyncDir(dir: string): void {
	try {
		const fd = fs.openSync(dir, "r");
		try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
	} catch {}
}

function atomicWrite(file: string, content: string): void {
	ensurePrivateDir(path.dirname(file));
	const tmp = `${file}.${process.pid}.${randomUUID()}.tmp`;
	let fd: number | undefined;
	try {
		fd = fs.openSync(tmp, "wx", 0o600);
		fs.writeFileSync(fd, content, "utf8");
		fs.fsyncSync(fd);
	} finally {
		if (fd !== undefined) fs.closeSync(fd);
	}
	fs.renameSync(tmp, file);
	try { fs.chmodSync(file, 0o600); } catch {}
	fsyncDir(path.dirname(file));
}

function atomicWriteJson(file: string, value: unknown): void {
	atomicWrite(file, JSON.stringify(value));
}

function log(message: string, event?: NotifyEvent): void {
	try {
		ensurePrivateDir(path.dirname(EVENT_LOG));
		fs.appendFileSync(EVENT_LOG, JSON.stringify({ at: new Date().toISOString(), message, eventId: event?.eventId,
			targetKind: event?.targetKind, targetId: event?.targetId, itemKey: event?.itemKey }) + "\n", { encoding: "utf8", mode: 0o600 });
	} catch {}
}

function pidAlive(pid: unknown): boolean {
	const value = Number(pid);
	if (!Number.isInteger(value) || value < 2) return false;
	try { process.kill(value, 0); return true; } catch { return false; }
}

function parseTime(value: unknown): number {
	if (typeof value === "number") return value < 10_000_000_000 ? value * 1000 : value;
	const parsed = Date.parse(String(value || ""));
	return Number.isFinite(parsed) ? parsed : NaN;
}

function hash(value: string): string {
	return createHash("sha256").update(value).digest("hex");
}

function scopeKey(value: string): string {
	return hash(path.resolve(value)).slice(0, 16);
}

function waitLeasePath(): string {
	return path.join(inboxDir, WAIT_LEASE_FILE);
}

function receiverIdentityPath(): string {
	return path.join(inboxDir, RECEIVER_IDENTITY_FILE);
}

function readWorkerRegistration(taskId: string): any | null {
	try {
		const registry = JSON.parse(fs.readFileSync(WORKER_REGISTRY, "utf8"));
		const record = registry?.workers?.[taskId];
		if (!record || !pidAlive(record.ownerPid ?? record.pid)) return null;
		if (record.ownershipMode === "receiver") {
			const workerPid = Number(record.workerPid);
			const expected = path.join(INBOX_ROOT, taskId, RECEIVER_IDENTITY_FILE);
			if (record.taskId !== taskId || Number(record.ownerPid) !== workerPid || !pidAlive(workerPid)
				|| typeof record.cwd !== "string" || !record.cwd || !isSafeTargetId(record.parentSessionId) || path.resolve(record.cwd || "") !== path.resolve(currentCtx?.cwd || "")
				|| path.resolve(record.receiverIdentityPath || "") !== expected || !fs.lstatSync(expected).isFile()) return null;
			const identity = JSON.parse(fs.readFileSync(expected, "utf8"));
			const heartbeat = parseTime(identity.heartbeatAt);
			const keys = registrationKeys(record).sort();
			const receiverKeys = [...new Set((Array.isArray(identity.itemKeys) ? identity.itemKeys : []).map(String))].sort();
			if (identity.version !== 2 || identity.taskId !== taskId || identity.targetId !== taskId
				|| identity.targetKind !== "worker" || Number(identity.pid) !== workerPid
				|| !isSafeTargetId(identity.runId) || typeof identity.nonce !== "string" || identity.nonce.length < 16 || identity.nonce.length > 200
				|| typeof identity.cwd !== "string" || !identity.cwd || path.resolve(identity.cwd) !== path.resolve(record.cwd)
				|| !Number.isFinite(heartbeat) || heartbeat > Date.now() + FUTURE_SKEW_MS || Date.now() - heartbeat > LIVE_HEARTBEAT_MS
				|| JSON.stringify(keys) !== JSON.stringify(receiverKeys)) return null;
			return record;
		}
		// Never treat an unknown ownership mode as a weaker legacy registration.
		if (record.ownershipMode && record.ownershipMode !== "parent") return null;
		const heartbeat = parseTime(record.heartbeatAt || record.startedAt);
		if (!Number.isFinite(heartbeat) || heartbeat > Date.now() + FUTURE_SKEW_MS || Date.now() - heartbeat > 120000) return null;
		return record;
	} catch { return null; }
}

function registrationKeys(record: any): string[] {
	return [...new Set([
		...(Array.isArray(record?.itemKeys) ? record.itemKeys : []),
		...(Array.isArray(record?.itemIds) ? record.itemIds : []),
	].map(String))];
}

function markerMatchesLease(value: any): boolean {
	try {
		const marker = JSON.parse(fs.readFileSync(value.producerMarkerPath, "utf8"));
		const markerItem = String(marker?.itemKey ?? marker?.itemId ?? marker?.item ?? "");
		const markerTarget = String(marker?.notifyTo ?? marker?.to ?? "");
		return Number(marker?.pid) === Number(value.producerPid)
			&& markerItem === String(value.itemKey)
			&& markerTarget === String(value.taskId);
	} catch { return false; }
}

function removeWaitLeaseFile(): void {
	try { fs.rmSync(waitLeasePath(), { force: true }); } catch {}
}

function readWaitLease(requireLiveProducer = false): WaitLease | null {
	if (!SUBAGENT_TASK_ID) return null;
	try {
		const value = JSON.parse(fs.readFileSync(waitLeasePath(), "utf8"));
		const valid = value?.version === 2
			&& value?.taskId === SUBAGENT_TASK_ID
			&& isSafeItemKey(value?.itemKey)
			&& isSafeTargetId(value?.runId)
			&& typeof value?.nonce === "string" && value.nonce.length >= 16
			&& Number.isFinite(value?.expiresAt) && value.expiresAt > Date.now()
			&& path.resolve(value?.cwd || "") === path.resolve(currentCtx?.cwd || value?.cwd || "");
		const producerValid = markerMatchesLease(value) && (!requireLiveProducer || pidAlive(value.producerPid));
		if (!valid || !producerValid) {
			removeWaitLeaseFile();
			return null;
		}
		return value as WaitLease;
	} catch (error: any) {
		if (error?.code !== "ENOENT") removeWaitLeaseFile();
		return null;
	}
}

function rearmReleasedLeaseIfProducerAlive(): WaitLease | null {
	const previous = lastEventReleasedLease;
	if (!previous || lastReleasedReason !== "matching_directed_event_delivered"
		|| lastEventReleasedAutoRearmed || previous.expiresAt <= Date.now()) return null;
	if (!markerMatchesLease(previous) || !pidAlive(previous.producerPid)) return null;
	try {
		atomicWriteJson(waitLeasePath(), previous);
		lastEventReleasedAutoRearmed = true;
		log(`auto re-armed released wait lease task=${SUBAGENT_TASK_ID} itemKey=${previous.itemKey} producer=${previous.producerPid}`);
		return readWaitLease(true);
	} catch { return null; }
}

function clearWaitLease(reason: string): void {
	if (waitLeaseTimer) clearTimeout(waitLeaseTimer);
	waitLeaseTimer = null;
	lastReleasedReason = reason;
	if (reason === "matching_directed_event_delivered" && activeWaitLease) {
		const sameAutoRearmedLease = lastEventReleasedAutoRearmed
			&& lastEventReleasedLease?.leaseId === activeWaitLease.leaseId;
		lastEventReleasedLease = activeWaitLease;
		if (!sameAutoRearmedLease) lastEventReleasedAutoRearmed = false;
	}
	removeWaitLeaseFile();
	activeWaitLease = null;
	const resolve = waitLeaseResolve;
	waitLeaseResolve = null;
	if (resolve) resolve();
	log(`wait lease released task=${SUBAGENT_TASK_ID || "main"} reason=${reason}`);
}

function mainIdentity(sessionId: string): { runId: string; nonce: string } {
	const globalKey = "__pi_agent_notify_main_identities__";
	const globalState = globalThis as any;
	if (!globalState[globalKey]) globalState[globalKey] = new Map<string, { runId: string; nonce: string }>();
	const identities = globalState[globalKey] as Map<string, { runId: string; nonce: string }>;
	let identity = identities.get(sessionId);
	if (!identity) {
		identity = { runId: `run-${randomUUID()}`, nonce: randomUUID().replaceAll("-", "") };
		identities.set(sessionId, identity);
	}
	return identity;
}

function mainIdentityPath(): string {
	return path.join(MAIN_IDENTITY_DIR, `${hash(currentSessionId).slice(0, 24)}.json`);
}

function canonicalSessionFile(ctx: any): string {
	const file = ctx.sessionManager?.getSessionFile?.();
	if (!file) return ""; // Ephemeral sessions have no offline recovery identity.
	const resolved = path.resolve(file);
	if (path.basename(resolved).includes("subagent-task")) throw new Error("notification controller requires a canonical session, not a task mirror");
	try { return fs.realpathSync(resolved); }
	catch (error: any) {
		if (error.code !== "ENOENT") throw error;
		return path.join(fs.realpathSync(path.dirname(resolved)), path.basename(resolved));
	}
}

async function acquireMainController(sessionId: string): Promise<void> {
	ensurePrivateDir(MAIN_IDENTITY_DIR);
	const lock = path.join(MAIN_IDENTITY_DIR, `${hash(sessionId).slice(0, 24)}.json.controller`);
	const token = randomUUID();
	const child = spawn("python3", [CONTROLLER_SCRIPT, lock, String(process.pid), token], { stdio: ["pipe", "pipe", "pipe"] });
	await new Promise<void>((resolve, reject) => {
		let output = "", errorOutput = "", settled = false;
		const fail = (error: Error) => {
			if (settled) return;
			settled = true; clearTimeout(timer); child.stdin?.end(); child.kill(); reject(error);
		};
		const timer = setTimeout(() => fail(new Error("main controller lock acquisition timed out")), 5000);
		child.on("error", fail);
		child.stderr?.on("data", (chunk) => { errorOutput += String(chunk); });
		child.on("exit", () => {
			if (!settled) fail(new Error(errorOutput.trim() || "main controller guardian exited"));
			if (controllerToken === token) {
				receiverEnabled = false;
				log("main controller lost; receiver disabled");
			}
		});
		child.stdout?.on("data", (chunk) => {
			output += String(chunk);
			if (!output.includes("\n") || settled) return;
			try {
				const reply = JSON.parse(output.split("\n")[0]);
				if (reply.ready !== true || reply.token !== token) throw new Error("invalid controller handshake");
				controller = child; controllerToken = token;
				settled = true; clearTimeout(timer); resolve();
			} catch (error) { fail(error as Error); }
		});
	});
	// First migration also excludes an old live Pi which has not loaded the
	// flock controller yet. A stale heartbeat is NOT permission to double-open
	// its exact session while its PID is still alive.
	try {
		const legacyFile = path.join(MAIN_REGISTRY_DIR, `${sessionId.replace(/[^A-Za-z0-9._-]/g, "_").slice(0, 160)}.json`);
		let registration: any;
		try { registration = JSON.parse(fs.readFileSync(legacyFile, "utf8")); }
		catch (error: any) { if (error.code !== "ENOENT") throw error; }
		if (registration?.sessionId === sessionId && Number(registration.pid) !== process.pid && pidAlive(registration.pid)) {
			throw new Error("main session already has an active controller (legacy registration)");
		}
	} catch (error) { await releaseMainController(); throw error; }
}

async function releaseMainController(): Promise<void> {
	const child = controller;
	controller = null; controllerToken = "";
	if (!child || child.exitCode !== null) return;
	await new Promise<void>((resolve) => {
		const timer = setTimeout(() => { child.kill("SIGTERM"); resolve(); }, 2000);
		child.once("exit", () => { clearTimeout(timer); resolve(); });
		child.stdin?.end();
	});
}

function restoreMainIdentity(ctx: any): { runId: string; nonce: string } {
	if (mainSessionFile && fs.existsSync(mainSessionFile)) {
		const header = JSON.parse(fs.readFileSync(mainSessionFile, "utf8").split("\n")[0]);
		if (header.type !== "session" || header.id !== currentSessionId || path.resolve(header.cwd || "") !== path.resolve(ctx.cwd)) {
			throw new Error("canonical main session header mismatch");
		}
	}
	const globalMap = (globalThis as any).__pi_agent_notify_main_identities__ as Map<string, any> | undefined;
	const existing = globalMap?.get(currentSessionId);
	let saved: any;
	try { saved = JSON.parse(fs.readFileSync(mainIdentityPath(), "utf8")); }
	catch (error: any) { if (error.code !== "ENOENT") throw error; }
	if (saved) {
		if (saved.version !== 2 || saved.targetKind !== "main" || saved.sessionId !== currentSessionId || saved.targetId !== currentSessionId
			|| !mainSessionFile || saved.sessionFile !== mainSessionFile || saved.cwd !== path.resolve(ctx.cwd)
			|| !isSafeTargetId(saved.runId) || typeof saved.nonce !== "string" || saved.nonce.length < 16 || saved.nonce.length > 200) {
			throw new Error("persistent main identity does not match exact session/cwd");
		}
		if (existing && (existing.runId !== saved.runId || existing.nonce !== saved.nonce)) {
			throw new Error("live main identity conflicts with persisted identity");
		}
		(globalThis as any).__pi_agent_notify_main_identities__ ||= new Map();
		(globalThis as any).__pi_agent_notify_main_identities__.set(currentSessionId, { runId: saved.runId, nonce: saved.nonce });
		return { runId: saved.runId, nonce: saved.nonce };
	}
	// First /reload migration MUST keep the already-live global identity.
	return existing || mainIdentity(currentSessionId);
}

function persistMainIdentity(ctx: any): void {
	if (!mainSessionFile || !controllerToken) return;
	atomicWriteJson(mainIdentityPath(), {
		version: 2, targetKind: "main", targetId: currentSessionId, sessionId: currentSessionId,
		sessionFile: mainSessionFile, cwd: path.resolve(ctx.cwd), runId: currentRunId, nonce: currentNonce,
		updatedAt: new Date().toISOString(), offlineUntil: new Date(Date.now() + OFFLINE_IDENTITY_MS).toISOString(),
	});
}

function inflightPath(): string {
	return path.join(STATE_ROOT, "inflight", `${hash(`${SUBAGENT_TASK_ID ? "worker" : "main"}:${SUBAGENT_TASK_ID || currentSessionId}`).slice(0, 24)}.json`);
}

function receiptMarker(event: NotifyEvent): string {
	return `[[agent-notify:${event.eventId}:${hash(`${event.runId}|${event.nonce}|${event.eventId}`).slice(0, 24)}]]`;
}

let persistedReceiptText = "";
let persistedPassiveReceipts = new Set<string>();
let receiptSnapshotKey = "";

function saveInflight(): void {
	const records = [...inflight.values(), ...pendingEvents.map((event) => ({ event, processId: "" }))];
	atomicWriteJson(inflightPath(), { version: 1, records });
}

function readPersistedReceipts(): void {
	const file = currentCtx?.sessionManager?.getSessionFile?.();
	if (!file) { persistedReceiptText = ""; persistedPassiveReceipts.clear(); receiptSnapshotKey = ""; return; }
	try {
		const stat = fs.statSync(file);
		const snapshotKey = `${path.resolve(file)}:${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeMs}`;
		if (snapshotKey === receiptSnapshotKey) return;
		persistedReceiptText = "";
		persistedPassiveReceipts.clear();
		const lines = fs.readFileSync(file, "utf8").split("\n");
		const header = JSON.parse(lines[0]);
		if (header.type !== "session" || header.id !== currentSessionId || path.resolve(header.cwd || "") !== path.resolve(currentCtx.cwd)) return;
		for (const line of lines.slice(1)) {
			if (!line) continue;
			let entry: any;
			try { entry = JSON.parse(line); } catch { continue; }
			if (entry.type === "message" && entry.message?.role === "user") {
				const content = entry.message.content;
				const text = typeof content === "string" ? content : (Array.isArray(content) ? content.filter((part: any) => part.type === "text").map((part: any) => part.text).join("\n") : "");
				if (text.startsWith("[agent-notify]\n")) persistedReceiptText += text + "\n";
			} else if (entry.type === "custom" && entry.customType === "agent-notify" && entry.data?.receiptMarker) {
				persistedPassiveReceipts.add(String(entry.data.receiptMarker));
			}
		}
		// Receipt bytes must survive a process/machine crash, not merely exist in
		// SessionManager's unflushed first-turn buffer or the kernel page cache.
		const fd = fs.openSync(file, "r");
		try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
		fsyncDir(path.dirname(file));
		// Cache the pre-read stat: an append during this read forces another scan.
		receiptSnapshotKey = snapshotKey;
	} catch { persistedReceiptText = ""; persistedPassiveReceipts.clear(); receiptSnapshotKey = ""; }
}

function hasPersistedReceipt(event: NotifyEvent): boolean {
	const marker = receiptMarker(event);
	return persistedReceiptText.includes(marker) || persistedPassiveReceipts.has(marker);
}

function reconcileReceipts(): void {
	if (inflight.size === 0) return;
	readPersistedReceipts();
	let changed = false;
	for (const [id, record] of inflight) {
		if (hasPersistedReceipt(record.event) && markDelivered(record.event)) { inflight.delete(id); changed = true; }
	}
	if (changed) {
		try { saveInflight(); } catch (error) { log(`receipt journal save failed: ${error}`); }
	}
}

function loadInflight(): void {
	let records: any[] = [];
	try {
		const journal = JSON.parse(fs.readFileSync(inflightPath(), "utf8"));
		if (journal.version !== 1 || !Array.isArray(journal.records)) throw new Error("invalid inflight journal");
		records = journal.records;
	} catch (error: any) {
		if (error.code !== "ENOENT") throw error; // Fail closed rather than overwrite recovery state.
	}
	if (records.length) readPersistedReceipts();
	for (const record of records) {
		const outbox = record.event?._outboxPath;
		if (outbox && path.dirname(path.resolve(outbox)) !== OUTBOX_DIR) throw new Error("unsafe journal outbox path");
		const event = normalizeEvent(record.event, JSON.stringify(record.event), outbox);
		if (!event || eventMatchesReceiver(event)) { log("ignored stale inflight identity"); continue; }
		if (hasPersistedReceipt(event)) {
			if (!markDelivered(event)) inflight.set(event.eventId, { event, processId: processIdentity });
			continue;
		}
		if (record.processId === processIdentity) inflight.set(event.eventId, { event, processId: processIdentity });
		else { pendingEventIds.add(event.eventId); pendingEvents.push(event); }
	}
	followUpOutstanding = inflight.size > 0;
}

function cleanupMainRegistrations(): void {
	let names: string[] = [];
	try { names = fs.readdirSync(MAIN_REGISTRY_DIR); } catch { return; }
	for (const name of names) {
		if (!name.endsWith(".json")) continue;
		const file = path.join(MAIN_REGISTRY_DIR, name);
		try {
			const record = JSON.parse(fs.readFileSync(file, "utf8"));
			const heartbeat = parseTime(record?.heartbeatAt ?? record?.heartbeat);
			if (!pidAlive(record?.pid) || !Number.isFinite(heartbeat) || Date.now() - heartbeat > LIVE_HEARTBEAT_MS) fs.rmSync(file, { force: true });
		} catch { fs.rmSync(file, { force: true }); }
	}
}

function writeSessionRegistration(ctx: any): void {
	if (SUBAGENT_TASK_ID || !sessionRegistryFile || !receiverEnabled) return;
	try {
		cleanupMainRegistrations();
		persistMainIdentity(ctx);
		atomicWriteJson(sessionRegistryFile, {
			version: 2,
			targetKind: "main",
			targetId: currentSessionId,
			sessionId: currentSessionId,
			runId: currentRunId,
			nonce: currentNonce,
			pid: process.pid,
			cwd: path.resolve(ctx.cwd),
			sessionFile: mainSessionFile || null,
			inbox: inboxDir,
			heartbeatAt: new Date().toISOString(),
			heartbeat: Date.now(), // protocol-v1 sender compatibility
			items: [],
		});
	} catch (error) { log(`session registration failed: ${error}`); }
}

function writeWorkerReceiverIdentity(ctx: any): void {
	if (!SUBAGENT_TASK_ID) return;
	const registration = readWorkerRegistration(SUBAGENT_TASK_ID);
	atomicWriteJson(receiverIdentityPath(), {
		version: 2, targetKind: "worker", targetId: SUBAGENT_TASK_ID,
		taskId: SUBAGENT_TASK_ID, runId: currentRunId, nonce: currentNonce,
		pid: process.pid, cwd: path.resolve(ctx.cwd),
		itemKeys: registrationKeys(registration), heartbeatAt: new Date().toISOString(),
	});
}

function removeWorkerReceiverIdentity(): void {
	if (!SUBAGENT_TASK_ID) return;
	try {
		const value = JSON.parse(fs.readFileSync(receiverIdentityPath(), "utf8"));
		if (Number(value?.pid) === process.pid && value?.runId === currentRunId) fs.rmSync(receiverIdentityPath(), { force: true });
	} catch {}
}

function removeSessionRegistration(): void {
	if (!sessionRegistryFile) return;
	try {
		const current = JSON.parse(fs.readFileSync(sessionRegistryFile, "utf8"));
		if (Number(current?.pid) === process.pid && current?.runId === currentRunId) fs.rmSync(sessionRegistryFile, { force: true });
	} catch {}
	sessionRegistryFile = "";
}

function dedupPath(): string {
	const target = SUBAGENT_TASK_ID || currentSessionId || "unbound";
	return path.join(STATE_ROOT, "dedup", `${hash(`${SUBAGENT_TASK_ID ? "worker" : "main"}:${target}`).slice(0, 24)}.json`);
}

function loadDedup(): void {
	try {
		const value = JSON.parse(fs.readFileSync(dedupPath(), "utf8"));
		if (value?.version === 1) dedupState = { version: 1, seen: value.seen || {}, semantic: value.semantic || {}, streams: value.streams || {} };
	} catch { dedupState = { version: 1, seen: {}, semantic: {}, streams: {} }; }
	pruneDedup();
}

function pruneDedup(): void {
	const cutoff = Date.now() - 14 * 24 * 60 * 60 * 1000;
	for (const map of [dedupState.seen, dedupState.semantic]) {
		for (const [key, timestamp] of Object.entries(map)) if (timestamp < cutoff) delete map[key];
	}
}

function saveDedup(): boolean {
	pruneDedup();
	try { atomicWriteJson(dedupPath(), dedupState); return true; } catch (error) { log(`dedup save failed: ${error}`); return false; }
}

function semanticKey(event: NotifyEvent): string {
	const source = event.level === "red" ? "*" : event.source;
	return hash(`${source}|${event.itemKey}|${event.state}|${event.message.slice(0, 120)}`);
}

function streamKey(event: NotifyEvent): string {
	return hash(`${event.targetKind}|${event.targetId}|${event.itemKey}|${event.runId}|${event.source}`);
}

function staleReason(event: NotifyEvent, now = Date.now()): string | null {
	const occurred = parseTime(event.occurredAt);
	if (!Number.isFinite(occurred)) return "invalid occurredAt";
	if (occurred > now + FUTURE_SKEW_MS) return "occurredAt too far in future";
	const expiry = event.expiresAt ? parseTime(event.expiresAt) : occurred + MAX_AGE_BY_LEVEL[event.level];
	if (!Number.isFinite(expiry) || expiry <= now) return "expired";
	if (dedupState.seen[event.eventId]) return "duplicate eventId";
	const semantic = dedupState.semantic[semanticKey(event)] || 0;
	if (now - semantic < DEDUP_COOLDOWN_MS) return "semantic duplicate";
	const latest = dedupState.streams[streamKey(event)];
	if (latest && (event.sequence < latest.sequence || (event.sequence === latest.sequence && occurred <= latest.occurredAt))) return "stale sequence";
	if (latest && occurred < latest.occurredAt) return "stale occurredAt";
	return null;
}

function markDelivered(event: NotifyEvent): boolean {
	const previous = dedupState;
	dedupState = { version: 1, seen: { ...previous.seen }, semantic: { ...previous.semantic }, streams: { ...previous.streams } };
	const now = Date.now();
	dedupState.seen[event.eventId] = now;
	dedupState.semantic[semanticKey(event)] = now;
	dedupState.streams[streamKey(event)] = { sequence: event.sequence, occurredAt: parseTime(event.occurredAt) };
	if (!saveDedup()) { dedupState = previous; return false; }
	if (event._outboxPath) {
		try { fs.rmSync(event._outboxPath, { force: true }); } catch {}
	}
	log("delivered (session persisted ACK)", event);
	return true;
}

function discardEvent(event: NotifyEvent, reason: string): void {
	if (event._outboxPath) {
		try { fs.rmSync(event._outboxPath, { force: true }); } catch {}
	}
	log(`discarded: ${reason}`, event);
}

function quarantine(file: string, reason: string): void {
	try {
		ensurePrivateDir(QUARANTINE_DIR);
		const target = path.join(QUARANTINE_DIR, `${Date.now()}-${path.basename(file).replace(/[^A-Za-z0-9._-]/g, "_")}`);
		fs.copyFileSync(file, target);
		try { fs.chmodSync(target, 0o600); } catch {}
		fs.appendFileSync(`${target}.reason`, reason + "\n", { encoding: "utf8", mode: 0o600 });
	} catch {}
	try { fs.rmSync(file, { force: true }); } catch {}
	log(`quarantined ${path.basename(file)}: ${reason}`);
}

function normalizeEvent(obj: any, raw: string, outboxPath?: string): NotifyEvent | null {
	if (!obj || typeof obj !== "object") return null;
	const legacy = obj.version !== 2;
	const declaredItem = String(obj.itemKey ?? obj.itemId ?? "");
	// Protocol-v1 allowed unscoped main-session notices. Preserve them under a
	// synthetic domain-neutral key; worker notices still require explicit ownership.
	const itemKey = declaredItem || (legacy && !SUBAGENT_TASK_ID ? `legacy:scope:${scopeKey(currentCtx?.cwd || ".")}` : "");
	const level = String(obj.level || (obj.state === "green" || obj.state === "yellow" || obj.state === "red" ? obj.state : "yellow"));
	const targetKind = String(obj.targetKind || (SUBAGENT_TASK_ID ? "worker" : "main"));
	const targetId = String(obj.targetId || (SUBAGENT_TASK_ID || currentSessionId));
	const occurredMs = parseTime(obj.occurredAt ?? obj.ts ?? Date.now());
	const eventId = String(obj.eventId || `legacy-${hash(raw)}`);
	const event: NotifyEvent = {
		version: 2,
		eventId,
		runId: String(obj.runId || (legacy ? currentRunId : "")),
		itemKey,
		targetKind: targetKind as any,
		targetId,
		state: String(obj.state || (level === "green" ? "progress" : "actionable")),
		sequence: Number(obj.sequence ?? occurredMs),
		checkpointPath: obj.checkpointPath ? String(obj.checkpointPath) : undefined,
		occurredAt: Number.isFinite(occurredMs) ? new Date(occurredMs).toISOString() : String(obj.occurredAt || ""),
		nonce: String(obj.nonce || (legacy ? currentNonce : "")),
		level: level as any,
		source: String(obj.source || "script").slice(0, 120),
		message: String(obj.message ?? obj.text ?? "").slice(0, 12000),
		actionable: obj.actionable === true,
		terminal: obj.terminal === true,
		expiresAt: obj.expiresAt ? new Date(parseTime(obj.expiresAt)).toISOString() : undefined,
		legacyItemId: obj.itemId ? String(obj.itemId) : undefined,
		_outboxPath: outboxPath,
	};
	if (!isSafeTargetId(event.eventId) || !isSafeTargetId(event.runId) || !isSafeItemKey(event.itemKey)
		|| !isSafeTargetId(event.targetId) || !["main", "worker"].includes(event.targetKind)
		|| !["green", "yellow", "red"].includes(event.level) || !event.message
		|| !Number.isSafeInteger(event.sequence) || event.sequence < 0 || !event.state || event.state.length > 120
		|| event.nonce.length < 16 || event.nonce.length > 200) return null;
	return event;
}

function eventMatchesReceiver(event: NotifyEvent): string | null {
	if (SUBAGENT_TASK_ID) {
		if (event.targetKind !== "worker" || event.targetId !== SUBAGENT_TASK_ID) return "wrong worker target";
		if (event.runId !== currentRunId || event.nonce !== currentNonce) return "stale worker run identity";
		const registration = readWorkerRegistration(SUBAGENT_TASK_ID);
		if (!registration || !registrationKeys(registration).includes(event.itemKey)) return "worker no longer owns itemKey";
		const lease = readWaitLease(false);
		if (lease && event.itemKey !== lease.itemKey) return "itemKey does not match active lease";
		return null;
	}
	if (event.targetKind !== "main" || event.targetId !== currentSessionId) return "wrong main target";
	if (event.runId !== currentRunId || event.nonce !== currentNonce) return "stale main run identity";
	return null;
}

function enqueueEvent(event: NotifyEvent): void {
	if (hasPersistedReceipt(event)) { markDelivered(event); return; }
	if (pendingEventIds.has(event.eventId) || inflight.has(event.eventId)) return;
	pendingEventIds.add(event.eventId);
	pendingEvents.push(event);
}

function collectDirectory(dir: string, outbox = false): void {
	let names: string[] = [];
	try {
		// Dot-prefixed entries (`.receiver-identity.json`, `.notification-wait-lease`,
		// `.last-event`, ...) are extension bookkeeping inside the same directory, not
		// inbound envelopes. They are valid JSON, so collecting them made every live
		// worker quarantine its OWN identity file on each scan; because that file is
		// rewritten on every heartbeat the loop never ended and the quarantine directory
		// grew without bound (observed: 71k entries / 279 MB).
		names = fs
			.readdirSync(dir)
			.filter((name) => name.endsWith(".json") && !name.startsWith("."))
			.slice(0, 1000);
	} catch { return; }
	for (const name of names) {
		const file = path.join(dir, name);
		if (outbox) {
			try {
				if (!fs.lstatSync(file).isFile()) continue;
				const raw = fs.readFileSync(file, "utf8");
				const rawObj = JSON.parse(raw);
				if (String(rawObj?.targetKind) !== (SUBAGENT_TASK_ID ? "worker" : "main")
					|| String(rawObj?.targetId) !== (SUBAGENT_TASK_ID || currentSessionId)) continue;
				const event = normalizeEvent(rawObj, raw, file);
				if (!event) { quarantine(file, "invalid outbox envelope"); continue; }
				const mismatch = eventMatchesReceiver(event);
				if (mismatch) { quarantine(file, mismatch); continue; }
				enqueueEvent(event);
			} catch (error) { quarantine(file, `outbox parse failed: ${error}`); }
			continue;
		}
		const processing = `${file}.${process.pid}.processing`;
		try {
			if (!fs.lstatSync(file).isFile()) continue;
			fs.renameSync(file, processing);
			const raw = fs.readFileSync(processing, "utf8");
			const event = normalizeEvent(JSON.parse(raw), raw);
			if (!event) { quarantine(processing, "invalid inbox envelope"); continue; }
			const mismatch = eventMatchesReceiver(event);
			if (mismatch) { quarantine(processing, mismatch); continue; }
			enqueueEvent(event);
			try { saveInflight(); } catch (error) { fs.renameSync(processing, file); log(`inbox journal failed; retained: ${error}`); continue; }
			fs.rmSync(processing, { force: true });
		} catch (error) { quarantine(processing, `inbox collection failed: ${error}`); }
	}
}

function collectFiles(): void {
	if (!receiverEnabled) return;
	reconcileReceipts();
	collectDirectory(inboxDir, false);
	if (!SUBAGENT_TASK_ID && pendingScopeDir) collectDirectory(pendingScopeDir, false);
	collectDirectory(OUTBOX_DIR, true);
}

function isImmediate(event: NotifyEvent): boolean {
	const lease = SUBAGENT_TASK_ID ? readWaitLease(false) : null;
	const directedWaitWake = !!lease && event.targetKind === "worker"
		&& event.targetId === SUBAGENT_TASK_ID && event.itemKey === lease.itemKey;
	return directedWaitWake || event.level === "red"
		|| (event.level === "yellow" && (event.actionable || event.terminal || ["actionable", "terminal"].includes(event.state.toLowerCase())));
}

async function scheduleFlush(): Promise<void> {
	collectFiles();
	if (pendingEvents.length === 0) return;
	if (pendingEvents.some(isImmediate)) {
		if (batchTimer) clearTimeout(batchTimer);
		batchTimer = null;
		await flushBatch();
		return;
	}
	if (!batchTimer) {
		batchTimer = setTimeout(() => {
			batchTimer = null;
			flushBatch().catch((error) => log(`batch failed: ${error}`));
		}, BATCH_WINDOW_MS);
		batchTimer.unref?.();
	}
}

function formatEvent(event: NotifyEvent): string {
	const icon = event.level === "red" ? "🔴" : event.level === "green" ? "🟢" : "🟡";
	return `${receiptMarker(event)} ${icon}[${event.source}] [${event.itemKey}] [${event.state}#${event.sequence}] ${event.message}`;
}

function passiveGreen(event: NotifyEvent): boolean {
	try {
		inflight.set(event.eventId, { event, processId: processIdentity });
		saveInflight();
		if (!piApi?.appendEntry) throw new Error("session appendEntry unavailable");
		piApi.appendEntry("agent-notify", { ...event, _outboxPath: undefined, receiptMarker: receiptMarker(event), receivedAt: new Date().toISOString() });
		if (currentCtx?.hasUI) currentCtx.ui.setStatus("agent-notify", `🟢 ${event.itemKey}: ${event.state}`);
		reconcileReceipts();
		return true;
	} catch (error) {
		inflight.delete(event.eventId);
		log(`passive green update failed: ${error}`, event);
		return false;
	}
}

let piApi: ExtensionAPI | null = null;

async function flushBatch(): Promise<void> {
	if (!receiverEnabled || pendingEvents.length === 0 || !piApi) return;
	const batch = pendingEvents;
	pendingEvents = [];
	for (const event of batch) pendingEventIds.delete(event.eventId);
	const actionable: NotifyEvent[] = [];
	for (const event of batch) {
		const mismatch = eventMatchesReceiver(event);
		if (mismatch) { if (event._outboxPath) quarantine(event._outboxPath, mismatch); else log(`rejected pending event: ${mismatch}`, event); continue; }
		const stale = staleReason(event);
		if (stale) { discardEvent(event, stale); continue; }
		const lease = SUBAGENT_TASK_ID ? readWaitLease(false) : null;
		const directedWaitWake = !!lease && event.targetKind === "worker"
			&& event.targetId === SUBAGENT_TASK_ID && event.itemKey === lease.itemKey;
		// Green remains passive for ordinary progress, but a directed event that
		// matches an armed wait must wake the worker regardless of display level.
		if (event.level === "green" && !directedWaitWake) {
			if (!passiveGreen(event)) enqueueEvent(event);
		} else actionable.push(event);
	}
	if (actionable.length === 0) return;

	if (!(currentCtx?.isIdle?.()) && followUpOutstanding) {
		for (const event of actionable) enqueueEvent(event);
		log(`followUp outstanding; retained ${actionable.length} event(s)`);
		return;
	}
	const body = `[agent-notify]\n${actionable.map(formatEvent).join("\n")}\n\nExternal directed events were validated. Re-read the authoritative state/checkpoint before acting; do not assume the notification itself proves completion. Delivery is at-least-once: make external effects idempotent by business/event identity.`;
	try {
		// Persist before calling the void host API. A new process replays this
		// journal; same-process /reload preserves already queued work.
		for (const event of actionable) inflight.set(event.eventId, { event, processId: processIdentity });
		saveInflight();
		if (currentCtx?.isIdle?.()) piApi.sendUserMessage(body);
		else {
			piApi.sendUserMessage(body, { deliverAs: "followUp" });
			followUpOutstanding = true;
		}
		// Releasing an agent_end hold permits the queued user message to run.
		// This is NOT a delivery ACK: outbox/journal remain until session persistence.
		if (SUBAGENT_TASK_ID) clearWaitLease("matching_directed_event_delivered");
		if (actionable.some((event) => event.level === "red") && currentCtx?.hasUI) currentCtx.ui.notify("🔴 External event needs attention", "error");
	} catch (error) {
		for (const event of actionable) { inflight.delete(event.eventId); enqueueEvent(event); }
		try { saveInflight(); } catch {}
		log(`injection failed; retained ${actionable.length}: ${error}`);
	}
}

function cleanupStaleFiles(): void {
	const now = Date.now();
	for (const dir of [inboxDir, OUTBOX_DIR, MAIN_REGISTRY_DIR, ...(!SUBAGENT_TASK_ID && pendingScopeDir ? [pendingScopeDir] : [])]) {
		let names: string[] = [];
		try { names = fs.readdirSync(dir); } catch { continue; }
		for (const name of names) {
			const file = path.join(dir, name);
			try {
				const age = now - fs.statSync(file).mtimeMs;
				if (name.includes(".tmp") && age > 5 * 60 * 1000) fs.rmSync(file, { force: true });
				else if (name.endsWith(".processing") && age > 5 * 60 * 1000) fs.renameSync(file, file.replace(/\.\d+\.processing$/, ""));
			} catch {}
		}
	}
	cleanupMainRegistrations();
	let rootNames: string[] = [];
	try { rootNames = fs.readdirSync(INBOX_ROOT); } catch {}
	const activeWorkers = (() => { try { return JSON.parse(fs.readFileSync(WORKER_REGISTRY, "utf8"))?.workers || {}; } catch { return {}; } })();
	for (const name of rootNames) {
		if (name.startsWith(".active-workers.json.") && name.endsWith(".tmp")) {
			try {
				const file = path.join(INBOX_ROOT, name);
				if (now - fs.statSync(file).mtimeMs > 5 * 60 * 1000) fs.rmSync(file, { force: true });
			} catch {}
			continue;
		}
		if (name.startsWith(".") || ["main", "main-pending"].includes(name) || activeWorkers[name]) continue;
		const dir = path.join(INBOX_ROOT, name);
		try {
			if (fs.statSync(dir).isDirectory() && now - fs.statSync(dir).mtimeMs > 24 * 60 * 60 * 1000) fs.rmSync(dir, { recursive: true, force: true });
		} catch {}
	}
}

export default function activate(pi: ExtensionAPI) {
	piApi = pi;
	if (!SUBAGENT_TASK_ID) {
		pi.registerTool({
			name: "notify_subagent",
			label: "Notify subagent",
			description: "Send a fail-closed follow-up instruction to a live durable subagent through pi-agent-notify. Defaults to the worker's automatic control itemKey worker:<taskId>. Use this instead of subagent_reload for ordinary steering; reload is for finished/paused sessions or runtime/tool refresh.",
			promptSnippet: "Send follow-up instructions to a live durable subagent without restarting it",
			promptGuidelines: [
				"Use notify_subagent instead of subagent_reload when only giving new instructions to a live worker; use subagent_reload only to resume a finished/paused session or load changed tools, extensions, or MCP runtime.",
			],
			parameters: {
				type: "object", additionalProperties: false,
				required: ["taskId", "message"],
				properties: {
					taskId: { type: "string", pattern: "^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$" },
					message: { type: "string", minLength: 1, maxLength: 12000 },
					itemKey: { type: "string", pattern: "^[A-Za-z0-9][A-Za-z0-9._:@+/-]{0,199}$", description: "Optional domain key; defaults to worker:<taskId>" },
					state: { type: "string", minLength: 1, maxLength: 120, default: "main.followup" },
					level: { type: "string", enum: ["yellow", "red"], default: "yellow" },
					requireLease: { type: "boolean", default: false, description: "Require an armed durable wait lease for watcher handoff messages" },
				},
			} as any,
			async execute(_id, params: any, signal) {
				const taskId = String(params.taskId || "");
				const itemKey = String(params.itemKey || `worker:${taskId}`);
				if (!SAFE_TARGET_ID.test(taskId)) throw new Error(`unsafe taskId: ${taskId}`);
				if (!isSafeItemKey(itemKey)) throw new Error(`unsafe itemKey: ${itemKey}`);
				if (!fs.existsSync(SENDER_SCRIPT)) throw new Error(`notify sender not found: ${SENDER_SCRIPT}`);
				const args = [
					SENDER_SCRIPT, "send", String(params.message),
					"--item", itemKey, "--to", taskId,
					"--state", String(params.state || "main.followup"),
					"--source", "main-agent", "--level", String(params.level || "yellow"),
				];
				if (params.requireLease) args.push("--require-lease");
				const result = await pi.exec("python3", args, { signal, timeout: 70000 });
				if (result.code !== 0) throw new Error((result.stderr || result.stdout || "notify sender failed").trim());
				let details: any;
				try { details = JSON.parse(result.stdout); }
				catch { details = { ok: true, raw: result.stdout.trim() }; }
				return {
					content: [{ type: "text", text: `Sent follow-up to ${taskId} via ${itemKey}.` }],
					details,
				};
			},
		});
	}
	if (SUBAGENT_TASK_ID) {
		pi.registerTool({
			name: "arm_notification_wait",
			label: "Arm notification wait",
			description: "Arm a bounded wait lease for a worker-owned domain item after starting one external watcher. The matching directed event must carry this lease's task, itemKey, runId, and nonce.",
			parameters: {
				type: "object", additionalProperties: false,
				required: ["itemKey", "reason", "wakeCondition", "producerPid", "producerMarkerPath"],
				properties: {
					itemKey: { type: "string", pattern: "^[A-Za-z0-9][A-Za-z0-9._:@+/-]{0,199}$", description: "Exact domain-neutral key owned by this worker" },
					reason: { type: "string", minLength: 1, maxLength: 300 },
					wakeCondition: { type: "string", minLength: 1, maxLength: 500 },
					leaseSeconds: { type: "integer", minimum: MIN_WAIT_LEASE_SECONDS, maximum: MAX_WAIT_LEASE_SECONDS, default: 21600 },
					checkpointPath: { type: "string", maxLength: 500 },
					producerPid: { type: "integer", minimum: 2 },
					producerMarkerPath: { type: "string", minLength: 1, maxLength: 500 },
				},
			} as any,
			prepareArguments(args: any) {
				if (!args || typeof args !== "object" || args.itemKey !== undefined || args.itemId === undefined) return args;
				const { itemId, ...rest } = args;
				return { ...rest, itemKey: String(itemId) };
			},
			async execute(_id, params: any, _signal, _update, ctx) {
				const itemKey = String(params.itemKey || "");
				if (!isSafeItemKey(itemKey)) return refused("itemKey is invalid", { itemKey });
				const existing = readWaitLease(true);
				activeWaitLease = existing;
				if (existing) {
					if (existing.taskId === SUBAGENT_TASK_ID && existing.itemKey === itemKey) return {
						content: [{ type: "text", text: `ALREADY_ARMED: ${SUBAGENT_TASK_ID} holds lease ${existing.leaseId} for ${itemKey}.` }],
						details: { armed: true, alreadyArmed: true, ...existing },
					};
					return refused(`worker already has lease for ${existing.itemKey}`, { existingLease: existing });
				}
				const registration = readWorkerRegistration(SUBAGENT_TASK_ID);
				if (!registration || !registrationKeys(registration).includes(itemKey)) return refused(`itemKey ${itemKey} is not owned by active worker ${SUBAGENT_TASK_ID}`, { itemKey, registeredKeys: registrationKeys(registration) });
				if (path.resolve(registration.cwd || "") !== path.resolve(ctx.cwd)) return refused("worker cwd mismatch", { itemKey });
				const leaseSeconds = Number(params.leaseSeconds || 21600);
				if (!Number.isInteger(leaseSeconds) || leaseSeconds < MIN_WAIT_LEASE_SECONDS || leaseSeconds > MAX_WAIT_LEASE_SECONDS) return refused("leaseSeconds out of range", { itemKey });
				const resolveRepoPath = (raw: unknown): string | null => {
					if (!raw) return null;
					const resolved = path.resolve(ctx.cwd, String(raw));
					const relative = path.relative(path.resolve(ctx.cwd), resolved);
					return relative.startsWith("..") || path.isAbsolute(relative) ? null : resolved;
				};
				const checkpointPath = params.checkpointPath ? resolveRepoPath(params.checkpointPath) : undefined;
				if (params.checkpointPath && !checkpointPath) return refused("checkpointPath must stay under worker cwd", { itemKey });
				const producerMarkerPath = resolveRepoPath(params.producerMarkerPath);
				const producerPid = Number(params.producerPid);
				if (!producerMarkerPath || !fs.existsSync(producerMarkerPath) || !pidAlive(producerPid)) return refused("wait lease requires a live producer and repository marker", { itemKey });
				let marker: any;
				try { marker = JSON.parse(fs.readFileSync(producerMarkerPath, "utf8")); } catch { return refused("producer marker is not valid JSON", { itemKey }); }
				const markerItem = String(marker?.itemKey ?? marker?.itemId ?? marker?.item ?? "");
				const markerTarget = String(marker?.notifyTo ?? marker?.to ?? "");
				if (Number(marker?.pid) !== producerPid || markerItem !== itemKey || markerTarget !== SUBAGENT_TASK_ID) return refused("producer marker PID/itemKey/target mismatch", { itemKey });
				const now = Date.now();
				const lease: WaitLease = {
					version: 2, leaseId: randomUUID(), taskId: SUBAGENT_TASK_ID, itemKey,
					itemId: /^\d{6}$/.test(itemKey) ? itemKey : undefined,
					runId: currentRunId, nonce: currentNonce,
					reason: String(params.reason), wakeCondition: String(params.wakeCondition), checkpointPath: checkpointPath || undefined,
					producerPid, producerMarkerPath, cwd: path.resolve(ctx.cwd), pid: process.pid,
					armedAt: now, expiresAt: now + leaseSeconds * 1000,
				};
				atomicWriteJson(waitLeasePath(), lease);
				activeWaitLease = lease;
				log(`wait lease armed task=${SUBAGENT_TASK_ID} itemKey=${itemKey} runId=${lease.runId}`);
				return {
					content: [{ type: "text", text: `ARMED: ${SUBAGENT_TASK_ID} remains alive for ${itemKey} until ${new Date(lease.expiresAt).toISOString()}. The notifier will read runId/nonce from the lease; finish without foreground polling.` }],
					details: { armed: true, ...lease },
				};
			},
		});
	}

	pi.on("session_start", async (_event, ctx) => {
		currentCtx = ctx;
		receiverEnabled = false;
		inflight = new Map();
		if (SUBAGENT_TASK_ID) {
			currentSessionId = String(ctx.sessionManager?.getSessionId?.() || SUBAGENT_TASK_ID);
			const identity = mainIdentity(`worker:${SUBAGENT_TASK_ID}:${currentSessionId}`);
			currentRunId = identity.runId;
			currentNonce = identity.nonce;
			inboxDir = path.join(INBOX_ROOT, SUBAGENT_TASK_ID);
			activeWaitLease = readWaitLease(false);
			if (activeWaitLease) {
				currentRunId = activeWaitLease.runId;
				currentNonce = activeWaitLease.nonce;
			}
		} else {
			currentSessionId = String(ctx.sessionManager?.getSessionId?.() || process.pid);
			try { await acquireMainController(currentSessionId); }
			catch (error) {
				// Lifecycle exceptions are otherwise only reported by Pi, leaving a
				// second transcript writer alive despite receiver exclusion.
				if (ctx.hasUI) ctx.ui.notify(`Main session controller refused: ${String(error)}`, "error");
				ctx.shutdown?.();
				throw error;
			}
			let identity: { runId: string; nonce: string };
			try { mainSessionFile = canonicalSessionFile(ctx); identity = restoreMainIdentity(ctx); }
			catch (error) { await releaseMainController(); throw error; }
			receiverEnabled = true;
			currentRunId = identity.runId;
			currentNonce = identity.nonce;
			inboxDir = path.join(INBOX_ROOT, "main", currentSessionId.replace(/[^A-Za-z0-9._-]/g, "_").slice(0, 160));
			pendingScopeDir = path.join(MAIN_PENDING_DIR, scopeKey(ctx.cwd));
			sessionRegistryFile = path.join(MAIN_REGISTRY_DIR, `${currentSessionId.replace(/[^A-Za-z0-9._-]/g, "_").slice(0, 160)}.json`);
			try { persistMainIdentity(ctx); }
			catch (error) { receiverEnabled = false; await releaseMainController(); throw error; }
			writeSessionRegistration(ctx);
			heartbeatTimer = setInterval(() => writeSessionRegistration(ctx), MAIN_HEARTBEAT_MS);
			heartbeatTimer.unref?.();
		}
		receiverEnabled = true;
		for (const dir of [inboxDir, OUTBOX_DIR, QUARANTINE_DIR, ...(!SUBAGENT_TASK_ID && pendingScopeDir ? [pendingScopeDir] : [])]) ensurePrivateDir(dir);
		if (SUBAGENT_TASK_ID) {
			writeWorkerReceiverIdentity(ctx);
			heartbeatTimer = setInterval(() => writeWorkerReceiverIdentity(ctx), MAIN_HEARTBEAT_MS);
			heartbeatTimer.unref?.();
		}
		loadDedup();
		try { loadInflight(); }
		catch (error) { receiverEnabled = false; removeSessionRegistration(); removeWorkerReceiverIdentity(); if (heartbeatTimer) clearInterval(heartbeatTimer); await releaseMainController(); throw error; }
		cleanupStaleFiles();
		for (const dir of [inboxDir, OUTBOX_DIR, ...(!SUBAGENT_TASK_ID && pendingScopeDir ? [pendingScopeDir] : [])]) {
			try {
				const watcher = fs.watch(dir, () => scheduleFlush().catch((error) => log(`watch flush failed: ${error}`)));
				watcher.on("error", () => {});
				watchers.push(watcher);
			} catch (error) { log(`watch setup failed for ${dir}: ${error}`); }
		}
		scanTimer = setInterval(() => scheduleFlush().catch((error) => log(`poll failed: ${error}`)), SCAN_INTERVAL_MS);
		scanTimer.unref?.();
		if (ctx.hasUI) ctx.ui.setStatus("agent-notify", `notify: ${SUBAGENT_TASK_ID || currentSessionId.slice(0, 8)}`);
		await scheduleFlush();
	});

	pi.on("message_end", (event: any) => {
		if (!receiverEnabled || event.message?.role !== "user") return;
		// Pi emits message_end before appendMessage. Check on the next scan/tick,
		// never acknowledge the event object (or unflushed in-memory entries).
		setImmediate(() => { if (receiverEnabled) reconcileReceipts(); });
	});

	pi.on("agent_start", async () => {
		if (!followUpOutstanding) return;
		followUpOutstanding = false;
		if (pendingEvents.length) await scheduleFlush();
	});

	pi.on("agent_end", async (_event, ctx) => {
		if (!SUBAGENT_TASK_ID || !receiverEnabled) return;
		collectFiles();
		if (pendingEvents.length && !followUpOutstanding) await flushBatch();
		let lease = readWaitLease(true);
		if (!lease) lease = rearmReleasedLeaseIfProducerAlive();
		activeWaitLease = lease;
		if (!lease) return;
		if (followUpOutstanding && ctx.hasPendingMessages?.()) return;
		followUpOutstanding = false;
		const remaining = lease.expiresAt - Date.now();
		if (remaining <= 0) { clearWaitLease("expired_before_hold"); return; }
		await new Promise<void>((resolve) => {
			waitLeaseResolve = resolve;
			waitLeaseTimer = setTimeout(() => {
				try {
					pi.sendUserMessage(`[agent-notify] Wait lease expired for ${lease.itemKey}. Re-read authoritative state and ${lease.checkpointPath || "the checkpoint"}; decide whether to continue, re-arm, or report.`, { deliverAs: "followUp" });
					followUpOutstanding = true;
					clearWaitLease("lease_timeout_followup");
				} catch (error) { log(`lease timeout injection failed: ${error}`); clearWaitLease("lease_timeout_failed"); }
			}, remaining);
		});
	});

	pi.on("session_shutdown", async (event: any) => {
		if (receiverEnabled) reconcileReceipts();
		receiverEnabled = false;
		for (const watcher of watchers) try { watcher.close(); } catch {}
		watchers = [];
		if (scanTimer) clearInterval(scanTimer);
		if (batchTimer) clearTimeout(batchTimer);
		if (heartbeatTimer) clearInterval(heartbeatTimer);
		scanTimer = batchTimer = heartbeatTimer = null;
		if (SUBAGENT_TASK_ID && (activeWaitLease || fs.existsSync(waitLeasePath()))) clearWaitLease("session_shutdown");
		removeSessionRegistration();
		removeWorkerReceiverIdentity();
		if (!SUBAGENT_TASK_ID && event?.reason !== "reload") {
			try { (globalThis as any).__pi_agent_notify_main_identities__?.delete(currentSessionId); } catch {}
		}
		// Version-2 events remain in the durable outbox. Requeue inbox-only legacy
		// events atomically so shutdown/new/resume cannot silently discard them.
		for (const pending of pendingEvents) {
			if (pending._outboxPath) continue;
			try { atomicWriteJson(path.join(inboxDir, `evt-requeue-${pending.eventId}.json`), { ...pending, _outboxPath: undefined }); } catch {}
		}
		pendingEvents = [];
		pendingEventIds.clear();
		followUpOutstanding = false;
		pendingScopeDir = "";
		if (currentCtx?.hasUI) currentCtx.ui.setStatus("agent-notify", undefined);
		currentCtx = null;
		mainSessionFile = "";
		inflight.clear();
		await releaseMainController();
	});
}

function refused(message: string, details: Record<string, unknown>) {
	return { content: [{ type: "text", text: `REFUSED: ${message}` }], details: { armed: false, taskId: SUBAGENT_TASK_ID, ...details } };
}
