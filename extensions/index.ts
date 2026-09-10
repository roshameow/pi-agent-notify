/**
 * agent-notify.ts — 脚本 → 主 agent 直连通知通道（v3：按项目/item 路由 + 合并缓冲 + 去重）
 *
 * 背景：TalentsAI 出题流程里大量 watcher 脚本（keepalive/reviewwatch/评测轮询等）
 * 发现问题后只能语音叫你或写 status.log，你需要再手动告诉 agent 处理。
 * 本扩展让外部脚本能把事件**直接注入主 agent 会话**：
 *   - 外部脚本往触发目录写一个 JSON 文件
 *   - 扩展监听目录，合并缓冲后作为"用户消息"注入
 *   - agent 收到后会自动处理（如评测完成→自动去质检/提交/返修）
 *
 * v3 关键改动（修复串会话与刷屏）：
 *   1. 合并缓冲：发现事件后攒 15s，同一批合成一条消息注入（不逐条发）
 *   2. 去重缓存：同 key（source+itemId+消息前缀）30 分钟内只注入一次，
 *      避免 keepalive 每 60s 反复报同一事件刷屏
 *   3. busy 时只排一条 followUp（合并后的），不逐条排队
 *
 * 用法（外部脚本）：
 *   python3 scripts/notify_agent.py "消息文本" --level yellow --source reviewwatch
 *
 * 触发目录：/tmp/pi-agent-notify/；主 session 使用 main/{sessionId} 专属收件箱，worker 使用 task-{id} 收件箱。
 *
 * level 语义：
 *   green  = 正常推进，注入 agent 备忘（不打扰）
 *   yellow = agent 可处理的事件，注入并触发一轮 agent 处理（默认）
 *   red    = 需用户拍板/卡死，注入 agent 并额外 ctx.ui.notify 高亮
 */

import * as fs from "node:fs";
import * as path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const TRIGGER_DIR_BASE = process.env.PI_AGENT_NOTIFY_DIR || "/tmp/pi-agent-notify";
// 身份感知：subagent 用独立的通知子目录（防多会话抢文件）。
// 主 session 不能共用基础目录：多个项目/会话会竞争同一个 evt 文件，导致通知串到别的对话。
// 主 session 的收件箱在 main/{sessionId}，由 notify_agent.py 按 itemId + cwd 路由。
const SUBAGENT_TASK_ID = process.env.PI_SUBAGENT_TASK_ID || "";
let TRIGGER_DIR = SUBAGENT_TASK_ID ? `${TRIGGER_DIR_BASE}/${SUBAGENT_TASK_ID}` : "";
let PENDING_SCOPE_DIR = "";
const MAIN_REGISTRY_DIR = `${TRIGGER_DIR_BASE}/.main-sessions`;
const MAIN_PENDING_DIR = `${TRIGGER_DIR_BASE}/main-pending`;
const MAIN_HEARTBEAT_MS = 30_000;
let sessionRegistryFile = "";
let sessionHeartbeat: ReturnType<typeof setInterval> | null = null;
let sessionScope = "";
let sessionId = "";

function safeSessionKey(value: string): string {
	return value.replace(/[^a-zA-Z0-9._-]/g, "_").slice(0, 160);
}
function scopeKey(value: string): string {
	return createHash("sha256").update(value).digest("hex").slice(0, 16);
}
const SCAN_INTERVAL_MS = Math.max(50, Number(process.env.PI_AGENT_NOTIFY_SCAN_MS || 2000));
const BATCH_WINDOW_MS = Math.max(0, Number(process.env.PI_AGENT_NOTIFY_BATCH_MS || 15000)); // 合并缓冲窗口：默认攒 15s
const DEDUP_COOLDOWN_MS = 30 * 60 * 1000; // 去重冷却 30 分钟
const WAIT_LEASE_FILE = ".notification-wait-lease";
const MIN_WAIT_LEASE_SECONDS = 30;
const MAX_WAIT_LEASE_SECONDS = 24 * 60 * 60;

interface NotifyEvent {
	level: string;
	source: string;
	message: string;
	itemId?: string;
	ts?: number;
}

interface WaitLease {
	version: 1;
	leaseId: string;
	taskId: string;
	itemId: string;
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

let currentCtx: any = null; // 最近一次 session_start 的 ctx
let pendingEvents: NotifyEvent[] = []; // 合并缓冲
let batchTimer: ReturnType<typeof setTimeout> | null = null;
let dedupCache = new Map<string, number>(); // key -> 上次注入时间戳
let followUpOutstanding = false; // 全局最多一条已排队 followUp；后续事件留在内存继续合并
let activeWaitLease: WaitLease | null = null;
let waitLeaseResolve: (() => void) | null = null;
// A directed event releases the lease so the resumed turn can run. If that turn ends without
// re-arming but the watcher it was waiting on is still alive, exiting here orphans the watcher and
// silently loses its wakeup (observed repeatedly 2026-09-11 across several items). Remember the
// lease that was released by event injection so agent_end can re-arm it exactly once.
let lastEventReleasedLease: WaitLease | null = null;
let lastEventReleasedAutoRearmed = false;
let lastReleasedReason = "";
let waitLeaseTimer: ReturnType<typeof setTimeout> | null = null;

function waitLeasePath(): string {
	return path.join(TRIGGER_DIR, WAIT_LEASE_FILE);
}

function readWorkerRegistration(taskId: string): any | null {
	try {
		const registry = JSON.parse(fs.readFileSync(path.join(TRIGGER_DIR_BASE, ".active-workers.json"), "utf-8"));
		return registry?.workers?.[taskId] || null;
	} catch (_) {
		return null;
	}
}

function removeWaitLeaseFile(): void {
	try { fs.rmSync(waitLeasePath(), { force: true }); } catch (_) { /* ignore */ }
}

function readWaitLease(): WaitLease | null {
	if (!SUBAGENT_TASK_ID) return null;
	try {
		const value = JSON.parse(fs.readFileSync(waitLeasePath(), "utf-8"));
		let producerAlive = false;
		try {
			producerAlive = Number.isInteger(value?.producerPid)
				&& value.producerPid >= 2
				&& typeof value?.producerMarkerPath === "string"
				&& fs.existsSync(value.producerMarkerPath);
			if (producerAlive) {
				const marker = JSON.parse(fs.readFileSync(value.producerMarkerPath, "utf-8"));
				producerAlive = Number(marker?.pid) === Number(value.producerPid)
					&& String(marker?.itemId ?? marker?.item ?? "") === String(value.itemId)
					&& String(marker?.notifyTo ?? marker?.to ?? "") === String(value.taskId);
			}
			if (producerAlive) process.kill(value.producerPid, 0);
		} catch { producerAlive = false; }
		const valid = value?.version === 1
			&& value?.taskId === SUBAGENT_TASK_ID
			&& /^\d{6}$/.test(String(value?.itemId || ""))
			&& Number.isFinite(value?.expiresAt)
			&& value.expiresAt > Date.now()
			&& producerAlive;
		if (!valid) {
			removeWaitLeaseFile();
			return null;
		}
		return value as WaitLease;
	} catch (e: any) {
		if (e?.code !== "ENOENT") removeWaitLeaseFile();
		return null;
	}
}

// Re-arm the lease that a directed event released, but only when its watcher is still alive and
// the lease has not expired. Bounded: at most once per released lease, and never beyond the
// original expiry, so a long-lived watcher cannot hold the process forever.
function rearmReleasedLeaseIfProducerAlive(): WaitLease | null {
	const previous = lastEventReleasedLease;
	if (!previous) return null;
	if (lastReleasedReason !== "directed_event_injected") return null;
	if (lastEventReleasedAutoRearmed) return null;
	if (!(previous.expiresAt > Date.now())) return null;
	try {
		if (!previous.producerMarkerPath || !fs.existsSync(previous.producerMarkerPath)) return null;
		const marker = JSON.parse(fs.readFileSync(previous.producerMarkerPath, "utf-8"));
		if (Number(marker?.pid) !== Number(previous.producerPid)) return null;
		if (String(marker?.itemId ?? marker?.item ?? "") !== String(previous.itemId)) return null;
		if (String(marker?.notifyTo ?? marker?.to ?? "") !== String(previous.taskId)) return null;
		process.kill(previous.producerPid, 0);
	} catch (_) {
		return null;
	}
	const tmp = `${waitLeasePath()}.${process.pid}.rearm.tmp`;
	try {
		fs.writeFileSync(tmp, JSON.stringify(previous), { encoding: "utf-8", mode: 0o600 });
		fs.renameSync(tmp, waitLeasePath());
	} catch (_) {
		try { fs.rmSync(tmp, { force: true }); } catch (_) { /* ignore */ }
		return null;
	}
	lastEventReleasedAutoRearmed = true;
	try {
		fs.appendFileSync("/tmp/agent-notify.log", `[${new Date().toISOString()}] auto re-armed released wait lease task=${SUBAGENT_TASK_ID} item=${previous.itemId} producer=${previous.producerPid} (worker ended turn without re-arming)\n`);
	} catch (_) { /* ignore */ }
	return readWaitLease();
}

function clearWaitLease(reason: string): void {
	if (waitLeaseTimer) {
		clearTimeout(waitLeaseTimer);
		waitLeaseTimer = null;
	}
	lastReleasedReason = reason;
	if (activeWaitLease) {
		lastEventReleasedLease = activeWaitLease;
		lastEventReleasedAutoRearmed = false;
	}
	removeWaitLeaseFile();
	activeWaitLease = null;
	const resolve = waitLeaseResolve;
	waitLeaseResolve = null;
	if (resolve) resolve();
	try {
		fs.appendFileSync("/tmp/agent-notify.log", `[${new Date().toISOString()}] wait lease released task=${SUBAGENT_TASK_ID || "main"} reason=${reason}\n`);
	} catch (_) { /* ignore */ }
}

function discoverSessionItems(ctx: any): string[] {
	// 只登记当前项目台账中的 item，避免把普通数字（日期、端口、金额）误当题目。
	try {
		const ledgerPath = path.join(ctx.cwd, "docs", "current_tasks.json");
		const ledger = JSON.parse(fs.readFileSync(ledgerPath, "utf-8"));
		const known = new Set(Object.keys(ledger.items || {}).filter((x) => /^\d+$/.test(x)));
		const entries = ctx.sessionManager?.getEntries?.() || [];
		const text = JSON.stringify(entries);
		return [...known].filter((id) => new RegExp(`(?:^|\\D)${id}(?:$|\\D)`).test(text));
	} catch (_) {
		return [];
	}
}

function writeSessionRegistration(ctx: any): void {
	if (SUBAGENT_TASK_ID || !sessionRegistryFile) return;
	try {
		const record = {
			sessionId,
			pid: process.pid,
			cwd: sessionScope,
			sessionFile: ctx.sessionManager?.getSessionFile?.() || null,
			inbox: TRIGGER_DIR,
			items: discoverSessionItems(ctx),
			heartbeat: Date.now(),
		};
		const tmp = `${sessionRegistryFile}.${process.pid}.tmp`;
		fs.mkdirSync(MAIN_REGISTRY_DIR, { recursive: true });
		fs.writeFileSync(tmp, JSON.stringify(record), "utf-8");
		fs.renameSync(tmp, sessionRegistryFile);
	} catch (e) {
		try {
			fs.appendFileSync("/tmp/agent-notify.log", `[${new Date().toISOString()}] session registration err ${e}\n`);
		} catch (_) { /* ignore */ }
	}
}

function removeSessionRegistration(): void {
	if (sessionHeartbeat) {
		clearInterval(sessionHeartbeat);
		sessionHeartbeat = null;
	}
	if (sessionRegistryFile) {
		try { fs.rmSync(sessionRegistryFile, { force: true }); } catch (_) { /* ignore */ }
	}
	sessionRegistryFile = "";
}

export default function (pi: ExtensionAPI) {
	let scanTimer: ReturnType<typeof setInterval> | null = null;

	if (SUBAGENT_TASK_ID) {
		pi.registerTool({
			name: "arm_notification_wait",
			label: "Arm notification wait",
			description: "Arm a bounded system-level wait lease for this durable worker after starting one external watcher. The current turn may then finish; agent-notify keeps the same worker process alive and a directed notify_agent.py event resumes it. This does not poll or start a watcher.",
			parameters: {
				type: "object",
				additionalProperties: false,
				required: ["itemId", "reason", "wakeCondition", "producerPid", "producerMarkerPath"],
				properties: {
					itemId: { type: "string", pattern: "^[0-9]{6}$", description: "Exact six-digit item owned by this worker" },
					reason: { type: "string", minLength: 1, maxLength: 300, description: "Why the worker must stay alive" },
					wakeCondition: { type: "string", minLength: 1, maxLength: 500, description: "Exact external event/state that should wake the worker" },
					leaseSeconds: { type: "integer", minimum: MIN_WAIT_LEASE_SECONDS, maximum: MAX_WAIT_LEASE_SECONDS, default: 21600 },
					checkpointPath: { type: "string", maxLength: 500, description: "Repository checkpoint/marker to re-read after wake" },
					producerPid: { type: "integer", minimum: 2, description: "PID of the unique detached watcher/notifier" },
					producerMarkerPath: { type: "string", minLength: 1, maxLength: 500, description: "Existing repository launch marker proving watcher identity" },
				},
			} as any,
			async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
				const itemId = String(params.itemId || "");
				// Always revalidate the durable lease and its producer. An in-memory
				// lease whose watcher died must not be returned as ALREADY_ARMED.
				const existingLease = readWaitLease();
				activeWaitLease = existingLease;
				if (existingLease) {
					// Idempotent re-arm: a worker may receive a duplicate/coalesced event
					// while its original lease is still held. Treat the same task+item lease
					// as success, otherwise workers can mistake a harmless duplicate for a
					// fatal wait failure and exit before the next external event.
					if (existingLease.taskId === SUBAGENT_TASK_ID && existingLease.itemId === itemId) {
						activeWaitLease = existingLease;
						return {
							content: [{ type: "text", text: `ALREADY_ARMED: worker ${SUBAGENT_TASK_ID} already holds wait lease ${existingLease.leaseId} for item ${itemId}; finish this turn without foreground sleep/polling.` }],
							details: { armed: true, alreadyArmed: true, ...existingLease },
						};
					}
					return {
						content: [{ type: "text", text: `REFUSED: worker ${SUBAGENT_TASK_ID} already has active wait lease ${existingLease.leaseId} for item ${existingLease.itemId}` }],
						details: { armed: false, existingLease },
					};
				}
				const registration = readWorkerRegistration(SUBAGENT_TASK_ID);
				const registeredItems = Array.isArray(registration?.itemIds) ? registration.itemIds.map(String) : [];
				if (!registration || !registeredItems.includes(itemId)) {
					return {
						content: [{ type: "text", text: `REFUSED: item ${itemId} is not owned by active worker ${SUBAGENT_TASK_ID}` }],
						details: { armed: false, taskId: SUBAGENT_TASK_ID, itemId, registeredItems },
					};
				}
				if (path.resolve(registration.cwd || "") !== path.resolve(ctx.cwd)) {
					return {
						content: [{ type: "text", text: `REFUSED: worker cwd mismatch for ${SUBAGENT_TASK_ID}` }],
						details: { armed: false, taskId: SUBAGENT_TASK_ID, itemId },
					};
				}
				const leaseSeconds = Number(params.leaseSeconds || 21600);
				if (!Number.isInteger(leaseSeconds) || leaseSeconds < MIN_WAIT_LEASE_SECONDS || leaseSeconds > MAX_WAIT_LEASE_SECONDS) {
					return {
						content: [{ type: "text", text: `REFUSED: leaseSeconds must be an integer from ${MIN_WAIT_LEASE_SECONDS} to ${MAX_WAIT_LEASE_SECONDS}` }],
						details: { armed: false, taskId: SUBAGENT_TASK_ID, itemId },
					};
				}
				const resolveRepoPath = (raw: unknown, label: string): string | null => {
					const resolved = path.resolve(ctx.cwd, String(raw || ""));
					const rel = path.relative(path.resolve(ctx.cwd), resolved);
					if (!raw || rel.startsWith("..") || path.isAbsolute(rel)) return null;
					return resolved;
				};
				let checkpointPath: string | undefined;
				if (params.checkpointPath) {
					checkpointPath = resolveRepoPath(params.checkpointPath, "checkpointPath") || undefined;
					if (!checkpointPath) {
						return {
							content: [{ type: "text", text: `REFUSED: checkpointPath must stay under worker cwd ${path.resolve(ctx.cwd)}` }],
							details: { armed: false, taskId: SUBAGENT_TASK_ID, itemId },
						};
					}
				}
				const producerPid = Number(params.producerPid);
				const producerMarkerPath = resolveRepoPath(params.producerMarkerPath, "producerMarkerPath");
				if (!Number.isInteger(producerPid) || producerPid < 2 || !producerMarkerPath || !fs.existsSync(producerMarkerPath)) {
					return {
						content: [{ type: "text", text: "REFUSED: wait lease requires a live detached watcher PID and an existing repository launch marker" }],
						details: { armed: false, taskId: SUBAGENT_TASK_ID, itemId, producerPid, producerMarkerPath },
					};
				}
				let producerMarker: any;
				try { producerMarker = JSON.parse(fs.readFileSync(producerMarkerPath, "utf-8")); }
				catch {
					return {
						content: [{ type: "text", text: "REFUSED: producer launch marker is not valid JSON" }],
						details: { armed: false, taskId: SUBAGENT_TASK_ID, itemId, producerPid, producerMarkerPath },
					};
				}
				const markerItem = String(producerMarker?.itemId ?? producerMarker?.item ?? "");
				const markerTarget = String(producerMarker?.notifyTo ?? producerMarker?.to ?? "");
				if (Number(producerMarker?.pid) !== producerPid || markerItem !== itemId || markerTarget !== SUBAGENT_TASK_ID) {
					return {
						content: [{ type: "text", text: "REFUSED: producer launch marker PID/item/notifyTo does not match this worker lease" }],
						details: { armed: false, taskId: SUBAGENT_TASK_ID, itemId, producerPid, producerMarkerPath, producerMarker },
					};
				}
				try { process.kill(producerPid, 0); }
				catch {
					return {
						content: [{ type: "text", text: `REFUSED: watcher PID ${producerPid} is not alive` }],
						details: { armed: false, taskId: SUBAGENT_TASK_ID, itemId, producerPid, producerMarkerPath },
					};
				}
				const now = Date.now();
				const lease: WaitLease = {
					version: 1,
					leaseId: randomUUID(),
					taskId: SUBAGENT_TASK_ID,
					itemId,
					reason: String(params.reason),
					wakeCondition: String(params.wakeCondition),
					checkpointPath,
					producerPid,
					producerMarkerPath,
					cwd: path.resolve(ctx.cwd),
					pid: process.pid,
					armedAt: now,
					expiresAt: now + leaseSeconds * 1000,
				};
				fs.mkdirSync(TRIGGER_DIR, { recursive: true });
				const tmp = `${waitLeasePath()}.${process.pid}.tmp`;
				fs.writeFileSync(tmp, JSON.stringify(lease), { encoding: "utf-8", mode: 0o600 });
				fs.renameSync(tmp, waitLeasePath());
				activeWaitLease = lease;
				piLog(`wait lease armed task=${SUBAGENT_TASK_ID} item=${itemId} lease=${lease.leaseId} seconds=${leaseSeconds}`);
				return {
					content: [{ type: "text", text: `ARMED: ${SUBAGENT_TASK_ID} remains alive for directed notifications about item ${itemId} until ${new Date(lease.expiresAt).toISOString()}. Finish this turn without foreground sleep/polling; the extension will hold the worker at agent_end.` }],
					details: { armed: true, ...lease },
				};
			},
		});
	}

	// 启动监听（session 开始时）
	pi.on("session_start", async (_event, ctx) => {
		// Restore only a structurally valid, unexpired lease for this exact task.
		// readWaitLease removes malformed/expired leftovers from a prior crash.
		if (SUBAGENT_TASK_ID) activeWaitLease = readWaitLease();
		if (!SUBAGENT_TASK_ID) {
			sessionId = String(ctx.sessionManager?.getSessionId?.() || process.pid);
			sessionScope = path.resolve(ctx.cwd);
			const key = safeSessionKey(sessionId);
			TRIGGER_DIR = path.join(TRIGGER_DIR_BASE, "main", key);
			PENDING_SCOPE_DIR = path.join(MAIN_PENDING_DIR, scopeKey(sessionScope));
			sessionRegistryFile = path.join(MAIN_REGISTRY_DIR, `${key}.json`);
			writeSessionRegistration(ctx);
			sessionHeartbeat = setInterval(() => writeSessionRegistration(ctx), MAIN_HEARTBEAT_MS);
			sessionHeartbeat.unref?.();
		}
		const watchDirs = [TRIGGER_DIR, ...(!SUBAGENT_TASK_ID && PENDING_SCOPE_DIR ? [PENDING_SCOPE_DIR] : [])];
		for (const dir of watchDirs) {
			try { fs.mkdirSync(dir, { recursive: true }); } catch (_) { /* ignore */ }
		}

		// fs.watch 作为第一通道；无活跃主会话时的 scope pending 也会在启动后消费。
		const dirWatchers: fs.FSWatcher[] = [];
		for (const dir of watchDirs) {
			try {
				const watcher = fs.watch(dir, () => scheduleFlush().catch((e) => piLog(`watch err ${e}`)));
				watcher.on("error", () => { /* 定时轮询兜底 */ });
				dirWatchers.push(watcher);
			} catch (e) { piLog(`watch setup err ${dir}: ${e}`); }
		}

		// 定时轮询兜底（fs.watch 在 macOS 目录场景可能漏事件）
		if (!scanTimer) {
			scanTimer = setInterval(() => {
				scheduleFlush().catch((e) => piLog(`poll err ${e}`));
			}, SCAN_INTERVAL_MS);
			if (scanTimer.unref) scanTimer.unref();
		}

		currentCtx = ctx;
		if (ctx.hasUI) {
			ctx.ui.notify(`agent-notify v3: watching ${TRIGGER_DIR}`, "info");
		}

		// reload/new/resume 会销毁旧 session runtime。清理旧 timer 很重要：
		// 否则旧 timer 会继续调用旧 pi.sendUserMessage，触发 stale ctx，导致通知丢失。
		pi.on("session_shutdown", () => {
			if (activeWaitLease || (SUBAGENT_TASK_ID && fs.existsSync(waitLeasePath()))) {
				clearWaitLease("session_shutdown");
			}
			for (const watcher of dirWatchers) {
				try { watcher.close(); } catch (_) { /* ignore */ }
			}
			if (scanTimer) {
				clearInterval(scanTimer);
				scanTimer = null;
			}
			if (batchTimer) {
				clearTimeout(batchTimer);
				batchTimer = null;
			}
			removeSessionRegistration();
			// reload/shutdown 时把尚未注入的内存事件写回 scope/worker inbox，避免静默丢失。
			const retryDir = (!SUBAGENT_TASK_ID && PENDING_SCOPE_DIR) ? PENDING_SCOPE_DIR : TRIGGER_DIR;
			for (const event of pendingEvents) {
				try {
					fs.mkdirSync(retryDir, { recursive: true });
					fs.writeFileSync(path.join(retryDir, `evt-requeue-${Date.now()}-${Math.random().toString(16).slice(2)}.json`), JSON.stringify(event), "utf-8");
				} catch (_) { /* leave shutdown best-effort */ }
			}
			pendingEvents = [];
			if (waitLeaseTimer) {
				clearTimeout(waitLeaseTimer);
				waitLeaseTimer = null;
			}
			waitLeaseResolve = null;
			activeWaitLease = null;
			currentCtx = null;
		});
	});

	// A queued follow-up has actually been consumed only when a new agent run
	// starts. Resetting at send time is too early; never resetting leaves later
	// events stuck behind "followUp already outstanding" forever.
	pi.on("agent_start", async () => {
		if (!followUpOutstanding) return;
		followUpOutstanding = false;
		piLog(`followUp consumed; queue reopened task=${SUBAGENT_TASK_ID || "main"}`);
		if (pendingEvents.length > 0) scheduleFlush().catch((e) => piLog(`post-consume flush err ${e}`));
	});

	// Durable workers run as `pi --mode json -p` and otherwise exit immediately
	// after agent_settled. Hold agent_end (before settled is emitted) only when
	// this worker explicitly armed a bounded lease. A directed event is queued
	// as a follow-up, then releases this handler so the same session continues.
	pi.on("agent_end", async (_event, ctx) => {
		if (!SUBAGENT_TASK_ID) return;
		try {
			await collectFiles();
			if (pendingEvents.length > 0 && !followUpOutstanding) {
				if (batchTimer) {
					clearTimeout(batchTimer);
					batchTimer = null;
				}
				await flushBatch();
			}
		} catch (e) {
			piLog(`agent_end preflush err ${e}`);
		}
		let lease = readWaitLease();
		if (!lease) lease = rearmReleasedLeaseIfProducerAlive();
		activeWaitLease = lease;
		if (!lease) return;
		if (followUpOutstanding) {
			// A follow-up can be queued while this run is busy. The first agent_end
			// should yield to a genuinely pending message, but after that message is
			// consumed some hosts do not emit another observable agent_start before
			// the next agent_end. Never let the stale flag bypass an armed lease.
			if (ctx.hasPendingMessages()) {
				piLog(`wait lease deferred for pending followUp task=${SUBAGENT_TASK_ID} item=${lease.itemId}`);
				return;
			}
			followUpOutstanding = false;
			piLog(`cleared stale followUpOutstanding before wait hold task=${SUBAGENT_TASK_ID}`);
		}
		activeWaitLease = lease;
		const remaining = lease.expiresAt - Date.now();
		if (remaining <= 0) {
			clearWaitLease("expired_before_hold");
			return;
		}
		piLog(`wait lease holding task=${SUBAGENT_TASK_ID} item=${lease.itemId} lease=${lease.leaseId}`);
		await new Promise<void>((resolve) => {
			waitLeaseResolve = resolve;
			waitLeaseTimer = setTimeout(() => {
				const body = `[脚本通知] 🟡[agent-notify] [item ${lease.itemId}] 等待租约已到期。请重新读取权威状态与 ${lease.checkpointPath || "checkpoint"}，决定续等、推进或上报；不要前台 sleep/轮询。`;
				try {
					pi.sendUserMessage(body, { deliverAs: "followUp" });
					followUpOutstanding = true;
					clearWaitLease("lease_timeout_followup");
				} catch (e) {
					piLog(`lease timeout injection failed ${e}`);
					clearWaitLease("lease_timeout_inject_failed");
				}
			}, remaining);
		});
	});

	function piLog(msg: string) {
		try {
			fs.appendFileSync(
				"/tmp/agent-notify.log",
				`[${new Date().toISOString()}] ${msg}\n`,
			);
		} catch (e) {
			/* ignore */
		}
	}

	// 有事件时：先合并缓冲，等 BATCH_WINDOW 后一次性处理
	async function scheduleFlush() {
		try {
			await collectFiles();
		} catch (e) {
			piLog(`collect err ${e}`);
		}
		if (pendingEvents.length === 0) return;
		if (!batchTimer) {
			batchTimer = setTimeout(() => {
				batchTimer = null;
				flushBatch().catch((e) => piLog(`batch err ${e}`));
			}, BATCH_WINDOW_MS);
			if (batchTimer.unref) batchTimer.unref();
		}
	}

	// 读专属 inbox 与 scope pending，加入合并缓冲（原子 rename，避免并发）。
	async function collectFiles() {
		const dirs = [TRIGGER_DIR, ...(!SUBAGENT_TASK_ID && PENDING_SCOPE_DIR ? [PENDING_SCOPE_DIR] : [])];
		for (const dir of dirs) {
			let files: string[];
			try { files = fs.readdirSync(dir).filter((f) => f.endsWith(".json")); }
			catch (_) { continue; }
			for (const f of files) {
				const full = path.join(dir, f);
				const proc = full + ".processing";
				try {
					fs.renameSync(full, proc);
					const raw = fs.readFileSync(proc, "utf-8");
					const obj = JSON.parse(raw);
					if (obj && (obj.message || obj.text)) {
						pendingEvents.push({ level: obj.level || "yellow", source: obj.source || "script",
							message: obj.message || obj.text, itemId: obj.itemId || undefined,
							ts: typeof obj.ts === "number" ? obj.ts : undefined });
					}
					fs.rmSync(proc, { force: true });
				} catch (e) {
					// rename 成功但读取/解析失败时保留 .processing 供人工审计，不静默删除。
					piLog(`collect failed ${proc}: ${e}`);
				}
			}
		}
	}

	// 去重 key：source + itemId + 消息前 40 字符（同类事件视为重复）
	function dedupKey(e: NotifyEvent): string {
		// red 按 item+状态消息跨 watcher 去重，避免不同检测器对同一人工阻塞重复注入。
		const source = e.level === "red" ? "*" : e.source;
		return `${source}|${e.itemId || "-"}|${e.message.slice(0, 40)}`;
	}

	// 录屏告警是瞬时状态：事件排队期间若健康快照已恢复，不应再注入一条过期红警。
	function isObsoleteRecordingAlert(e: NotifyEvent, now: number): boolean {
		if (e.source !== "recording_supervisor" || e.level !== "red") return false;
		const eventMs = e.ts ? (e.ts < 10_000_000_000 ? e.ts * 1000 : e.ts) : 0;
		if (eventMs && now - eventMs > 3 * 60 * 1000) return true;
		if (!e.itemId || !currentCtx?.cwd) return false;
		try {
			const p = path.join(currentCtx.cwd, "docs", "recording_health.json");
			const h = JSON.parse(fs.readFileSync(p, "utf-8"));
			const generated = Date.parse(h.generatedAt || "");
			const inst = h.instances?.[String(e.itemId)];
			return Number.isFinite(generated) && now - generated < 120_000 && inst?.state === "running";
		} catch (_) {
			return false;
		}
	}

	// 合并缓冲到期：丢弃过期状态 → 去重 → 合成一条 → 注入
	async function flushBatch() {
		if (pendingEvents.length === 0) return;
		const batch = pendingEvents;
		pendingEvents = [];

		// 去重：同 key 冷却期内跳过
		const now = Date.now();
		const unique: NotifyEvent[] = [];
		for (const e of batch) {
			if (isObsoleteRecordingAlert(e, now)) {
				piLog(`stale recording alert skip ${e.itemId || "-"}: ${e.message.slice(0, 80)}`);
				continue;
			}
			const key = dedupKey(e);
			const last = dedupCache.get(key) || 0;
			if (now - last < DEDUP_COOLDOWN_MS) {
				piLog(`dedup skip ${key.slice(0, 80)}`);
				continue;
			}
			// red 同样必须去重；真正的新状态由消息前缀/状态类形成不同 key。
			dedupCache.set(key, now);
			unique.push(e);
		}
		if (unique.length === 0) return;

		// 合成一条注入消息
		const parts = unique.map((e) => {
			const item = e.itemId ? ` [item ${e.itemId}]` : "";
			const lvl = e.level === "red" ? "🔴" : e.level === "green" ? "🟢" : "🟡";
			return `${lvl}[${e.source}]${item} ${e.message}`;
		});
		const text = parts.join("\n");
		const body = `[脚本通知] ${text}\n\n（来自自动监控脚本，请判断是否需要处理并自行推进，无需告知用户；若需用户拍板则红色提醒。）`;

		let injected = false;
		try {
			if (currentCtx?.isIdle && currentCtx.isIdle()) {
				pi.sendUserMessage(body);
				piLog(`injected(${unique.length}条合并): ${text.slice(0, 120)}`);
			} else if (!followUpOutstanding) {
				// busy：全局只允许一条已排队 followUp。
				pi.sendUserMessage(body, { deliverAs: "followUp" });
				followUpOutstanding = true;
				piLog(`injected(followUp,${unique.length}条合并): ${text.slice(0, 120)}`);
			} else {
				// 已有 followUp 等待消费：撤销 dedup 占位并留在内存，继续与后续事件合并。
				for (const event of unique) dedupCache.delete(dedupKey(event));
				pendingEvents.unshift(...unique);
				piLog(`followUp already outstanding; retained ${unique.length} event(s)`);
				return;
			}
			injected = true;
			if (SUBAGENT_TASK_ID && (activeWaitLease || readWaitLease())) clearWaitLease("directed_event_injected");
		} catch (e) {
			// 注入失败：撤销本轮 dedup 占位并放回缓冲，下一轮重试。
			for (const event of unique) dedupCache.delete(dedupKey(event));
			pendingEvents.unshift(...unique);
			piLog(`inject failed; requeued ${unique.length}: ${e}`);
		}

		// red 事件仅在成功注入后额外 TUI 高亮；主会话语音由 sender 去重后负责。
		if (injected && unique.some((e) => e.level === "red")) {
			try {
				if (currentCtx?.ui) currentCtx.ui.notify("🔴 脚本事件需用户拍板（见对话）", "error");
			} catch (e) {
				piLog(`ui notify err ${e}`);
			}
		}
	}
}
