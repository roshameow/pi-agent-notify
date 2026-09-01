/**
 * agent-notify.ts — 脚本 → 主 agent 直连通知通道（v2：合并缓冲 + 去重）
 *
 * 背景：TalentsAI 出题流程里大量 watcher 脚本（keepalive/reviewwatch/评测轮询等）
 * 发现问题后只能语音叫你或写 status.log，你需要再手动告诉 agent 处理。
 * 本扩展让外部脚本能把事件**直接注入主 agent 会话**：
 *   - 外部脚本往触发目录写一个 JSON 文件
 *   - 扩展监听目录，合并缓冲后作为"用户消息"注入
 *   - agent 收到后会自动处理（如评测完成→自动去质检/提交/返修）
 *
 * v2 关键改动（修复刷屏）：
 *   1. 合并缓冲：发现事件后攒 15s，同一批合成一条消息注入（不逐条发）
 *   2. 去重缓存：同 key（source+itemId+消息前缀）30 分钟内只注入一次，
 *      避免 keepalive 每 60s 反复报同一事件刷屏
 *   3. busy 时只排一条 followUp（合并后的），不逐条排队
 *
 * 用法（外部脚本）：
 *   python3 scripts/notify_agent.py "消息文本" --level yellow --source reviewwatch
 *
 * 触发目录：/tmp/pi-agent-notify/  （脚本往这写 *.json，扩展处理后删除）
 *
 * level 语义：
 *   green  = 正常推进，注入 agent 备忘（不打扰）
 *   yellow = agent 可处理的事件，注入并触发一轮 agent 处理（默认）
 *   red    = 需用户拍板/卡死，注入 agent 并额外 ctx.ui.notify 高亮
 */

import * as fs from "node:fs";
import * as path from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const TRIGGER_DIR_BASE = process.env.PI_AGENT_NOTIFY_DIR || "/tmp/pi-agent-notify";
// 身份感知：subagent 用独立的通知子目录（防多会话抢文件）
// 主 session 无 PI_SUBAGENT_TASK_ID → 用基础目录；subagent → /tmp/pi-agent-notify/{taskId}/
const SUBAGENT_TASK_ID = process.env.PI_SUBAGENT_TASK_ID || "";
const TRIGGER_DIR = SUBAGENT_TASK_ID
	? `${TRIGGER_DIR_BASE}/${SUBAGENT_TASK_ID}`
	: TRIGGER_DIR_BASE;
const SCAN_INTERVAL_MS = 2000;
const BATCH_WINDOW_MS = 15000; // 合并缓冲窗口：攒 15s 一次注入
const DEDUP_COOLDOWN_MS = 30 * 60 * 1000; // 去重冷却 30 分钟

interface NotifyEvent {
	level: string;
	source: string;
	message: string;
	itemId?: string;
}

let currentCtx: any = null; // 最近一次 session_start 的 ctx
let pendingEvents: NotifyEvent[] = []; // 合并缓冲
let batchTimer: ReturnType<typeof setTimeout> | null = null;
let dedupCache = new Map<string, number>(); // key -> 上次注入时间戳

export default function (pi: ExtensionAPI) {
	let scanTimer: ReturnType<typeof setInterval> | null = null;

	// 启动监听（session 开始时）
	pi.on("session_start", async (_event, ctx) => {
		try {
			fs.mkdirSync(TRIGGER_DIR, { recursive: true });
		} catch (e) {
			/* ignore */
		}

		// fs.watch 作为第一通道
		try {
			const watcher = fs.watch(TRIGGER_DIR, () => {
				scheduleFlush().catch((e) => piLog(`watch err ${e}`));
			});
			watcher.on("error", () => {
				/* 目录被删等情况，定时轮询兜底 */
			});
			pi.on("session_shutdown", () => {
				try {
					watcher.close();
				} catch (e) {
					/* ignore */
				}
			});
		} catch (e) {
			piLog(`watch setup err ${e}`);
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
			ctx.ui.notify(`agent-notify v2: watching ${TRIGGER_DIR}`, "info");
		}
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

	// 读触发目录新文件，加入合并缓冲（原子 rename，避免并发）
	async function collectFiles() {
		let files: string[];
		try {
			files = fs.readdirSync(TRIGGER_DIR).filter((f) => f.endsWith(".json"));
		} catch (e) {
			return;
		}
		if (files.length === 0) return;
		for (const f of files) {
			const full = path.join(TRIGGER_DIR, f);
			const proc = full + ".processing";
			try {
				fs.renameSync(full, proc);
				const raw = fs.readFileSync(proc, "utf-8");
				fs.rmSync(proc, { force: true });
				const obj = JSON.parse(raw);
				if (obj && (obj.message || obj.text)) {
					pendingEvents.push({
						level: obj.level || "yellow",
						source: obj.source || "script",
						message: obj.message || obj.text,
						itemId: obj.itemId || undefined,
					});
				}
			} catch (e) {
				try {
					fs.rmSync(proc, { force: true });
				} catch (e2) {
					/* ignore */
				}
			}
		}
	}

	// 去重 key：source + itemId + 消息前 40 字符（同类事件视为重复）
	function dedupKey(e: NotifyEvent): string {
		const base = `${e.source}|${e.itemId || "-"}|${e.message.slice(0, 40)}`;
		return base;
	}

	// 合并缓冲到期：去重 → 合成一条 → 注入
	async function flushBatch() {
		if (pendingEvents.length === 0) return;
		const batch = pendingEvents;
		pendingEvents = [];

		// 去重：同 key 冷却期内跳过
		const now = Date.now();
		const unique: NotifyEvent[] = [];
		for (const e of batch) {
			const key = dedupKey(e);
			const last = dedupCache.get(key) || 0;
			if (now - last < DEDUP_COOLDOWN_MS) {
				piLog(`dedup skip ${key.slice(0, 80)}`);
				continue;
			}
			// red 永不合并去重（每次都报）；green/yellow 才去重
			if (e.level !== "red") {
				dedupCache.set(key, now);
			}
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

		try {
			if (currentCtx?.isIdle && currentCtx.isIdle()) {
				pi.sendUserMessage(body);
				piLog(`injected(${unique.length}条合并): ${text.slice(0, 120)}`);
			} else {
				// busy：只排一条 followUp（合并后），不逐条排队
				pi.sendUserMessage(body, { deliverAs: "followUp" });
				piLog(`injected(followUp,${unique.length}条合并): ${text.slice(0, 120)}`);
			}
		} catch (e) {
			// 注入失败：把事件放回缓冲尾，下轮再试（但防无限重试，最多保留）
			piLog(`inject failed: ${e}`);
		}

		// red 事件额外 TUI 高亮
		if (unique.some((e) => e.level === "red")) {
			try {
				if (currentCtx?.ui) currentCtx.ui.notify("🔴 脚本事件需用户拍板（见对话）", "error");
			} catch (e) {
				piLog(`ui notify err ${e}`);
			}
		}
	}
}
