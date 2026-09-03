# pi-agent-notify

Script → agent direct notification channel for [pi](https://www.npmjs.com/package/@earendil-works/pi-coding-agent). External scripts (watchers, pollers, cron) write JSON events to a trigger dir; the extension batches, dedups and injects them into the active agent session as **user messages**, so the agent can act on them autonomously instead of waiting for the user to relay.

## Why this exists

Long-running automation (e.g. TalentsAI 出题流水线) has many background watcher scripts that detect state changes (evaluation done, recording disconnected, review returned). Previously they could only alert the user (voice/sound) or write log files — the agent had to be manually told. This extension closes the loop: **scripts talk directly to the agent session**.

## Features

| | |
|---|---|
| **Batch buffer** | Events collected over a 15s window are merged into one injection (no per-event spam) |
| **Dedup cache** | green/yellow use source+itemId+message prefix; red ignores source for cross-watcher dedup. Same state is injected at most once per 30min |
| **Level semantics** | `green` = progress memo · `yellow` = agent-handleable event (default) · `red` = needs user decision (TUI highlight + macOS voice for main-session alerts; still deduped to prevent repeated stale alerts) |
| **Identity-aware** | Each main session has an inbox under `/tmp/pi-agent-notify/main/{sessionId}/`; when no main session is live, events stay in `main-pending/{cwdHash}/`. Subagents listen on `/tmp/pi-agent-notify/{taskId}/` |
| **Busy-safe** | Globally at most one `followUp` is queued while busy; later events remain buffered and merge until the agent is idle |

## Install

```bash
# from a local checkout (this package)
pi install ./pi-agent-notify          # global
pi install -l ./pi-agent-notify       # project-local

# or from git
pi install git:github.com/roshameow/pi-agent-notify
```

## Usage (external scripts)

Write a JSON file to the trigger dir:

```bash
# companion script (recommended)
python3 scripts/notify_agent.py "evaluation done for item 167161" --level yellow --source evaluation --item 167161
python3 scripts/notify_agent.py "recording disconnected, please restore" --level red --source recwatch --item 174366

# directed to a specific subagent (方案C 闭环): writes to /tmp/pi-agent-notify/task-<id>/
python3 scripts/notify_agent.py "v6 models ready, continue grading" --to mtj6zcy5-x0k9 --level yellow
```

Event file shape (written by scripts):

```json
{ "level": "yellow", "source": "reviewwatch", "message": "...", "itemId": "167161", "ts": 1788298000123 }
```

- Trigger dir: `/tmp/pi-agent-notify/` (override with `PI_AGENT_NOTIFY_DIR` env)
- Level `red` events trigger `ctx.ui.notify` highlight and are deduped across watcher sources by item/message prefix for 30min
- Stale `recording_supervisor` red events are dropped after 3min, or when a fresh `docs/recording_health.json` already says the item is `running`
- Consumed event files are removed after processing

## Bidirectional communication / directed subagent wake-up

The channel is bidirectional, but each session has its own directory:

- Main session → worker: `notify_agent.py --to <taskId> "msg"` writes to `/tmp/pi-agent-notify/task-<taskId>/`.
- Worker → main session: omit `--to`; the event is routed by `cwd + itemId` to the matching main-session inbox. If no matching session is live, it is durably queued under `main-pending/{cwdHash}/` for the next matching session.
- Worker → itself: use `--to "$PI_SUBAGENT_TASK_ID"`; this does **not** appear in the main session.

A directed notification is consumed only while the target pi process is alive and has this extension loaded. `subagent_reload` is a control/resume operation, not a worker-to-main return channel. If a subagent process does not load project extensions, it must use the documented `notify_check.py` fallback; otherwise the extension consumes the event automatically.

## Logging

All injections are logged to `/tmp/agent-notify.log` (dedup skips, inject results).

## Requirements

- pi `>= 0.80`
- Node `>= 20`