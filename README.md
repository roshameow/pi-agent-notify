# pi-agent-notify

Script → agent direct notification channel for [pi](https://www.npmjs.com/package/@earendil-works/pi-coding-agent). External scripts (watchers, pollers, cron) write JSON events to a trigger dir; the extension batches, dedups and injects them into the active agent session as **user messages**, so the agent can act on them autonomously instead of waiting for the user to relay.

## Why this exists

Long-running automation (e.g. TalentsAI 出题流水线) has many background watcher scripts that detect state changes (evaluation done, recording disconnected, review returned). Previously they could only alert the user (voice/sound) or write log files — the agent had to be manually told. This extension closes the loop: **scripts talk directly to the agent session**.

## Features

| | |
|---|---|
| **Batch buffer** | Events collected over a 15s window are merged into one injection (no per-event spam) |
| **Dedup cache** | Same event key (source+itemId+message prefix) injected at most once per 30min — watchers that poll every 60s don't spam |
| **Level semantics** | `green` = progress memo · `yellow` = agent-handleable event (default) · `red` = needs user decision (extra TUI highlight, never deduped) |
| **Identity-aware** | Main session listens on base dir; subagents with `PI_SUBAGENT_TASK_ID` listen on `/tmp/pi-agent-notify/{taskId}/` for directed wake-ups |
| **Busy-safe** | When the agent is busy, injects one merged `followUp` instead of queueing per-event |

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
python3 scripts/notify_agent.py "evaluation done for item 167161" --level yellow --source evaluation --itemId 167161
python3 scripts/notify_agent.py "recording disconnected, please restore" --level red --source recwatch --itemId 174366

# directed to a specific subagent (方案C 闭环): writes to /tmp/pi-agent-notify/task-<id>/
python3 scripts/notify_agent.py "v6 models ready, continue grading" --to mtj6zcy5-x0k9 --level yellow
```

Event file shape (written by scripts):

```json
{ "level": "yellow", "source": "reviewwatch", "message": "...", "itemId": "167161", "ts": 1788298000123 }
```

- Trigger dir: `/tmp/pi-agent-notify/` (override with `PI_AGENT_NOTIFY_DIR` env)
- Level `red` events never dedup and trigger `ctx.ui.notify` highlight
- Consumed event files are removed after processing

## Directed subagent wake-up (方案 C 闭环)

Subagent processes spawned with `PI_SUBAGENT_TASK_ID` (e.g. by `pi-subagent-durable`) listen on their **own subdirectory**. External scripts can send a directed notification:

1. `notify_agent.py --to <taskId> "msg"` → writes `/tmp/pi-agent-notify/task-<taskId>/evt-*.json` (auto-prefixes `task-`)
2. The subagent (if its extension instance is loaded) or the subagent itself (polling its dir via `scripts/notify_check.py`) reacts

> Note: subagent spawned pi processes may not load project extensions (mechanism limitation) — see no reload; the worker-side pattern is to poll its own dir with `notify_check.py` while waiting.

## Logging

All injections are logged to `/tmp/agent-notify.log` (dedup skips, inject results).

## Requirements

- pi `>= 0.80`
- Node `>= 20`