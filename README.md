# pi-agent-notify

**English** | [简体中文](README.zh-CN.md) | [Changelog / 更新记录](CHANGELOG.md)

Fail-closed external event → exact Pi session/worker delivery. Watchers publish a versioned event; the extension validates target identity, run nonce, item ownership, expiry, sequence and dedup state before injecting a user/follow-up message.

Notifications are **wake-up hints, not authoritative proof**. After waking, re-query the source system. Delivery is **at-least-once**, not exactly-once execution: make external effects idempotent by business/event identity.

## Features

- Domain-neutral `itemKey`, such as `ci:run-42` or `task:example-42`; legacy six-digit item IDs remain supported.
- Exact routing by `targetKind + targetId + runId + nonce`; ambiguous same-cwd main sessions are rejected.
- `notify_subagent` steers live durable workers without restarting them. An armed watcher handoff additionally uses `arm_notification_wait` and a live producer marker.
- Receiver-owned worker registrations survive a prepared parent exit without restarting workers; exact task/PID/cwd/key-set/freshness checks remain mandatory.
- Persistent main run identity and a POSIX exclusive controller guardian allow recovery of offline events for the same canonical session.
- Durable inflight journals and **session-disk receipt ACKs**, rather than sender/queue acceptance, govern delivery acknowledgment and replay.
- Per-stream monotonic sequence, event-ID and semantic dedup, TTL/future-skew rejection and invalid-envelope quarantine.
- Concurrency-safe sender state and outbox publication via `flock`, fsync and atomic rename.
- Protocol-v1 inbox ingestion remains available during migration; new senders emit v2 envelopes.

## Installation and companion requirements

```bash
pi install git:github.com/roshameow/pi-agent-notify
```

For a local checkout, run `pi install .` in this repository, or configure Pi to load `extensions/index.ts` directly. The default script locations assume the packaged layout: keep `scripts/notify_agent.py` and `scripts/session_controller.py` beside the extension's parent directory. Both are shipped in the package.

Requires Node.js **20+**, Python **3.9+**, a Pi runtime providing the `@earendil-works` SDK peer packages, and a POSIX host (macOS/Linux). Python uses `fcntl`; native Windows is not supported. Compatibility with every Pi/provider version has not been verified.

Directed durable workers, ownership registration and parent-upgrade recovery additionally require the public [pi-subagent-durable](https://github.com/roshameow/pi-subagent-durable) package:

```bash
pi install git:github.com/roshameow/pi-subagent-durable
```

Its current documented requirements are Pi **0.80+** and Node.js **22+**; use the stricter Node requirement when installing both. The worker-preserving upgrade path requires **asynchronous RMUX workers** and a matching durable revision implementing receiver ownership, `/agent:prepare-upgrade`, the external bootstrap/keeper, and exact-parent-session recovery. Older releases with only basic delegation/reload do not provide this handoff. The package version number alone is not a capability check; verify the installed source and successful preparation report. Main-only notification delivery does not require durable, RMUX, a private repository or personal configuration.

## Native MCP compatibility

Notification routing is independent of the MCP client. Native Pi MCP/codemode and an older adapter can both supply watcher producer handles; this package does not require `pi-mcp-adapter`, create MCP connections, or start a service per worker. Pass the exact task/item/generation and producer marker to the same wait protocol below. A transport reconnect or `/reload` is not proof that an external job completed.

Use the existing `notify_subagent` and `arm_notification_wait` tools directly, or discover their current codemode identifiers. Do not copy adapter-specific `mcpScript` result envelopes into the native tool path. These notes describe integration only; they do not add official Pi Durable, alter leases, or change the at-least-once receipt contract.

## Main-agent tool: steering vs. watcher handoff

Main sessions register `notify_subagent`:

```json
{
  "taskId": "task-example",
  "message": "Re-read the updated instructions and continue"
}
```

With current durable, every worker automatically owns `worker:<taskId>`. If `itemKey` is omitted, the tool uses that control key. Specify the worker-owned domain key for domain messages:

```json
{
  "taskId": "task-example",
  "message": "CI changed; re-query its authoritative status",
  "itemKey": "ci:run-42",
  "requireLease": true
}
```

- Ordinary steering: `requireLease=false` (default). The sender uses a matching wait lease if present, otherwise the live receiver identity; a busy worker receives a follow-up.
- Durable watcher handoff: `requireLease=true`. The sender requires and binds to the exact armed lease; it does not fall back to an unarmed receiver.
- An armed worker accepts **only its lease's domain `itemKey`**. Even ordinary steering must use that key while armed; `worker:<taskId>` is not a bypass.
- Optional `state` defaults to `main.followup`; `level` is `yellow` (default) or `red`. A successful tool result means **published**, not received or completed.

Use `notify_subagent` for instructions to a live worker. Use `subagent_reload` only to resume a finished/paused worker or when a process restart is needed to load changed tools, extensions or MCP runtime. A worker reload is not a worker-preserving upgrade.

## Sender CLI

External scripts should use the companion sender instead of manually writing inbox files. Examples below assume the package checkout as cwd; otherwise use its installed script path.

```bash
python3 scripts/notify_agent.py send \
  "CI run completed; re-read its authoritative status" \
  --item ci:run-42 \
  --to task-example \
  --state ci.run.completed \
  --source ci-watcher \
  --event-id ci-run-42-completed-1 \
  --level yellow --ttl 300

# Normalize a domain event and route it to the current receiver:
python3 scripts/notify_agent.py send --event-file /path/to/event.json

# A watcher that must wake an agent_end-held worker:
python3 scripts/notify_agent.py send --event-file /path/to/event.json --require-lease
```

Input event shape (generate current timestamps; these dates are illustrative):

```json
{
  "schemaVersion": 1,
  "eventId": "ci-run-42-completed-1",
  "producer": "ci-watcher",
  "occurredAt": "2026-10-02T12:30:00Z",
  "expiresAt": "2026-10-02T12:35:00Z",
  "itemKey": "ci:run-42",
  "target": {"taskId": "task-example"},
  "eventType": "ci.run.completed",
  "terminal": true,
  "payload": {"status": "completed"}
}
```

The input's `schemaVersion: 1` is a domain-event shape, not the inbox protocol version. The sender resolves registration/receiver identity, allocates a monotonic sequence and atomically publishes a normalized **v2** envelope in the durable outbox. Top-level `terminal: true` (or an event type ending in `.terminal`) sets the terminal hint; a flag only inside `payload` does not. Hints are not business-state validation.

CLI options:

| Option | Purpose / default |
| --- | --- |
| `--item KEY`, `--to TASK` | Explicit worker-owned item and task ID |
| `--session-id ID` | Exact main session, including eligible offline sessions |
| `--cwd DIR` | Main routing by cwd; must resolve exactly one live session |
| `--event-file FILE` | Domain JSON input; `target.taskId` / `target.sessionId` select the receiver |
| `--event-id ID` | Stable identity for retries; otherwise derived from input |
| `--state TYPE`, `--source NAME` | Defaults: `actionable`, `script` |
| `--level green\|yellow\|red` | Default: `yellow` |
| `--ttl SECONDS` | Default: `3600`; event-file `expiresAt` takes precedence |
| `--require-lease` | Require an armed worker wait instead of receiver fallback |
| `--lease-wait-seconds N` | Wait for lease creation, default `60`; `0` refuses immediately if absent |

For main notifications without an item, the sender derives a cwd-scoped key; workers require an explicit owned key. Item keys are 1–200 characters matching `[A-Za-z0-9][A-Za-z0-9._:@+/-]*`, excluding `.`/`..` and `//`. Target/event IDs use `[A-Za-z0-9][A-Za-z0-9._:-]*` (no slash).

Output is JSON: success includes `ok`, `alreadySent`, `path` and `event`; refusal writes `ok=false` plus error details to stderr and exits nonzero. Reusing an already-published `eventId` is idempotent within the retained sender history (14 days): `alreadySent=true` does **not** replace/rebind the original event or prove delivery. Supply a stable event ID when retrying; newly generated timestamps can change an automatically derived ID. Expired, unsafe, unowned or ambiguous inputs fail closed.

Legacy invocation without `send` remains accepted:

```bash
python3 scripts/notify_agent.py "CI changed" --item ci:run-42 --to task-example
```

## Durable worker wait protocol

Declare domain ownership explicitly in the delegated task:

```text
itemKey: ci:run-42
```

Then:

1. Start **one bounded external watcher**, targeting this worker's current `PI_SUBAGENT_TASK_ID`; never reuse a previous worker's task ID.
2. Persist a repository-local JSON producer marker, for example:
   ```json
   {"pid": 12345, "itemKey": "ci:run-42", "notifyTo": "task-example"}
   ```
3. Verify the producer PID is live and marker PID/item/target match.
4. Call `arm_notification_wait` with `itemKey`, `reason`, a precise `wakeCondition`, `producerPid`, `producerMarkerPath`, bounded `leaseSeconds`, and optional `checkpointPath`. Marker/checkpoint paths must stay under worker cwd; repository-relative paths are recommended. The extension records `wakeCondition` but does not query the external system for you.
5. Finish the current turn. Do not block a foreground tool with sleep/polling. The lease holds this process at `agent_end`.
6. The watcher sends with `--require-lease`. A matching task/item/runId/nonce event consumes the lease and resumes the **same session**. On resume, re-query authoritative state before acting.
7. Normally explicitly arm the next wait. As a bounded safety net, if the original producer is still alive and the turn ends without re-arming, the extension restores that same released lease **once**, never beyond its original expiry. Dead-producer waits are not restored. A terminal hint alone does not suppress this safety net: complete/stop the producer after confirming authoritative terminal state.

Lease location: `${PI_AGENT_NOTIFY_DIR:-/tmp/pi-agent-notify}/<taskId>/.notification-wait-lease`. Duration is **30 seconds–24 hours**, default **6 hours**. Missing/dead producer, marker/item mismatch, cwd escape, stale ownership/receiver heartbeat or wrong nonce causes refusal. Expiry queues a follow-up asking the worker to re-check state/checkpoint.

Releasing the `agent_end` hold lets a queued wake-up run; it is **not an ACK**. Worker shutdown/**reload still releases and deletes the lease**. This protocol preserves existing workers across a prepared parent exit, not arbitrary worker-process restarts; it does not guarantee automatic worker restart, job recovery or host survival.

## Receiver ownership and exact identity

Durable supplies the locked/atomic, ownership-token-checked `.active-workers.json` registry. A receiver-owned record includes:

```json
{
  "ownershipMode": "receiver",
  "taskId": "task-example",
  "workerPid": 12345,
  "ownerPid": 12345,
  "parentSessionId": "parent-session-example",
  "cwd": "/path/to/project",
  "receiverIdentityPath": "/path/to/notify/task-example/.receiver-identity.json",
  "itemKeys": ["ci:run-42", "worker:task-example"]
}
```

These are illustrative paths, not a file to construct manually. Notify validates:

- Exact task/target and expected receiver-identity path, not a symlink substitute.
- Live `workerPid`, `ownerPid == workerPid` and receiver `pid == workerPid`.
- Matching resolved cwd and **identical item-key sets**, plus ownership of the requested key.
- Valid runId/nonce and fresh receiver heartbeat (no older than **90 seconds**; notify rejects timestamps more than **5 minutes** ahead).
- When binding to a lease, matching receiver/lease runId, nonce, PID and cwd.

Receiver-owned liveness does **not** depend on a dead parent's heartbeat. Legacy parent-owned registrations still require a live parent owner and an ownership heartbeat no older than **120 seconds**. Unknown ownership modes are refused rather than downgraded. Durable preparation may impose stricter freshness checks; follow its refusal/report rather than weakening identity checks.

## Main-session routing and offline recovery

Pass exact `target.sessionId` / `--session-id`, or route by cwd when **exactly one live registered main session** matches. Multiple same-cwd sessions are intentionally rejected; there is no “pick newest” fallback.

A persistent main stores identity at `main-identities/<sha256(sessionId)[0:24]>.json` under the durable state root. Resuming **the same canonical session file, session ID and cwd** restores runId/nonce. The first in-process migration `/reload`, if used, preserves the already-live `globalThis` identity; it is **not required for the external durable bootstrap** below. New/fork session IDs never inherit another session's identity. A same-ID mirror with a different canonical path/cwd, malformed state or another active controller fails closed.

A Python guardian holds `<identity>.controller` with exclusive POSIX `flock` until Pi closes its private pipe, including abrupt exit. Exclusion disables notification ingestion and requests Pi shutdown so the excluded process does not remain a second transcript writer. Stale PID metadata is not the lock; **never delete a controller lock file to bypass contention**. Keep the controller script installed alongside the sender and keep notify/state roots unchanged.

Only exact IDs can route offline, and only with a persisted identity whose canonical session header validates:

```bash
python3 scripts/notify_agent.py send "Re-read CI status" \
  --item ci:run-42 --session-id EXACT_SESSION_ID \
  --event-id ci-run-42-offline-1 --ttl 300
```

Offline routing is available for **24 hours after the last main heartbeat**. An offline event must expire within **one hour of sending** and within that recovery window; choose a shorter TTL near the boundary. The 24-hour window is not a 24-hour event lifetime: expired events are not replayed. Offline cwd routing, unknown/ephemeral sessions and task mirror files are refused. Re-sending a published event ID does not refresh its TTL or rebind its nonce.

## Upgrade the parent without restarting workers

Requires the matching durable capabilities described under installation. Only **async RMUX** workers are eligible; plain-spawn, synchronous and chain tasks are not covered. Keep RMUX/host/session storage alive and preserve cwd plus any `PI_CODING_AGENT_DIR`, `PI_AGENT_NOTIFY_DIR` and `PI_AGENT_NOTIFY_STATE_DIR` overrides.

### First migration: the old parent has no prepare command

**Do not `/reload` the root/parent just to prepare.** With updated notify/durable sources available, use `/session` in the old parent to obtain its exact canonical JSONL path. Keep that parent idle, with no pending conversation work, and stop dispatching new tasks. From another terminal:

```bash
node /path/to/pi-subagent-durable/scripts/prepare-upgrade.mjs \
  --session '/absolute/canonical-parent.jsonl'
```

The bootstrap runs without loading Pi or signaling/reloading business workers. It must preserve the old main's registered runId/nonce **and** worker ownership. Require successful `Prepared …` and `Preserved exact main notification identity …` reports, the expected task count and receiver-keeper PID/deadline. On refusal, **do not exit yet**. After successful preparation, exit **only that parent immediately**; old code cannot automatically freeze later dispatch.

### Subsequent prepared upgrades

1. In the updated **idle parent**, run `/agent:prepare-upgrade`. It validates canonical parent/child sessions, RMUX workers and receiver identity, migrates ownership, persists a handoff, freezes dispatch and prints the exact restart command. Do not exit on refusal.
2. Exit only the parent Pi. Do not use `subagent_stop`, `subagent_reload`, `/agent:stop-all`, or stop the workers' RMUX workspace.
3. Prefer durable's side-by-side launcher; inspect the dry-run, then run without `--dry-run`:
   ```bash
   /path/to/pi-subagent-durable/scripts/pi-safe-upgrade.sh \
     --version EXACT_VERSION --session '/absolute/canonical-parent.jsonl' --dry-run
   ```
   It checks preparation and installs the exact Pi version separately, without replacing the global installation used by live workers. Resume the printed **exact parent session**, not `--continue`, newest-session guessing or a fork. Also avoid overwriting extension files still used by old workers.
4. Durable cold-start recovery restores monitoring for that parent; `/agent:recover` explicitly reconciles if needed. Verify its recovery report and authoritative task state. Existing workers/watchers keep their task IDs, receiver PIDs and lease identities; they are **not restarted**.

### Old-worker migration boundary

Old notify code may still check the parent registry heartbeat and clear receiver item keys after **120 seconds** offline. Matching durable preparation starts a bounded detached **receiver-heartbeat keeper before parent exit**. It mirrors only fresh verified receiver timestamps, does not fabricate liveness or send events, and is bounded to at most **24 hours**. Require its successful handoff/PID/deadline report; PID migration alone is insufficient. Resume the parent within the reported bound.

Migration does not hot-patch old worker code: old queue-time dedup/ACK behavior remains until the worker naturally starts with the new extension. Session-disk ACK guarantees below apply only to receivers running the updated code. Outages beyond the keeper's bound, machine/RMUX restart, missing `/tmp` leases, dead producers and worker-process reloads are not covered. The keeper bound and main's offline-routing window are separate limits.

## Session-disk receipt ACK and replay

Each actionable user message carries a stable `[[agent-notify:<eventId>:<identity digest>]]` marker. Publication precedes reception; queue acceptance precedes ACK.

The extension journals inflight events **before** calling Pi, suppresses repeated queueing on scans and same-process reload, and retains outbox until the matching user-message bytes exist in the **exact session JSONL** and are fsynced. `message_end` only schedules a disk check: Pi may emit it **before** persisting the message. Passive green entries likewise need a persisted custom-entry receipt.

On cold start, a persisted receipt completes ACK even if the old process crashed before saving dedup. Valid, unexpired, unacknowledged journal/outbox events can replay **at least once**. In-memory first-turn SessionManager entries are not durable receipts; ephemeral sessions cannot promise recovery. Ambiguous async queue failures can remain inflight until a controller restart. Receipt ACK proves transcript persistence, **not agent execution or business completion**. Query authoritative state and make side effects idempotent.

## Envelope v2 and levels

```json
{
  "version": 2,
  "eventId": "ci-run-42-completed-1",
  "runId": "run-example",
  "itemKey": "ci:run-42",
  "targetKind": "worker",
  "targetId": "task-example",
  "state": "ci.run.completed",
  "sequence": 4,
  "occurredAt": "2026-10-02T12:30:00Z",
  "expiresAt": "2026-10-02T12:35:00Z",
  "nonce": "0123456789abcdef0123456789abcdef",
  "level": "yellow",
  "source": "ci-watcher",
  "message": "CI changed; re-query status",
  "actionable": true,
  "terminal": true
}
```

Use the sender to obtain real identity fields; do not copy example run IDs/nonces.

- `green`: passive memo/custom entry, normally no agent turn. **A directed green event matching an armed lease still wakes the worker.**
- `yellow`: agent-actionable; busy agents receive follow-ups.
- `red`: user attention/decision request, with UI alert when available; still deduped.

Actionable/terminal events and matching lease wake-ups flush promptly; ordinary non-immediate events use the batch window. Event-ID history is retained for 14 days; semantic duplicates use a configurable cooldown (default 30 minutes), so similar messages may be coalesced even with distinct IDs. Sequence and timestamp checks reject stale/reordered stream events. Notifications are not an exhaustive audit log or an exactly-once work queue.

## State directories and configuration

| Environment variable | Default / purpose |
| --- | --- |
| `PI_AGENT_NOTIFY_DIR` | `/tmp/pi-agent-notify`: inboxes, live registrations and wait leases |
| `PI_AGENT_NOTIFY_STATE_DIR` | `~/.pi/agent/agent-notify`: durable outbox, identity, dedup, inflight and quarantine |
| `PI_AGENT_NOTIFY_LOG` | `<state>/events.log`: receiver event log |
| `PI_AGENT_NOTIFY_SENDER` | Packaged `scripts/notify_agent.py`: main tool sender override |
| `PI_AGENT_NOTIFY_CONTROLLER` | `session_controller.py` alongside the selected sender: guardian override |
| `PI_AGENT_NOTIFY_SCAN_MS` | `2000`: scan interval, minimum `50` ms |
| `PI_AGENT_NOTIFY_BATCH_MS` | `15000`: batch window, minimum `0` ms |
| `PI_AGENT_NOTIFY_DEDUP_MS` | `1800000`: semantic cooldown, minimum `1000` ms |
| `PI_SUBAGENT_TASK_ID` | Set by durable for worker routing; unset for main sessions |

Keep common inbox/state overrides across sender, parent and workers; export them **before** starting those processes. `PI_CODING_AGENT_DIR` is durable/Pi configuration, not a replacement for notify's independent state-root override.

Important paths:

- `<inbox>/.active-workers.json`: durable-owned worker registry.
- `<inbox>/<taskId>/`: worker inbox, `.receiver-identity.json`, `.notification-wait-lease` and sender `.last-event` marker.
- `<inbox>/.main-sessions/*.json` and `<inbox>/main/<sessionId>/`: main registration/inbox.
- `<inbox>/main-pending/<cwdHash>/`: legacy main pending bridge.
- `<state>/main-identities/`: persistent main identities and controller lock files.
- `<state>/outbox/`, `sender-state.json`, `sender-state.lock`: durable sender publication/sequence history.
- `<state>/inflight/` and `dedup/`: recoverable pending/inflight events and receipt-acknowledged dedup state.
- `<state>/quarantine/`: invalid/mismatched envelopes with reason files.

Private directories/files are created with restrictive permissions. Stale temporary/processing files, dead main registrations and orphan worker inboxes are cleaned conservatively; registered workers are not removed by orphan cleanup. Bookkeeping dotfiles are **never** collected as inbound envelopes. Do not delete state/lock/lease files to force recovery; inspect logs and authoritative state first.

## Protocol-v1 migration compatibility

The extension still ingests old `{level, source, message, itemId, ts}` files in worker/main inboxes. Worker ownership remains enforced; missing v2 identity binds only to the live receiver. Main registrations expose a legacy numeric `heartbeat` alias and main sessions drain `main-pending/<cwdHash>`.

This is an **ingestion bridge**, not offline exact-identity safety for old cwd-based senders. New code should always use the generic sender and v2 envelopes.

## Development and verification boundaries

From a source checkout (tests/dev dependencies are not shipped in the runtime package):

```bash
npm ci
npm test                 # same suite as npm run check
npm pack --dry-run       # inspect the actual published file allowlist
git diff --check
```

GitHub Actions CI (`.github/workflows/ci.yml`) is configured to run `npm ci` and this same `npm test` suite on `ubuntu-latest` with Node.js **22** and Python **3.12**, for pushes to `main` and pull requests. This is the configured CI coverage, not a broader runtime compatibility matrix or evidence of a completed live upgrade.

Tests cover generic item keys, v1 worker/main ingestion and legacy pending routing, busy-worker identity fallback, stale lease cleanup, ownership/path refusal, exact nonce routing, one-time auto-rearm (including green wakes), replay/expiry rejection, steering-tool defaults, bookkeeping dotfile exclusion, sender idempotence and concurrent sequence allocation.

Real OS subprocess tests with a controllable Pi API/JSONL fixture exercise main SIGKILL/cold restart, offline exact sender/TTL, same-session controller contention and shutdown request, first-reload identity preservation, no per-scan/same-process-reload duplicate queueing, crashes before/after persisted receipts, wrong-nonce quarantine, receiver-owned delivery after parent exit and fork isolation. Sender tests also refuse receiver PID/task/cwd/key-set/freshness mismatches and unknown ownership modes.

These are **fixture/regression tests**, not a live model/RMUX parent-upgrade test, arbitrary worker-restart recovery, a host-failure guarantee or a Pi/provider compatibility matrix. Validate the matching durable integration before production upgrades. Reproducible checks: inspect `npm pack --dry-run` rather than trusting ignore rules; queue acceptance/`message_end` alone is not a disk ACK; a 24-hour offline identity does not extend a one-hour event TTL.

## Public documentation and private material

Keep personal documentation in ignored `docs/private/` or `docs/local/`; keep actual configuration/state outside the public source tree (or in ignored local paths). The package uses an explicit file allowlist including both READMEs and the changelog. Ignore rules do not remove files already tracked in Git or older commits. Public examples should use generic paths/items, never private deployment or business material.

[MIT License](LICENSE)
