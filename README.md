# pi-agent-notify

Fail-closed external event → exact pi session/worker delivery. Watchers publish a versioned event; the extension validates target identity, run nonce, item ownership, expiry, sequence and dedup state before injecting a user/follow-up message.

## Properties

- Domain-neutral `itemKey`: examples `mission:hkg_super_v13`, `alpha:KPO237EN`, `ci:run-42`, and legacy six-digit item IDs.
- Exact routing: `targetKind + targetId + runId + nonce`; ambiguous same-cwd main sessions are rejected.
- Busy-worker delivery uses a short-lived `.receiver-identity.json`; durable watcher handoffs additionally require `arm_notification_wait` and a live producer marker.
- Protocol-v1 inbox events remain readable during migration, while all new senders emit version-2 envelopes.
- Monotonic per-stream sequence, event-ID + semantic dedup, TTL/future-skew rejection and quarantine.
- Concurrency-safe sender state and durable outbox via lock + fsync + atomic rename.
- Worker ownership registry is supplied by `pi-subagent-durable`; its writes are also locked/atomic and lease-token checked.
- Notifications are wake-up hints, never authoritative proof. Resumed agents must re-read the source system.

## Install

```bash
pi install ./pi-agent-notify
# settings.json may also load extensions/index.ts directly.
```

Node 20+ and pi 0.80+ are required.

## Sender CLI

Always prefer the companion sender instead of manually writing inbox files:

```bash
python3 scripts/notify_agent.py send \
  "QuantNight mission reached terminal item states" \
  --item mission:hkg_super_v13 \
  --to task-mtz... \
  --state quantnight.mission.terminal \
  --source quantnight-watcher \
  --level yellow
```

A watcher can create a domain event and ask the sender to normalize/route it:

```bash
python3 scripts/notify_agent.py send --event-file /path/to/event.json
# watcher that must wake an agent_end-held worker:
python3 scripts/notify_agent.py send --event-file /path/to/event.json --require-lease
```

Input event shape:

```json
{
  "schemaVersion": 1,
  "eventId": "quantnight:hkg-v13:terminal:abc123",
  "producer": "quantnight-watcher",
  "occurredAt": "2026-09-13T12:30:00Z",
  "expiresAt": "2026-09-13T13:30:00Z",
  "itemKey": "mission:hkg_super_v13",
  "target": {"taskId": "task-mtz..."},
  "eventType": "quantnight.mission.terminal",
  "payload": {"terminal": true}
}
```

The sender resolves the active worker registration and current receiver identity. For a durable watcher handoff, add `--require-lease`; it then waits for and binds to the exact lease. It allocates a monotonic sequence and atomically writes the normalized v2 envelope to `~/.pi/agent/agent-notify/outbox/`. Re-sending the same `eventId` is idempotent (`alreadySent=true`). Expired, unsafe, unowned or ambiguous events fail closed.

Legacy invocation without the `send` word remains accepted:

```bash
python3 scripts/notify_agent.py "review ready" --item 168373 --to task-...
```

## Durable worker protocol

The delegated task text must declare ownership explicitly:

```text
itemKey: mission:hkg_super_v13
```

Then:

1. Start exactly one bounded external watcher.
2. Persist a repository-local JSON marker containing:
   ```json
   {"pid": 12345, "itemKey": "mission:hkg_super_v13", "notifyTo": "task-mtz..."}
   ```
3. Verify the PID is live.
4. Call `arm_notification_wait` with `itemKey`, precise `wakeCondition`, bounded `leaseSeconds`, `producerPid`, repository-relative `producerMarkerPath`, and optional checkpoint path.
5. Finish the current turn. Do not keep a foreground tool blocked by sleep/polling.
6. The matching directed event consumes the lease and resumes the same session. On resume, query authoritative state before acting.
7. Normally the worker explicitly arms its next wait. As a bounded safety net, if the released lease's original producer is still alive and the turn ends without re-arming, the extension restores that same lease **once**, never beyond its original expiry. Terminal/dead-producer waits are not restored.

The lease lives at `${PI_AGENT_NOTIFY_DIR:-/tmp/pi-agent-notify}/<taskId>/.notification-wait-lease`. It is bounded to 30 seconds–24 hours. Missing/dead producer, item mismatch, cwd escape, stale ownership heartbeat or wrong nonce causes refusal.

## Main-session routing

For a main session, pass an exact `target.sessionId` in the event or provide `cwd`. Cwd routing succeeds only when exactly one live registered main session matches. Multiple same-cwd sessions are intentionally rejected; there is no “pick newest” fallback.

## Envelope v2

```json
{
  "version": 2,
  "eventId": "...",
  "runId": "run-...",
  "itemKey": "mission:...",
  "targetKind": "worker",
  "targetId": "task-...",
  "state": "quantnight.mission.terminal",
  "sequence": 4,
  "occurredAt": "...",
  "expiresAt": "...",
  "nonce": "...",
  "level": "yellow",
  "source": "quantnight-watcher",
  "message": "...",
  "actionable": true,
  "terminal": true
}
```

`green` is a passive memo. `yellow` is agent-actionable. `red` indicates a user decision/attention request; it is still deduped.

## State and cleanup

- Inbox root: `${PI_AGENT_NOTIFY_DIR:-/tmp/pi-agent-notify}`
- Durable outbox/dedup/log: `${PI_AGENT_NOTIFY_STATE_DIR:-~/.pi/agent/agent-notify}`
- Active workers: `<inbox>/.active-workers.json`
- Main registrations: `<inbox>/.main-sessions/*.json`
- Invalid envelopes: durable `quarantine/` with reason files

Stale temp/processing files, dead main registrations and orphan worker inboxes are cleaned conservatively. Live workers are never removed by this cleanup.

## Protocol-v1 migration compatibility

The extension can ingest old `{level, source, message, itemId, ts}` files from an exact worker/main inbox. Worker ownership is still enforced and missing v2 identity is bound to that live receiver only. Main registrations expose a legacy numeric `heartbeat` alias, and a main session drains the legacy `main-pending/<cwdHash>` directory. This compatibility is an ingestion bridge; new code should always use the generic sender and v2 envelope.

## Development

```bash
npm run check
```

Tests cover generic item keys, protocol-v1 worker/main ingestion, legacy pending routing, busy-worker receiver identity, stale lease cleanup, ownership/path refusal, exact nonce routing, one-time auto-rearm, replay/expiry rejection, sender idempotence and concurrent sequence allocation.
