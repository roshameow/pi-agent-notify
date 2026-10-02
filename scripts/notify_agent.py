#!/usr/bin/env python3
"""Fail-closed sender for pi-agent-notify version-2 event envelopes."""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

INBOX_ROOT = Path(os.environ.get("PI_AGENT_NOTIFY_DIR", "/tmp/pi-agent-notify")).resolve()
STATE_ROOT = Path(os.environ.get("PI_AGENT_NOTIFY_STATE_DIR", str(Path.home() / ".pi/agent/agent-notify"))).resolve()
OUTBOX = STATE_ROOT / "outbox"
SAFE_KEY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@+/-]{0,199}$")
SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$")


def now_ms() -> int: return int(time.time() * 1000)
def iso(value: float | None = None) -> str: return datetime.fromtimestamp(value or time.time(), timezone.utc).isoformat()
def alive(pid: Any) -> bool:
    try: os.kill(int(pid), 0); return int(pid) >= 2
    except (OSError, TypeError, ValueError): return False


def read_json(path: Path) -> dict[str, Any]:
    with path.open() as fh:
        value = json.load(fh)
    if not isinstance(value, dict): raise ValueError(f"expected JSON object: {path}")
    return value


def validate_key(value: Any, label: str, regex=SAFE_KEY) -> str:
    text = str(value or "")
    if not regex.fullmatch(text) or text in {".", ".."} or "//" in text:
        raise ValueError(f"unsafe {label}: {text!r}")
    return text


def registrations() -> dict[str, Any]:
    try: return read_json(INBOX_ROOT / ".active-workers.json").get("workers", {})
    except FileNotFoundError: return {}


def record_keys(record: dict[str, Any]) -> set[str]:
    return {str(x) for x in (record.get("itemKeys", []) + record.get("itemIds", []))}


def live_worker(task_id: str, item_key: str) -> dict[str, Any]:
    record = registrations().get(task_id)
    if not record or not alive(record.get("ownerPid", record.get("pid"))):
        raise RuntimeError(f"target worker is not live: {task_id}")
    mode = record.get("ownershipMode")
    if mode == "receiver":
        worker_pid = int(record.get("workerPid", 0))
        expected = INBOX_ROOT / task_id / ".receiver-identity.json"
        if (record.get("taskId") != task_id or int(record.get("ownerPid", 0)) != worker_pid
                or not alive(worker_pid) or not record.get("cwd")
                or not SAFE_ID.fullmatch(str(record.get("parentSessionId", "")))
                or Path(str(record.get("receiverIdentityPath", ""))).resolve() != expected.resolve()
                or expected.is_symlink()):
            raise RuntimeError("receiver-owned worker registration mismatch")
        identity = worker_receiver_identity(task_id, item_key)
        if (identity.get("targetKind") != "worker" or identity.get("targetId") != task_id
                or not SAFE_ID.fullmatch(str(identity.get("runId", "")))
                or not isinstance(identity.get("nonce"), str) or not 16 <= len(identity["nonce"]) <= 200
                or int(identity.get("pid", 0)) != worker_pid or not identity.get("cwd")
                or Path(identity["cwd"]).resolve() != Path(record["cwd"]).resolve()
                or {str(x) for x in identity.get("itemKeys", [])} != record_keys(record)):
            raise RuntimeError("receiver-owned worker identity/cwd/items mismatch")
        record["_receiverIdentity"] = identity
    else:
        if mode not in (None, "", "parent"):
            raise RuntimeError("unsupported worker ownership mode")
        heartbeat = datetime.fromisoformat(str(record.get("heartbeatAt", record.get("startedAt"))).replace("Z", "+00:00")).timestamp()
        if heartbeat > time.time() + 300 or time.time() - heartbeat > 120:
            raise RuntimeError(f"target worker lease is stale: {task_id}")
    if item_key not in record_keys(record): raise RuntimeError(f"worker {task_id} does not own {item_key}")
    return record


def wait_lease(task_id: str, item_key: str, seconds: int) -> dict[str, Any]:
    path = INBOX_ROOT / task_id / ".notification-wait-lease"
    deadline = time.time() + seconds
    while True:
        try:
            lease = read_json(path)
            if lease.get("version") != 2 or lease.get("taskId") != task_id or lease.get("itemKey") != item_key:
                raise RuntimeError("active wait lease does not match target/itemKey")
            if float(lease.get("expiresAt", 0)) <= now_ms(): raise RuntimeError("wait lease expired")
            if not alive(lease.get("producerPid")): raise RuntimeError("wait lease producer is dead")
            return lease
        except FileNotFoundError:
            if time.time() >= deadline: raise RuntimeError(f"no active wait lease for {task_id}/{item_key}")
            time.sleep(0.25)


def worker_receiver_identity(task_id: str, item_key: str) -> dict[str, Any]:
    path = INBOX_ROOT / task_id / ".receiver-identity.json"
    identity = read_json(path)
    if identity.get("version") != 2 or identity.get("taskId") != task_id:
        raise RuntimeError("worker receiver identity mismatch")
    if not alive(identity.get("pid")):
        raise RuntimeError("worker receiver process is not live")
    heartbeat = datetime.fromisoformat(str(identity.get("heartbeatAt", "")).replace("Z", "+00:00")).timestamp()
    if heartbeat > time.time() + 300 or time.time() - heartbeat > 90:
        raise RuntimeError("worker receiver identity is stale")
    if item_key not in {str(x) for x in identity.get("itemKeys", [])}:
        raise RuntimeError(f"worker receiver does not own {item_key}")
    if not identity.get("runId") or not identity.get("nonce"):
        raise RuntimeError("worker receiver identity is incomplete")
    return identity


def resolve_worker_identity(task_id: str, item_key: str, require_lease: bool, wait_seconds: int) -> dict[str, Any]:
    if require_lease:
        return wait_lease(task_id, item_key, wait_seconds)
    try:
        return wait_lease(task_id, item_key, 0)
    except (FileNotFoundError, RuntimeError):
        return worker_receiver_identity(task_id, item_key)


def resolve_main(cwd: str, session_id: str | None) -> dict[str, Any]:
    directory = INBOX_ROOT / ".main-sessions"
    candidates = []
    for path in directory.glob("*.json"):
        try:
            row = read_json(path)
            if not alive(row.get("pid")): continue
            try:
                heartbeat = datetime.fromisoformat(str(row.get("heartbeatAt", "")).replace("Z", "+00:00")).timestamp()
                if heartbeat > time.time() + 300 or time.time() - heartbeat > 90: continue
            except ValueError:
                continue
            if session_id and row.get("sessionId") != session_id: continue
            if not session_id and Path(str(row.get("cwd", ""))).resolve() != Path(cwd).resolve(): continue
            candidates.append(row)
        except Exception: continue
    if len(candidates) == 1:
        return candidates[0]
    if candidates or not session_id:
        raise RuntimeError(f"main-session routing must resolve exactly one live session, found {len(candidates)}")
    # Offline routing is exact-ID only. Never guess a historical session by cwd.
    session_id = validate_key(session_id, "sessionId", SAFE_ID)
    saved = read_json(STATE_ROOT / "main-identities" / f"{hashlib.sha256(session_id.encode()).hexdigest()[:24]}.json")
    file = Path(str(saved.get("sessionFile", "")))
    if (saved.get("version") != 2 or saved.get("targetKind") != "main"
            or saved.get("sessionId") != session_id or saved.get("targetId") != session_id
            or not file.is_absolute() or "subagent-task" in file.name or file.resolve() != file
            or not saved.get("cwd") or not SAFE_ID.fullmatch(str(saved.get("runId", "")))
            or not isinstance(saved.get("nonce"), str) or len(saved["nonce"]) < 16):
        raise RuntimeError("invalid persistent main-session identity")
    with file.open() as fh:
        header = json.loads(fh.readline())
    if (header.get("type") != "session" or header.get("id") != session_id
            or Path(str(header.get("cwd", ""))).resolve() != Path(saved["cwd"]).resolve()):
        raise RuntimeError("offline target is not the exact canonical session")
    until = datetime.fromisoformat(str(saved.get("offlineUntil", "")).replace("Z", "+00:00")).timestamp()
    if until <= time.time() or until > time.time() + 86400 + 300:
        raise RuntimeError("offline main-session recovery window expired/invalid")
    return {**saved, "_offline": True, "_offlineUntil": until}


def atomic_write(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    fd = os.open(tmp, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        payload = json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()
        os.write(fd, payload); os.fsync(fd)
    finally: os.close(fd)
    os.replace(tmp, path)
    try:
        dfd = os.open(path.parent, os.O_RDONLY); os.fsync(dfd); os.close(dfd)
    except OSError: pass


def commit_envelope(stream: str, event_id: str, envelope: dict[str, Any]) -> tuple[Path | None, bool]:
    """Allocate sequence + publish under one inter-process lock."""
    state_path = STATE_ROOT / "sender-state.json"
    lock_path = STATE_ROOT / "sender-state.lock"
    STATE_ROOT.mkdir(parents=True, exist_ok=True, mode=0o700)
    with lock_path.open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try: state = read_json(state_path)
        except FileNotFoundError: state = {"version": 1, "streams": {}, "eventIds": {}}
        cutoff = time.time() - 14 * 86400
        state["eventIds"] = {k: v for k, v in state.get("eventIds", {}).items() if float(v) >= cutoff}
        if event_id in state["eventIds"]:
            return None, True
        sequence = int(state.setdefault("streams", {}).get(stream, 0)) + 1
        envelope["sequence"] = sequence
        filename = f"evt-{int(time.time() * 1000)}-{event_id}.json"
        output = OUTBOX / filename
        atomic_write(output, envelope)
        state["streams"][stream] = sequence
        state["eventIds"][event_id] = time.time()
        state["updatedAt"] = iso()
        atomic_write(state_path, state)
        return output, False


def normalize_raw(args: argparse.Namespace) -> dict[str, Any]:
    if args.event_file:
        raw = read_json(Path(args.event_file))
    else:
        raw = {
            "eventId": args.event_id, "occurredAt": iso(), "itemKey": args.item,
            "target": {"taskId": args.to} if args.to else {"sessionId": args.session_id},
            "eventType": args.state, "source": args.source, "payload": {},
            "message": args.message, "level": args.level,
            "expiresAt": iso(time.time() + args.ttl),
        }
    declared_item = raw.get("itemKey", raw.get("itemId"))
    target_hint = raw.get("target") if isinstance(raw.get("target"), dict) else {}
    has_worker_target = bool(target_hint.get("taskId") or raw.get("targetId") or args.to)
    if not declared_item and not has_worker_target:
        scope = str(raw.get("cwd") or args.cwd or os.getcwd())
        declared_item = f"scope:{hashlib.sha256(str(Path(scope).resolve()).encode()).hexdigest()[:16]}"
    item_key = validate_key(declared_item, "itemKey")
    event_id = raw.get("eventId") or f"evt-{hashlib.sha256(json.dumps(raw, sort_keys=True, default=str).encode()).hexdigest()[:32]}"
    event_id = validate_key(event_id, "eventId", SAFE_ID)
    occurred = datetime.fromisoformat(str(raw.get("occurredAt", iso())).replace("Z", "+00:00"))
    expiry = datetime.fromisoformat(str(raw.get("expiresAt", iso(time.time() + args.ttl))).replace("Z", "+00:00"))
    if expiry.timestamp() <= time.time(): raise RuntimeError("refusing expired event")
    if occurred.timestamp() > time.time() + 300: raise RuntimeError("refusing event from the future")
    target = raw.get("target") if isinstance(raw.get("target"), dict) else {}
    task_id = target.get("taskId") or raw.get("targetId") or args.to
    event_type = str(raw.get("eventType", raw.get("state", args.state or "actionable")))
    terminal = bool(raw.get("terminal") or event_type.endswith(".terminal"))
    payload = raw.get("payload", {})
    message = str(raw.get("message") or f"{event_type}: {json.dumps(payload, default=str, ensure_ascii=False)}")[:12000]
    source = raw.get("producer", raw.get("source", args.source))
    if isinstance(source, dict): source = source.get("kind", "external")
    level = str(raw.get("level", args.level or "yellow"))
    if level not in {"green", "yellow", "red"}: raise ValueError("level must be green/yellow/red")
    return {"raw": raw, "itemKey": item_key, "eventId": event_id, "occurredAt": occurred.isoformat(), "expiresAt": expiry.isoformat(), "taskId": task_id, "sessionId": target.get("sessionId") or args.session_id, "state": event_type[:120], "terminal": terminal, "message": message, "source": str(source)[:120], "level": level}


def send(args: argparse.Namespace) -> dict[str, Any]:
    value = normalize_raw(args)
    item_key = value["itemKey"]
    task_id = value["taskId"]
    if task_id:
        task_id = validate_key(task_id, "taskId", SAFE_ID)
        registration = live_worker(task_id, item_key)
        identity = resolve_worker_identity(task_id, item_key, args.require_lease, args.lease_wait_seconds)
        if registration.get("ownershipMode") == "receiver":
            receiver = registration["_receiverIdentity"]
            if (identity.get("runId") != receiver.get("runId") or identity.get("nonce") != receiver.get("nonce")
                    or Path(str(identity.get("cwd", ""))).resolve() != Path(registration["cwd"]).resolve()
                    or int(identity.get("pid", 0)) != int(registration["workerPid"])):
                raise RuntimeError("wait/receiver identity does not match receiver-owned worker")
        target_kind, target_id = "worker", task_id
        run_id, nonce = identity["runId"], identity["nonce"]
    else:
        raw = value["raw"]
        cwd = raw.get("cwd") or args.cwd or os.getcwd()
        main = resolve_main(cwd, value["sessionId"])
        if main.get("_offline"):
            expiry = datetime.fromisoformat(value["expiresAt"]).timestamp()
            if expiry > min(time.time() + 3600, main["_offlineUntil"]) + 1:
                raise RuntimeError("offline notification TTL exceeds one hour/recovery window")
        target_kind, target_id = "main", main["sessionId"]
        run_id, nonce = main["runId"], main["nonce"]
    stream = f"{target_kind}|{target_id}|{item_key}|{run_id}|{value['source']}"
    envelope = {
        "version": 2, "eventId": value["eventId"], "runId": run_id,
        "itemKey": item_key, "targetKind": target_kind, "targetId": target_id,
        "state": value["state"], "sequence": 0,
        "occurredAt": value["occurredAt"], "nonce": nonce, "level": value["level"],
        "source": value["source"], "message": value["message"],
        "actionable": value["level"] != "green", "terminal": value["terminal"],
        "expiresAt": value["expiresAt"],
    }
    if item_key.isdigit() and len(item_key) == 6:
        envelope["itemId"] = item_key
    output, duplicate = commit_envelope(stream, value["eventId"], envelope)
    if target_kind == "worker" and not duplicate:
        # Non-consuming compatibility marker used by generic filesystem waiters.
        atomic_write(INBOX_ROOT / target_id / ".last-event", envelope)
    return {"ok": True, "alreadySent": duplicate, "path": str(output) if output else None, "event": envelope}


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="command")
    send_p = sub.add_parser("send")
    send_p.add_argument("message", nargs="?"); send_p.add_argument("--event-file")
    send_p.add_argument("--item"); send_p.add_argument("--to"); send_p.add_argument("--session-id")
    send_p.add_argument("--cwd"); send_p.add_argument("--event-id"); send_p.add_argument("--state", default="actionable")
    send_p.add_argument("--level", choices=["green", "yellow", "red"], default="yellow")
    send_p.add_argument("--source", default="script"); send_p.add_argument("--ttl", type=int, default=3600)
    send_p.add_argument("--require-lease", action="store_true", help="Require an armed durable wait lease (watcher handoff)")
    send_p.add_argument("--lease-wait-seconds", type=int, default=60)
    return p


def main() -> int:
    argv = sys.argv[1:]
    # Backward compatible invocation: notify_agent.py "message" --item ...
    if argv and argv[0] != "send": argv.insert(0, "send")
    args = parser().parse_args(argv)
    if args.command != "send":
        raise SystemExit("usage: notify_agent.py send [message] --item KEY [--to TASK]")
    try:
        print(json.dumps(send(args), ensure_ascii=False)); return 0
    except Exception as exc:
        print(json.dumps({"ok": False, "errorType": type(exc).__name__, "error": str(exc)}), file=sys.stderr); return 1


if __name__ == "__main__": raise SystemExit(main())
