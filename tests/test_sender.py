import json, os, subprocess, sys, tempfile, unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCRIPT = Path(__file__).parents[1] / "scripts/notify_agent.py"


class SenderTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.root = Path(self.tmp.name); self.inbox = self.root / "inbox"; self.state = self.root / "state"
        self.task = "task-sender-test"; self.item = "mission:hkg_super_v13"; (self.inbox / self.task).mkdir(parents=True)
        now = datetime.now(timezone.utc).isoformat()
        (self.inbox / ".active-workers.json").write_text(json.dumps({"version": 2, "workers": {self.task: {"taskId": self.task, "ownerPid": os.getpid(), "cwd": str(self.root), "itemKeys": [self.item], "heartbeatAt": now}}}))
        (self.inbox / self.task / ".notification-wait-lease").write_text(json.dumps({"version": 2, "taskId": self.task, "itemKey": self.item, "runId": "run-test", "nonce": "0123456789abcdef", "producerPid": os.getpid(), "expiresAt": int((datetime.now(timezone.utc) + timedelta(minutes=5)).timestamp() * 1000)}))
        self.env = {**os.environ, "PI_AGENT_NOTIFY_DIR": str(self.inbox), "PI_AGENT_NOTIFY_STATE_DIR": str(self.state)}

    def tearDown(self): self.tmp.cleanup()

    def send(self, event, check=True):
        file = self.root / f"{event['eventId']}.json"; file.write_text(json.dumps(event)); result = subprocess.run([sys.executable, str(SCRIPT), "send", "--event-file", str(file), "--lease-wait-seconds", "0"], env=self.env, text=True, capture_output=True)
        if check and result.returncode: self.fail(result.stderr)
        return result

    def event(self, event_id="evt-one"):
        return {"schemaVersion": 1, "eventId": event_id, "producer": "quantnight-watcher", "occurredAt": datetime.now(timezone.utc).isoformat(), "itemKey": self.item, "target": {"taskId": self.task}, "eventType": "quantnight.mission.terminal", "payload": {"terminal": True}, "expiresAt": (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()}

    def test_idempotent_and_versioned(self):
        first = json.loads(self.send(self.event()).stdout); second = json.loads(self.send(self.event()).stdout)
        self.assertFalse(first["alreadySent"]); self.assertTrue(second["alreadySent"])
        files = list((self.state / "outbox").glob("*.json")); self.assertEqual(len(files), 1)
        envelope = json.loads(files[0].read_text()); self.assertEqual(envelope["version"], 2); self.assertEqual(envelope["targetId"], self.task); self.assertEqual(envelope["itemKey"], self.item); self.assertEqual(envelope["runId"], "run-test")

    def test_busy_worker_uses_receiver_identity_without_wait_lease(self):
        (self.inbox / self.task / ".notification-wait-lease").unlink()
        (self.inbox / self.task / ".receiver-identity.json").write_text(json.dumps({"version": 2, "taskId": self.task, "targetId": self.task, "pid": os.getpid(), "cwd": str(self.root), "itemKeys": [self.item], "runId": "run-busy", "nonce": "busy0123456789ab", "heartbeatAt": datetime.now(timezone.utc).isoformat()}))
        result = json.loads(self.send(self.event("evt-busy")).stdout)
        self.assertEqual(result["event"]["runId"], "run-busy")
        marker = json.loads((self.inbox / self.task / ".last-event").read_text())
        self.assertEqual(marker["eventId"], "evt-busy")

    def test_wrong_owner_and_expiry_fail_closed(self):
        wrong = self.event("evt-wrong"); wrong["itemKey"] = "mission:other"
        self.assertNotEqual(self.send(wrong, check=False).returncode, 0)
        expired = self.event("evt-expired"); expired["expiresAt"] = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
        self.assertNotEqual(self.send(expired, check=False).returncode, 0)

    def test_ambiguous_main_cwd_is_rejected(self):
        sessions = self.inbox / ".main-sessions"; sessions.mkdir()
        base = {"version": 2, "targetKind": "main", "pid": os.getpid(), "cwd": str(self.root), "heartbeatAt": datetime.now(timezone.utc).isoformat(), "nonce": "0123456789abcdef"}
        for i in range(2):
            (sessions / f"s{i}.json").write_text(json.dumps({**base, "sessionId": f"session-{i}", "targetId": f"session-{i}", "runId": f"run-{i}"}))
        event = self.event("evt-main"); event["target"] = {}; event["cwd"] = str(self.root)
        self.assertNotEqual(self.send(event, check=False).returncode, 0)

    def receiver_owned(self):
        identity = {"version": 2, "targetKind": "worker", "targetId": self.task, "taskId": self.task,
                    "pid": os.getpid(), "cwd": str(self.root), "itemKeys": [self.item],
                    "runId": "run-test", "nonce": "0123456789abcdef", "heartbeatAt": datetime.now(timezone.utc).isoformat()}
        identity_path = self.inbox / self.task / ".receiver-identity.json"
        identity_path.write_text(json.dumps(identity))
        record = {"ownershipMode": "receiver", "taskId": self.task, "ownerPid": os.getpid(),
                  "workerPid": os.getpid(), "parentSessionId": "parent-exited", "cwd": str(self.root),
                  "receiverIdentityPath": str(identity_path), "itemKeys": [self.item], "heartbeatAt": "2000-01-01T00:00:00Z"}
        (self.inbox / ".active-workers.json").write_text(json.dumps({"workers": {self.task: record}}))
        lease_path = self.inbox / self.task / ".notification-wait-lease"
        lease = json.loads(lease_path.read_text()); lease.update(pid=os.getpid(), cwd=str(self.root))
        lease_path.write_text(json.dumps(lease))
        return record, identity, identity_path

    def test_receiver_owner_does_not_depend_on_parent_heartbeat(self):
        self.receiver_owned()
        self.assertEqual(self.send(self.event("evt-receiver-owned")).returncode, 0)

    def test_receiver_owner_mismatches_fail_closed(self):
        for field, value in [("pid", 1), ("cwd", str(self.root / "wrong")), ("itemKeys", [self.item, "ci:extra"]),
                             ("heartbeatAt", "2000-01-01T00:00:00Z"), ("taskId", "wrong-task")]:
            record, identity, file = self.receiver_owned()
            identity[field] = value; file.write_text(json.dumps(identity))
            with self.subTest(field=field):
                self.assertNotEqual(self.send(self.event(f"evt-mismatch-{field}"), check=False).returncode, 0)
        record, identity, file = self.receiver_owned(); record["workerPid"] = 1
        (self.inbox / ".active-workers.json").write_text(json.dumps({"workers": {self.task: record}}))
        self.assertNotEqual(self.send(self.event("evt-registry-pid"), check=False).returncode, 0)

    def test_unknown_mode_does_not_fall_back_to_parent(self):
        record, identity, file = self.receiver_owned(); record["ownershipMode"] = "unknown"
        (self.inbox / ".active-workers.json").write_text(json.dumps({"workers": {self.task: record}}))
        self.assertNotEqual(self.send(self.event("evt-unknown-mode"), check=False).returncode, 0)

    def test_offline_exact_session_identity_and_ttl(self):
        import hashlib
        session = "offline-main"
        file = (self.root / "canonical.jsonl").resolve()
        file.write_text(json.dumps({"type": "session", "id": session, "cwd": str(self.root)}) + "\n")
        directory = self.state / "main-identities"; directory.mkdir(parents=True)
        identity_path = directory / f"{hashlib.sha256(session.encode()).hexdigest()[:24]}.json"
        row = {"version": 2, "targetKind": "main", "sessionId": session, "targetId": session,
               "sessionFile": str(file), "cwd": str(self.root), "runId": "run-offline", "nonce": "0123456789abcdef",
               "offlineUntil": (datetime.now(timezone.utc) + timedelta(hours=24)).isoformat()}
        identity_path.write_text(json.dumps(row))
        event = self.event("evt-offline"); event["target"] = {"sessionId": session}
        self.assertEqual(json.loads(self.send(event).stdout)["event"]["runId"], "run-offline")
        event["eventId"] = "evt-offline-ttl"; event["expiresAt"] = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()
        self.assertNotEqual(self.send(event, check=False).returncode, 0)
        event["expiresAt"] = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
        row["offlineUntil"] = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat(); identity_path.write_text(json.dumps(row))
        self.assertNotEqual(self.send(event, check=False).returncode, 0)

    def test_concurrent_sequences_are_unique(self):
        processes = []
        for i in range(8):
            event = self.event(f"evt-{i}"); file = self.root / f"evt-{i}.json"; file.write_text(json.dumps(event))
            processes.append(subprocess.Popen([sys.executable, str(SCRIPT), "send", "--event-file", str(file), "--lease-wait-seconds", "0"], env=self.env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE))
        for proc in processes:
            out, err = proc.communicate(); self.assertEqual(proc.returncode, 0, err)
        sequences = [json.loads(path.read_text())["sequence"] for path in (self.state / "outbox").glob("*.json")]
        self.assertEqual(sorted(sequences), list(range(1, 9)))


if __name__ == "__main__": unittest.main()
