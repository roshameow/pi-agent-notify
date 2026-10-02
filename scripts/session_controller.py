#!/usr/bin/env python3
"""Hold a POSIX flock for one exact main session until Pi closes stdin.

The lock belongs to this guardian's open FD, not to stale PID metadata. Abrupt
parent termination closes the pipe and releases the lock without deleting it.
"""
import fcntl
import json
import os
import sys


def main():
    filename, parent, token = sys.argv[1:]
    parent = int(parent)
    fd = os.open(filename, os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("main session already has an active controller", file=sys.stderr)
            return 2
        os.kill(parent, 0)
        os.fchmod(fd, 0o600)
        os.ftruncate(fd, 0)
        os.write(fd, json.dumps({"pid": parent, "guardianPid": os.getpid(), "token": token}).encode())
        os.fsync(fd)
        print(json.dumps({"ready": True, "token": token}), flush=True)
        # Only the owning Pi has this write end. EOF is also the crash path.
        while sys.stdin.buffer.read(4096):
            pass
        return 0
    finally:
        os.close(fd)


if __name__ == "__main__":
    raise SystemExit(main())
