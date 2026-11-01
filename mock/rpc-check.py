#!/usr/bin/env python3
"""Shape assertions for stub sess RPC responses. Response JSON arrives on stdin.

usage: rpc-check.py <check> [args...]
  ok-true | exists true|false | msglen N | fork-pairs N
  new-session-differs OTHER_ID | deleted true|false
"""

import json
import sys

data = json.load(sys.stdin)
check = sys.argv[1]


def fail(message):
    sys.exit(f"rpc-check {check} FAILED: {message} (response: {json.dumps(data)[:300]})")


if check == "ok-true":
    if data.get("ok") is not True:
        fail("ok is not true")
elif check == "exists":
    want = sys.argv[2] == "true"
    if data.get("exists") is not want:
        fail(f"exists={data.get('exists')} want {want}")
elif check == "msglen":
    want = int(sys.argv[2])
    if len(data.get("messages", [])) != want:
        fail(f"messages={len(data.get('messages', []))} want {want}")
elif check == "msglen-min":
    want = int(sys.argv[2])
    got = len(data.get("messages", []))
    if got < want:
        fail(f"messages={got} want >= {want}")
elif check == "fork-pairs":
    want = int(sys.argv[2])
    pairs = data.get("pairs", [])
    if len(pairs) != want:
        fail(f"pairs={len(pairs)} want {want}")
    fresh = [p[1] for p in pairs]
    if len(set(fresh)) != len(fresh):
        fail("new uuids are not distinct")
    if any(old == new for old, new in pairs):
        fail("a new uuid equals its old uuid")
elif check == "new-session-differs":
    if data.get("sessionId") == sys.argv[2]:
        fail("forked sessionId equals source")
elif check == "deleted":
    want = sys.argv[2] == "true"
    if data.get("deleted") is not want:
        fail(f"deleted={data.get('deleted')} want {want}")
else:
    fail(f"unknown check {check}")

print(f"rpc-check {check} OK")
