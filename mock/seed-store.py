#!/usr/bin/env python3
"""Seed a session store with two JSONL sessions for projection tests."""

import json
import os
import sys

root = sys.argv[1]
proj = os.path.join(root, "projects", "proj-a")
os.makedirs(proj, exist_ok=True)


def write(session_id, count):
    lines = []
    parent = None
    for i in range(count):
        lines.append({
            "type": "user",
            "uuid": f"u-{session_id}-{i}",
            "sessionId": session_id,
            "parentUuid": parent,
            "message": {"role": "user", "content": f"message {i}"},
        })
        parent = f"u-{session_id}-{i}"
    with open(os.path.join(proj, f"{session_id}.jsonl"), "w") as handle:
        for line in lines:
            handle.write(json.dumps(line) + "\n")


write("session_aaa", 5)
write("session_bbb", 3)
print(f"seeded {proj}")
