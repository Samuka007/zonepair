#!/usr/bin/env python3
"""File-level fork verification on machine B (native e2e only).

usage: verify-fork.py STORE ORIG_ID NEW_ID PAIRS_JSON
"""

import json
import os
import sys

root, orig_id, new_id, pairs = sys.argv[1], sys.argv[2], sys.argv[3], json.loads(sys.argv[4])


def load(sid):
    for dirpath, _, files in os.walk(root):
        if f"{sid}.jsonl" in files:
            path = os.path.join(dirpath, f"{sid}.jsonl")
            return [json.loads(line) for line in open(path) if line.strip()]
    raise SystemExit(f"{sid}.jsonl not found under {root}")


new_lines = load(new_id)
orig_lines = load(orig_id)
by_old = {line["uuid"]: line for line in orig_lines}

assert len(new_lines) == len(pairs), f"{len(new_lines)} lines vs {len(pairs)} pairs"
prev_new = None
for i, (old, new) in enumerate(pairs):
    line = new_lines[i]
    assert line["uuid"] == new, f"line {i}: uuid not the fresh id"
    assert line["sessionId"] == new_id, f"line {i}: sessionId not rewritten"
    source = by_old[old]
    assert line.get("message") == source.get("message"), f"line {i}: payload drifted from original"
    if i == 0:
        assert line.get("parentUuid") == source.get("parentUuid"), "chain root parent changed"
    else:
        assert line["parentUuid"] == prev_new, f"line {i}: parentUuid chain broken"
    prev_new = new

print(f"verify-fork OK ({len(pairs)} lines, chain consistent)")
