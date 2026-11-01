#!/usr/bin/env python3
"""Print provider entries of models.yml with secret values masked.

usage: mask-models.py [PATH]        (default ~/.omp/agent/models.yml)
"""

import os
import re
import sys

path = sys.argv[1] if len(sys.argv) > 1 else os.path.expanduser("~/.omp/agent/models.yml")
secret = re.compile(r"(key|token|secret|password)", re.I)

with open(path) as handle:
    for raw in handle:
        line = raw.rstrip("\n")
        stripped = line.lstrip()
        if stripped.startswith("#") or ":" not in stripped:
            print(line)
            continue
        key, _, value = stripped.partition(":")
        value = value.strip()
        if value and secret.search(key):
            print(f"{line[: len(line) - len(stripped)]}{key}: ***masked(len={len(value)})***")
        else:
            print(line)
