#!/usr/bin/env python3
"""Print the uuid of the first user message in a messages JSON array (stdin)."""

import json
import sys

data = json.load(sys.stdin)
messages = data["messages"] if isinstance(data, dict) else data
for line in messages:
    if line.get("type") == "user" and line.get("uuid"):
        print(line["uuid"])
        sys.exit(0)
sys.exit("no user message with uuid found")
