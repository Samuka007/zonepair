#!/usr/bin/env python3
"""Print a top-level field of a JSON object from stdin (objects as compact JSON)."""

import json
import sys

data = json.load(sys.stdin)
key = sys.argv[1]
value = data[key]
print(value if isinstance(value, str) else json.dumps(value))
