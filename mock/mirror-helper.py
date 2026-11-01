#!/usr/bin/env python3
"""Seed or count a mirror-side session file for the mirror-sync e2e phase.

usage:
  mirror-helper.py seed <jsonl-file> <source-messages-json> <n> <session-id>
      Copy the first <n> messages from <source-messages-json> (the output of
      `stub-client sess messages`), assign fresh uuids, rewrite sessionId —
      simulating what opencode's in-process forkSession writes on A.
  mirror-helper.py count <jsonl-file>
      Print the number of lines.
"""

import json
import sys
import uuid


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    command = sys.argv[1]
    if command == "seed":
        _, _, dest, source, count, session = sys.argv
        payload = json.load(open(source))
        lines = payload["messages"][: int(count)]
        fresh = [uuid.uuid4().hex for _ in lines]
        with open(dest, "w", encoding="utf-8") as handle:
            for i, line in enumerate(lines):
                obj = dict(line)
                obj["uuid"] = fresh[i]
                obj["sessionId"] = session
                obj["parentUuid"] = None if i == 0 else fresh[i - 1]
                handle.write(json.dumps(obj) + "\n")
    elif command == "count":
        _, _, path = sys.argv
        with open(path, encoding="utf-8") as handle:
            print(sum(1 for line in handle if line.strip()))
    else:
        raise SystemExit(__doc__)


if __name__ == "__main__":
    main()
