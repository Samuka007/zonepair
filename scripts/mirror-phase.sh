#!/usr/bin/env bash
# Mirror-sync e2e phase (ADR-0004): a fork written on A's mirror must reach
# B's store (push), and messages written on B during a turn must come back to
# the mirror (pull). Usage: mirror-phase.sh STUB MOCK DRIVER [PREFIX]
set -euo pipefail

STUB="$1"
MOCK="$2"
DRIVER="$3"
PREFIX="${4:-}"

MIRROR="$(mktemp -d)"
cleanup() { rm -rf "$MIRROR"; }
trap cleanup EXIT
mkdir -p "$MIRROR/projects/lab"
export CLAUDE_CONFIG_DIR="$MIRROR"

MIRROR_SESS="${PREFIX}sess-mirror-fork"
MIRROR_FILE="$MIRROR/projects/lab/$MIRROR_SESS.jsonl"

# opencode's in-process forkSession writes a forked transcript on A:
"$STUB" sess messages session_aaa > "$MIRROR/source.json"
python3 "$MOCK/mirror-helper.py" seed "$MIRROR_FILE" "$MIRROR/source.json" 3 "$MIRROR_SESS"

# push: the A-side fork must appear in B's store
"$STUB" sync
"$STUB" sess messages "$MIRROR_SESS" | python3 "$MOCK/rpc-check.py" msglen 3

# a turn appends on B; pull must bring it back into the mirror
CLAUDE_STUB_MIRROR=1 node "$DRIVER" --stub "$STUB" --label mirror --session "$MIRROR_SESS" --tool 0
"$STUB" sync

AFTER="$(python3 "$MOCK/mirror-helper.py" count "$MIRROR_FILE")"
if [ "$AFTER" -le 3 ]; then
  echo "mirror pull did not grow the mirror (lines=$AFTER)" >&2
  exit 1
fi
echo "mirror sync ok (pushed fork visible on B, pulled $AFTER lines back)"
