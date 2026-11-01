#!/usr/bin/env bash
# Runs inside container A. Requires CLAUDE_STUB_ENDPOINT to point at service b.
set -euo pipefail

ENDPOINT="${CLAUDE_STUB_ENDPOINT:?CLAUDE_STUB_ENDPOINT must point at the stub-server service}"
HOST="${ENDPOINT%%:*}"
PORT="${ENDPOINT##*:}"
STUB="/usr/local/bin/stub-client"
MOCK="/opt/mock"
DRIVER="/opt/driver/sdk-driver.mjs"

for _ in $(seq 1 50); do
  (exec 3<>"/dev/tcp/$HOST/$PORT") 2>/dev/null && { exec 3>&-; break; }
  sleep 0.2
done
export CLAUDE_STUB_ENDPOINT="$ENDPOINT"

echo "== docker: basic =="
node "$DRIVER" --stub "$STUB" --label basic --session sess-docker-basic --tool 0

echo "== docker: mcp round trip over stdio =="
node "$DRIVER" --stub "$STUB" --label mcp --session sess-docker-mcp --tool 1

echo "== docker: multiple clients, one server =="
for i in 1 2 3; do
  node "$DRIVER" --stub "$STUB" --label "par$i" --session "sess-docker-par-$i" --tool 1 &
done
wait

echo "== docker: session projection rpc =="
"$STUB" sess exists no-such-session | python3 "$MOCK/rpc-check.py" exists false
"$STUB" sess exists session_aaa | python3 "$MOCK/rpc-check.py" exists true
"$STUB" sess messages session_aaa | python3 "$MOCK/rpc-check.py" msglen 5

FORK_JSON="$("$STUB" sess fork session_aaa u-session_aaa-2)"
NEW_ID="$(printf '%s' "$FORK_JSON" | python3 "$MOCK/json-get.py" sessionId)"
printf '%s' "$FORK_JSON" | python3 "$MOCK/rpc-check.py" fork-pairs 3
"$STUB" sess messages "$NEW_ID" | python3 "$MOCK/rpc-check.py" msglen 3
"$STUB" sess delete "$NEW_ID" | python3 "$MOCK/rpc-check.py" deleted true

echo "== docker: store mirror sync (ADR-0004) =="
/opt/scripts/mirror-phase.sh "$STUB" "$MOCK" "$DRIVER"

echo "DOCKER E2E PASS"
