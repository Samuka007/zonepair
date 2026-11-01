#!/usr/bin/env bash
# Native end-to-end: machine B is emulated on 127.0.0.2 (second loopback
# address), machine A is the default host zone. Builds through the flake
# devShell when binaries are missing.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/target/debug"
PORT="${PORT:-17899}"
HOST_B="${HOST_B:-127.0.0.2}"

if [ ! -x "$BIN/stub-server" ] || [ ! -x "$BIN/stub-client" ]; then
  (cd "$ROOT" && nix develop -c cargo build)
fi

WORK="$(mktemp -d)"
STORE="$WORK/store"
LOGS="$WORK/logs"
mkdir -p "$STORE" "$LOGS"
SRV_PID=""
cleanup() {
  if [ -n "$SRV_PID" ]; then
    kill "$SRV_PID" 2>/dev/null || true
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

python3 "$ROOT/mock/seed-store.py" "$STORE" > /dev/null
"$BIN/stub-server" --listen "$HOST_B:$PORT" --store "$STORE" \
  --claude "python3 $ROOT/mock/claude-mock.py" --log-dir "$LOGS" &
SRV_PID=$!

for _ in $(seq 1 50); do
  (exec 3<>"/dev/tcp/$HOST_B/$PORT") 2>/dev/null && { exec 3>&-; break; }
  sleep 0.1
done
export CLAUDE_STUB_ENDPOINT="$HOST_B:$PORT"

echo "== single client: basic =="
node "$ROOT/driver/sdk-driver.mjs" --stub "$BIN/stub-client" --label basic --session sess-e2e-basic --tool 0

echo "== single client: mcp round trip over stdio =="
node "$ROOT/driver/sdk-driver.mjs" --stub "$BIN/stub-client" --label mcp --session sess-e2e-mcp --tool 1

echo "== multiple clients, one server =="
pids=()
for i in 1 2 3; do
  node "$ROOT/driver/sdk-driver.mjs" --stub "$BIN/stub-client" --label "par$i" --session "sess-par-$i" --tool 1 &
  pids+=("$!")
done
for p in "${pids[@]}"; do
  wait "$p"
done

echo "== session projection rpc =="
"$BIN/stub-client" sess exists no-such-session | python3 "$ROOT/mock/rpc-check.py" exists false
"$BIN/stub-client" sess exists session_aaa | python3 "$ROOT/mock/rpc-check.py" exists true
"$BIN/stub-client" sess messages session_aaa | python3 "$ROOT/mock/rpc-check.py" msglen 5

FORK_JSON="$("$BIN/stub-client" sess fork session_aaa u-session_aaa-2)"
NEW_ID="$(printf '%s' "$FORK_JSON" | python3 "$ROOT/mock/json-get.py" sessionId)"
PAIRS_JSON="$(printf '%s' "$FORK_JSON" | python3 "$ROOT/mock/json-get.py" pairs)"
printf '%s' "$FORK_JSON" | python3 "$ROOT/mock/rpc-check.py" fork-pairs 3
printf '%s' "$FORK_JSON" | python3 "$ROOT/mock/rpc-check.py" new-session-differs session_aaa
"$BIN/stub-client" sess messages "$NEW_ID" | python3 "$ROOT/mock/rpc-check.py" msglen 3
python3 "$ROOT/mock/verify-fork.py" "$STORE" session_aaa "$NEW_ID" "$PAIRS_JSON"
"$BIN/stub-client" sess delete "$NEW_ID" | python3 "$ROOT/mock/rpc-check.py" deleted true
"$BIN/stub-client" sess exists "$NEW_ID" | python3 "$ROOT/mock/rpc-check.py" exists false

echo "== concurrent forks of one source =="
fork_ids="$WORK/fork-ids.txt"
: > "$fork_ids"
for i in 1 2 3; do
  "$BIN/stub-client" sess fork session_bbb u-session_bbb-1 > "$WORK/fork-$i.json"
  python3 "$ROOT/mock/json-get.py" sessionId < "$WORK/fork-$i.json" >> "$fork_ids"
done
distinct="$(sort -u "$fork_ids" | wc -l)"
if [ "$distinct" -ne 3 ]; then
  echo "FAIL: expected 3 distinct fork ids, got $distinct" >&2
  exit 1
fi
echo "3 concurrent forks produced 3 distinct sessions"

"$ROOT/scripts/assert-nodial.sh" "$SRV_PID" "$PORT"
"$ROOT/scripts/mirror-phase.sh" "$BIN/stub-client" "$ROOT/mock" "$ROOT/driver/sdk-driver.mjs"
"$ROOT/scripts/assert-nodial.sh" "$SRV_PID" "$PORT"
echo "NATIVE E2E PASS (store=$STORE preserved nowhere; logs=$LOGS removed with workdir)"
