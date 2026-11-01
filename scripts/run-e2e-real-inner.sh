#!/usr/bin/env bash
# Runs inside container A against a REAL claude on machine B.
# Requires on entry:
#   CLAUDE_STUB_ENDPOINT     stub-server address (b:7788)
#   ANTHROPIC_BASE_URL       provider gateway (from mock/wiring-env.py)
#   ANTHROPIC_AUTH_TOKEN     provider credential (never printed)
#   WIRING                   deepseek|anthropic (for the PASS line only)
set -euo pipefail

ENDPOINT="${CLAUDE_STUB_ENDPOINT:?CLAUDE_STUB_ENDPOINT must point at the stub-server}"
: "${ANTHROPIC_BASE_URL:?ANTHROPIC_BASE_URL required (wiring-env.py)}"
: "${ANTHROPIC_AUTH_TOKEN:?ANTHROPIC_AUTH_TOKEN required (wiring-env.py)}"
WIRING="${WIRING:-anthropic}"
HOST="${ENDPOINT%%:*}"
PORT="${ENDPOINT##*:}"
STORE_BASE="http://${HOST}:${STORE_HTTP_PORT:-7789}"
STUB="/usr/local/bin/stub-client"
MOCK="/opt/mock"
DRIVER="/opt/driver/sdk-driver.mjs"

for _ in $(seq 1 60); do
  (exec 3<>"/dev/tcp/$HOST/$PORT") 2>/dev/null && { exec 3>&-; break; }
  sleep 0.5
done
export CLAUDE_STUB_ENDPOINT="$ENDPOINT"
# Keep the container's claude from phoning Anthropic for telemetry/updates;
# both keys are inside the stub env allowlist and ride the hello header.
export CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1
export DISABLE_AUTOUPDATER=1

fail() {
  echo "REAL-E2E FAIL: $*" >&2
  exit 1
}

MARKER="dpsk-marker-$RANDOM-$$"

echo "== real claude via stub: turn 1 (model must echo the marker) =="
OUT1="$(timeout 240 node "$DRIVER" --stub "$STUB" --real --label turn1 \
  --prompt "Reply with exactly this phrase and nothing else: $MARKER")"
echo "$OUT1"
printf '%s' "$OUT1" | grep -q "$MARKER" || fail "model reply does not contain the marker"
SESSION="$(printf '%s' "$OUT1" | awk '/\] SESSION /{print $3}')"
[ -n "$SESSION" ] || fail "no session id from driver"

echo "== real session JSONL landed in machine B store (HTTP store) =="
node "$MOCK/store-http.mjs" "$STORE_BASE" exists "$SESSION" | python3 "$MOCK/rpc-check.py" exists true
node "$MOCK/store-http.mjs" "$STORE_BASE" messages "$SESSION" | python3 "$MOCK/rpc-check.py" msglen-min 2

echo "== session projection: fork the real store via RPC =="
FIRST_UUID="$(node "$MOCK/store-http.mjs" "$STORE_BASE" messages "$SESSION" | python3 "$MOCK/first-user-uuid.py")"
FORK_JSON="$(node "$MOCK/store-http.mjs" "$STORE_BASE" fork "$SESSION" "$FIRST_UUID")"
printf '%s' "$FORK_JSON" | python3 "$MOCK/rpc-check.py" fork-pairs 1
NEW_ID="$(printf '%s' "$FORK_JSON" | python3 "$MOCK/json-get.py" sessionId)"

echo "== real claude resumes the FORKED session through the stub =="
OUT2="$(timeout 240 node "$DRIVER" --stub "$STUB" --real --label forkresume --resume "$NEW_ID" \
  --prompt "Earlier I asked you to reply with a marker phrase. Reply with only that phrase now.")"
echo "$OUT2"
printf '%s' "$OUT2" | grep -q "$MARKER" || fail "forked resume lost the earlier context"

node "$MOCK/store-http.mjs" "$STORE_BASE" delete "$NEW_ID" | python3 "$MOCK/rpc-check.py" deleted true

echo "== mcp: real tool call executed on machine A =="
SECRET="a-side-secret-$RANDOM"
printf 'the file says %s\n' "$SECRET" > /work/secret.txt
OUT3="$(timeout 240 node "$DRIVER" --stub "$STUB" --real --mcp --label mcptool \
  --prompt "Use the lab_read_file tool to read /work/secret.txt, then reply with exactly the file's content and nothing else.")"
echo "$OUT3"
printf '%s' "$OUT3" | grep -q "$SECRET" || fail "tool call did not surface the A-side file content"

echo "DOCKER REAL E2E PASS (wiring=$WIRING)"
