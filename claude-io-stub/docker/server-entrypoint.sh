#!/usr/bin/env bash
set -euo pipefail

STORE="${STORE:-/data/claude}"
LOG_DIR="${LOG_DIR:-/data/logs}"
CLAUDE_MOCK="${CLAUDE_MOCK:-/opt/mock/claude-mock.py}"
HTTP_LISTEN="${HTTP_LISTEN:-0.0.0.0:7789}"

# Default to the real claude when it is installed; the mock e2e pins
# B_CLAUDE_CMD explicitly so both suites stay deterministic.
CLAUDE_CMD="${CLAUDE_CMD:-}"
if [ -z "$CLAUDE_CMD" ]; then
  if command -v claude > /dev/null 2>&1; then
    CLAUDE_CMD="$(command -v claude)"
  else
    CLAUDE_CMD="python3 $CLAUDE_MOCK"
  fi
fi

python3 /opt/mock/seed-store.py "$STORE" > /dev/null

TOKEN_ARGS=()
if [ -n "${HTTP_TOKEN:-}" ]; then
  TOKEN_ARGS=(--http-token "$HTTP_TOKEN")
fi

exec /usr/local/bin/stub-server \
  --listen 0.0.0.0:7788 \
  --http-listen "$HTTP_LISTEN" \
  "${TOKEN_ARGS[@]}" \
  --store "$STORE" \
  --claude "$CLAUDE_CMD" \
  --log-dir "$LOG_DIR"
