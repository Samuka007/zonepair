#!/usr/bin/env bash
# Docker end-to-end with the REAL claude binary in machine B, driven through
# the stub pair with a non-Anthropic model backend.
#
# WIRING=deepseek  official DeepSeek <-> Claude Code integration; apiKey from
#                  models.yml providers.deepseek (ANTHROPIC_BASE_URL=
#                  https://api.deepseek.com/anthropic, model deepseek-flash).
# WIRING=anthropic models.yml providers.anthropic (anthropic-messages gateway,
#                  baseUrl+apiKey verbatim).
#
# Credentials are injected as container env only; they are never written to
# disk in the repo and never printed.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

WIRING="${WIRING:-deepseek}"
MODELS_YML="${MODELS_YML:-$HOME/.omp/agent/models.yml}"

# Load ANTHROPIC_* wiring into this shell without echoing it.
eval "$(python3 "$ROOT/mock/wiring-env.py" "$WIRING" "$MODELS_YML")"
export ANTHROPIC_BASE_URL ANTHROPIC_AUTH_TOKEN
if [ -n "${ANTHROPIC_MODEL:-}" ]; then
  export ANTHROPIC_MODEL
fi

docker compose build b a
# Boot machine B with the real claude binary as the served command.
B_CLAUDE_CMD="claude" docker compose up -d b
trap 'docker compose down -v --remove-orphans' EXIT

run_args=(-e CLAUDE_STUB_ENDPOINT=b:7789 -e STORE_HTTP_PORT=7789 -e ANTHROPIC_BASE_URL -e ANTHROPIC_AUTH_TOKEN -e WIRING -e ENABLE_TOOL_SEARCH=false)
if [ -n "${ANTHROPIC_MODEL:-}" ]; then
  run_args+=(-e ANTHROPIC_MODEL)
fi
docker compose run --rm "${run_args[@]}" a bash /opt/scripts/run-e2e-real-inner.sh

echo "== no-dial assertion on machine B =="
docker compose exec -T b /opt/scripts/assert-nodial.sh 1 7788

echo "DOCKER REAL E2E PASS (wiring=$WIRING)"
