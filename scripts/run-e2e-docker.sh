#!/usr/bin/env bash
# Docker end-to-end: builds both images, boots machine B, runs the client-side
# suite inside machine A, then proves the no-dial constraint on machine B.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

"$ROOT/scripts/nix-docker-load.sh"
# The mock suite pins machine B to the mock claude; the real-claude runner
# (run-e2e-docker-real.sh) boots b with B_CLAUDE_CMD=claude instead.
# Exported (not inline) so `compose run` later cannot recreate b unpinned.
export B_CLAUDE_CMD="python3 /opt/mock/claude-mock.py"
docker compose up -d b
trap 'docker compose down -v --remove-orphans' EXIT

docker compose run --rm a

echo "== no-dial assertion on machine B =="
docker compose exec -T b /opt/scripts/assert-nodial.sh 1 7788

echo "DOCKER E2E PASS"
