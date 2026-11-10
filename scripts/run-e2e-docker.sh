#!/usr/bin/env bash
# Container e2e (ADR-0006): this script only starts the containers and execs
# the Rust harness inside machine B — every assertion lives in the harness
# (src/bin/zonepair-e2e.rs), which runs in B's own network namespace so the
# ADR-0001 no-dial check can attribute sockets to the server process.
#
# Machine B is booted with the mock claude (`/bin/claude-mock`); the
# real-claude/real-provider suite needs credentials and is a follow-up.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

scripts/nix-docker-load.sh

export B_IMAGE=claude-io-stub-server-test:latest
export B_CLAUDE_CMD=/usr/local/bin/claude-mock
docker compose up -d b
trap 'docker compose down -v --remove-orphans' EXIT

# The harness needs to find the binaries inside the image.
docker compose exec -T b \
  env ZONEPAIR_SERVER_BIN=/usr/local/bin/stub-server ZONEPAIR_MOCK_BIN=/usr/local/bin/claude-mock \
  /usr/local/bin/zonepair-e2e

echo "DOCKER E2E PASS"
