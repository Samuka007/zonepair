#!/usr/bin/env bash
# Build the two OCI images with nix (flake outputs `docker-server` /
# `docker-client`, nixpkgs dockerTools) and load them into the local docker
# daemon. This replaces the old Dockerfile-based build.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

for img in docker-server docker-client; do
  nix build ".#$img" -o "result-$img"
  docker load < "result-$img"
done
