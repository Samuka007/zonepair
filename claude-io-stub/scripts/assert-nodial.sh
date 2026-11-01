#!/usr/bin/env bash
# assert-nodial.sh SERVER_PID LISTEN_PORT
#
# Fails if the stub-server owns any established TCP connection whose LOCAL port
# is not the listen port. Every legitimate connection is one it accepted, so a
# dial-out (B -> A) would show up as an ephemeral local port.
set -euo pipefail

pid="${1:?usage: assert-nodial.sh SERVER_PID LISTEN_PORT}"
port="${2:?usage: assert-nodial.sh SERVER_PID LISTEN_PORT}"

bad="$(ss -tnp state established 2>/dev/null | awk -v want="pid=${pid}," -v lport=":${port}" \
  '$0 ~ want { if ($3 !~ lport "$") bad++ } END { print bad + 0 }')"

if [ "$bad" -ne 0 ]; then
  echo "FAIL: server pid ${pid} holds ${bad} established connection(s) NOT accepted on :${port} (outbound dial detected)" >&2
  exit 1
fi
echo "OK: pid ${pid} holds only accepted connections on :${port} (no outbound dials)"
