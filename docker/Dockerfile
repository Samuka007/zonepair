# syntax=docker/dockerfile:1

FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release --bins

FROM debian:bookworm-slim AS server
RUN apt-get update \
  && apt-get install -y --no-install-recommends python3 curl ca-certificates iproute2 procps \
  && rm -rf /var/lib/apt/lists/*
# Native installer bundles its own runtime; npm's claude-code needs node >= 22
# which bookworm does not ship.
RUN curl -fsSL https://claude.ai/install.sh | bash \
  && ln -s /root/.local/bin/claude /usr/local/bin/claude \
  && /usr/local/bin/claude --version
COPY --from=build /src/target/release/stub-server /usr/local/bin/stub-server
COPY --from=build /src/target/release/stub-client /usr/local/bin/stub-client
COPY mock/seed-store.py mock/claude-mock.py /opt/mock/
COPY docker/mcp.json /opt/mcp.json
COPY scripts/assert-nodial.sh /opt/scripts/assert-nodial.sh
COPY docker/server-entrypoint.sh /opt/server-entrypoint.sh
ENV STORE=/data/claude
ENV LOG_DIR=/data/logs
ENV CLAUDE_MOCK=/opt/mock/claude-mock.py
ENTRYPOINT ["/opt/server-entrypoint.sh"]

FROM debian:bookworm-slim AS client
RUN apt-get update \
  && apt-get install -y --no-install-recommends python3 nodejs npm bash ca-certificates \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /work
COPY --from=build /src/target/release/stub-client /usr/local/bin/stub-client
COPY mock/ /opt/mock/
COPY driver/ /opt/driver/
RUN cd /opt/driver && npm init -y > /dev/null && npm install ws@8 --no-audit --no-fund \
  && rm -rf /opt/driver/package-lock.json
COPY scripts/ /opt/scripts/
CMD ["bash", "/opt/scripts/run-e2e-inner.sh"]
