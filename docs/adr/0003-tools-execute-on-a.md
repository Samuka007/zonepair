# Tool execution stays on machine A

Accepted, 2026-10-31.

Boundary decision: claude never executes tools on machine B. In the
production OpenCode integration this holds by construction — the provider
replaces claude's built-in tools and surfaces OpenCode's tools (built-in plus
every MCP server configured on A) as the single `mcp__opencode__*` server,
bridged over the same stdio control channel the relay already carries. More
MCP servers on the OpenCode side require zero stub changes; claude's own
MCP servers/skills on B are never loaded (isolation env).

The lab proof (`--mcp` driver mode) demonstrates the same boundary with a
real claude: since `--mcp-config` forces claude to spawn a stdio MCP helper
**on B**, the helper dials B's own loopback and gets paired with a
pre-opened A-initiated tunnel, so the tool implementation runs on A.

## Considered Options

- **Direct B→A dial from the helper**: simplest, but violates ADR-0001.
- **Multiplexing tools over the stdio control channel in the lab**: that is
  the production shape, but it would mean re-implementing claude's MCP client
  side in the stub; the tunnel proves the boundary with the stock binary.

## Consequences

- Every extra stdio MCP server in a lab experiment costs one more queued
  tunnel + one more `mcpServers` entry.
- `ENABLE_TOOL_SEARCH=false` is required: claude ≥ 2.1.316 otherwise hides
  MCP tools behind its ToolSearch meta tool, which breaks the proof.
