#!/usr/bin/env node
// Minimal MCP stdio server (newline-delimited JSON-RPC 2.0), zero deps.
// Runs on machine A next to the driver; every tool executes HERE, proving
// A-side execution through the stub relay. Add more tools => extend TOOLS.
// Add more servers => more entries in mcp.json + one tunnel each.

import { readFileSync } from "node:fs";
import { createInterface } from "node:readline";

const TOOLS = [
  {
    name: "read_work_file",
    description: "Read a UTF-8 text file from machine A's /work directory",
    inputSchema: {
      type: "object",
      properties: { path: { type: "string", description: "Path under /work" } },
      required: ["path"],
    },
  },
  {
    name: "ping",
    description: "Liveness probe; returns pong plus the machine A hostname",
    inputSchema: { type: "object", properties: {} },
  },
];

function callTool(name, args) {
  try {
    return callToolInner(name, args);
  } catch (e) {
    return { isError: true, content: [{ type: "text", text: String(e?.message ?? e) }] };
  }
}

function callToolInner(name, args) {
  if (name === "read_work_file") {
    const path = args?.path ?? "";
    const resolved = path.startsWith("/") ? path : `/work/${path}`;
    if (!resolved.startsWith("/work/") || resolved.includes("..")) {
      return { isError: true, content: [{ type: "text", text: `path escapes /work: ${path}` }] };
    }
    const text = readFileSync(resolved, "utf8");
    return { content: [{ type: "text", text }] };
  }
  if (name === "ping") {
    return { content: [{ type: "text", text: `pong from machine A (${os_hostname()})` }] };
  }
  return { isError: true, content: [{ type: "text", text: `unknown tool ${name}` }] };
}

function os_hostname() {
  try {
    return readFileSync("/etc/hostname", "utf8").trim();
  } catch {
    return "unknown";
  }
}

const respond = (id, result) => {
  process.stdout.write(JSON.stringify({ jsonrpc: "2.0", id, result }) + "\n");
};

const interface_ = createInterface({ input: process.stdin });
interface_.on("line", (line) => {
  if (!line.trim()) return;
  let msg;
  try {
    msg = JSON.parse(line);
  } catch {
    return;
  }
  if (msg.id === undefined) return; // notification
  if (msg.method === "initialize") {
    respond(msg.id, {
      protocolVersion: msg.params?.protocolVersion ?? "2024-11-05",
      capabilities: { tools: {} },
      serverInfo: { name: "lab", version: "0.1.0" },
    });
  } else if (msg.method === "tools/list") {
    respond(msg.id, { tools: TOOLS });
  } else if (msg.method === "tools/call") {
    respond(msg.id, callTool(msg.params?.name, msg.params?.arguments));
  } else {
    respond(msg.id, { error: { code: -32601, message: `unknown method ${msg.method}` } });
  }
});
