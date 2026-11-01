#!/usr/bin/env node
// Debug probe: ws /v1/stdio raw frame dump.
// usage: ws-stdio-probe.mjs ENDPOINT [PROMPT]   (every frame printed raw)

import { Buffer } from "node:buffer";

const endpoint = process.argv[2];
const prompt = process.argv[3] ?? "Reply with exactly: PROBEPONG";
if (!endpoint?.includes(":")) {
  console.error("usage: ws-stdio-probe.mjs HOST:PORT [PROMPT]");
  process.exit(1);
}

const { WebSocket } = await import("ws");
const url = `ws://${endpoint}/v1/stdio`;
console.log(`connecting ${url}`);
const ws = new WebSocket(url);

const ENV_PREFIXES = ["ANTHROPIC_", "CLAUDE_", "DISABLE_", "ENABLE_", "MCP_", "MAX_MCP", "SSL_CERT", "REQUESTS_CA", "CURL_CA"];
const env = Object.fromEntries(
  Object.entries(process.env).filter(([k, v]) => v !== undefined && ENV_PREFIXES.some((p) => k.startsWith(p))),
);

let stage = "connecting";
const timer = setTimeout(() => {
  console.log(`TIMEOUT in stage=${stage}`);
  process.exit(3);
}, 45000);

ws.on("open", () => {
  stage = "hello-sent";
  console.log("open -> hello + turn + eof");
  ws.send(JSON.stringify({ proto: "stdio-v1", argv: ["-p", "--input-format", "stream-json", "--output-format", "stream-json", "--verbose"], env, cwd: process.cwd() }));
  ws.send(JSON.stringify({ type: "user", message: { role: "user", content: [{ type: "text", text: prompt }] } }));
  ws.send(JSON.stringify({ proto: "stdin-eof" }));
});
ws.on("message", (data) => {
  const text = typeof data === "string" ? data : Buffer.from(data).toString("utf8");
  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch {
    console.log(`RAW  ${text.slice(0, 200)}`);
    return;
  }
  if (parsed.ok === false) {
    console.log(`ACK-ERR ${text}`);
    process.exit(4);
  }
  if (parsed.ok === true) {
    stage = "spawned";
    console.log("ACK ok=true");
    return;
  }
  if (parsed.proto) {
    console.log(`CTRL ${text}`);
    return;
  }
  const type = parsed.type ?? "?";
  const extra = type === "result" ? ` subtype=${parsed.subtype} result=${JSON.stringify(parsed.result)?.slice(0, 80)}` : type === "assistant" ? ` text=${JSON.stringify(parsed.message?.content)?.slice(0, 80)}` : "";
  console.log(`MSG  type=${type}${extra}`);
  if (type === "result") {
    clearTimeout(timer);
    console.log("PROBE PASS");
    setTimeout(() => process.exit(0), 200);
  }
});
ws.on("close", (code) => {
  clearTimeout(timer);
  console.log(`close code=${code} stage=${stage}`);
  if (stage !== "spawned") process.exit(5);
});
ws.on("error", (e) => {
  console.log(`WS-ERR ${e.message}`);
  process.exit(6);
});
