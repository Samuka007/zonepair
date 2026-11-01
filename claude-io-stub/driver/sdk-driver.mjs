#!/usr/bin/env node
// Simulates the Agent-SDK side of the pipe against the stub server.
//
// Mock mode (default):  sdk-driver.mjs --stub PATH --label L --session S --tool 0|1
//   Raw transport: spawns stub-client (stdio-v1 line protocol over TCP).
// Real mode:            ... --real --prompt "text" [--resume UUID] [--mcp]
//   ws-line-v1 transport: connects to ws://$CLAUDE_STUB_ENDPOINT/v1/stdio
//   directly; the server spawns the real `claude` on machine B. Frames are
//   exactly one protocol line each; `{"proto":...}` frames are transport
//   control (hello ack / stdin-eof). An MCP tool call round trip proves that
//   tool execution happens on machine A through the reverse tunnel.

import { spawn } from "node:child_process";
import { Buffer } from "node:buffer";
import { randomUUID } from "node:crypto";

const args = process.argv.slice(2);
const get = (name, dflt) => {
  const i = args.indexOf(name);
  return i >= 0 ? args[i + 1] : dflt;
};
const has = (name) => args.includes(name);
const stub = get("--stub", "/usr/local/bin/stub-client");
const label = get("--label", "drv");
const withTool = get("--tool", "0") === "1";
const realMode = has("--real");
const withMcp = has("--mcp");
const resume = get("--resume");
const promptText = get("--prompt", withTool ? "__TOOL__ run echo" : "hello");
const session = get("--session", realMode ? randomUUID() : "sess-default");
const expectedSession = resume ?? session;
const stubEndpoint = process.env.CLAUDE_STUB_ENDPOINT ?? "";

if (!realMode && !stub) {
  console.error("missing --stub PATH");
  process.exit(1);
}
if (realMode && !stubEndpoint.includes(":")) {
  console.error("real mode needs CLAUDE_STUB_ENDPOINT (host:port of the http/ws listener)");
  process.exit(1);
}
if (withMcp && !stubEndpoint.includes(":")) {
  console.error("--mcp needs CLAUDE_STUB_ENDPOINT (host:port) for the reverse tunnel");
  process.exit(1);
}

const claudeArgs = ["--input-format", "stream-json"];
if (realMode) {
  claudeArgs.push("-p", "--output-format", "stream-json", "--verbose");
  if (resume) claudeArgs.push("--resume", resume);
  if (withMcp) {
    // Static config baked into machine B; the declared stdio server runs on B
    // but immediately dials B's own loopback, landing on the A-initiated
    // tunnel opened below — so tool execution happens on A with no B->A dial.
    claudeArgs.push("--mcp-config", "/opt/mcp.json", "--strict-mcp-config", "--allowedTools", "mcp__lab__*");
  }
} else {
  claudeArgs.push("--session-id", session);
}

// Mirror of src/envf.rs — the server merges only this allowlist into the
// claude child env.
const ENV_PREFIXES = ["ANTHROPIC_", "CLAUDE_", "DISABLE_", "ENABLE_", "MCP_", "MAX_MCP", "SSL_CERT", "REQUESTS_CA", "CURL_CA"];
const ENV_EXACT = ["HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "ALL_PROXY", "API_TIMEOUT_MS"];
const envAllow = () => Object.fromEntries(
  Object.entries(process.env).filter(([k, v]) =>
    v !== undefined && (ENV_EXACT.includes(k) || ENV_PREFIXES.some((p) => k.startsWith(p)))),
);

async function openWS(url) {
  if (globalThis.WebSocket) return new globalThis.WebSocket(url);
  const mod = await import("ws").catch(() => null);
  if (!mod?.default) {
    console.error("no WebSocket available (need node >= 22 or the `ws` package)");
    process.exit(1);
  }
  return new mod.default(url);
}

// ---- state -------------------------------------------------------------------
let buffer = "";
let sawInit = false;
let sawToolUse = false;
let sawToolAck = false;
let sawResult = false;
let initSession;
let resultSession;
let lastText = "";
let echoTool = false;
let mcpProc;
let failMessage;

const fail = (message) => {
  console.error(`[${label}] FAIL: ${message}`);
  if (mcpProc) mcpProc.kill("SIGKILL");
  process.exit(1);
};

const finalize = () => {
  if (!sawInit) fail("no init message");
  if (withTool && (!sawToolUse || !sawToolAck)) fail("mcp round trip incomplete");
  if (!sawResult) fail("no result message");
  if (realMode && !resume && lastText.trim().length === 0) fail("empty reply");
  if (realMode) {
    console.log(`[${label}] SESSION ${initSession ?? resultSession ?? expectedSession}`);
    console.log(`[${label}] REPLY ${lastText.replace(/\n+/g, " ").slice(0, 400)}`);
    // The open tunnel WS + mini-mcp child keep the event loop alive; the
    // driver's job is done once the asserts above passed.
    if (mcpProc) mcpProc.kill("SIGKILL");
    process.exit(0);
  }
  console.log(`[${label}] OK session=${expectedSession} tool=${withTool}`);
};

const assertSession = () => {
  const seen = initSession ?? resultSession;
  if (seen !== expectedSession) fail(`session ${seen} != ${expectedSession}`);
};

// ---- per-line protocol handling (shared by both transports) -------------------
// sendFinish is mode-specific: it emits the payload and, when `final`, signals
// end-of-input so one-shot claude flushes and exits.
let sendFinish = () => fail("transport not ready");

const feedFrame = (line) => {
  if (!line.trim()) return;
  let msg;
  try {
    msg = JSON.parse(line);
  } catch {
    // Real claude prints bracket-tag warnings (e.g. [claude-code:unrecognized_model])
    // on stdout alongside the protocol stream.
    if (line.startsWith("[claude-code:")) return;
    fail(`non-JSON frame: ${line.slice(0, 120)}`);
    return;
  }
  if (msg.type === "system" && msg.subtype === "init") {
    if (!sawInit) {
      initSession = msg.session_id;
      sawInit = true;
    }
    return;
  }
  if (msg.type === "assistant" && Array.isArray(msg.message?.content)) {
    for (const block of msg.message.content) {
      if (block.type === "tool_use") {
        const labTool = block.name.startsWith("mcp__lab__");
        if (block.name === "mcp__opencode__echo" || labTool) {
          sawToolUse = true;
          echoTool = !labTool;
          sendFinish({
            type: "user",
            message: {
              role: "user",
              content: [{
                type: "tool_result",
                tool_use_id: block.id,
                content: [{ type: "text", text: `echo-payload-${label}` }],
              }],
            },
          });
        } else {
          // Built-in meta tools (e.g. ToolSearch when tool search is on) are
          // not executed by this harness; steer the model back to the offered
          // MCP tools. Non-final: the turn may continue with a real call.
          sendFinish({
            type: "user",
            message: {
              role: "user",
              content: [{
                type: "tool_result",
                tool_use_id: block.id,
                is_error: true,
                content: [{ type: "text", text: `Tool ${block.name} is not available. Call the offered MCP tools directly.` }],
              }],
            },
          }, false);
        }
      }
      if (block.type === "text" && block.text) {
        lastText += block.text;
        if (sawToolUse && echoTool && !block.text.includes(`echo-payload-${label}`)) {
          fail(`tool ack text wrong: ${block.text}`);
        }
        if (sawToolUse) sawToolAck = true;
      }
    }
    return;
  }
  if (msg.type === "result") {
    resultSession = msg.session_id;
    if (msg.subtype !== "success") {
      fail(`result subtype ${msg.subtype}: ${JSON.stringify(msg).slice(0, 200)}`);
      return;
    }
    if (!lastText && typeof msg.result === "string") lastText = msg.result;
    sawResult = true;
  }
};

// ---- mock mode: raw stdio-v1 transport via stub-client ------------------------
if (!realMode) {
  const child = spawn(stub, claudeArgs, { stdio: ["pipe", "pipe", "inherit"] });
  sendFinish = (payload) => {
    child.stdin.write(JSON.stringify(payload) + "\n", () => child.stdin.end());
  };
  child.stdin.write(
    JSON.stringify({
      type: "user",
      message: { role: "user", content: [{ type: "text", text: promptText }] },
    }) + "\n",
  );
  child.stdout.setEncoding("utf8");
  child.stdout.on("data", (chunk) => {
    buffer += chunk;
    let cut;
    while ((cut = buffer.indexOf("\n")) >= 0) {
      const line = buffer.slice(0, cut);
      buffer = buffer.slice(cut + 1);
      feedFrame(line);
      if (failMessage) return;
    }
  });
  child.on("exit", (code) => {
    if (code !== 0 && !sawResult) fail(`exit code ${code}`);
    finalize();
  });
} else {

// ---- real mode: ws-line-v1 transport ------------------------------------------
const userTurn = {
  type: "user",
  message: { role: "user", content: [{ type: "text", text: promptText }] },
};
const wsUrl = `ws://${stubEndpoint}/v1/stdio`;
const ws = await openWS(wsUrl);
ws.binaryType = "arraybuffer";

const wsSendLine = (payload) => {
  ws.send(JSON.stringify(payload));
};
console.error(`[dbg] ws created, waiting open (${wsUrl})`);

// Real-mode sendFinish: after the final payload, ask the server to close the
// child's stdin so claude flushes buffered output and exits (one-shot -p).
sendFinish = (payload, final = true) => {
  wsSendLine(payload);
  if (final) wsSendLine({ proto: "stdin-eof" });
};

bind(ws, "open", () => {
  console.error("[dbg] open -> hello + turn + eof");
  wsSendLine({ proto: "stdio-v1", argv: claudeArgs, env: envAllow(), cwd: process.cwd() });
  // Feed the turn immediately: real claude emits `init` only after it receives
  // the first stdin message; waiting for init would deadlock with it.
  wsSendLine(userTurn);
  if (!withTool) wsSendLine({ proto: "stdin-eof" });
});
bind(ws, "message", (ev) => {
  const data = ev.data ?? ev;
  const text = typeof data === "string" ? data : Buffer.from(data).toString("utf8");
  console.error(`[dbg] frame ${text.slice(0, 80)}`);
  for (const line of text.split("\n")) {
    if (!line.trim()) continue;
    let ctrl;
    try {
      ctrl = JSON.parse(line);
    } catch {
      if (line.startsWith("[claude-code:")) continue;
      fail(`non-JSON frame: ${line.slice(0, 120)}`);
      return;
    }
    if (ctrl.proto !== undefined) continue; // transport control (ack / echo)
    feedFrame(line);
    if (failMessage) return;
  }
});
bind(ws, "close", () => {
  console.error("[dbg] close");
  if (!sawResult) fail(`closed before result${failMessage ? `: ${failMessage}` : ""}`);
  finalize();
});
bind(ws, "error", (e) => {
  // 1006-style drops after a completed turn are just the server closing.
  if (!sawResult) fail(`ws error: ${e?.message ?? e}`);
});
if (withTool) {
  // Tool mode waits for claude's tool_use before sending the tool_result; the
  // initial user turn is not final in that case.
  sendFinish = (payload, final = true) => {
    wsSendLine(payload);
    if (final) wsSendLine({ proto: "stdin-eof" });
  };
  wsSendLine(userTurn);
}

if (withMcp) {
  // Pre-open the reverse tunnel: bridge an A-initiated WS to the A-side MCP
  // server process. claude's helper on B dials B loopback only.
  mcpProc = spawn("node", ["/opt/driver/mini-mcp-server.mjs"], { stdio: ["pipe", "pipe", "inherit"] });
  const at = stubEndpoint.lastIndexOf(":");
  const tunnel = await openWS(`ws://${stubEndpoint.slice(0, at)}:${stubEndpoint.slice(at + 1)}/v1/tunnel?side=a`);
  bind(tunnel, "open", () => {});
  bind(tunnel, "message", (ev) => {
    const data = ev.data ?? ev;
    const text = typeof data === "string" ? data : Buffer.from(data).toString("utf8");
    mcpProc.stdin.write(text.endsWith("\n") ? text : text + "\n");
  });
  bind(tunnel, "error", (e) => fail(`tunnel ws error: ${e?.message ?? e}`));
  mcpProc.stdout.setEncoding("utf8");
  mcpProc.stdout.on("data", (chunk) => {
    for (const line of String(chunk).split("\n")) {
      if (line.trim()) tunnel.send(line);
    }
  });
  mcpProc.on("exit", (code) => {
    if (!sawResult) fail(`mcp server exited early (code ${code})`);
  });
}

function bind(target, event, handler) {
  if (typeof target.on === "function") target.on(event, handler);
  else target.addEventListener(event, handler);
}
}
