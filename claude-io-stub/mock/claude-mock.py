#!/usr/bin/env python3
"""Mock `claude` speaking a minimal slice of the stream-json protocol.

init -> (assistant text | assistant tool_use -> tool_result -> assistant text)
     -> result

Just enough to prove the stub relays the Agent-SDK control protocol verbatim,
including an MCP tool-call round trip riding the same stdio pipe.
"""

import json
import os
import sys


def emit(obj):
    print(json.dumps(obj, ensure_ascii=False), flush=True)


def arg(name, default=None):
    args = sys.argv[1:]
    return args[args.index(name) + 1] if name in args else default


session = arg("--session-id", "mock-session")
emit({
    "type": "system",
    "subtype": "init",
    "session_id": session,
    "cwd": os.getcwd(),
    "tools": ["mcp__opencode__echo"],
    "model": "mock-opus",
    "permissionMode": "dontAsk",
})


def assistant(blocks):
    emit({
        "type": "assistant",
        "session_id": session,
        "parent_tool_use_id": None,
        "message": {"id": f"msg-{session}", "role": "assistant", "content": blocks},
    })


for raw in sys.stdin:
    raw = raw.strip()
    if not raw:
        continue
    msg = json.loads(raw)
    if msg.get("type") != "user":
        continue
    content = msg.get("message", {}).get("content", [])
    texts = [b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text"]
    tool_results = [b for b in content if isinstance(b, dict) and b.get("type") == "tool_result"]

    if tool_results:
        payload = ""
        for tr in tool_results:
            inner = tr.get("content")
            if isinstance(inner, list):
                payload += "".join(
                    b.get("text", "") for b in inner if isinstance(b, dict) and b.get("type") == "text"
                )
            elif isinstance(inner, str):
                payload += inner
        assistant([{"type": "text", "text": f"TOOL OK {payload}"}])
        emit({"type": "result", "subtype": "success", "session_id": session, "num_turns": 2, "result": "done"})
        break

    if any("__TOOL__" in t for t in texts):
        assistant([{
            "type": "tool_use",
            "id": "toolu_mock_1",
            "name": "mcp__opencode__echo",
            "input": {"text": "ping"},
        }])
        continue

    assistant([{"type": "text", "text": "ECHO " + " ".join(texts)}])
    emit({"type": "result", "subtype": "success", "session_id": session, "num_turns": 1, "result": "done"})
    break
