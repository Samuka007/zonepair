#!/usr/bin/env python3
"""Emit provider wiring as KEY=VALUE lines for driving real claude.

usage: wiring-env.py <deepseek|anthropic> [MODELS_YML]

  deepseek  official DeepSeek <-> Claude Code integration (context7-confirmed):
            ANTHROPIC_BASE_URL=https://api.deepseek.com/anthropic with the
            models.yml providers.deepseek apiKey; model deepseek-flash.
  anthropic models.yml providers.anthropic (anthropic-messages gateway),
            baseUrl + apiKey verbatim; no model override so the gateway routes
            claude model names itself.

Output goes to stdout for `eval "$(wiring-env.py ...)"` — never print it.
"""

import os
import re
import sys

mode = sys.argv[1] if len(sys.argv) > 1 else "deepseek"
path = sys.argv[2] if len(sys.argv) > 2 else os.path.expanduser("~/.omp/agent/models.yml")

providers = {}
current = None
entry = re.compile(r"^  ([A-Za-z0-9_-]+):\s*$")
field = re.compile(r"^    (apiKey|baseUrl):\s*(.+?)\s*$")
for raw in open(path):
    line = raw.rstrip("\n")
    match = entry.match(line)
    if match:
        current = match.group(1)
        providers.setdefault(current, {})
        continue
    match = field.match(line)
    if match and current:
        providers[current][match.group(1)] = match.group(2).strip().strip('"').strip("'")


def emit(pairs):
    for key, value in pairs:
        print(f"{key}={value}")


if mode == "deepseek":
    api_key = providers.get("deepseek", {}).get("apiKey")
    if not api_key:
        raise SystemExit("providers.deepseek.apiKey not found in " + path)
    emit([
        ("ANTHROPIC_BASE_URL", "https://api.deepseek.com/anthropic"),
        ("ANTHROPIC_AUTH_TOKEN", api_key),
        ("ANTHROPIC_MODEL", "deepseek-flash"),
        ("ANTHROPIC_DEFAULT_HAIKU_MODEL", "deepseek-flash"),
        ("ANTHROPIC_SMALL_FAST_MODEL", "deepseek-flash"),
    ])
elif mode == "anthropic":
    entry_cfg = providers.get("anthropic", {})
    base_url = entry_cfg.get("baseUrl")
    api_key = entry_cfg.get("apiKey")
    if not (base_url and api_key):
        raise SystemExit("providers.anthropic baseUrl/apiKey not found in " + path)
    emit([
        ("ANTHROPIC_BASE_URL", base_url),
        ("ANTHROPIC_AUTH_TOKEN", api_key),
    ])
else:
    raise SystemExit(f"unknown wiring mode {mode!r} (deepseek|anthropic)")
