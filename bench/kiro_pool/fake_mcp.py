"""Minimal stdio MCP server for benchmarking agent start-up cost.

Speaks newline-delimited JSON-RPC (initialize, tools/list, tools/call, ping).
``--startup-ms`` delays the ``initialize`` reply to imitate a heavy server (an
``npx`` download, a JVM, a DB handshake). Every process start is appended to
``$BENCH_SPAWN_LOG`` so a run can prove how many times each server was spawned.
Standard library only.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True)
    ap.add_argument("--tools", type=int, default=5)
    ap.add_argument("--startup-ms", type=int, default=0)
    args = ap.parse_args()

    log = os.environ.get("BENCH_SPAWN_LOG")
    if log:
        with open(log, "a", encoding="utf-8") as fh:
            fh.write(f"{args.name}\t{os.getpid()}\t{time.time():.3f}\n")

    tools = [
        {
            "name": f"{args.name}_tool_{i}",
            "description": f"Benchmark tool {i} of {args.name}; does nothing useful.",
            "inputSchema": {"type": "object", "properties": {"x": {"type": "string"}}},
        }
        for i in range(args.tools)
    ]

    def send(msg: dict) -> None:
        sys.stdout.write(json.dumps(msg) + "\n")
        sys.stdout.flush()

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except json.JSONDecodeError:
            continue
        method, rid = req.get("method"), req.get("id")
        if rid is None:  # notification
            continue
        if method == "initialize":
            time.sleep(args.startup_ms / 1000)
            send(
                {
                    "jsonrpc": "2.0",
                    "id": rid,
                    "result": {
                        "protocolVersion": req.get("params", {}).get("protocolVersion", "2024-11-05"),
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": args.name, "version": "0.0.1"},
                    },
                }
            )
        elif method == "tools/list":
            send({"jsonrpc": "2.0", "id": rid, "result": {"tools": tools}})
        elif method == "tools/call":
            send({"jsonrpc": "2.0", "id": rid, "result": {"content": [{"type": "text", "text": "ok"}]}})
        elif method == "ping":
            send({"jsonrpc": "2.0", "id": rid, "result": {}})
        else:
            send({"jsonrpc": "2.0", "id": rid, "error": {"code": -32601, "message": "method not found"}})


if __name__ == "__main__":
    main()
