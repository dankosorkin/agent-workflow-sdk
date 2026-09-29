"""
Kiro ACP client.

Manages a kiro-cli acp subprocess, speaks JSON-RPC 2.0 over
stdin/stdout, streams agent output, and auto-approves permissions.
"""

import json
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent


class AcpError(RuntimeError):
    pass


class KiroAcpClient:

    def __init__(
        self,
        agent: str,
        model: str | None = None,
        cwd: Path = ROOT,
        engine: str | None = None,
    ):
        self.agent = agent
        self.model = model
        self.cwd = cwd
        self.engine = engine
        self.process: subprocess.Popen[str] | None = None
        self.session_id: str | None = None
        self._request_id = 0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        print(f"Starting Kiro ACP agent: {self.agent}")

        cmd = ["kiro-cli", "acp"]

        if self.engine:
            cmd += ["--agent-engine", self.engine]

        if self.engine == "v3":
            # v3 does NOT accept --agent (the agent is selected as a per-session
            # mode via session/set_mode) or --model (the session default model
            # is used as-is); it resolves tokens from the CLI credential store.
            cmd += ["--auth-method", "cli"]
        else:
            # v2 and the default engine take the agent on the command line.
            cmd += ["--agent", self.agent]

        self.process = subprocess.Popen(
            cmd,
            cwd=self.cwd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=sys.stderr,
            text=True,
            encoding="utf-8",
            bufsize=1,
        )

        self._initialize()
        self._create_session()

        # v2 sets the model via session/set_model. v3 does not support it
        # (neither the RPC nor a --model flag), so it uses the session default.
        if self.model and self.engine != "v3":
            self._set_model(self.model)

    def close(self) -> None:
        if self.process is None:
            return
        try:
            if self.process.stdin:
                self.process.stdin.close()
            self.process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            self.process.terminate()
            try:
                self.process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.process.kill()
        finally:
            self.process = None

    # ------------------------------------------------------------------
    # Session setup
    # ------------------------------------------------------------------

    def _initialize(self) -> None:
        result = self._request(
            "initialize",
            {
                "protocolVersion": 1,
                "clientCapabilities": {},
                "clientInfo": {
                    "name": "aggregation-optimizer",
                    "version": "2.0.0",
                },
            },
        )
        agent_info = result.get("agentInfo", {})
        print(f"ACP initialized: {agent_info.get('name', 'kiro')} {agent_info.get('version', '?')}")

    def _create_session(self) -> None:
        result = self._request(
            "session/new",
            {"cwd": str(self.cwd), "mcpServers": []},
        )
        session_id = result.get("sessionId")
        if not session_id:
            raise AcpError(f"session/new did not return sessionId: {result}")
        self.session_id = session_id
        print(f"ACP session: {self.session_id}")

        # In v3, the agent is not passed on the command line. Agents are
        # exposed as ACP "modes"; select ours via session/set_mode. The
        # current mode may already be correct — check availableModes first.
        if self.engine == "v3":
            self._select_agent_mode(result)

    def _select_agent_mode(self, session_result: dict[str, Any]) -> None:
        modes = session_result.get("modes", {})
        current = modes.get("currentModeId")
        if current == self.agent:
            print(f"Agent mode: {self.agent} (already active)")
            return

        available = {m.get("id") for m in modes.get("availableModes", [])}
        if self.agent not in available:
            raise AcpError(
                f"Agent '{self.agent}' not in available modes: {sorted(available)}"
            )

        self._request(
            "session/set_mode",
            {"sessionId": self.session_id, "modeId": self.agent},
        )
        print(f"Agent mode: {self.agent}")

    def _set_model(self, model: str) -> None:
        if self.session_id is None:
            raise AcpError("ACP session has not been created")
        self._request(
            "session/set_model",
            {"sessionId": self.session_id, "modelId": model},
        )
        print(f"Model: {model}")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def prompt(self, text: str) -> dict[str, Any]:
        if self.session_id is None:
            raise AcpError("ACP session has not been created")

        result = self._request(
            "session/prompt",
            {
                "sessionId": self.session_id,
                "prompt": [{"type": "text", "text": text}],
            },
        )

        print()
        stop_reason = result.get("stopReason")
        if stop_reason:
            print(f"[ACP] turn finished: {stop_reason}")

        return result

    # ------------------------------------------------------------------
    # JSON-RPC transport
    # ------------------------------------------------------------------

    def _request(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        request_id = self._next_request_id()
        self._send({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})

        while True:
            message = self._read()

            # Agent → client request (e.g. permission)
            if "method" in message and "id" in message:
                self._handle_agent_request(message)
                continue

            # Notification / streaming
            if "method" in message and "id" not in message:
                self._handle_notification(message)
                continue

            # Response to our request
            if message.get("id") != request_id:
                continue

            if "error" in message:
                raise AcpError(
                    f"{method} failed:\n"
                    f"{json.dumps(message['error'], indent=2, ensure_ascii=False)}"
                )

            result = message.get("result")
            if result is None:
                return {}
            if isinstance(result, dict):
                return result
            return {"value": result}

    def _send(self, message: dict[str, Any]) -> None:
        if self.process is None or self.process.stdin is None:
            raise AcpError("Kiro ACP process is not running")
        data = json.dumps(message, ensure_ascii=False, separators=(",", ":"))
        self.process.stdin.write(data + "\n")
        self.process.stdin.flush()

    def _read(self) -> dict[str, Any]:
        if self.process is None or self.process.stdout is None:
            raise AcpError("Kiro ACP process is not running")
        while True:
            line = self.process.stdout.readline()
            if line == "":
                raise AcpError(
                    f"Kiro ACP process terminated unexpectedly "
                    f"(exit code {self.process.poll()})"
                )
            line = line.strip()
            if not line:
                continue
            try:
                return json.loads(line)
            except json.JSONDecodeError as exc:
                raise AcpError(f"Invalid JSON from Kiro ACP:\n{line}") from exc

    def _next_request_id(self) -> int:
        self._request_id += 1
        return self._request_id

    # ------------------------------------------------------------------
    # Agent → client requests
    # ------------------------------------------------------------------

    def _handle_agent_request(self, message: dict[str, Any]) -> None:
        method = message.get("method")
        request_id = message.get("id")
        params = message.get("params", {})

        if method == "session/request_permission":
            self._handle_permission(request_id, params)
            return

        self._send({
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32601, "message": f"Unsupported client method: {method}"},
        })

    def _handle_permission(self, request_id: int, params: dict[str, Any]) -> None:
        options = params.get("options", [])
        selected = next(
            (o for o in options if o.get("kind") == "allow_always"), None
        ) or next(
            (o for o in options if o.get("kind") == "allow_once"), None
        )

        if selected is None:
            self._send({
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {"outcome": {"outcome": "cancelled"}},
            })
            return

        print(f"\n[permission] auto-approved: {selected.get('name', selected['optionId'])}")
        self._send({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "outcome": {
                    "outcome": "selected",
                    "optionId": selected["optionId"],
                }
            },
        })

    # ------------------------------------------------------------------
    # Notifications / streaming
    # ------------------------------------------------------------------

    def _handle_notification(self, message: dict[str, Any]) -> None:
        method = message.get("method")
        params = message.get("params", {})

        if method not in ("session/update", "session/notification"):
            return

        update = params.get("update") or params.get("notification") or params
        if not isinstance(update, dict):
            return

        update_type = (
            update.get("sessionUpdate")
            or update.get("type")
            or update.get("kind")
        )

        if update_type in ("agent_message_chunk", "AgentMessageChunk"):
            content = update.get("content") or update.get("chunk")
            text = None
            if isinstance(content, dict):
                text = content.get("text")
            elif isinstance(content, str):
                text = content
            if text:
                print(text, end="", flush=True)

        elif update_type in ("tool_call", "ToolCall"):
            title = (
                update.get("title")
                or update.get("name")
                or update.get("toolName")
                or update.get("tool")
                or "tool"
            )
            print(f"\n[tool] {title}", flush=True)

        elif update_type in ("tool_call_update", "ToolCallUpdate"):
            status = update.get("status")
            if status:
                print(f"[tool] status: {status}", flush=True)

        elif update_type in ("turn_end", "TurnEnd"):
            print("\n[ACP] turn end", flush=True)
