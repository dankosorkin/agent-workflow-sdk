# Copyright (C) 2026 Daniel Sorkin
#
# This file is part of agent-workflow-sdk.
#
# agent-workflow-sdk is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License
# as published by the Free Software Foundation, version 3.
#
# See the LICENSE file for the full license text.

"""Pluggable backends. Import the specific adapter you need.

The core (:mod:`agentflow`) never imports these, so it stays
dependency-free. Agent backends (Kiro, Codex, Claude) and LLM backends
(Ollama, OpenAI-compatible) both implement the protocols in
:mod:`agentflow.backends.base`.

Concrete adapters (``KiroBackend``, ``OllamaBackend``) are intentionally NOT
imported here: importing one must not pull in a subprocess or an HTTP
dependency you did not ask for. Import them from their own modules, e.g.
``from agentflow.backends.kiro import KiroBackend``.
"""

from agentflow.backends._http import RetryPolicy
from agentflow.backends.base import (
    AgentBackend,
    AllowAll,
    Backend,
    BaseAgentBackend,
    BaseLLMBackend,
    DenyAll,
    Interactive,
    LLMBackend,
    PermissionPolicy,
    ToolAllowlist,
)

__all__ = [
    "Backend",
    "AgentBackend",
    "LLMBackend",
    "BaseAgentBackend",
    "BaseLLMBackend",
    "PermissionPolicy",
    "AllowAll",
    "DenyAll",
    "Interactive",
    "ToolAllowlist",
    "RetryPolicy",
]
