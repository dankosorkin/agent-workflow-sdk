# Copyright 2026 Daniel Sorkin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
