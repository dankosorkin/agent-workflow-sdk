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

"""Prebuilt graph patterns built entirely from core primitives."""

from agentflow.prebuilt.effects import IdempotentOp, IncompleteEffectError, effect_key
from agentflow.prebuilt.gate import Evaluator, GateResult, GateState, add_quality_gate
from agentflow.prebuilt.idempotent import artifact_key, skip_if_done
from agentflow.prebuilt.loop import Candidate, iterate_until_converged
from agentflow.prebuilt.resilience import with_retry, with_timeout
from agentflow.prebuilt.tool_loop import Tool, ToolLoopState, tool_loop
from agentflow.prebuilt.watch import (
    CommandWatcher,
    Watcher,
    WatchResult,
    route_watch,
    watch_node,
)

__all__ = [
    "Candidate",
    "iterate_until_converged",
    "GateResult",
    "GateState",
    "Evaluator",
    "add_quality_gate",
    "artifact_key",
    "skip_if_done",
    "IdempotentOp",
    "IncompleteEffectError",
    "effect_key",
    "Tool",
    "ToolLoopState",
    "tool_loop",
    "with_retry",
    "with_timeout",
    "WatchResult",
    "Watcher",
    "CommandWatcher",
    "watch_node",
    "route_watch",
]
