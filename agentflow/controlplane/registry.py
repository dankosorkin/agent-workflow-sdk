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

"""GraphRegistry: name -> CompiledGraph factory.

A run request in the queue references a graph by name (graphs are code and
can't be serialized). Each process that enqueues or executes runs holds a
registry mapping that name to a factory. A worker resolves the name to a
compiled graph in its own process, so the enqueuer and worker must share
equivalent registrations (and the same checkpointer, or the worker won't see
the run's state).
"""

from __future__ import annotations

from collections.abc import Callable

from agentflow.compiled import CompiledGraph

__all__ = ["GraphFactory", "GraphRegistry"]

GraphFactory = Callable[[], CompiledGraph]


class GraphRegistry:
    """Maps graph names to factories, caching one compiled instance per name.

    The factory is called at most once per name; the resulting
    :class:`CompiledGraph` is reused (it is immutable and safe to share across
    concurrent runs on different threads).
    """

    def __init__(self) -> None:
        self._factories: dict[str, GraphFactory] = {}
        self._cache: dict[str, CompiledGraph] = {}

    def register(self, name: str, factory: GraphFactory) -> None:
        if not name:
            raise ValueError("graph name must be non-empty")
        if name in self._factories:
            raise ValueError(f"graph {name!r} is already registered")
        self._factories[name] = factory

    def get(self, name: str) -> CompiledGraph:
        cached = self._cache.get(name)
        if cached is not None:
            return cached
        factory = self._factories.get(name)
        if factory is None:
            raise KeyError(f"no graph registered under {name!r}")
        graph = factory()
        self._cache[name] = graph
        return graph

    def names(self) -> list[str]:
        return sorted(self._factories)

    def __contains__(self, name: object) -> bool:
        return name in self._factories
