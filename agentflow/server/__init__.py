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

"""An HTTP adapter over the control-plane run queue (optional ``server`` extra).

This is the thin service layer DESIGN.md defers to: a FastAPI app that maps 1:1
onto :class:`~agentflow.controlplane.queue.RunQueue` methods, so a run can be
enqueued, inspected, resumed, or cancelled over HTTP from anywhere. It adds no
behavior the queue does not already have.

    from agentflow import PostgresRunQueue
    from agentflow.server import create_app

    app = create_app(PostgresRunQueue(dsn))   # an ASGI app; serve with uvicorn

What it exposes is the *enqueue and observe* plane: ``POST /runs``, ``GET
/runs/{id}``, ``GET /runs``, ``POST /runs/{id}/resume``, ``POST
/runs/{id}/cancel``, ``GET /stats``, ``GET /healthz``. The worker protocol
(``claim`` / ``heartbeat`` / ``complete``) is deliberately *not* exposed —
those mutate run state and belong to workers, which talk to the queue directly
(in-process or via the same Postgres queue), not over this API.

Scope, stated plainly: there is no authentication, authorization, rate
limiting, or multi-tenancy here — DESIGN.md defers those as properties of a
service. Run this inside your own trust boundary, or behind a reverse proxy
that supplies them. It is an adapter, not a hardened public endpoint.

Requires the ``server`` extra: ``pip install 'agent-workflow-sdk[server]'``.
Serve the returned ASGI app with any ASGI server (e.g. ``uvicorn``), which you
install separately.
"""

from agentflow.server.app import create_app

__all__ = ["create_app"]
