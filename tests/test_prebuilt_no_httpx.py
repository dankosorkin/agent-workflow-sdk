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

"""Regression guard: the core, prebuilt, and backend protocols import without
the http extra.

``import agentflow.prebuilt`` used to pull ``tool_loop`` ->
``agentflow.backends.base`` -> the ``backends`` package ``__init__`` ->
``_http`` -> ``httpx``, so the dependency-free promise broke for anyone using a
prebuilt without the http extra. ``httpx`` must load lazily, only when an HTTP
stream is actually opened. We assert that in a subprocess with ``httpx`` import
blocked, so the check is real regardless of what the rest of the suite has
already imported.
"""

from __future__ import annotations

import subprocess
import sys

# Runs in a fresh interpreter with httpx blocked at import time.
_PROBE = """
import builtins
_real_import = builtins.__import__
def _blocked(name, *a, **k):
    if name == "httpx" or name.startswith("httpx."):
        raise ModuleNotFoundError("httpx blocked for test")
    return _real_import(name, *a, **k)
builtins.__import__ = _blocked

import agentflow                       # core
import agentflow.backends              # package __init__ (re-exports RetryPolicy)
from agentflow.backends import RetryPolicy
from agentflow.backends.base import LLMBackend, AllowAll, Callback
import agentflow.prebuilt              # the module that regressed
from agentflow.prebuilt import tool_loop, add_quality_gate, GateResult, skip_if_done

assert RetryPolicy().max_retries == 2
print("OK")
"""


def test_prebuilt_imports_without_httpx():
    result = subprocess.run(
        [sys.executable, "-c", _PROBE],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"prebuilt/backends import pulled in httpx.\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )
    assert "OK" in result.stdout
