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

"""Structured findings shared by every validator in this package.

One shape for plan, binding, inventory and capability problems, so the approval
screen and the designer's feedback loop consume a single list.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Literal

__all__ = ["Issue", "errors_of", "has_errors"]

Severity = Literal["error", "warning"]


@dataclass(frozen=True)
class Issue:
    """One finding. ``code`` is machine-readable and stable; ``message`` is for people."""

    code: str
    message: str
    severity: Severity = "error"
    step: str | None = None
    input: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def errors_of(issues: list[Issue]) -> list[Issue]:
    return [i for i in issues if i.severity == "error"]


def has_errors(issues: list[Issue]) -> bool:
    return any(i.severity == "error" for i in issues)
