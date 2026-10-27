from __future__ import annotations

from typing import Any

import pytest
from wf_helpers import make_inventory, make_plan


@pytest.fixture
def plan() -> dict[str, Any]:
    return make_plan()


@pytest.fixture
def inventory() -> dict[str, Any]:
    return make_inventory()
