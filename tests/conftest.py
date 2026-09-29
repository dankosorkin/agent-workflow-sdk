# Copyright (C) 2026 Daniel Sorkin
#
# This file is part of agent-workflow-sdk.
#
# agent-workflow-sdk is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License
# as published by the Free Software Foundation, version 3.
#
# See the LICENSE file for the full license text.

"""Test setup: load a local .env into the environment for live tests.

Stdlib-only, no python-dotenv dependency. Real values in .env (gitignored)
override nothing already set in the environment. Placeholder values like
REPLACE_ME are ignored so a half-filled .env doesn't make a live test think a
key is present.
"""

from __future__ import annotations

import os
from pathlib import Path

_PLACEHOLDERS = {"", "REPLACE_ME", "sk-...", "sk-ant-..."}


def _load_dotenv() -> None:
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        return
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if value in _PLACEHOLDERS:
            continue
        # Don't clobber a value already exported in the real environment.
        os.environ.setdefault(key, value)


_load_dotenv()
