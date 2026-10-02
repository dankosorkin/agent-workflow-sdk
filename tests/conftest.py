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

"""Test setup: load a local .env into the environment for live tests.

Stdlib-only, no python-dotenv dependency. Real values in .env (gitignored)
override nothing already set in the environment. Placeholder values like
REPLACE_ME are ignored so a half-filled .env doesn't make a live test think a
key is present.
"""

from __future__ import annotations

import os
import uuid
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


# ---------------------------------------------------------------------------
# Postgres test support (opt-in, marker `pg`)
# ---------------------------------------------------------------------------
#
# The pg-marked tests exercise the real SQL paths of PostgresStore,
# PostgresCheckpointer, and PostgresRunQueue against a live server. They are
# skipped unless POSTGRES_TEST_DSN points at one (CI starts a `postgres`
# service and sets it). Each test gets a unique table name so runs never
# collide and cleanup is a single DROP TABLE.

POSTGRES_TEST_DSN = os.environ.get("POSTGRES_TEST_DSN")


def unique_table(prefix: str) -> str:
    """A collision-free, identifier-safe table name for one test."""
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


async def drop_table(dsn: str, table: str) -> None:
    """Best-effort teardown: drop a test table (asyncpg must be importable)."""
    import asyncpg

    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(f"DROP TABLE IF EXISTS {table}")
    finally:
        await conn.close()
