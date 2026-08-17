"""Unit tests for src/scheduler/config.py."""

from __future__ import annotations

import os
from unittest.mock import patch

from src.scheduler.config import (
    _env_enabled,
    _env_float,
    _env_int,
    _scheduler_uses_sqlite_database,
)


def test_env_enabled():
    with patch.dict(os.environ, {"TEST_KEY": "1"}):
        assert _env_enabled("TEST_KEY") is True
    with patch.dict(os.environ, {"TEST_KEY": "true"}):
        assert _env_enabled("TEST_KEY") is True
    with patch.dict(os.environ, {"TEST_KEY": "0"}):
        assert _env_enabled("TEST_KEY") is False
    with patch.dict(os.environ, {"TEST_KEY": "false"}):
        assert _env_enabled("TEST_KEY") is False
    with patch.dict(os.environ, {}, clear=True):
        assert _env_enabled("NON_EXISTENT", default="1") is True
        assert _env_enabled("NON_EXISTENT", default="0") is False


def test_env_int():
    with patch.dict(os.environ, {"TEST_PORT": "9000"}):
        assert _env_int("TEST_PORT", 8000) == 9000
    with patch.dict(os.environ, {"TEST_PORT": "invalid"}):
        assert _env_int("TEST_PORT", 8000) == 8000
    with patch.dict(os.environ, {}, clear=True):
        assert _env_int("NON_EXISTENT", 8000) == 8000


def test_env_float():
    with patch.dict(os.environ, {"TEST_FLOAT": "1.5"}):
        assert _env_float("TEST_FLOAT", 0.5) == 1.5
    with patch.dict(os.environ, {"TEST_FLOAT": "invalid"}):
        assert _env_float("TEST_FLOAT", 0.5) == 0.5
    with patch.dict(os.environ, {}, clear=True):
        assert _env_float("NON_EXISTENT", 0.5) == 0.5


def test_scheduler_uses_sqlite_database():
    with patch.dict(os.environ, {"DATABASE_URL": "sqlite:///data/kbo.db"}):
        assert _scheduler_uses_sqlite_database() is True
    with patch.dict(os.environ, {"DATABASE_URL": "oracle+oracledb://kbo:pass@host:1521/service"}):
        assert _scheduler_uses_sqlite_database() is False
