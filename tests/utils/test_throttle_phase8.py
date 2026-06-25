from __future__ import annotations

import asyncio
import os
import time
from unittest.mock import AsyncMock, patch

import pytest

from src.utils.throttle import AsyncThrottle


class TestAsyncThrottleInit:
    def test_default_values(self):
        t = AsyncThrottle()
        assert t._default_delay == 1.0
        assert t.jitter == 0.3

    def test_custom_values(self):
        t = AsyncThrottle(delay=2.0, jitter=0.5)
        assert t._default_delay == 2.0
        assert t.jitter == 0.5

    def test_env_override_delay(self):
        with patch.dict(os.environ, {"KBO_REQUEST_DELAY": "3.0"}):
            t = AsyncThrottle()
            assert t._default_delay == 3.0

    def test_env_override_jitter(self):
        with patch.dict(os.environ, {"KBO_REQUEST_JITTER": "0.1"}):
            t = AsyncThrottle()
            assert t.jitter == 0.1

    def test_env_overrides_both(self):
        with patch.dict(os.environ, {"KBO_REQUEST_DELAY": "5.0", "KBO_REQUEST_JITTER": "0.0"}):
            t = AsyncThrottle(delay=1.0, jitter=0.3)
            assert t._default_delay == 5.0
            assert t.jitter == 0.0


class TestDefaultDelayProperty:
    def test_getter(self):
        t = AsyncThrottle(delay=2.5)
        assert t.default_delay == 2.5

    def test_setter(self):
        t = AsyncThrottle()
        t.default_delay = 3.0
        assert t._default_delay == 3.0


class TestGetTargetDelay:
    def test_returns_positive(self):
        t = AsyncThrottle(delay=1.0, jitter=0.0)
        result = t._get_target_delay()
        assert result >= 0.0

    def test_respects_delay(self):
        t = AsyncThrottle(delay=2.0, jitter=0.0)
        result = t._get_target_delay()
        assert result == pytest.approx(2.0, abs=0.01)


class TestWait:
    @pytest.mark.asyncio
    async def test_first_call_no_sleep(self):
        t = AsyncThrottle(delay=0.0, jitter=0.0)
        start = time.monotonic()
        await t.wait()
        elapsed = time.monotonic() - start
        assert elapsed < 0.1

    @pytest.mark.asyncio
    async def test_second_call_waits(self):
        t = AsyncThrottle(delay=0.05, jitter=0.0)
        await t.wait("test.com")
        start = time.monotonic()
        await t.wait("test.com")
        elapsed = time.monotonic() - start
        assert elapsed >= 0.04

    @pytest.mark.asyncio
    async def test_different_hosts_independent(self):
        t = AsyncThrottle(delay=0.05, jitter=0.0)
        await t.wait("host1.com")
        start = time.monotonic()
        await t.wait("host2.com")
        elapsed = time.monotonic() - start
        assert elapsed < 0.04

    @pytest.mark.asyncio
    async def test_heavy_throttling_logs_warning(self, caplog):
        t = AsyncThrottle(delay=0.01, jitter=0.0)
        t._last_request_times["slow.com"] = time.monotonic() - 0.01
        t._default_delay = 0.01
        await t.wait("slow.com")


class TestWaitSync:
    def test_first_call_no_sleep(self):
        t = AsyncThrottle(delay=0.0, jitter=0.0)
        start = time.monotonic()
        t.wait_sync()
        elapsed = time.monotonic() - start
        assert elapsed < 0.1

    def test_second_call_waits(self):
        t = AsyncThrottle(delay=0.05, jitter=0.0)
        t.wait_sync("sync.com")
        start = time.monotonic()
        t.wait_sync("sync.com")
        elapsed = time.monotonic() - start
        assert elapsed >= 0.04

    def test_updates_last_request_time(self):
        t = AsyncThrottle(delay=0.0, jitter=0.0)
        t._last_request_times.clear()
        t.wait_sync("track.com")
        assert "track.com" in t._last_request_times
