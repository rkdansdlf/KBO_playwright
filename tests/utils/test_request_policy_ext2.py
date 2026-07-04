from __future__ import annotations

import asyncio
import os
from unittest.mock import MagicMock, patch

import pytest

from src.utils.request_policy import RequestPolicy, RequestPolicyConfig

_ENV_KEYS = ("KBO_REQUEST_DELAY_MIN", "KBO_REQUEST_DELAY_MAX", "KBO_REQUEST_MAX_RETRIES", "KBO_REQUEST_BACKOFF")


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for key in _ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.delenv("KBO_USER_AGENTS", raising=False)
    yield


class TestLoadUserAgents:
    def test_override_pool_used(self):
        config = RequestPolicyConfig(user_agents=["CustomAgent/1.0"])
        policy = RequestPolicy(config=config)
        assert "CustomAgent/1.0" in policy.user_agents

    def test_env_var_parsing(self, monkeypatch):
        monkeypatch.setenv("KBO_USER_AGENTS", "Agent/1.0|Agent/2.0")
        config = RequestPolicyConfig()
        policy = RequestPolicy(config=config)
        assert "Agent/1.0" in policy.user_agents
        assert "Agent/2.0" in policy.user_agents

    def test_env_var_comma_separated(self, monkeypatch):
        monkeypatch.setenv("KBO_USER_AGENTS", "Agent/1.0, Agent/2.0")
        config = RequestPolicyConfig()
        policy = RequestPolicy(config=config)
        assert "Agent/1.0" in policy.user_agents
        assert "Agent/2.0" in policy.user_agents

    def test_defaults_when_no_override_or_env(self):
        config = RequestPolicyConfig()
        policy = RequestPolicy(config=config)
        assert len(policy.user_agents) > 0


class TestRandomUserAgent:
    def test_returns_from_pool(self):
        config = RequestPolicyConfig(user_agents=["UA/1.0", "UA/2.0"])
        policy = RequestPolicy(config=config)
        ua = policy.random_user_agent()
        assert ua in ["UA/1.0", "UA/2.0"]


class TestBuildContextKwargs:
    def test_includes_user_agent(self):
        config = RequestPolicyConfig(user_agents=["TestAgent/1.0"])
        policy = RequestPolicy(config=config)
        kwargs = policy.build_context_kwargs()
        assert "user_agent" in kwargs

    def test_overrides_applied(self):
        config = RequestPolicyConfig(user_agents=["TestAgent/1.0"])
        policy = RequestPolicy(config=config)
        kwargs = policy.build_context_kwargs(timeout=30)
        assert kwargs["timeout"] == 30


class TestRandomDelay:
    def test_within_range(self):
        config = RequestPolicyConfig(min_delay=1.0, max_delay=5.0)
        policy = RequestPolicy(config=config)
        for _ in range(10):
            delay = policy._random_delay()
            assert 1.0 <= delay <= 5.0


class TestDelay:
    def test_calls_throttle_wait_sync(self):
        config = RequestPolicyConfig(min_delay=1.0, max_delay=2.0)
        policy = RequestPolicy(config=config)
        with patch("src.utils.request_policy.throttle") as mock_throttle:
            policy.delay("koreabaseball.com")
            mock_throttle.wait_sync.assert_called_once_with("koreabaseball.com")

    @pytest.mark.asyncio
    async def test_calls_throttle_wait_async(self):
        config = RequestPolicyConfig(min_delay=1.0, max_delay=2.0)
        policy = RequestPolicy(config=config)
        with patch("src.utils.request_policy.throttle") as mock_throttle:

            async def mock_wait(*args, **kwargs):
                return None

            mock_throttle.wait = mock_wait
            await policy.delay_async("koreabaseball.com")


class TestRunWithRetry:
    def test_succeeds_first_try(self):
        config = RequestPolicyConfig(max_retries=3)
        policy = RequestPolicy(config=config)
        result = policy.run_with_retry(lambda: 42)
        assert result == 42

    def test_retries_on_exception(self, monkeypatch):
        config = RequestPolicyConfig(max_retries=3, backoff_factor=0.0)
        policy = RequestPolicy(config=config)
        monkeypatch.setattr("time.sleep", lambda *a, **kw: None)
        call_count = 0

        def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("transient")
            return "ok"

        result = policy.run_with_retry(flaky)
        assert result == "ok"
        assert call_count == 3

    def test_raises_after_max_retries(self, monkeypatch):
        config = RequestPolicyConfig(max_retries=2, backoff_factor=0.0)
        policy = RequestPolicy(config=config)
        monkeypatch.setattr("time.sleep", lambda *a, **kw: None)

        def always_fail():
            raise ValueError("permanent")

        with pytest.raises(ValueError, match="permanent"):
            policy.run_with_retry(always_fail)


class TestRunWithRetryAsync:
    @pytest.mark.asyncio
    async def test_async_succeeds_first_try(self):
        config = RequestPolicyConfig(max_retries=3)
        policy = RequestPolicy(config=config)

        async def coro():
            return 42

        result = await policy.run_with_retry_async(coro)
        assert result == 42

    @pytest.mark.asyncio
    async def test_async_retries_then_succeeds(self, monkeypatch):
        config = RequestPolicyConfig(max_retries=3, backoff_factor=0.0)
        policy = RequestPolicy(config=config)

        async def mock_sleep(*args, **kwargs):
            return None

        monkeypatch.setattr("asyncio.sleep", mock_sleep)
        call_count = 0

        async def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise RuntimeError("transient")
            return "ok"

        result = await policy.run_with_retry_async(flaky)
        assert result == "ok"

    @pytest.mark.asyncio
    async def test_async_raises_after_max_retries(self, monkeypatch):
        config = RequestPolicyConfig(max_retries=2, backoff_factor=0.0)
        policy = RequestPolicy(config=config)

        async def mock_sleep(*args, **kwargs):
            return None

        monkeypatch.setattr("asyncio.sleep", mock_sleep)

        async def always_fail():
            raise RuntimeError("permanent")

        with pytest.raises(RuntimeError, match="permanent"):
            await policy.run_with_retry_async(always_fail)


class TestEnvVarOverrides:
    def test_delay_env_vars(self, monkeypatch):
        monkeypatch.setenv("KBO_REQUEST_DELAY_MIN", "0.5")
        monkeypatch.setenv("KBO_REQUEST_DELAY_MAX", "1.0")
        config = RequestPolicyConfig()
        policy = RequestPolicy(config=config)
        assert policy.min_delay == 0.5
        assert policy.max_delay == 1.0

    def test_max_retries_env_var(self, monkeypatch):
        monkeypatch.setenv("KBO_REQUEST_MAX_RETRIES", "5")
        config = RequestPolicyConfig()
        policy = RequestPolicy(config=config)
        assert policy.max_retries == 5

    def test_backoff_env_var(self, monkeypatch):
        monkeypatch.setenv("KBO_REQUEST_BACKOFF", "2.0")
        config = RequestPolicyConfig()
        policy = RequestPolicy(config=config)
        assert policy.backoff_factor == 2.0

    def test_min_greater_than_max_swapped(self, monkeypatch):
        monkeypatch.setenv("KBO_REQUEST_DELAY_MIN", "5.0")
        monkeypatch.setenv("KBO_REQUEST_DELAY_MAX", "1.0")
        config = RequestPolicyConfig()
        policy = RequestPolicy(config=config)
        assert policy.min_delay == 1.0
        assert policy.max_delay == 5.0


class TestConfigErrors:
    def test_both_config_and_overrides_raises(self):
        config = RequestPolicyConfig(min_delay=1.0)
        with pytest.raises(TypeError):
            RequestPolicy(config=config, max_retries=5)
