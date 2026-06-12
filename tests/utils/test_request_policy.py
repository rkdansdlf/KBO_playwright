"""Tests for request_policy — throttling, retry, and UA rotation."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.utils.request_policy import DEFAULT_USER_AGENTS, RequestPolicy


class TestInit:
    def test_default_values(self):
        with patch.dict("os.environ", {}, clear=True):
            p = RequestPolicy()
            assert p.min_delay == 1.5
            assert p.max_delay == 2.5
            assert p.max_retries == 3
            assert p.backoff_factor == 1.5
            assert p.user_agents == DEFAULT_USER_AGENTS

    def test_env_overrides(self):
        with patch.dict(
            "os.environ",
            {
                "KBO_REQUEST_DELAY_MIN": "3.0",
                "KBO_REQUEST_DELAY_MAX": "5.0",
                "KBO_REQUEST_MAX_RETRIES": "5",
                "KBO_REQUEST_BACKOFF": "2.0",
            },
            clear=True,
        ):
            p = RequestPolicy()
            assert p.min_delay == 3.0
            assert p.max_delay == 5.0
            assert p.max_retries == 5
            assert p.backoff_factor == 2.0

    def test_swapped_min_max(self):
        with patch.dict(
            "os.environ",
            {
                "KBO_REQUEST_DELAY_MIN": "5.0",
                "KBO_REQUEST_DELAY_MAX": "3.0",
            },
            clear=True,
        ):
            p = RequestPolicy()
            assert p.min_delay == 3.0
            assert p.max_delay == 5.0

    def test_constructor_overrides(self):
        with patch.dict("os.environ", {}, clear=True):
            p = RequestPolicy(min_delay=0.5, max_delay=1.0, max_retries=2, backoff_factor=1.0)
            assert p.min_delay == 0.5
            assert p.max_delay == 1.0
            assert p.max_retries == 2
            assert p.backoff_factor == 1.0

    def test_user_agents_override(self):
        with patch.dict("os.environ", {}, clear=True):
            p = RequestPolicy(user_agents=["Custom UA"])
            assert p.user_agents == ["Custom UA"]

    def test_env_user_agents_csv(self):
        with patch.dict("os.environ", {"KBO_USER_AGENTS": "UA1,UA2,UA3"}, clear=True):
            p = RequestPolicy()
            assert p.user_agents == ["UA1", "UA2", "UA3"]

    def test_env_user_agents_pipe(self):
        with patch.dict("os.environ", {"KBO_USER_AGENTS": "UA1|UA2"}, clear=True):
            p = RequestPolicy()
            assert p.user_agents == ["UA1", "UA2"]

    def test_env_wins_over_constructor_arg(self):
        with patch.dict("os.environ", {"KBO_REQUEST_DELAY_MIN": "3.0", "KBO_REQUEST_DELAY_MAX": "5.0"}, clear=True):
            p = RequestPolicy(min_delay=1.0)
            assert p.min_delay == 3.0  # env takes precedence


class TestUserAgent:
    def test_random_returns_string(self):
        with patch.dict("os.environ", {}, clear=True):
            p = RequestPolicy()
            ua = p.random_user_agent()
            assert isinstance(ua, str)
            assert "Mozilla" in ua


class TestBackoffDelay:
    def test_linear_backoff(self):
        with patch.dict("os.environ", {}, clear=True):
            p = RequestPolicy()
            assert p._backoff_delay(1) == 1.5
            assert p._backoff_delay(2) == 3.0
            assert p._backoff_delay(3) == 4.5

    def test_custom_backoff(self):
        with patch.dict("os.environ", {}, clear=True):
            p = RequestPolicy(backoff_factor=3.0)
            assert p._backoff_delay(1) == 3.0
            assert p._backoff_delay(2) == 6.0


class TestBuildContextKwargs:
    def test_contains_user_agent(self):
        with patch.dict("os.environ", {}, clear=True):
            p = RequestPolicy()
            kwargs = p.build_context_kwargs()
            assert "user_agent" in kwargs
            assert "Mozilla" in kwargs["user_agent"]

    def test_with_overrides(self):
        with patch.dict("os.environ", {}, clear=True):
            p = RequestPolicy()
            kwargs = p.build_context_kwargs(headless=True)
            assert kwargs["headless"] is True
            assert "user_agent" in kwargs


class TestRunWithRetry:
    def test_success_first_attempt(self):
        with patch.dict("os.environ", {}, clear=True):
            p = RequestPolicy()
            func = MagicMock(return_value=42)
            result = p.run_with_retry(func, 1, 2, key="val")
            assert result == 42
            func.assert_called_once_with(1, 2, key="val")

    def test_retry_then_success(self):
        with patch.dict("os.environ", {}, clear=True):
            p = RequestPolicy()
            func = MagicMock(side_effect=[ValueError("fail"), "ok"])
            with patch("time.sleep"):
                result = p.run_with_retry(func)
            assert result == "ok"
            assert func.call_count == 2

    def test_retry_exhausted_raises(self):
        with patch.dict("os.environ", {}, clear=True):
            p = RequestPolicy(max_retries=2)
            func = MagicMock(side_effect=ValueError("fail"))
            with patch("time.sleep"):
                with pytest.raises(ValueError, match="fail"):
                    p.run_with_retry(func)
            assert func.call_count == 2


class TestRunWithRetryAsync:
    @pytest.mark.asyncio
    async def test_success_first_attempt(self):
        with patch.dict("os.environ", {}, clear=True):
            p = RequestPolicy()
            func = AsyncMock(return_value=42)
            with patch("asyncio.sleep"):
                result = await p.run_with_retry_async(func)
            assert result == 42

    @pytest.mark.asyncio
    async def test_retry_then_success(self):
        with patch.dict("os.environ", {}, clear=True):
            p = RequestPolicy()
            func = AsyncMock(side_effect=[ValueError("fail"), "ok"])
            with patch("asyncio.sleep"):
                result = await p.run_with_retry_async(func)
            assert result == "ok"

    @pytest.mark.asyncio
    async def test_retry_exhausted_raises(self):
        with patch.dict("os.environ", {}, clear=True):
            p = RequestPolicy(max_retries=2)
            func = AsyncMock(side_effect=ValueError("fail"))
            with patch("asyncio.sleep"):
                with pytest.raises(ValueError):
                    await p.run_with_retry_async(func)
