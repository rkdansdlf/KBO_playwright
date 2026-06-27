"""Extended tests for request_policy — covers delay, with_delay, error paths."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.utils.request_policy import RequestPolicy, RequestPolicyConfig


class TestInitExtended:
    def test_both_config_and_overrides_raises(self):
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(TypeError, match="Pass either RequestPolicyConfig"):
                RequestPolicy(RequestPolicyConfig(), min_delay=1.0)

    def test_with_delay_classmethod(self):
        with patch.dict("os.environ", {}, clear=True):
            p = RequestPolicy.with_delay(0.5, 1.0)
            assert p.min_delay == 0.5
            assert p.max_delay == 1.0

    def test_with_delay_none_max(self):
        with patch.dict("os.environ", {}, clear=True):
            p = RequestPolicy.with_delay(2.0)
            assert p.min_delay == 2.0

    def test_user_agents_override_empty_list(self):
        with patch.dict("os.environ", {}, clear=True):
            from src.utils.request_policy import DEFAULT_USER_AGENTS

            p = RequestPolicy(user_agents=[])
            assert p.user_agents == DEFAULT_USER_AGENTS

    def test_user_agents_override_whitespace_only(self):
        with patch.dict("os.environ", {}, clear=True):
            from src.utils.request_policy import DEFAULT_USER_AGENTS

            p = RequestPolicy(user_agents=["  ", ""])
            assert p.user_agents == DEFAULT_USER_AGENTS

    def test_env_user_agents_empty_string(self):
        with patch.dict("os.environ", {"KBO_USER_AGENTS": ""}, clear=True):
            from src.utils.request_policy import DEFAULT_USER_AGENTS

            p = RequestPolicy()
            assert p.user_agents == DEFAULT_USER_AGENTS

    def test_retry_exceptions_custom(self):
        with patch.dict("os.environ", {}, clear=True):
            p = RequestPolicy(retry_exceptions=(ValueError,))
            assert p.retry_exceptions == (ValueError,)


class TestDelay:
    def test_delay_calls_throttle(self):
        with patch.dict("os.environ", {}, clear=True):
            p = RequestPolicy(min_delay=1.0, max_delay=2.0)
            with patch("src.utils.request_policy.throttle") as mock_throttle:
                p.delay("example.com")
                assert mock_throttle.default_delay == 1.0
                mock_throttle.wait_sync.assert_called_once_with("example.com")

    def test_delay_default_host(self):
        with patch.dict("os.environ", {}, clear=True):
            p = RequestPolicy(min_delay=1.0, max_delay=2.0)
            with patch("src.utils.request_policy.throttle") as mock_throttle:
                p.delay()
                mock_throttle.wait_sync.assert_called_once_with("koreabaseball.com")


class TestDelayAsync:
    @pytest.mark.asyncio
    async def test_delay_async_calls_throttle(self):
        with patch.dict("os.environ", {}, clear=True):
            p = RequestPolicy(min_delay=1.0, max_delay=2.0)
            with patch("src.utils.request_policy.throttle") as mock_throttle:
                mock_throttle.wait = AsyncMock()
                await p.delay_async("example.com")
                assert mock_throttle.default_delay == 1.0
                mock_throttle.wait.assert_awaited_once_with("example.com")

    @pytest.mark.asyncio
    async def test_delay_async_default_host(self):
        with patch.dict("os.environ", {}, clear=True):
            p = RequestPolicy(min_delay=1.0, max_delay=2.0)
            with patch("src.utils.request_policy.throttle") as mock_throttle:
                mock_throttle.wait = AsyncMock()
                await p.delay_async()
                mock_throttle.wait.assert_awaited_once_with("koreabaseball.com")


class TestRunWithRetryExtended:
    def test_retry_exhausted_raises_last_exception(self):
        with patch.dict("os.environ", {}, clear=True):
            p = RequestPolicy(max_retries=3)
            func = MagicMock(side_effect=ValueError("final"))
            with patch("time.sleep"):
                with pytest.raises(ValueError, match="final"):
                    p.run_with_retry(func)
            assert func.call_count == 3

    def test_retry_with_custom_exceptions(self):
        with patch.dict("os.environ", {}, clear=True):
            p = RequestPolicy(max_retries=2, retry_exceptions=(KeyError,))
            func = MagicMock(side_effect=KeyError("missing"))
            with patch("time.sleep"):
                with pytest.raises(KeyError):
                    p.run_with_retry(func)

    def test_retry_non_matching_exception_propagates_immediately(self):
        with patch.dict("os.environ", {}, clear=True):
            p = RequestPolicy(max_retries=3, retry_exceptions=(ValueError,))
            func = MagicMock(side_effect=TypeError("wrong"))
            with patch("time.sleep") as mock_sleep:
                with pytest.raises(TypeError):
                    p.run_with_retry(func)
            mock_sleep.assert_not_called()
            assert func.call_count == 1


class TestRunWithRetryAsyncExtended:
    @pytest.mark.asyncio
    async def test_retry_exhausted_raises_last_exception(self):
        with patch.dict("os.environ", {}, clear=True):
            p = RequestPolicy(max_retries=3)
            func = AsyncMock(side_effect=ValueError("final"))
            with patch("asyncio.sleep"):
                with pytest.raises(ValueError, match="final"):
                    await p.run_with_retry_async(func)
            assert func.call_count == 3

    @pytest.mark.asyncio
    async def test_retry_with_custom_exceptions(self):
        with patch.dict("os.environ", {}, clear=True):
            p = RequestPolicy(max_retries=2, retry_exceptions=(KeyError,))
            func = AsyncMock(side_effect=KeyError("missing"))
            with patch("asyncio.sleep"):
                with pytest.raises(KeyError):
                    await p.run_with_retry_async(func)

    @pytest.mark.asyncio
    async def test_retry_non_matching_exception_propagates_immediately(self):
        with patch.dict("os.environ", {}, clear=True):
            p = RequestPolicy(max_retries=3, retry_exceptions=(ValueError,))
            func = AsyncMock(side_effect=TypeError("wrong"))
            with patch("asyncio.sleep") as mock_sleep:
                with pytest.raises(TypeError):
                    await p.run_with_retry_async(func)
            mock_sleep.assert_not_called()
            assert func.call_count == 1
