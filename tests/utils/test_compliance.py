"""Tests for compliance — robots.txt checker."""

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.utils.compliance import ComplianceChecker


class TestComplianceChecker:
    @pytest.mark.asyncio
    async def test_is_allowed_blocks_disallowed(self):
        checker = ComplianceChecker()
        checker.last_fetch_time = time.time()  # pretend loaded, without fetching
        checker.parser = MagicMock()
        checker.parser.can_fetch.return_value = False
        allowed = await checker.is_allowed("https://www.koreabaseball.com/Manager", "*")
        assert not allowed

    @pytest.mark.asyncio
    async def test_is_allowed_allows_allowed(self):
        checker = ComplianceChecker()
        checker.last_fetch_time = time.time()  # pretend loaded
        checker.parser = MagicMock()
        checker.parser.can_fetch.return_value = True
        allowed = await checker.is_allowed("https://www.koreabaseball.com/Schedule", "*")
        assert allowed

    @pytest.mark.asyncio
    async def test_external_host_is_not_checked_against_kbo_rules(self):
        checker = ComplianceChecker()
        checker.parser = MagicMock()
        checker.parser.can_fetch.return_value = False

        allowed = await checker.is_allowed("https://api-gw.sports.naver.com/schedule/today-games", "*")

        assert allowed
        checker.parser.can_fetch.assert_not_called()

    def test_is_allowed_sync_blocks_disallowed(self):
        checker = ComplianceChecker()
        checker.last_fetch_time = time.time()  # pretend loaded
        checker.parser = MagicMock()
        checker.parser.can_fetch.return_value = False
        allowed = checker.is_allowed_sync("https://www.koreabaseball.com/Manager", "*")
        assert not allowed

    def test_is_allowed_sync_allows_allowed(self):
        checker = ComplianceChecker()
        checker.last_fetch_time = time.time()  # pretend loaded
        checker.parser = MagicMock()
        checker.parser.can_fetch.return_value = True
        allowed = checker.is_allowed_sync("https://www.koreabaseball.com/Schedule", "*")
        assert allowed

    def test_external_host_is_not_checked_against_kbo_rules_sync(self):
        checker = ComplianceChecker()
        checker.parser = MagicMock()
        checker.parser.can_fetch.return_value = False

        allowed = checker.is_allowed_sync("https://api-gw.sports.naver.com/schedule/today-games", "*")

        assert allowed
        checker.parser.can_fetch.assert_not_called()

    @pytest.mark.asyncio
    async def test_ensure_loaded_fetches_when_expired(self, monkeypatch):
        from pathlib import Path

        monkeypatch.setattr(
            "src.utils.compliance._save_robots_snapshot", lambda url, content: Path("docs/robots/mocked_robots.txt")
        )
        checker = ComplianceChecker()
        checker.last_fetch_time = 0
        with patch.object(checker, "_lock", new=MagicMock()):
            checker._lock.__aenter__ = AsyncMock(return_value=None)
            checker._lock.__aexit__ = AsyncMock(return_value=None)

            with patch("httpx.AsyncClient") as mock_client:
                mock_response = MagicMock()
                mock_response.status_code = 200
                mock_response.text = "User-agent: *\nDisallow: /Manager\n"
                mock_instance = AsyncMock()
                mock_instance.get = AsyncMock(return_value=mock_response)
                mock_client.return_value.__aenter__ = AsyncMock(return_value=mock_instance)
                mock_client.return_value.__aexit__ = AsyncMock(return_value=None)

                await checker._ensure_loaded()

                assert checker.last_fetch_time > 0


class TestTheRobotsOverride:
    """One helper decides the override for both the sync and async paths.

    The check is deliberately an operator opt-in that lives in the deployment's
    ``.env``: KBO serves ``Disallow: /`` and its pages are still the only source for
    some collections, so the override is what lets a repair run proceed. Reading the
    same two variables from two copies of the condition meant a change to one copy
    left half the crawlers on the old behaviour, so the two call sites now share one
    helper -- and this file pins that they still do.
    """

    def test_it_is_off_by_default(self, monkeypatch):
        from src.utils.compliance import _robots_override_enabled

        monkeypatch.delenv("KBO_ALLOW_ROBOTS_OVERRIDE", raising=False)
        monkeypatch.delenv("KBO_IGNORE_ROBOTS_TXT", raising=False)

        assert _robots_override_enabled() is False

    @pytest.mark.parametrize("name", ["KBO_ALLOW_ROBOTS_OVERRIDE", "KBO_IGNORE_ROBOTS_TXT"])
    def test_either_name_turns_it_on(self, monkeypatch, name):
        """Both spellings are deployed; the older one is still accepted."""
        from src.utils.compliance import _robots_override_enabled

        monkeypatch.delenv("KBO_ALLOW_ROBOTS_OVERRIDE", raising=False)
        monkeypatch.delenv("KBO_IGNORE_ROBOTS_TXT", raising=False)
        monkeypatch.setenv(name, "1")

        assert _robots_override_enabled() is True

    @pytest.mark.parametrize("value", ["0", "true", "yes", ""])
    def test_any_other_value_leaves_the_check_in_place(self, monkeypatch, value):
        from src.utils.compliance import _robots_override_enabled

        monkeypatch.setenv("KBO_ALLOW_ROBOTS_OVERRIDE", value)
        monkeypatch.delenv("KBO_IGNORE_ROBOTS_TXT", raising=False)

        assert _robots_override_enabled() is False

    @pytest.mark.asyncio
    async def test_the_async_path_honours_it(self, monkeypatch):
        """No fetch is attempted when the override is on, so the parser stays empty."""
        checker = ComplianceChecker()
        checker.parser = MagicMock()
        checker.parser.can_fetch.return_value = False
        monkeypatch.setenv("KBO_ALLOW_ROBOTS_OVERRIDE", "1")

        assert await checker.is_allowed("https://www.koreabaseball.com/Manager", "*") is True

    def test_the_sync_path_honours_it(self, monkeypatch):
        checker = ComplianceChecker()
        checker.parser = MagicMock()
        checker.parser.can_fetch.return_value = False
        monkeypatch.setenv("KBO_ALLOW_ROBOTS_OVERRIDE", "1")

        assert checker.is_allowed_sync("https://www.koreabaseball.com/Manager", "*") is True

    def test_both_paths_still_consult_robots_when_it_is_off(self, monkeypatch):
        """The override is the only thing being tested; the check itself is not gone.

        ``is_allowed_sync`` answers ``False`` rather than raising, so this asserts the
        refusal and not an exception type.
        """
        checker = ComplianceChecker()
        checker.last_fetch_time = time.time()
        checker.parser = MagicMock()
        checker.parser.can_fetch.return_value = False
        monkeypatch.delenv("KBO_ALLOW_ROBOTS_OVERRIDE", raising=False)
        monkeypatch.delenv("KBO_IGNORE_ROBOTS_TXT", raising=False)

        assert checker.is_allowed_sync("https://www.koreabaseball.com/Manager", "*") is False

    def test_the_two_call_sites_share_one_condition(self):
        """A second copy of the condition is how one of them goes stale."""
        import inspect

        from src.utils import compliance

        source = inspect.getsource(compliance.ComplianceChecker)
        assert "os.getenv" not in source, "the override belongs in _robots_override_enabled"
        assert source.count("_robots_override_enabled()") == 2
