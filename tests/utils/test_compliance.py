"""Tests for compliance — robots.txt checker."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.utils.compliance import ComplianceChecker


class TestComplianceChecker:
    @pytest.mark.asyncio
    async def test_is_allowed_blocks_disallowed(self):
        checker = ComplianceChecker()
        checker.last_fetch_time = 1  # pretend loaded
        checker.parser = MagicMock()
        checker.parser.can_fetch.return_value = False
        allowed = await checker.is_allowed("https://www.koreabaseball.com/Manager", "*")
        assert not allowed

    @pytest.mark.asyncio
    async def test_is_allowed_allows_allowed(self):
        checker = ComplianceChecker()
        checker.last_fetch_time = 1
        checker.parser = MagicMock()
        checker.parser.can_fetch.return_value = True
        allowed = await checker.is_allowed("https://www.koreabaseball.com/Schedule", "*")
        assert allowed

    def test_is_allowed_sync_blocks_disallowed(self):
        checker = ComplianceChecker()
        checker.last_fetch_time = 1
        checker.parser = MagicMock()
        checker.parser.can_fetch.return_value = False
        allowed = checker.is_allowed_sync("https://www.koreabaseball.com/Manager", "*")
        assert not allowed

    def test_is_allowed_sync_allows_allowed(self):
        checker = ComplianceChecker()
        checker.last_fetch_time = 1
        checker.parser = MagicMock()
        checker.parser.can_fetch.return_value = True
        allowed = checker.is_allowed_sync("https://www.koreabaseball.com/Schedule", "*")
        assert allowed

    @pytest.mark.asyncio
    async def test_ensure_loaded_fetches_when_expired(self):
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
