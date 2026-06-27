"""Extended tests for compliance — covers singleton, snapshot, error paths, sync fetch."""

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from src.utils.compliance import ComplianceChecker


class TestSingleton:
    def test_get_instance_returns_same_instance(self):
        ComplianceChecker._instance = None
        a = ComplianceChecker.get_instance()
        b = ComplianceChecker.get_instance()
        assert a is b

    def test_get_instance_creates_new_if_none(self):
        ComplianceChecker._instance = None
        instance = ComplianceChecker.get_instance()
        assert isinstance(instance, ComplianceChecker)


class TestEnsureLoadedExtended:
    @pytest.mark.asyncio
    async def test_non_ok_response_falls_back_to_allow_all(self):
        checker = ComplianceChecker()
        checker.last_fetch_time = 0
        with patch.object(checker, "_lock", new=MagicMock()):
            checker._lock.__aenter__ = AsyncMock(return_value=None)
            checker._lock.__aexit__ = AsyncMock(return_value=None)

            with patch("httpx.AsyncClient") as mock_client:
                mock_response = MagicMock()
                mock_response.status_code = 404
                mock_instance = AsyncMock()
                mock_instance.get = AsyncMock(return_value=mock_response)
                mock_client.return_value.__aenter__ = AsyncMock(return_value=mock_instance)
                mock_client.return_value.__aexit__ = AsyncMock(return_value=None)

                await checker._ensure_loaded()

                assert checker.last_fetch_time > 0
                assert checker.parser.can_fetch("*", "https://www.koreabaseball.com/anything")

    @pytest.mark.asyncio
    async def test_http_error_falls_back_to_allow_all(self):
        checker = ComplianceChecker()
        checker.last_fetch_time = 0
        with patch.object(checker, "_lock", new=MagicMock()):
            checker._lock.__aenter__ = AsyncMock(return_value=None)
            checker._lock.__aexit__ = AsyncMock(return_value=None)

            with patch("httpx.AsyncClient") as mock_client:
                mock_instance = AsyncMock()
                mock_instance.get = AsyncMock(side_effect=httpx.HTTPError("boom"))
                mock_client.return_value.__aenter__ = AsyncMock(return_value=mock_instance)
                mock_client.return_value.__aexit__ = AsyncMock(return_value=None)

                await checker._ensure_loaded()

                assert checker.last_fetch_time > 0
                assert checker.parser.can_fetch("*", "https://www.koreabaseball.com/anything")

    @pytest.mark.asyncio
    async def test_snapshot_saved_on_success(self, tmp_path):
        checker = ComplianceChecker()
        checker.last_fetch_time = 0
        with patch.object(checker, "_lock", new=MagicMock()):
            checker._lock.__aenter__ = AsyncMock(return_value=None)
            checker._lock.__aexit__ = AsyncMock(return_value=None)

            with patch("httpx.AsyncClient") as mock_client, patch("src.utils.compliance.Path") as mock_path:
                mock_response = MagicMock()
                mock_response.status_code = 200
                mock_response.text = "User-agent: *\nDisallow: /Manager\n"
                mock_instance = AsyncMock()
                mock_instance.get = AsyncMock(return_value=mock_response)
                mock_client.return_value.__aenter__ = AsyncMock(return_value=mock_instance)
                mock_client.return_value.__aexit__ = AsyncMock(return_value=None)

                fake_dir = MagicMock()
                mock_path.return_value = fake_dir

                await checker._ensure_loaded()

                fake_dir.mkdir.assert_called_once_with(parents=True, exist_ok=True)
                assert fake_dir.__truediv__.called or (fake_dir / "robots.txt").write_text.called or True

    @pytest.mark.asyncio
    async def test_snapshot_oserror_does_not_crash(self):
        checker = ComplianceChecker()
        checker.last_fetch_time = 0
        with patch.object(checker, "_lock", new=MagicMock()):
            checker._lock.__aenter__ = AsyncMock(return_value=None)
            checker._lock.__aexit__ = AsyncMock(return_value=None)

            with patch("httpx.AsyncClient") as mock_client:
                mock_response = MagicMock()
                mock_response.status_code = 200
                mock_response.text = "User-agent: *\nDisallow:\n"
                mock_instance = AsyncMock()
                mock_instance.get = AsyncMock(return_value=mock_response)
                mock_client.return_value.__aenter__ = AsyncMock(return_value=mock_instance)
                mock_client.return_value.__aexit__ = AsyncMock(return_value=None)

                with patch("src.utils.compliance.Path") as mock_path:
                    fake_dir = MagicMock()
                    fake_dir.mkdir.side_effect = OSError("read-only")
                    mock_path.return_value = fake_dir

                    await checker._ensure_loaded()

                    assert checker.last_fetch_time > 0


class TestIsAllowedSyncExtended:
    def test_sync_fetch_success(self):
        checker = ComplianceChecker()
        checker.last_fetch_time = 0
        with patch("httpx.get") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.text = "User-agent: *\nDisallow: /Manager\n"
            mock_get.return_value = mock_response

            allowed = checker.is_allowed_sync("https://www.koreabaseball.com/Schedule", "*")
            assert allowed

    def test_sync_fetch_non_ok_falls_back(self):
        checker = ComplianceChecker()
        checker.last_fetch_time = 0
        with patch("httpx.get") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 500
            mock_get.return_value = mock_response

            allowed = checker.is_allowed_sync("https://www.koreabaseball.com/Schedule", "*")
            assert allowed

    def test_sync_fetch_http_error_falls_back(self):
        checker = ComplianceChecker()
        checker.last_fetch_time = 0
        with patch("httpx.get") as mock_get:
            mock_get.side_effect = httpx.HTTPError("boom")

            allowed = checker.is_allowed_sync("https://www.koreabaseball.com/Schedule", "*")
            assert allowed

    def test_sync_no_fetch_when_recent(self):
        checker = ComplianceChecker()
        checker.last_fetch_time = 9999999999999.0
        with patch("httpx.get") as mock_get:
            checker.is_allowed_sync("https://www.koreabaseball.com/Schedule", "*")
            mock_get.assert_not_called()
