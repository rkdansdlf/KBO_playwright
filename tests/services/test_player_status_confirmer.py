from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.player_status_confirmer import PlayerStatusConfirmer


@pytest.fixture
def mock_pool():
    pool = MagicMock()
    pool.acquire = AsyncMock()
    pool.release = AsyncMock()
    pool.start = AsyncMock()
    pool.close = AsyncMock()
    return pool


class TestPlayerStatusConfirmer:
    @pytest.mark.asyncio
    async def test_empty_entries(self, mock_pool):
        confirmer = PlayerStatusConfirmer(pool=mock_pool)
        result = await confirmer.confirm_entries([])
        assert result == {"attempted": 0, "confirmed": 0}

    @pytest.mark.asyncio
    async def test_no_suspects(self, mock_pool):
        confirmer = PlayerStatusConfirmer(pool=mock_pool)
        entries = [{"player_id": "1", "status": "active"}]
        result = await confirmer.confirm_entries(entries)
        assert result == {"attempted": 0, "confirmed": 0}

    @pytest.mark.asyncio
    async def test_confirms_retired_entry(self, mock_pool):
        page = AsyncMock()
        page.goto = AsyncMock()
        page.inner_text = AsyncMock(return_value="은퇴 선수 정보")
        mock_pool.acquire.return_value = page
        confirmer = PlayerStatusConfirmer(pool=mock_pool)
        entries = [{"player_id": "123", "status": "retired"}]
        with patch("src.services.player_status_confirmer.parse_status_from_text", return_value=("retired", None)):
            result = await confirmer.confirm_entries(entries)
        assert result == {"attempted": 1, "confirmed": 1}
        assert entries[0]["status"] == "retired"
        assert entries[0]["status_source"] == "profile"

    @pytest.mark.asyncio
    async def test_confirms_staff_entry(self, mock_pool):
        page = AsyncMock()
        page.goto = AsyncMock()
        page.inner_text = AsyncMock(return_value="코치 정보")
        mock_pool.acquire.return_value = page
        confirmer = PlayerStatusConfirmer(pool=mock_pool)
        entries = [{"player_id": "456", "status": "staff"}]
        with patch("src.services.player_status_confirmer.parse_status_from_text", return_value=("staff", "coach")):
            result = await confirmer.confirm_entries(entries)
        assert result == {"attempted": 1, "confirmed": 1}
        assert entries[0]["staff_role"] == "coach"

    @pytest.mark.asyncio
    async def test_no_player_id_skipped(self, mock_pool):
        confirmer = PlayerStatusConfirmer(pool=mock_pool)
        entries = [{"status": "retired"}]
        result = await confirmer.confirm_entries(entries)
        assert result == {"attempted": 0, "confirmed": 0}

    @pytest.mark.asyncio
    async def test_max_confirmations_respected(self, mock_pool):
        page = AsyncMock()
        page.goto = AsyncMock()
        page.inner_text = AsyncMock(return_value="text")
        mock_pool.acquire.return_value = page
        confirmer = PlayerStatusConfirmer(pool=mock_pool, max_confirmations=2)
        entries = [{"player_id": str(i), "status": "retired"} for i in range(10)]
        with patch("src.services.player_status_confirmer.parse_status_from_text", return_value=("retired", None)):
            result = await confirmer.confirm_entries(entries)
        assert result == {"attempted": 2, "confirmed": 2}

    @pytest.mark.asyncio
    async def test_parse_returning_none_skips(self, mock_pool):
        page = AsyncMock()
        page.goto = AsyncMock()
        page.inner_text = AsyncMock(return_value="some text")
        mock_pool.acquire.return_value = page
        confirmer = PlayerStatusConfirmer(pool=mock_pool)
        entries = [{"player_id": "1", "status": "retired"}]
        with patch("src.services.player_status_confirmer.parse_status_from_text", return_value=None):
            result = await confirmer.confirm_entries(entries)
        assert result == {"attempted": 1, "confirmed": 0}
        assert "status_source" not in entries[0]

    @pytest.mark.asyncio
    async def test_owns_pool_starts_and_closes(self):
        with patch("src.services.player_status_confirmer.AsyncPlaywrightPool") as MockPool:
            mock_pool_instance = AsyncMock()
            page = AsyncMock()
            page.goto = AsyncMock()
            page.inner_text = AsyncMock(return_value="text")
            mock_pool_instance.acquire = AsyncMock(return_value=page)
            MockPool.return_value = mock_pool_instance
            confirmer = PlayerStatusConfirmer()
            with patch("src.services.player_status_confirmer.parse_status_from_text", return_value=None):
                await confirmer.confirm_entries([{"player_id": "1", "status": "retired"}])
            mock_pool_instance.start.assert_called_once()
            mock_pool_instance.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_injected_pool_not_closed(self, mock_pool):
        page = AsyncMock()
        page.goto = AsyncMock()
        page.inner_text = AsyncMock(return_value="text")
        mock_pool.acquire.return_value = page
        mock_pool.start.return_value = None
        confirmer = PlayerStatusConfirmer(pool=mock_pool)
        with patch("src.services.player_status_confirmer.parse_status_from_text", return_value=None):
            await confirmer.confirm_entries([{"player_id": "1", "status": "retired"}])
        mock_pool.start.assert_called_once()
        mock_pool.close.assert_not_called()
