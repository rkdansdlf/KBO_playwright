from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.crawlers.team_history_crawler import TeamHistoryCrawler


def _locator(*, count: int, text: str = "", alt: str | None = None, src: str | None = None) -> MagicMock:
    locator = MagicMock()
    locator.count = AsyncMock(return_value=count)
    locator.inner_text = AsyncMock(return_value=text)
    locator.get_attribute = AsyncMock(side_effect=[alt, src])
    return locator


@pytest.mark.asyncio
async def test_crawl_raises_when_start_does_not_initialize_page() -> None:
    crawler = TeamHistoryCrawler()
    crawler.start = AsyncMock()

    with pytest.raises(RuntimeError, match="Page not initialized"):
        await crawler.crawl()


@pytest.mark.asyncio
async def test_history_parsers_handle_missing_and_invalid_values() -> None:
    crawler = TeamHistoryCrawler()
    missing_year_row = MagicMock()
    missing_year_row.locator.return_value = _locator(count=0)
    invalid_year_row = MagicMock()
    invalid_year_row.locator.return_value = _locator(count=1, text="unknown")

    assert await crawler._parse_history_year(missing_year_row) is None
    assert await crawler._parse_history_year(invalid_year_row) is None

    cell = MagicMock()
    cell.locator.return_value = _locator(count=0)
    assert await crawler._parse_rank(cell) is None

    invalid_rank = MagicMock()
    invalid_rank.locator.return_value = _locator(count=1, text="-")
    assert await crawler._parse_rank(invalid_rank) is None


@pytest.mark.asyncio
async def test_team_identity_uses_image_name_then_text_fallback() -> None:
    crawler = TeamHistoryCrawler()
    image = _locator(count=1, alt="Twins", src="/twins.png")
    name = _locator(count=1, text="Bears")
    image_cell = MagicMock()
    image_cell.locator.side_effect = lambda selector: image if selector == "img" else name

    assert await crawler._parse_team_identity(image_cell) == ("Twins", "/twins.png")

    no_image = _locator(count=0)
    text_cell = MagicMock()
    text_cell.locator.side_effect = lambda selector: no_image if selector == "img" else name
    assert await crawler._parse_team_identity(text_cell) == ("Bears", None)


@pytest.mark.asyncio
async def test_parse_history_cell_preserves_slot_identity_between_rows() -> None:
    crawler = TeamHistoryCrawler()
    crawler._parse_rank = AsyncMock(side_effect=[1, 2])
    crawler._parse_team_identity = AsyncMock(side_effect=[("Twins", "/twins.png"), (None, None)])
    slots = [{"name": None, "logo": None}]

    first = await crawler._parse_history_cell(MagicMock(), 0, 2024, slots)
    second = await crawler._parse_history_cell(MagicMock(), 0, 2025, slots)

    assert first == {
        "season": 2024,
        "team_name": "Twins",
        "logo_url": "/twins.png",
        "ranking": 1,
        "slot_index": 0,
    }
    assert second == {
        "season": 2025,
        "team_name": "Twins",
        "logo_url": "/twins.png",
        "ranking": 2,
        "slot_index": 0,
    }


@pytest.mark.asyncio
async def test_save_skips_unresolved_and_unmapped_teams() -> None:
    crawler = TeamHistoryCrawler()
    session = MagicMock()
    session.execute.return_value.scalars.return_value.all.return_value = []
    data = [{"season": 2024, "team_name": "Unknown", "logo_url": None, "ranking": 1}]

    with patch("src.crawlers.team_history_crawler.SessionLocal") as session_local:
        session_local.return_value.__enter__.return_value = session
        with patch("src.crawlers.team_history_crawler.save_raw_snapshots", return_value=0):
            with patch("src.crawlers.team_history_crawler.resolve_team_code", return_value=None):
                await crawler.save(data)
            with patch("src.crawlers.team_history_crawler.resolve_team_code", return_value="LG"):
                await crawler.save(data)

    session.add.assert_not_called()
    assert session.commit.call_count == 2


@pytest.mark.asyncio
async def test_save_updates_existing_history_entry() -> None:
    crawler = TeamHistoryCrawler()
    session = MagicMock()
    team = MagicMock(team_id="LG", franchise_id=3)
    existing = MagicMock()
    team_result = MagicMock()
    team_result.scalars.return_value.all.return_value = [team]
    existing_result = MagicMock()
    existing_result.scalars.return_value.first.return_value = existing
    session.execute.side_effect = [team_result, existing_result]
    data = [{"season": 2024, "team_name": "LG Twins", "logo_url": "/new.png", "ranking": 1}]

    with patch("src.crawlers.team_history_crawler.SessionLocal") as session_local:
        session_local.return_value.__enter__.return_value = session
        with patch("src.crawlers.team_history_crawler.save_raw_snapshots", return_value=1):
            with patch("src.crawlers.team_history_crawler.resolve_team_code", return_value="LG"):
                await crawler.save(data)

    assert existing.team_name == "LG Twins"
    assert existing.logo_url == "/new.png"
    assert existing.ranking == 1
    assert existing.franchise_id == 3
    session.add.assert_not_called()
