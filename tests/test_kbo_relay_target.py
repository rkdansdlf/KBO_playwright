"""Unit tests for KboRelayTarget and canonical URL resolution."""

from __future__ import annotations

import pytest
from src.utils.kbo_relay_target import (
    KboRelayTarget,
    resolve_kbo_relay_target,
)


def test_regular_season_official_href_resolution() -> None:
    """Validate parsing from official regular season href with seriesId=0."""
    href = "https://www.koreabaseball.com/Game/LiveText.aspx?leagueId=1&seriesId=0&gameId=20240930NCHT0&gyear=2024"
    target = resolve_kbo_relay_target(href=href)

    assert target.game_id == "20240930NCHT0"
    assert target.gyear == 2024
    assert target.league_id == 1
    assert target.series_id == 0
    assert target.endpoint_path == "/Game/LiveText.aspx"
    assert target.resolved_from == "official_href"
    assert target.is_futures is False
    assert target.to_url() == href


def test_exhibition_official_href_preserves_series_id_1() -> None:
    """Validate that seriesId=1 in official href is strictly preserved and not overwritten."""
    href = "https://www.koreabaseball.com/Game/LiveText.aspx?leagueId=1&seriesId=1&gameId=20260315SSOB0&gyear=2026"
    target = resolve_kbo_relay_target(href=href)

    assert target.game_id == "20260315SSOB0"
    assert target.gyear == 2026
    assert target.league_id == 1
    assert target.series_id == 1
    assert target.resolved_from == "official_href"
    assert target.to_url() == href


def test_scoreboard_link_relative_url() -> None:
    """Validate parsing from relative scoreboard link."""
    link = "/Game/LiveText.aspx?leagueId=1&seriesId=0&gameId=20240930NCHT0&gyear=2024"
    target = resolve_kbo_relay_target(scoreboard_link=link)

    assert target.game_id == "20240930NCHT0"
    assert target.series_id == 0
    assert target.resolved_from == "scoreboard_link"
    assert target.to_url() == f"https://www.koreabaseball.com{link}"


def test_postseason_fixture_preserves_series_id() -> None:
    """Validate resolution from verified postseason fixture preserving Korean Series series_id=7."""
    fixture = {
        "game_id": "20241028HTSS0",
        "gyear": 2024,
        "league_id": 1,
        "series_id": 7,
    }
    target = resolve_kbo_relay_target(fixture=fixture)

    assert target.game_id == "20241028HTSS0"
    assert target.series_id == 7
    assert target.resolved_from == "verified_target_fixture"
    assert "seriesId=7" in target.to_url()


def test_futures_fixture_preserves_endpoint_and_league_id() -> None:
    """Validate resolution from Futures League fixture."""
    fixture = {
        "game_id": "20250510LGNC0",
        "gyear": 2025,
        "league_id": 2,
        "series_id": 0,
        "endpoint_path": "/Futures/Schedule/LiveText.aspx",
    }
    target = resolve_kbo_relay_target(fixture=fixture)

    assert target.league_id == 2
    assert target.series_id == 0
    assert target.is_futures is True
    assert target.endpoint_path == "/Futures/Schedule/LiveText.aspx"
    assert "/Futures/Schedule/LiveText.aspx" in target.to_url()


def test_metadata_dict_resolution() -> None:
    """Validate resolution from structured GameCenter and schedule metadata."""
    gc_meta = {
        "game_id": "20240930NCHT0",
        "gyear": 2024,
        "league_id": 1,
        "series_id": 0,
    }
    target = resolve_kbo_relay_target(gamecenter_meta=gc_meta)
    assert target.resolved_from == "gamecenter_meta"
    assert target.series_id == 0

    sch_meta = {
        "game_id": "20240930NCHT0",
        "gyear": 2024,
        "league_id": 1,
        "series_id": 0,
    }
    target_sch = resolve_kbo_relay_target(schedule_meta=sch_meta)
    assert target_sch.resolved_from == "schedule_metadata"


def test_fail_closed_on_unresolved_metadata_without_evidence() -> None:
    """Validate that passing bare game_id without evidence raises R2_TARGET_METADATA_UNRESOLVED."""
    with pytest.raises(ValueError, match="R2_TARGET_METADATA_UNRESOLVED"):
        resolve_kbo_relay_target(game_id="20240930NCHT0")


def test_fail_closed_on_missing_url_params() -> None:
    """Validate fail-closed when href is missing required query parameters."""
    missing_series = "https://www.koreabaseball.com/Game/LiveText.aspx?leagueId=1&gameId=20240930NCHT0&gyear=2024"
    with pytest.raises(ValueError, match="missing 'seriesId'"):
        resolve_kbo_relay_target(href=missing_series)

    missing_league = "https://www.koreabaseball.com/Game/LiveText.aspx?seriesId=0&gameId=20240930NCHT0&gyear=2024"
    with pytest.raises(ValueError, match="missing 'leagueId'"):
        resolve_kbo_relay_target(href=missing_league)


def test_fail_closed_on_unauthorized_host() -> None:
    """Validate fail-closed when href has unauthorized host."""
    bad_host = "https://evil-spoof.com/Game/LiveText.aspx?leagueId=1&seriesId=0&gameId=20240930NCHT0&gyear=2024"
    with pytest.raises(ValueError, match="Unauthorized host"):
        resolve_kbo_relay_target(href=bad_host)


def test_fail_closed_on_invalid_endpoint_path() -> None:
    """Validate fail-closed when href path is not LiveText.aspx."""
    bad_path = (
        "https://www.koreabaseball.com/Game/Scoreboard.aspx?leagueId=1&seriesId=0&gameId=20240930NCHT0&gyear=2024"
    )
    with pytest.raises(ValueError, match="Invalid endpoint path"):
        resolve_kbo_relay_target(href=bad_path)


def test_immutability_and_validation() -> None:
    """Validate dataclass immutability and attribute validation."""
    target = KboRelayTarget(
        game_id="20240930NCHT0",
        gyear=2024,
        league_id=1,
        series_id=0,
        endpoint_path="/Game/LiveText.aspx",
        resolved_from="manual_unit_test",
    )
    with pytest.raises((AttributeError, Exception)):  # FrozenInstanceError
        target.series_id = 1  # type: ignore[misc]
