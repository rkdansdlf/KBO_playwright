from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from scripts.maintenance.backfill_futures_team_codes import (
    BATTING_MISSING_QUERY,
    PITCHING_MISSING_QUERY,
    TeamCodeResolution,
    _resolve_batting_team_code,
    _run_backfill,
    parse_career_team,
)


def _query_result(*, rows: list[tuple[str]] | None = None, row: tuple[str] | None = None) -> MagicMock:
    result = MagicMock()
    result.fetchall.return_value = rows or []
    result.fetchone.return_value = row
    return result


@pytest.mark.parametrize(
    ("career", "year", "expected"),
    [
        ("삼성(2010~2015)-한화(2016~2019)-롯데(2020-)", 2014, "SS"),
        ("삼성(2010~2015)-한화(2016~2019)-롯데(2020-)", 2018, "HH"),
        ("삼성(2010~2015)-한화(2016~2019)-롯데(2020-)", 2026, "LT"),
        ("두산 베어스(2017~2018)", 2018, "DB"),
        ("삼성(2010~2015)", 2016, None),
        ("", 2026, None),
    ],
)
def test_parse_career_team_selects_matching_period(career: str, year: int, expected: str | None) -> None:
    assert parse_career_team(career, year) == expected


def test_parse_career_team_rejects_same_year_transfer() -> None:
    assert parse_career_team("삼성(2020~2020)-한화(2020-)", 2020) is None


def test_batting_resolver_uses_unique_same_season_game_evidence() -> None:
    session = MagicMock()
    session.execute.return_value = _query_result(rows=[("SS",)])

    resolution = _resolve_batting_team_code(session, 20, 2025)

    assert resolution == TeamCodeResolution("SS", "same_season_game")
    assert session.execute.call_count == 1


def test_batting_resolver_rejects_conflicting_same_season_game_evidence() -> None:
    session = MagicMock()
    session.execute.return_value = _query_result(rows=[("SS",), ("HH",)])

    resolution = _resolve_batting_team_code(session, 20, 2025)

    assert resolution == TeamCodeResolution(None, "ambiguous_same_season_game")
    assert session.execute.call_count == 1


def test_batting_resolver_uses_matching_futures_pitching_record() -> None:
    session = MagicMock()
    session.execute.side_effect = [
        _query_result(),
        _query_result(rows=[("HH",)]),
    ]

    resolution = _resolve_batting_team_code(session, 20, 2025)

    assert resolution == TeamCodeResolution("HH", "same_season_pitching")


def test_batting_resolver_uses_exact_career_period_not_current_team_or_nearest_season() -> None:
    session = MagicMock()
    session.execute.side_effect = [
        _query_result(),
        _query_result(),
        _query_result(row=("삼성(2010~2015)-한화(2016~2019)-롯데(2020-)",)),
    ]

    resolution = _resolve_batting_team_code(session, 20, 2018)

    assert resolution == TeamCodeResolution("HH", "career_period")


def test_futures_missing_queries_are_league_and_level_scoped() -> None:
    assert "league = 'FUTURES'" in BATTING_MISSING_QUERY
    assert "level = 'KBO2'" in BATTING_MISSING_QUERY
    assert "league = 'FUTURES'" in PITCHING_MISSING_QUERY
    assert "level = 'KBO2'" in PITCHING_MISSING_QUERY


def test_run_backfill_reports_resolved_rows_without_writing_by_default() -> None:
    session = MagicMock()
    session.execute.return_value.fetchall.return_value = [(10, 20, 2025), (11, 21, 2025)]
    resolver = MagicMock(
        side_effect=[
            TeamCodeResolution("SS", "same_season_game"),
            TeamCodeResolution(None, "ambiguous_same_season_game"),
        ],
    )

    with patch("scripts.maintenance.backfill_futures_team_codes.SessionLocal", return_value=session):
        report = _run_backfill(
            table_name="player_season_batting",
            missing_query="SELECT id, player_id, season FROM player_season_batting WHERE team_code IS NULL",
            update_query="UPDATE player_season_batting SET team_code = :code WHERE id = :id",
            resolver=resolver,
        )

    assert resolver.call_args_list[0].args[1:] == (20, 2025)
    assert resolver.call_args_list[1].args[1:] == (21, 2025)
    assert report.resolved == 1
    assert report.applied == 0
    assert report.dry_run is True
    assert report.reason_counts == {"same_season_game": 1, "ambiguous_same_season_game": 1}
    assert session.execute.call_count == 1
    session.commit.assert_not_called()
    session.close.assert_called_once()


def test_run_backfill_applies_only_resolved_rows_when_requested() -> None:
    session = MagicMock()
    session.execute.return_value.fetchall.return_value = [(10, 20, 2025), (11, 21, 2025)]
    resolver = MagicMock(
        side_effect=[TeamCodeResolution("SS", "same_season_game"), TeamCodeResolution(None, "no_evidence")]
    )

    with patch("scripts.maintenance.backfill_futures_team_codes.SessionLocal", return_value=session):
        report = _run_backfill(
            table_name="player_season_batting",
            missing_query="SELECT id, player_id, season FROM player_season_batting WHERE team_code IS NULL",
            update_query="UPDATE player_season_batting SET team_code = :code WHERE id = :id",
            resolver=resolver,
            apply=True,
        )

    assert report.applied == 1
    assert report.dry_run is False
    assert session.execute.call_args_list[-1].args[1] == {"code": "SS", "id": 10}
    session.commit.assert_called_once()
