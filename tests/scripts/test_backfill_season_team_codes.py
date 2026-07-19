from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from scripts.maintenance.backfill_season_team_codes import (
    BATTING_MISSING_QUERY,
    PITCHING_MISSING_QUERY,
    TeamCodeResolution,
    _build_update_query,
    _resolve_batting_team_code,
    _resolve_from_player_team,
    _resolve_pitching_team_code,
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


def test_batting_resolver_falls_back_to_unique_same_season_roster() -> None:
    session = MagicMock()
    session.execute.side_effect = [
        _query_result(),
        _query_result(rows=[("KH",)]),
    ]

    resolution = _resolve_batting_team_code(session, 20, 2025)

    assert resolution == TeamCodeResolution("KH", "same_season_roster")


def test_batting_resolver_rescues_ambiguous_roster_via_current_team() -> None:
    # Conflicting roster evidence is overridden by the player's current team
    # (best-effort last resort) when it yields a single canonical code.
    session = MagicMock()
    session.execute.side_effect = [
        _query_result(),
        _query_result(rows=[("KH",), ("LT",)]),
        _query_result(row=("두산",)),
    ]

    resolution = _resolve_batting_team_code(session, 52204, 2026)

    assert resolution == TeamCodeResolution("DB", "current_team")
    # game, roster, team -> 3 execute calls (career is skipped when roster is set).
    assert session.execute.call_count == 3


def test_batting_resolver_preserves_ambiguous_roster_without_current_team() -> None:
    # When even the current team is unknown, the ambiguous reason is preserved.
    session = MagicMock()
    session.execute.side_effect = [
        _query_result(),
        _query_result(rows=[("KH",), ("LT",)]),
        _query_result(row=(None,)),
    ]

    resolution = _resolve_batting_team_code(session, 665, 2025)

    assert resolution == TeamCodeResolution(None, "ambiguous_same_season_roster")
    assert session.execute.call_count == 3


def test_batting_resolver_uses_exact_career_period_when_no_game_or_roster() -> None:
    session = MagicMock()
    session.execute.side_effect = [
        _query_result(),
        _query_result(),
        _query_result(row=("삼성(2010~2015)-한화(2016~2019)-롯데(2020-)",)),
    ]

    resolution = _resolve_batting_team_code(session, 20, 2018)

    assert resolution == TeamCodeResolution("HH", "career_period")


def test_pitching_resolver_uses_unique_same_season_game_evidence() -> None:
    session = MagicMock()
    session.execute.return_value = _query_result(rows=[("NC",)])

    resolution = _resolve_pitching_team_code(session, 30, 2024)

    assert resolution == TeamCodeResolution("NC", "same_season_game")


def test_missing_queries_are_not_league_scoped() -> None:
    # Regular player_season rows (any league) are in scope, not only Futures.
    assert "league = 'FUTURES'" not in BATTING_MISSING_QUERY
    assert "league = 'FUTURES'" not in PITCHING_MISSING_QUERY
    assert "team_code IS NULL" in BATTING_MISSING_QUERY
    assert "team_code IS NULL" in PITCHING_MISSING_QUERY


def test_run_backfill_reports_resolved_rows_without_writing_by_default() -> None:
    session = MagicMock()
    session.execute.return_value.fetchall.return_value = [(10, 20, 2025), (11, 21, 2025)]
    resolver = MagicMock(
        side_effect=[
            TeamCodeResolution("SS", "same_season_game"),
            TeamCodeResolution(None, "ambiguous_same_season_game"),
        ],
    )

    with patch("scripts.maintenance.backfill_season_team_codes.SessionLocal", return_value=session):
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
    session.commit.assert_not_called()
    session.close.assert_called_once()


def test_run_backfill_applies_only_resolved_rows_when_requested() -> None:
    session = MagicMock()
    session.execute.return_value.fetchall.return_value = [(10, 20, 2025), (11, 21, 2025)]
    resolver = MagicMock(
        side_effect=[TeamCodeResolution("SS", "same_season_game"), TeamCodeResolution(None, "no_evidence")],
    )

    with patch("scripts.maintenance.backfill_season_team_codes.SessionLocal", return_value=session):
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


def test_run_backfill_filters_by_year_when_requested() -> None:
    session = MagicMock()
    session.execute.return_value.fetchall.return_value = [(10, 20, 2025), (11, 21, 2024)]
    resolver = MagicMock(return_value=TeamCodeResolution("SS", "same_season_game"))

    with patch("scripts.maintenance.backfill_season_team_codes.SessionLocal", return_value=session):
        report = _run_backfill(
            table_name="player_season_batting",
            missing_query="SELECT id, player_id, season FROM player_season_batting WHERE team_code IS NULL",
            update_query="UPDATE player_season_batting SET team_code = :code WHERE id = :id",
            resolver=resolver,
            year=2025,
        )

    assert report.total == 1
    assert report.resolved == 1
    resolver.assert_called_once_with(session, 20, 2025)


def test_resolve_from_player_team_normalizes_current_team_name() -> None:
    session = MagicMock()
    session.execute.return_value.fetchone.return_value = ("두산",)

    resolution = _resolve_from_player_team(session, 52204, 2026)

    assert resolution == TeamCodeResolution("DB", "current_team")


def test_resolve_from_player_team_handles_unknown_name() -> None:
    session = MagicMock()
    session.execute.return_value.fetchone.return_value = ("UNKNOWN_TEAM",)

    resolution = _resolve_from_player_team(session, 99, 2025)

    assert resolution == TeamCodeResolution(None, "unknown_team_name")


def test_resolve_from_player_team_handles_missing_team() -> None:
    session = MagicMock()
    session.execute.return_value.fetchone.return_value = (None,)

    resolution = _resolve_from_player_team(session, 665, 2025)

    assert resolution == TeamCodeResolution(None, "missing_team_evidence")


def test_batting_resolver_falls_through_to_current_team() -> None:
    # game + roster + career give no evidence; the player's current team resolves it.
    session = MagicMock()
    game_qr = _query_result(rows=[])
    roster_qr = _query_result(rows=[])
    career_qr = _query_result(row=(None,))
    team_qr = _query_result(row=("두산",))
    session.execute.side_effect = [game_qr, roster_qr, career_qr, team_qr]

    resolution = _resolve_batting_team_code(session, 52204, 2026)

    assert resolution == TeamCodeResolution("DB", "current_team")
    # game, roster, career, team -> 4 execute calls in order.
    assert session.execute.call_count == 4


def test_batting_resolver_uses_oci_game_stats_when_player_game_table_is_missing() -> None:
    session = MagicMock()
    session.execute.return_value = _query_result(rows=[("KT",)])

    with patch(
        "scripts.maintenance.backfill_season_team_codes._available_tables",
        return_value={"game_batting_stats", "player_basic"},
    ):
        resolution = _resolve_batting_team_code(session, 2365, 2021)

    assert resolution == TeamCodeResolution("KT", "same_season_game")
    session.execute.assert_called_once()
    assert "game_batting_stats" in str(session.execute.call_args.args[0])


def test_batting_resolver_skips_missing_oci_evidence_tables() -> None:
    session = MagicMock()
    session.execute.side_effect = [
        _query_result(row=(None,)),
        _query_result(row=("두산",)),
    ]

    with patch(
        "scripts.maintenance.backfill_season_team_codes._available_tables",
        return_value={"player_basic", "player_season_batting"},
    ):
        resolution = _resolve_batting_team_code(session, 60181, 2021)

    assert resolution == TeamCodeResolution("DB", "current_team")
    assert session.execute.call_count == 2


def test_build_update_query_omits_missing_canonical_column() -> None:
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import Session

    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE player_season_batting (id INTEGER, team_code TEXT, updated_at TEXT)"))
    with Session(engine) as session:
        query = _build_update_query(session, "player_season_batting")

    assert "team_code = :code" in query
    assert "updated_at = CURRENT_TIMESTAMP" in query
    assert "canonical_team_code" not in query
