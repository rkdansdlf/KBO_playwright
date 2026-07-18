"""Unit tests for the 2009-2025 completeness audit helpers."""

from __future__ import annotations

from sqlalchemy import create_engine, text

from scripts.maintenance.audit_completeness_2009_2025 import (
    _count_by_dimension,
    _expected_games_per_team,
    _remediation_for,
    check_season_aggregates,
    check_team_code_null_rate,
    render_markdown,
)


def test_expected_games_per_team_eras() -> None:
    assert _expected_games_per_team(2014) == 133
    assert _expected_games_per_team(2015) == 144
    assert _expected_games_per_team(2009) == 133


def test_remediation_for_known_and_unknown_dimension() -> None:
    assert "crawl_schedule" in _remediation_for("missing_parent_games")
    assert "backfill_player_ids" in _remediation_for("regression:game_pitching_null_player_id")
    assert _remediation_for("coverage:game_events") == _remediation_for("coverage:game_events")
    assert _remediation_for("totally_unknown_dimension") == "manual review"


def test_count_by_dimension_tallies_and_sorts() -> None:
    findings = [
        {"dimension": "a", "count": 3},
        {"dimension": "b", "count": 1},
        {"dimension": "a", "count": 2},
    ]
    assert _count_by_dimension(findings) == {"a": 5, "b": 1}


def test_render_markdown_summarizes_defects_and_limitations() -> None:
    report = {
        "metadata": {"start_year": 2024, "end_year": 2024, "league_types": "all (0-5)"},
        "summary": {
            "total_checks": 3,
            "ok": 1,
            "defects": 1,
            "known_limitations": 1,
            "defect_counts_by_dimension": {"coverage:game_lineups": 2},
        },
        "defects": [
            {
                "year": 2024,
                "dimension": "coverage:game_lineups",
                "classification": "DEFECT",
                "count": 2,
                "detail": "missing lineups",
                "remediation": "python3 -m src.cli.collect_games --year <Y> --month <M>",
            },
        ],
        "known_limitations": [
            {
                "year": 2024,
                "dimension": "coverage:game_play_by_play",
                "detail": "pbp absent",
            },
        ],
    }
    md = render_markdown(report)
    assert "2024" in md
    assert "DEFECT" in md
    assert "KNOWN_LIMITATION" in md
    assert "coverage:game_lineups: 2" in md
    assert "python3 -m src.cli.collect_games" in md


def test_season_aggregate_check_scopes_player_games_to_each_year() -> None:
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE game (game_id TEXT, season_id INTEGER)"))
        conn.execute(
            text(
                "CREATE TABLE kbo_seasons (season_id INTEGER, season_year INTEGER, league_type_code INTEGER)",
            ),
        )
        conn.execute(text("CREATE TABLE player_game_batting (game_id TEXT, player_id INTEGER)"))
        conn.execute(text("CREATE TABLE player_game_pitching (game_id TEXT, player_id INTEGER)"))
        conn.execute(text("CREATE TABLE player_season_batting (season INTEGER, league TEXT, player_id INTEGER)"))
        conn.execute(text("CREATE TABLE player_season_pitching (season INTEGER, league TEXT, player_id INTEGER)"))
        conn.execute(
            text(
                "INSERT INTO kbo_seasons VALUES (1, 2024, 0), (2, 2025, 0)",
            ),
        )
        conn.execute(
            text("INSERT INTO game VALUES ('20241001LGSS0', 1), ('20251001LGSS0', 2)"),
        )
        conn.execute(
            text(
                "INSERT INTO player_game_batting VALUES ('20241001LGSS0', 100), ('20251001LGSS0', 200)",
            ),
        )
        conn.execute(
            text(
                "INSERT INTO player_game_pitching VALUES ('20241001LGSS0', 101), ('20251001LGSS0', 201)",
            ),
        )
        conn.execute(
            text(
                "INSERT INTO player_season_batting VALUES (2024, 'REGULAR', 100), (2025, 'REGULAR', 200)",
            ),
        )
        conn.execute(
            text(
                "INSERT INTO player_season_pitching VALUES (2024, 'REGULAR', 101), (2025, 'REGULAR', 201)",
            ),
        )

        findings = check_season_aggregates(conn, 2024, 2025)

    assert findings == []


def test_team_code_check_reports_only_nonzero_null_rates() -> None:
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE player_season_batting (season INTEGER, team_code TEXT)"))
        conn.execute(
            text(
                "INSERT INTO player_season_batting VALUES (2024, 'LG'), (2024, NULL), (2025, 'LG'), (2025, 'SS')",
            ),
        )

        findings = check_team_code_null_rate(conn, 2024, 2025)

    assert [(finding["year"], finding["count"]) for finding in findings] == [(2024, 1)]
