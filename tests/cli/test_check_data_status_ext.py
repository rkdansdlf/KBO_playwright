"""Branch-coverage tests for the larger CLI functions in check_data_status.

The shared test files cover the default (empty-DB) path and the pure/safe
helpers. This module pushes coverage of the data-heavy branches (non-empty
query results, the pregame-pitcher block, verbose output, and the P0 JSON /
failures logging paths) toward 100%.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.cli import check_data_status
from src.cli.check_data_status import (
    _collect_full_status,
    _collect_status_warnings,
    _configure_cli_logging,
    _log_full_status_summary,
    _log_p0_readiness,
    _log_warnings,
    _run_p0_readiness_check,
    check_futures_data,
    check_game_data,
    check_players,
    check_pregame_pitcher_coverage,
    check_schedules,
)


def _result(scalar: object = 0, rows: list | None = None, first: object = None) -> MagicMock:
    r = MagicMock()
    r.scalar.return_value = scalar
    r.all.return_value = rows if rows is not None else []
    r.fetchall.return_value = rows if rows is not None else []
    r.first.return_value = first
    return r


def _session(
    scalar_map: dict[str, object] | None = None,
    rows_map: dict[str, list] | None = None,
    first_map: dict[str, object] | None = None,
    *,
    scheduled_count: int = 0,
    default_scalar: object = 0,
) -> MagicMock:
    scalar_map = scalar_map or {}
    rows_map = rows_map or {}
    first_map = first_map or {}
    sess = MagicMock()
    sess.__enter__.return_value = sess

    def _execute(stmt: object) -> MagicMock:
        sql = str(stmt).lower()
        r = _result(scalar=default_scalar)
        scalar_hit = max((k for k in scalar_map if k in sql), key=len, default=None)
        if scalar_hit is not None:
            r.scalar.return_value = scalar_map[scalar_hit]
        rows_hit = max((k for k in rows_map if k in sql), key=len, default=None)
        if rows_hit is not None:
            r.all.return_value = rows_map[rows_hit]
            r.fetchall.return_value = rows_map[rows_hit]
        first_hit = max((k for k in first_map if k in sql), key=len, default=None)
        if first_hit is not None:
            r.first.return_value = first_map[first_hit]
        return r

    sess.execute.side_effect = _execute
    q = sess.query.return_value
    f = q.filter.return_value
    f.count.return_value = scheduled_count
    q.count.return_value = scheduled_count
    return sess


class TestCheckSchedulesWithData:
    def test_total_present_exercises_validation_and_loops(self) -> None:
        sess = _session(
            scalar_map={
                "from game_schedules": 100,
                "from game": 50,
            },
            rows_map={
                "group by season_type": [("regular", 720), ("postseason", 7), ("preseason", 42)],
                "group by season_year": [(2025, 100), (2024, 90)],
            },
            first_map={
                "from game_schedules": ("2025-01-01", "2025-12-31"),
                "max(game_date)": ("2025-01-01", "2025-12-31"),
            },
        )
        result = check_schedules(sess)
        assert result["total"] == 100
        assert result["by_type"]["regular"] == 720


class TestCheckSchedulesFallback:
    def test_zero_schedules_uses_operational_fallback(self) -> None:
        sess = _session(
            scalar_map={
                "from game_schedules": 0,
                "from game": 50,
            },
            rows_map={"group by season_type": []},
            first_map={"from game_schedules": (None, None), "max(game_date)": ("2025-01-01", "2025-12-31")},
            scheduled_count=5,
        )
        result = check_schedules(sess)
        assert result["source"] == "game"
        assert result["operational_total"] == 50

    def test_zero_schedules_operational_dates_missing(self) -> None:
        sess = _session(
            scalar_map={
                "from game_schedules": 0,
                "from game": 50,
            },
            rows_map={"group by season_type": []},
            first_map={"from game_schedules": (None, None), "max(game_date)": (None, None)},
            scheduled_count=5,
        )
        result = check_schedules(sess)
        assert result["source"] == "game"


class TestCheckPlayers:
    def test_status_breakdown_logged(self) -> None:
        sess = _session(
            scalar_map={"count(players.id": 10},
            rows_map={"group by players.status": [("ACTIVE", 8), (None, 2)]},
        )
        result = check_players(sess)
        assert result["total"] == 10


class TestCheckFuturesData:
    def test_season_breakdown_logged(self) -> None:
        sess = _session(
            scalar_map={"count(player_season_batting.id": 5, "count(player_season_pitching.id": 3},
            rows_map={"group by player_season_batting.season": [(2025, 5), (2024, 3)]},
        )
        result = check_futures_data(sess)
        assert result["batting"] == 5
        assert result["pitching"] == 3


class TestCheckGameData:
    def test_anomaly_and_coverage_branches(self) -> None:
        sess = _session(
            scalar_map={
                "count(player_game_batting.id": 100,
                "count(player_game_pitching.id": 80,
                "having count(*) > 1": 1,
                "player_id is null": 1,
                "player_name is null": 0,
                "team_side is null": 0,
                "avg > obp": 0,
                "avg < 0 or avg > 1": 1,
                "obp < 0 or obp > 1": 0,
                "slg < 0 or slg > 5": 0,
                "era < 0 or era > 200": 0,
                "whip < 0 or whip > 30": 0,
                "left join player_game_batting pgb": 0,
                "left join game_batting_stats gbs": 0,
            },
            rows_map={
                "left join player_game_batting pgb": [("COMPLETED", 10, 9)],
            },
        )
        result = check_game_data(sess)
        assert result["batting"] == 100


class TestCheckPregamePitcherCoverage:
    def test_nonzero_scheduled_exercises_main_block(self) -> None:
        sess = _session(
            default_scalar=1,
            rows_map={"프리뷰": []},
            scheduled_count=3,
        )
        result = check_pregame_pitcher_coverage(sess, verbose=False)
        assert result["scheduled_total"] == 3
        assert result["away_ok"] == 1

    def test_verbose_path_logs_by_date(self) -> None:
        sess = _session(
            default_scalar=1,
            rows_map={"limit 40": [("2025-01-01", 2, 2, 1)]},
            scheduled_count=2,
        )
        result = check_pregame_pitcher_coverage(sess, verbose=True)
        assert result["scheduled_total"] == 2

    def test_zero_scheduled_oci_ready_branch(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PREGAME_SYNC_TO_OCI", "1")
        monkeypatch.setenv("OCI_DB_URL", "postgresql://x")
        sess = _session(scheduled_count=0)
        result = check_pregame_pitcher_coverage(sess, verbose=False)
        assert result["oci_sync_ready"] is True

    def test_zero_scheduled_env_disabled_branch(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PREGAME_SYNC_TO_OCI", "0")
        monkeypatch.delenv("OCI_DB_URL", raising=False)
        sess = _session(scheduled_count=0)
        result = check_pregame_pitcher_coverage(sess, verbose=False)
        assert result["oci_sync_ready"] is False

    def test_zero_scheduled_oci_url_missing_branch(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PREGAME_SYNC_TO_OCI", "1")
        monkeypatch.delenv("OCI_DB_URL", raising=False)
        sess = _session(scheduled_count=0)
        result = check_pregame_pitcher_coverage(sess, verbose=False)
        assert result["oci_sync_ready"] is False

    def test_nonzero_oci_ready_branch(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PREGAME_SYNC_TO_OCI", "1")
        monkeypatch.setenv("OCI_DB_URL", "postgresql://x")
        sess = _session(default_scalar=1, rows_map={"프리뷰": []}, scheduled_count=3)
        result = check_pregame_pitcher_coverage(sess, verbose=False)
        assert result["oci_sync_ready"] is True

    def test_nonzero_env_disabled_branch(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PREGAME_SYNC_TO_OCI", "0")
        monkeypatch.delenv("OCI_DB_URL", raising=False)
        sess = _session(default_scalar=1, rows_map={"프리뷰": []}, scheduled_count=3)
        result = check_pregame_pitcher_coverage(sess, verbose=False)
        assert result["oci_sync_ready"] is False


class TestLoggingHelpers:
    def test_log_full_status_summary(self) -> None:
        stats = {
            "total": 100,
            "game_schedules_total": 100,
            "operational_total": 100,
            "operational_scheduled": 5,
            "source": "game_schedules",
            "by_type": {},
            "warnings": [],
        }
        _log_full_status_summary(
            stats,
            {"total": 10},
            {"batting": 5, "pitching": 3},
            {"batting": 100, "pitching": 80},
            {
                "scheduled_total": 3,
                "both_ok": 2,
                "away_ok": 2,
                "home_ok": 2,
                "both_missing": 0,
                "preview_rows": 1,
                "preview_missing_starters": 0,
                "sync_candidate_games": 1,
                "sync_complete_starters": 1,
                "oci_sync_ready": False,
                "coverage_pct": 66.6,
            },
        )

    def test_log_warnings_empty_returns_early(self) -> None:
        _log_warnings([])

    def test_log_warnings_with_items(self) -> None:
        _log_warnings(["warn one", "warn two"])

    def test_collect_status_warnings_all_branches(self) -> None:
        warnings = _collect_status_warnings(
            {"total": 0, "warnings": ["sched warn"]},
            {"batting": 0},
            {
                "preview_missing_starters": 1,
                "sync_candidate_games": 2,
                "oci_sync_ready": False,
            },
        )
        assert len(warnings) >= 4

    def test_log_p0_readiness_with_failures(self) -> None:
        readiness = {
            "start_date": "20250101",
            "end_date": "20250101",
            "schedule": 1,
            "pregame": 1,
            "live": 1,
            "postgame": 1,
            "relay": 1,
            "roster": 1,
            "broadcast": 1,
            "oci": 1,
            "failures": [
                {
                    "severity": "HIGH",
                    "dataset": "live",
                    "game_date": "20250101",
                    "game_id": "G1",
                    "reason": "missing",
                },
            ],
        }
        _log_p0_readiness("20250101", readiness)


class TestConfigureCliLogging:
    def test_adds_handler_when_missing(self) -> None:
        saved = list(logging.getLogger().handlers)
        logging.getLogger().handlers.clear()
        try:
            _configure_cli_logging()
        finally:
            logging.getLogger().handlers[:] = saved
        assert logging.getLogger().handlers

    def test_skips_when_handlers_present(self) -> None:
        _configure_cli_logging()


class TestP0ReadinessBranches:
    def test_json_output_branch(self) -> None:
        args = SimpleNamespace(date="20250101", lookback_days=7, lookahead_days=1, json_output=True)
        sess = MagicMock()
        sess.__enter__.return_value = sess
        with patch("src.cli.check_data_status.SessionLocal", return_value=sess):
            with patch("src.cli.check_data_status.build_p0_readiness") as mock_p0:
                mock_p0.return_value = {"start_date": "20250101", "end_date": "20250101", "failures": []}
                _run_p0_readiness_check(args)

    def test_text_output_branch_with_failures(self) -> None:
        args = SimpleNamespace(date="20250101", lookback_days=7, lookahead_days=1, json_output=False)
        sess = MagicMock()
        sess.__enter__.return_value = sess
        with patch("src.cli.check_data_status.SessionLocal", return_value=sess):
            with patch("src.cli.check_data_status.build_p0_readiness") as mock_p0:
                mock_p0.return_value = {
                    "start_date": "20250101",
                    "end_date": "20250101",
                    "schedule": 1,
                    "pregame": 1,
                    "live": 1,
                    "postgame": 1,
                    "relay": 1,
                    "roster": 1,
                    "broadcast": 1,
                    "oci": 1,
                    "failures": [
                        {
                            "severity": "LOW",
                            "dataset": "roster",
                            "game_date": None,
                            "game_id": None,
                            "reason": "x",
                        },
                    ],
                }
                _run_p0_readiness_check(args)


class TestCollectFullStatus:
    def test_runs_all_checks_with_rich_session(self) -> None:
        sess = _session(
            default_scalar=1,
            scalar_map={"from game_schedules": 100, "from game": 50},
            rows_map={
                "group by season_type": [("regular", 720)],
                "group by season_year": [(2025, 100)],
                "group by players.status": [("ACTIVE", 8)],
                "프리뷰": [],
                "limit 40": [("2025-01-01", 2, 2, 1)],
            },
            first_map={
                "from game_schedules": ("2025-01-01", "2025-12-31"),
                "max(game_date)": ("2025-01-01", "2025-12-31"),
            },
            scheduled_count=3,
        )
        with patch("src.cli.check_data_status.SessionLocal", return_value=sess):
            result = _collect_full_status(verbose=True)
        assert len(result) == 5
