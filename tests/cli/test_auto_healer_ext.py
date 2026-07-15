"""Branch-coverage tests for auto_healer edge cases.

The shared test files already cover most of auto_healer; this module targets
the few remaining branch gaps: non-str source payloads in the PBP scan, the
targeted-mode payload parsing (string payloads, valid and invalid JSON), the
`all_found` empty early-return, the `incon_count == 0` anomaly-logging branch,
and the recovery loop's "cancelled" outcome (neither completed nor unresolved).
"""

from __future__ import annotations

import asyncio
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

import src.cli.auto_healer as auto_healer
from src.cli.auto_healer import (
    _find_unverified_pbp_games,
    _log_anomaly_summary,
    run_healer_async,
    run_pbp_healer_async,
)


def _game(game_id: str, status: str = "SCHEDULED") -> SimpleNamespace:
    return SimpleNamespace(game_id=game_id, game_status=status, game_date=date(2025, 6, 15))


def _session_factory() -> tuple[MagicMock, MagicMock]:
    session = MagicMock()
    context = MagicMock()
    context.__enter__.return_value = session
    context.__exit__.return_value = False
    return MagicMock(return_value=context), session


def _db_factory(rows: list) -> MagicMock:
    session = MagicMock()
    result = MagicMock()
    result.fetchall.return_value = rows
    session.execute.return_value = result
    ctx = MagicMock()
    ctx.__enter__.return_value = session
    ctx.__exit__.return_value = False
    return MagicMock(return_value=ctx)


def _row(payload: object, game_id: str = "G1") -> SimpleNamespace:
    return SimpleNamespace(
        source_payload=payload,
        game_id=game_id,
        game_date=date(2025, 6, 15),
        away_team="KIA",
        home_team="DB",
    )


class TestFindUnverifiedPbpGamesNonStrPayload:
    def test_dict_payload_skips_json_parse(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(auto_healer, "SessionLocal", _db_factory([_row({"pbp_validation_error": "x"})]))
        results = _find_unverified_pbp_games(lookback_days=3)
        assert results[0]["error_reason"] == "x"


class TestPbpHealerTargetedStrPayload:
    def test_parses_valid_and_invalid_json_payloads(self, monkeypatch: pytest.MonkeyPatch) -> None:
        rows = [
            _row('{"pbp_validation_error": "boom"}', "G1"),
            _row("not-json", "G2"),
        ]
        monkeypatch.setattr(auto_healer, "SessionLocal", _db_factory(rows))
        result = asyncio.run(run_pbp_healer_async(target_game_ids=["G1", "G2"], dry_run=True))
        assert result["found"] == 2
        assert result["skipped"] == 2


class TestRunHealerEmptyAllFound:
    def test_returns_when_all_found_empty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        manager = MagicMock()
        monkeypatch.setattr(auto_healer, "RecoveryManager", MagicMock(return_value=manager))
        monkeypatch.setattr(auto_healer, "_find_recovery_targets", MagicMock(return_value=([], [], [])))
        assert asyncio.run(run_healer_async(target_game_ids=["G1"])) == 0


class TestLogAnomalySummaryNoInconsistenciesInPending:
    def test_skips_incon_log_when_count_zero(self) -> None:
        inconsistent = [_game("G1", status="COMPLETED")]
        _log_anomaly_summary([], inconsistent, set(), [])


class TestRunRecoveryCancelledOutcome:
    def test_cancelled_outcome_continues_loop(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _, _ = _session_factory()
        resolver = MagicMock()
        write_contract = MagicMock()
        write_contract.summary.return_value = "summary"
        cancelled = SimpleNamespace(detail_saved=False, failure_reason="cancelled")
        collection_result = SimpleNamespace(items={"G1": cancelled})
        recovery_manager = MagicMock()
        monkeypatch.setattr(auto_healer, "SessionLocal", _session_factory()[0])
        monkeypatch.setattr(auto_healer, "PlayerIdResolver", MagicMock(return_value=resolver))
        monkeypatch.setattr(auto_healer, "GameDetailCrawler", MagicMock())
        monkeypatch.setattr(auto_healer, "GameWriteContract", MagicMock(return_value=write_contract))
        monkeypatch.setattr(auto_healer, "crawl_and_save_game_details", AsyncMock(return_value=collection_result))
        monkeypatch.setattr(auto_healer, "update_game_status", MagicMock())

        results = asyncio.run(
            auto_healer._run_recovery([_game("G1")], [date(2025, 6, 15)], recovery_manager, dry_run=False),
        )

        assert results["cancelled"] == 1
        recovery_manager.mark_completed.assert_not_called()
        recovery_manager.mark_failed.assert_not_called()
