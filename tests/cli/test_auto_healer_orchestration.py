from __future__ import annotations

import asyncio
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import src.cli.auto_healer as auto_healer


def _game(game_id: str, status: str = "SCHEDULED"):
    return SimpleNamespace(game_id=game_id, game_status=status, game_date=date(2025, 6, 15))


def _session_factory():
    session = MagicMock()
    context = MagicMock()
    context.__enter__.return_value = session
    context.__exit__.return_value = False
    return MagicMock(return_value=context), session


class TestRecoveryExecution:
    def test_dry_run_counts_candidates_without_collecting(self, monkeypatch):
        session_factory, _ = _session_factory()
        resolver = MagicMock()
        monkeypatch.setattr(auto_healer, "SessionLocal", session_factory)
        monkeypatch.setattr(auto_healer, "PlayerIdResolver", MagicMock(return_value=resolver))
        monkeypatch.setattr(auto_healer, "GameDetailCrawler", MagicMock())
        monkeypatch.setattr(auto_healer, "GameWriteContract", MagicMock())
        collect = AsyncMock()
        monkeypatch.setattr(auto_healer, "crawl_and_save_game_details", collect)

        results = asyncio.run(
            auto_healer._run_recovery([_game("G1"), _game("G2")], [date(2025, 6, 15)], MagicMock(), dry_run=True),
        )

        assert results == {"completed": 0, "cancelled": 0, "unresolved": 0, "dry_run": 2}
        resolver.preload_season_index.assert_called_once_with(2025)
        collect.assert_not_awaited()

    def test_applies_collection_outcomes_and_updates_checkpoint(self, monkeypatch):
        session_factory, _ = _session_factory()
        resolver = MagicMock()
        write_contract = MagicMock()
        write_contract.summary.return_value = "summary"
        complete = SimpleNamespace(detail_saved=True, failure_reason=None)
        failed = SimpleNamespace(detail_saved=False, failure_reason="timeout")
        collection_result = SimpleNamespace(items={"G1": complete, "G2": failed})
        recovery_manager = MagicMock()
        update_status = MagicMock()
        monkeypatch.setattr(auto_healer, "SessionLocal", session_factory)
        monkeypatch.setattr(auto_healer, "PlayerIdResolver", MagicMock(return_value=resolver))
        monkeypatch.setattr(auto_healer, "GameDetailCrawler", MagicMock())
        monkeypatch.setattr(auto_healer, "GameWriteContract", MagicMock(return_value=write_contract))
        monkeypatch.setattr(auto_healer, "crawl_and_save_game_details", AsyncMock(return_value=collection_result))
        monkeypatch.setattr(auto_healer, "update_game_status", update_status)

        results = asyncio.run(
            auto_healer._run_recovery(
                [_game("G1"), _game("G2")],
                [date(2025, 6, 15)],
                recovery_manager,
                dry_run=False,
            ),
        )

        assert results == {"completed": 1, "cancelled": 0, "unresolved": 1, "dry_run": 0}
        recovery_manager.mark_completed.assert_called_once_with("G1")
        recovery_manager.mark_failed.assert_called_once_with("G2", "timeout")
        update_status.assert_called_once_with("G2", auto_healer.GAME_STATUS_UNRESOLVED)


class TestDefaultHealerOrchestration:
    def test_recovery_targets_merge_and_sort_unique_anomalies(self, monkeypatch):
        first = _game("G1")
        second = _game("G2", status="COMPLETED")
        monkeypatch.setattr(auto_healer, "_find_stuck_games", MagicMock(return_value=[second, first]))
        monkeypatch.setattr(auto_healer, "_find_inconsistent_games", MagicMock(return_value=[second]))

        all_found, stuck, inconsistent = auto_healer._find_recovery_targets(None)

        assert [game.game_id for game in all_found] == ["G1", "G2"]
        assert stuck == [second, first]
        assert inconsistent == [second]

    def test_clears_checkpoint_when_no_anomalies_exist(self, monkeypatch):
        manager = MagicMock()
        monkeypatch.setattr(auto_healer, "RecoveryManager", MagicMock(return_value=manager))
        monkeypatch.setattr(auto_healer, "_find_recovery_targets", MagicMock(return_value=([], [], [])))

        assert asyncio.run(auto_healer.run_healer_async()) == 0
        manager.clear.assert_called_once()

    def test_returns_when_checkpoint_has_no_pending_games(self, monkeypatch):
        manager = MagicMock()
        game = _game("G1")
        monkeypatch.setattr(auto_healer, "RecoveryManager", MagicMock(return_value=manager))
        monkeypatch.setattr(auto_healer, "_find_recovery_targets", MagicMock(return_value=([game], [game], [])))
        monkeypatch.setattr(auto_healer, "_pending_recovery_candidates", MagicMock(return_value=(set(), [])))

        assert asyncio.run(auto_healer.run_healer_async()) == 0

    def test_runs_recovery_and_returns_unresolved_count(self, monkeypatch):
        manager = MagicMock()
        game = _game("G1")
        recovery = AsyncMock(return_value={"completed": 1, "cancelled": 0, "unresolved": 1, "dry_run": 0})
        summary = MagicMock()
        start_alert = MagicMock()
        monkeypatch.setattr(auto_healer, "RecoveryManager", MagicMock(return_value=manager))
        monkeypatch.setattr(auto_healer, "_find_recovery_targets", MagicMock(return_value=([game], [game], [])))
        monkeypatch.setattr(auto_healer, "_pending_recovery_candidates", MagicMock(return_value=({"G1"}, [game])))
        monkeypatch.setattr(auto_healer, "_run_recovery", recovery)
        monkeypatch.setattr(auto_healer, "_log_healer_summary", summary)
        monkeypatch.setattr(auto_healer, "_send_healer_start_alert", start_alert)

        assert asyncio.run(auto_healer.run_healer_async()) == 1
        start_alert.assert_called_once()
        recovery.assert_awaited_once()
        summary.assert_called_once()


class TestPbpHealerOrchestration:
    def test_returns_empty_summary_when_scan_finds_nothing(self, monkeypatch):
        monkeypatch.setattr(auto_healer, "_find_unverified_pbp_games", MagicMock(return_value=[]))

        assert asyncio.run(auto_healer.run_pbp_healer_async()) == {
            "found": 0,
            "recovered": 0,
            "failed": 0,
            "skipped": 0,
        }

    def test_dry_run_reports_without_sending_notifications(self, monkeypatch):
        result = {
            "game_id": "G1",
            "game_date": "2025-06-15",
            "away_team": "LG",
            "home_team": "SSG",
            "error_reason": "gap",
        }
        telegram = MagicMock()
        monkeypatch.setattr(auto_healer, "_find_unverified_pbp_games", MagicMock(return_value=[result]))
        monkeypatch.setattr(auto_healer, "TelegramBotClient", telegram)

        assert asyncio.run(auto_healer.run_pbp_healer_async(dry_run=True)) == {
            "found": 1,
            "recovered": 0,
            "failed": 0,
            "skipped": 1,
        }
        telegram.send_message.assert_not_called()

    def test_recovers_pbp_and_reports_partial_failure(self, monkeypatch):
        results = [
            {"game_id": "G1", "game_date": "2025-06-15", "away_team": "LG", "home_team": "SSG", "error_reason": "gap"},
            {"game_id": "G2", "game_date": "2025-06-15", "away_team": "KT", "home_team": "NC", "error_reason": "gap"},
        ]
        recovery_result = SimpleNamespace(saved_games=1, report_rows=[{"game_id": "G1", "status": "saved"}])
        telegram = MagicMock()
        recover = AsyncMock(return_value=recovery_result)
        monkeypatch.setattr(auto_healer, "_find_unverified_pbp_games", MagicMock(return_value=results))
        monkeypatch.setattr(auto_healer, "TelegramBotClient", telegram)
        monkeypatch.setattr("src.services.relay_recovery_service.recover_relay_data", recover)
        monkeypatch.setattr("src.sources.relay.derive_bucket_id", lambda game_id: f"bucket:{game_id}")

        result = asyncio.run(auto_healer.run_pbp_healer_async())

        assert result == {"found": 2, "recovered": 1, "failed": 1, "skipped": 0}
        recover.assert_awaited_once()
        assert telegram.send_message.call_count == 2


class TestHealerCli:
    def test_pbp_cli_returns_failure_when_any_game_failed(self, monkeypatch):
        monkeypatch.setattr(
            auto_healer,
            "run_pbp_healer_async",
            AsyncMock(return_value={"found": 1, "recovered": 0, "failed": 1, "skipped": 0}),
        )

        assert auto_healer.run_pbp_healer(["--dry-run", "--game-id", "G1"]) == 1

    def test_default_cli_forwards_reset_and_dry_run_flags(self, monkeypatch):
        healer = AsyncMock(return_value=0)
        monkeypatch.setattr(auto_healer, "run_healer_async", healer)

        assert auto_healer.run_healer(["--dry-run", "--reset"]) == 0
        healer.assert_awaited_once_with(dry_run=True, reset_checkpoint=True)

    def test_default_cli_dispatches_pbp_arguments(self, monkeypatch):
        pbp = MagicMock(return_value=0)
        monkeypatch.setattr(auto_healer, "run_pbp_healer", pbp)

        assert auto_healer.run_healer(["--pbp", "--dry-run", "--lookback-days", "5", "--game-id", "G1", "G2"]) == 0
        assert pbp.call_args.args[0] == ["--dry-run", "--lookback-days", "5", "--game-id", "G1", "G2"]
