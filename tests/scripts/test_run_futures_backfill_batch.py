from __future__ import annotations

from unittest.mock import MagicMock, patch

from scripts.maintenance.run_futures_backfill_batch import (
    FUTURES_BACKFILL_CHUNK_SIZE,
    FUTURES_MISSING_PLAYER_QUERY,
    _futures_backfill_command,
    run_batch_backfill,
)


def test_missing_player_query_is_limited_to_futures_kbo2_rows() -> None:
    assert "league = 'FUTURES'" in FUTURES_MISSING_PLAYER_QUERY
    assert "level = 'KBO2'" in FUTURES_MISSING_PLAYER_QUERY


def test_futures_backfill_command_preserves_player_id_chunk() -> None:
    assert _futures_backfill_command(["10", "20"]) == [
        "venv/bin/python",
        "-m",
        "src.cli.crawl_futures",
        "--player-ids",
        "10,20",
        "--concurrency",
        "8",
    ]


def test_run_batch_backfill_skips_runner_when_no_futures_rows() -> None:
    session = MagicMock()
    session.execute.return_value.fetchall.return_value = []
    runner = MagicMock()

    with patch("scripts.maintenance.run_futures_backfill_batch.SessionLocal") as session_local:
        session_local.return_value.__enter__.return_value = session
        run_batch_backfill(runner=runner)

    runner.assert_not_called()


def test_run_batch_backfill_splits_futures_players_into_bounded_chunks() -> None:
    session = MagicMock()
    player_ids = list(range(FUTURES_BACKFILL_CHUNK_SIZE + 1))
    session.execute.return_value.fetchall.return_value = [(player_id,) for player_id in player_ids]
    runner = MagicMock()

    with patch("scripts.maintenance.run_futures_backfill_batch.SessionLocal") as session_local:
        session_local.return_value.__enter__.return_value = session
        run_batch_backfill(runner=runner)

    assert runner.call_count == 2
    first_command = runner.call_args_list[0].args[0]
    second_command = runner.call_args_list[1].args[0]
    assert first_command[4].split(",") == [str(player_id) for player_id in player_ids[:-1]]
    assert second_command[4] == str(player_ids[-1])
