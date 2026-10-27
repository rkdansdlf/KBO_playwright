"""Tests for the doubles/triples backfill command."""

from argparse import Namespace
from unittest.mock import MagicMock, patch

from src.cli.backfill.backfill_base_hits_from_innings import (
    APPLY_GUARD_ENV,
    _load_candidates,
    main,
    parse_args,
)


def _args(**overrides: object) -> Namespace:
    values: dict[str, object] = {
        "season": "2026",
        "apply": False,
        "limit": None,
    }
    values.update(overrides)
    return Namespace(**values)


def _row(game_id: str, player_name: str, doubles: int, triples: int, cells: dict[str, str]) -> dict[str, object]:
    return {
        "game_id": game_id,
        "player_id": 1,
        "player_name": player_name,
        "doubles": doubles,
        "triples": triples,
        "cells": cells,
    }


def test_parse_args_defaults_to_a_dry_run() -> None:
    with patch("sys.argv", ["backfill_base_hits", "--season", "2026"]):
        args = parse_args()
    assert args.season == "2026"
    assert args.apply is False


def test_load_candidates_derives_doubles_and_triples() -> None:
    session = MagicMock()
    session.execute.return_value.mappings.return_value.all.return_value = [
        _row("20260312KTLT0", "홍길동", 0, 0, {"inn1": "좌2", "inn2": "중3"}),
    ]

    candidates = _load_candidates(session, _args())

    assert len(candidates) == 1
    assert candidates[0]["doubles"] == 1
    assert candidates[0]["triples"] == 1


def test_load_candidates_skips_rows_that_already_match() -> None:
    session = MagicMock()
    session.execute.return_value.mappings.return_value.all.return_value = [
        _row("20260312KTLT0", "홍길동", 1, 0, {"inn1": "좌2"}),
    ]

    assert _load_candidates(session, _args()) == []


def test_load_candidates_skips_rows_without_cells() -> None:
    session = MagicMock()
    session.execute.return_value.mappings.return_value.all.return_value = [
        _row("20260312KTLT0", "홍길동", 0, 0, {}),
    ]

    assert _load_candidates(session, _args()) == []


def test_apply_without_the_guard_writes_nothing() -> None:
    session = MagicMock()
    session.execute.return_value.mappings.return_value.all.return_value = [
        _row("20260312KTLT0", "홍길동", 0, 0, {"inn1": "좌2"}),
    ]

    with (
        patch("src.cli.backfill.backfill_base_hits_from_innings.SessionLocal") as session_local,
        patch.dict("os.environ", {}, clear=False),
    ):
        session_local.return_value.__enter__.return_value = session
        with patch.dict("os.environ", {APPLY_GUARD_ENV: ""}):
            exit_code = main(["--season", "2026", "--apply"])

    assert exit_code == 3
    session.commit.assert_not_called()


def test_dry_run_does_not_commit() -> None:
    session = MagicMock()
    session.execute.return_value.mappings.return_value.all.return_value = [
        _row("20260312KTLT0", "홍길동", 0, 0, {"inn1": "좌2"}),
    ]

    with patch("src.cli.backfill.backfill_base_hits_from_innings.SessionLocal") as session_local:
        session_local.return_value.__enter__.return_value = session
        exit_code = main(["--season", "2026"])

    assert exit_code == 0
    session.commit.assert_not_called()


def test_apply_with_the_guard_updates_the_rows() -> None:
    session = MagicMock()
    session.execute.return_value.mappings.return_value.all.return_value = [
        _row("20260312KTLT0", "홍길동", 0, 0, {"inn1": "좌2"}),
    ]
    session.execute.return_value.rowcount = 1

    with (
        patch("src.cli.backfill.backfill_base_hits_from_innings.SessionLocal") as session_local,
        patch.dict("os.environ", {APPLY_GUARD_ENV: "1"}),
    ):
        session_local.return_value.__enter__.return_value = session
        exit_code = main(["--season", "2026", "--apply"])

    assert exit_code == 0
    session.commit.assert_called_once()
