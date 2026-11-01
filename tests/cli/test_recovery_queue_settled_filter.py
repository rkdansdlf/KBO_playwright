"""The detail-recovery queue must not re-queue a game an operator has settled.

The queue is a file (``data/recovery/detail_recovery_queue.json``) and knows nothing
about ``crawl_dead_letters``. That is the right split -- a file manager should not
read the database -- but it means the rule belongs to whoever drains it, and until
now nobody applied it. Measured 2026-11-02: five games that had been settled the day
before (``20241110PRUS0``, ``20111025SKSS7``, ``20150708HTWO0`` and two more) came
back through the queue and were crawled again, each spending another five attempts
against a source that has no record for them.

``load_game_targets_from_db`` filters the same tuple for exactly this reason. This is
the same rule applied to the other path.

The failure mode is pinned too: a database error leaves the queue **as it found it**.
A filter that cannot run must not silently empty work that should happen -- the
alternative (dropping everything on error) turns a connectivity blip into skipped
recoveries.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from sqlalchemy.exc import SQLAlchemyError

from src.cli.pipelines.run_daily_update import _without_settled_detail_games


class _FakeSession:
    def __enter__(self) -> _FakeSession:
        return self

    def __exit__(self, *exc: object) -> bool:
        return False


@pytest.fixture
def _fake_db(monkeypatch):
    monkeypatch.setattr("src.cli.pipelines.run_daily_update.SessionLocal", _FakeSession)
    return monkeypatch


class TestWhenTheDatabaseAnswers:
    def test_a_settled_game_is_dropped(self, _fake_db) -> None:
        _fake_db.setattr(
            "src.cli.pipelines.run_daily_update.settled_game_ids",
            lambda session, ids: {"20241110PRUS0"},
        )

        assert _without_settled_detail_games({"20241110PRUS0", "20261102LGLT0"}) == {"20261102LGLT0"}

    def test_a_queue_with_nothing_settled_is_kept_whole(self, _fake_db) -> None:
        _fake_db.setattr("src.cli.pipelines.run_daily_update.settled_game_ids", lambda session, ids: set())

        given = {"20261102LGLT0", "20261101SSHT0"}

        assert _without_settled_detail_games(given) == given

    def test_every_settled_game_is_dropped_not_just_the_first(self, _fake_db) -> None:
        _fake_db.setattr(
            "src.cli.pipelines.run_daily_update.settled_game_ids",
            lambda session, ids: {"A", "B", "C"},
        )

        assert _without_settled_detail_games({"A", "B", "C", "D"}) == {"D"}

    def test_the_lookup_is_given_every_id(self, _fake_db) -> None:
        """A partial lookup would settle some games and re-queue the rest."""
        seen: list[set[str]] = []

        def _capture(session, ids):
            seen.append(set(ids))
            return set()

        _fake_db.setattr("src.cli.pipelines.run_daily_update.settled_game_ids", _capture)

        _without_settled_detail_games({"A", "B"})

        assert seen == [{"A", "B"}]


class TestTheEdges:
    def test_an_empty_queue_is_not_looked_up(self, _fake_db) -> None:
        called: list[object] = []
        _fake_db.setattr(
            "src.cli.pipelines.run_daily_update.settled_game_ids",
            lambda session, ids: called.append(ids) or set(),
        )

        assert _without_settled_detail_games(set()) == set()
        assert called == [], "an empty set does not need a query"

    def test_a_database_error_keeps_the_queue(self, _fake_db) -> None:
        """A filter that cannot run must not drop the work it was filtering."""

        def _boom(session, ids):
            raise SQLAlchemyError("connection lost")

        _fake_db.setattr("src.cli.pipelines.run_daily_update.settled_game_ids", _boom)

        given = {"20241110PRUS0", "20261102LGLT0"}

        assert _without_settled_detail_games(given) == given

    def test_a_logger_failure_does_not_escape(self, _fake_db) -> None:
        """The check is a filter; nothing about it should take the run down."""
        given = {"20241110PRUS0"}
        with (
            patch("src.cli.pipelines.run_daily_update.logger") as fake_logger,
            patch("src.cli.pipelines.run_daily_update.settled_game_ids", side_effect=SQLAlchemyError("boom")),
        ):
            fake_logger.info.side_effect = RuntimeError("logger down")

            assert _without_settled_detail_games(given) == given
