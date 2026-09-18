"""Direct regression tests for scripts.maintenance.fix_relay_state public functions."""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from scripts.maintenance.fix_relay_state import (
    audit_relay_source_states,
    fix_source_mismatch,
    fix_unknown_sources,
    remove_redundant_sources,
)
from src.models.base import Base
from src.models.game import Game, GameEvent, GamePlayByPlay


@pytest.fixture
def _local_session(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    monkeypatch.setattr("scripts.maintenance.fix_relay_state.SessionLocal", factory)
    sess = factory()
    try:
        sess.add(
            Game(
                game_id="20250505LGSS0",
                game_date=date(2025, 5, 5),
                home_team="SS",
                away_team="LG",
                game_status="COMPLETED",
            )
        )
        sess.add(
            GameEvent(
                game_id="20250505LGSS0",
                inning=1,
                inning_half="T",
                event_seq=1,
                description="우전 안타",
            )
        )
        sess.add(
            GamePlayByPlay(
                game_id="20250505LGSS0",
                inning=1,
                inning_half="T",
                play_description="우전 안타",
                source_name="mystery_adapter",
            )
        )
        sess.commit()
        yield sess
    finally:
        sess.close()


def test_audit_runs_against_real_session(_local_session) -> None:
    """Regression: audit must pass its session into the game-ID collector."""
    summary = audit_relay_source_states()
    assert summary.total_games == 1
    assert summary.total_pbp_rows == 1
    assert summary.total_events == 1


def test_fix_functions_accept_sample_size(_local_session) -> None:
    """Regression: fix helpers accept the sample_size kwarg used by the scheduler job."""
    unknown = fix_unknown_sources(dry_run=True, sample_size=100)
    assert unknown["dry_run"] is True
    mismatch = fix_source_mismatch(dry_run=True, sample_size=100)
    assert mismatch["dry_run"] is True
    redundant = remove_redundant_sources(dry_run=True, sample_size=100)
    assert redundant["dry_run"] is True
