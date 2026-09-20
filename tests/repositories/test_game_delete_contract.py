from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from sqlalchemy import create_engine
from sqlalchemy.dialects import oracle
from sqlalchemy.orm import sessionmaker

from src.models.game import GameInningScore, GamePlayByPlay
from src.repositories.game_helpers import _delete_records


def test_oracle_delete_uses_serial_hint_without_committing() -> None:
    """Keep Oracle replacement deletes inside the caller-owned transaction."""
    session = MagicMock()
    session.get_bind.return_value = SimpleNamespace(dialect=SimpleNamespace(name="oracle"))

    _delete_records(session, GamePlayByPlay, GamePlayByPlay.game_id == "g1")

    statement = session.execute.call_args.args[0]
    sql = str(statement.compile(dialect=oracle.dialect()))
    assert "NO_PARALLEL" in sql
    session.commit.assert_not_called()


def test_delete_and_reinsert_stay_rollbackable() -> None:
    """A failed replacement restores the deleted source rows."""
    engine = create_engine("sqlite:///:memory:")
    GameInningScore.__table__.create(engine)
    session = sessionmaker(bind=engine)()
    try:
        session.add(GameInningScore(game_id="g1", team_side="away", inning=1, runs=1))
        session.commit()

        _delete_records(session, GameInningScore, GameInningScore.game_id == "g1")
        session.add(GameInningScore(game_id="g1", team_side="away", inning=1, runs=2))
        session.flush()
        assert session.query(GameInningScore).one().runs == 2

        session.rollback()

        restored = session.query(GameInningScore).one()
        assert restored.runs == 1
    finally:
        session.close()
        engine.dispose()
