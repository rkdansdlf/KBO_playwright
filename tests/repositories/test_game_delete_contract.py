from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from sqlalchemy.dialects import oracle

from src.models.game import GamePlayByPlay
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
