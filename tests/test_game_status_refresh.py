from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import src.repositories.game_status as game_status_module
from src.models.game import Game, GameBattingStat, GameInningScore, GameLineup, GameMetadata, GamePitchingStat
from src.models.player import PlayerBasic
from src.utils.game_status import GAME_STATUS_COMPLETED, GAME_STATUS_UNRESOLVED


def test_refresh_game_status_recovers_past_scores_from_inning_totals(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    for table in (
        PlayerBasic.__table__,
        Game.__table__,
        GameMetadata.__table__,
        GameInningScore.__table__,
        GameLineup.__table__,
        GameBattingStat.__table__,
        GamePitchingStat.__table__,
    ):
        table.create(bind=engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    monkeypatch.setattr(game_status_module, "SessionLocal", SessionLocal)

    with SessionLocal() as session:
        session.add(
            Game(
                game_id="20260531KTWO0",
                game_date=date(2026, 5, 31),
                away_team="KT",
                home_team="WO",
                game_status=GAME_STATUS_UNRESOLVED,
            ),
        )
        session.add_all(
            [
                GameInningScore(game_id="20260531KTWO0", team_side="away", inning=1, runs=5),
                GameInningScore(game_id="20260531KTWO0", team_side="home", inning=1, runs=1),
            ],
        )
        session.commit()

    result = game_status_module.refresh_game_status_for_date("20260531", today=date(2026, 6, 1))

    assert result["status_counts"] == {GAME_STATUS_COMPLETED: 1}
    with SessionLocal() as session:
        game = session.query(Game).one()
        assert game.game_status == GAME_STATUS_COMPLETED
        assert game.away_score == 5
        assert game.home_score == 1
