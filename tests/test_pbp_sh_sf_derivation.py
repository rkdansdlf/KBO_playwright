import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.base import Base
from src.models.game import GameBattingStat, GameEvent
from src.services.pbp_sh_sf_derivation import (
    apply_sh_sf_to_batting_stats,
    derive_sh_sf_for_game,
)


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


def test_derive_sh_sf_for_game_basic(db_session):
    # Setup mock game and batting stats
    game_id = "20260607TEST0"

    # Add batting stats (Player A: id=1, Player B: id=2)
    db_session.add_all(
        [
            GameBattingStat(
                game_id=game_id,
                player_id=1,
                player_name="PlayerA",
                team_side="home",
                appearance_seq=1,
                plate_appearances=5,
                at_bats=4,
            ),
            GameBattingStat(
                game_id=game_id,
                player_id=2,
                player_name="PlayerB",
                team_side="home",
                appearance_seq=1,
                plate_appearances=4,
                at_bats=3,
            ),
        ]
    )

    # Add PBP events:
    # 1. Player A: 희생번트 (Sacrifice bunt) - has batter_id
    # 2. Player B: 희생플라이 (Sacrifice fly) - has batter_id, outs = 2 (outs after play, was previously filtered out)
    # 3. Player C (unresolved): 희생번트 - no batter_id, batter_name = "PlayerC"
    db_session.add_all(
        [
            GameEvent(
                game_id=game_id,
                event_seq=1,
                batter_id=1,
                batter_name="PlayerA",
                description="PlayerA : 투수 희생번트 아웃",
                outs=1,
            ),
            GameEvent(
                game_id=game_id,
                event_seq=2,
                batter_id=2,
                batter_name="PlayerB",
                description="PlayerB : 중견수 희생플라이 아웃",
                outs=2,
            ),
            GameEvent(
                game_id=game_id,
                event_seq=3,
                batter_id=None,
                batter_name="PlayerC",
                description="PlayerC : 포수 희생번트 아웃",
                outs=1,
            ),
        ]
    )
    db_session.commit()

    # Execute derivation
    result = derive_sh_sf_for_game(db_session, game_id)

    # Assertions
    assert 1 in result
    assert result[1]["sh"] == 1
    assert result[1]["sf"] == 0

    # Player B should be correctly derived as having a sacrifice fly (even with outs = 2)
    assert 2 in result
    assert result[2]["sh"] == 0
    assert result[2]["sf"] == 1

    # Unresolved Player C should fallback to name string
    assert "PlayerC" in result
    assert result["PlayerC"]["sh"] == 1
    assert result["PlayerC"]["sf"] == 0


def test_derive_sh_sf_for_game_name_to_id_resolution(db_session):
    game_id = "20260607TEST1"

    # Add batting stats (Player C: id=3, player_name="PlayerC")
    db_session.add_all(
        [
            GameBattingStat(
                game_id=game_id,
                player_id=3,
                player_name="PlayerC",
                team_side="home",
                appearance_seq=1,
            ),
        ]
    )

    # Add event where batter_id is NULL but batter_name matches "PlayerC"
    db_session.add_all(
        [
            GameEvent(
                game_id=game_id,
                event_seq=1,
                batter_id=None,
                batter_name="PlayerC",
                description="PlayerC : 투수 희생번트 아웃",
            ),
        ]
    )
    db_session.commit()

    result = derive_sh_sf_for_game(db_session, game_id)

    # It should have resolved PlayerC to player_id = 3!
    assert 3 in result
    assert result[3]["sh"] == 1
    assert result[3]["sf"] == 0
    assert "PlayerC" not in result


def test_apply_sh_sf_to_batting_stats(db_session):
    game_id = "20260607TEST2"

    # Add batting stats
    stat_a = GameBattingStat(
        game_id=game_id,
        player_id=1,
        player_name="PlayerA",
        team_side="home",
        appearance_seq=1,
        sacrifice_hits=0,
        sacrifice_flies=0,
    )
    stat_b = GameBattingStat(
        game_id=game_id,
        player_id=None,
        player_name="PlayerB",
        team_side="home",
        appearance_seq=1,
        sacrifice_hits=0,
        sacrifice_flies=0,
    )
    db_session.add_all([stat_a, stat_b])

    # Add events
    db_session.add_all(
        [
            GameEvent(
                game_id=game_id,
                event_seq=1,
                batter_id=1,
                batter_name="PlayerA",
                description="PlayerA : 투수 희생번트 아웃",
            ),
            GameEvent(
                game_id=game_id,
                event_seq=2,
                batter_id=None,
                batter_name="PlayerB",
                description="PlayerB : 우익수 희생플라이 아웃",
            ),
        ]
    )
    db_session.commit()

    updated = apply_sh_sf_to_batting_stats(db_session, game_id)
    db_session.commit()

    assert updated == 2

    # Verify values updated in DB
    refreshed_a = (
        db_session.query(GameBattingStat)
        .filter(GameBattingStat.game_id == game_id, GameBattingStat.player_id == 1)
        .one()
    )
    assert refreshed_a.sacrifice_hits == 1
    assert refreshed_a.sacrifice_flies == 0

    refreshed_b = (
        db_session.query(GameBattingStat)
        .filter(GameBattingStat.game_id == game_id, GameBattingStat.player_name == "PlayerB")
        .one()
    )
    assert refreshed_b.sacrifice_hits == 0
    assert refreshed_b.sacrifice_flies == 1
