from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.models.game import Game, GameBattingStat, GamePitchingStat, PlayerGameBatting, PlayerGamePitching
from src.models.player import PlayerBasic
from src.repositories.player_game_stats import (
    _compute_batting_rates,
    _compute_pitching_rates,
    aggregate_game_batting,
    aggregate_game_pitching,
    upsert_player_game_batting,
    upsert_player_game_pitching,
)
from src.utils.game_status import GAME_STATUS_COMPLETED, GAME_STATUS_SCHEDULED


def _build_session():
    engine = create_engine("sqlite:///:memory:")
    for table in (
        Game.__table__,
        GameBattingStat.__table__,
        GamePitchingStat.__table__,
        PlayerGameBatting.__table__,
        PlayerGamePitching.__table__,
        PlayerBasic.__table__,
    ):
        table.create(bind=engine)
    return Session(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def _seed_game(session, game_id="20250401LGSS0"):
    session.add(
        Game(
            game_id=game_id,
            game_date=date(2025, 4, 1),
            away_team="LG",
            home_team="SS",
            away_score=4,
            home_score=2,
            game_status=GAME_STATUS_COMPLETED,
        )
    )
    session.flush()


def test_compute_batting_rates():
    r = _compute_batting_rates(hits=2, at_bats=5, walks=1, hbp=0, sf=0, strikeouts=1, doubles=1, triples=0, home_runs=0)
    assert r["avg"] == 0.400
    assert r["obp"] == 0.500
    assert r["slg"] == 0.600
    assert r["ops"] == round(0.500 + 0.600, 3)


def test_compute_pitching_rates():
    r = _compute_pitching_rates(total_outs=18, hits=3, bb=1, er=1, k=5, hr=0)
    assert r["era"] == round(1 * 9 / 6.0, 2)
    assert r["whip"] == round((1 + 3) / 6.0, 2)


def test_aggregate_game_batting_empty_game_id():
    session = _build_session()
    result = aggregate_game_batting(session, "nonexistent")
    assert result == []


def test_aggregate_game_batting_single_appearance():
    session = _build_session()
    _seed_game(session)
    session.add(PlayerBasic(player_id=1001, name="타자1"))
    session.add(
        GameBattingStat(
            game_id="20250401LGSS0",
            team_side="away",
            team_code="LG",
            player_id=1001,
            player_name="타자1",
            batting_order=1,
            position="CF",
            is_starter=True,
            appearance_seq=1,
            plate_appearances=4,
            at_bats=3,
            runs=1,
            hits=2,
            doubles=1,
        )
    )
    session.flush()

    result = aggregate_game_batting(session, "20250401LGSS0")
    assert len(result) == 1
    r = result[0]
    assert r["player_id"] == 1001
    assert r["player_name"] == "타자1"
    assert r["is_starter"] is True
    assert r["hits"] == 2
    assert r["plate_appearances"] == 4


def test_aggregate_game_batting_multiple_appearances():
    session = _build_session()
    _seed_game(session)
    session.add(PlayerBasic(player_id=1001, name="타자1"))
    session.add_all(
        [
            GameBattingStat(
                game_id="20250401LGSS0",
                team_side="away",
                team_code="LG",
                player_id=1001,
                player_name="타자1",
                batting_order=1,
                position="CF",
                is_starter=True,
                appearance_seq=1,
                plate_appearances=3,
                at_bats=2,
                hits=1,
            ),
            GameBattingStat(
                game_id="20250401LGSS0",
                team_side="away",
                team_code="LG",
                player_id=1001,
                player_name="타자1",
                batting_order=1,
                position="PH",
                is_starter=False,
                appearance_seq=2,
                plate_appearances=1,
                at_bats=1,
                hits=0,
            ),
        ]
    )
    session.flush()

    result = aggregate_game_batting(session, "20250401LGSS0")
    assert len(result) == 1
    r = result[0]
    assert r["plate_appearances"] == 4
    assert r["at_bats"] == 3
    assert r["hits"] == 1
    assert r["is_starter"] is True  # any appearance was starter


def test_aggregate_game_pitching_single_appearance():
    session = _build_session()
    _seed_game(session)
    session.add(PlayerBasic(player_id=2001, name="투수1"))
    session.add(
        GamePitchingStat(
            game_id="20250401LGSS0",
            team_side="away",
            team_code="LG",
            player_id=2001,
            player_name="투수1",
            is_starting=True,
            appearance_seq=1,
            innings_outs=18,
            hits_allowed=3,
            earned_runs=1,
            strikeouts=5,
            walks_allowed=1,
        )
    )
    session.flush()

    result = aggregate_game_pitching(session, "20250401LGSS0")
    assert len(result) == 1
    r = result[0]
    assert r["player_id"] == 2001
    assert r["innings_outs"] == 18
    assert r["era"] == round(1 * 9 / 6.0, 2)


def test_upsert_then_reaggregate():
    session = _build_session()
    _seed_game(session)
    session.add(PlayerBasic(player_id=1001, name="타자1"))
    session.add(
        GameBattingStat(
            game_id="20250401LGSS0",
            team_side="away",
            team_code="LG",
            player_id=1001,
            player_name="타자1",
            batting_order=1,
            position="CF",
            is_starter=True,
            appearance_seq=1,
            plate_appearances=4,
            at_bats=3,
            hits=2,
        )
    )
    session.flush()

    records = aggregate_game_batting(session, "20250401LGSS0")
    assert records

    saved = upsert_player_game_batting(session, records)
    assert saved == 1

    stored = session.query(PlayerGameBatting).filter_by(game_id="20250401LGSS0").all()
    assert len(stored) == 1
    assert stored[0].player_id == 1001
    assert stored[0].hits == 2


def test_upsert_pitching_then_reaggregate():
    session = _build_session()
    _seed_game(session)
    session.add(PlayerBasic(player_id=2001, name="투수1"))
    session.add(
        GamePitchingStat(
            game_id="20250401LGSS0",
            team_side="away",
            team_code="LG",
            player_id=2001,
            player_name="투수1",
            is_starting=True,
            appearance_seq=1,
            innings_outs=18,
            hits_allowed=3,
            earned_runs=1,
            strikeouts=5,
        )
    )
    session.flush()

    records = aggregate_game_pitching(session, "20250401LGSS0")
    saved = upsert_player_game_pitching(session, records)
    assert saved == 1

    stored = session.query(PlayerGamePitching).filter_by(game_id="20250401LGSS0").all()
    assert len(stored) == 1
    assert stored[0].player_id == 2001
    assert stored[0].innings_outs == 18


def test_aggregate_batting_sacrifice_fly_avg_gt_obp():
    session = _build_session()
    _seed_game(session)
    session.add(PlayerBasic(player_id=1002, name="타자2"))
    session.add(
        GameBattingStat(
            game_id="20250401LGSS0",
            team_side="away",
            team_code="LG",
            player_id=1002,
            player_name="타자2",
            batting_order=2,
            position="SS",
            is_starter=True,
            appearance_seq=1,
            plate_appearances=4,
            at_bats=3,
            hits=1,
            sacrifice_flies=1,
        )
    )
    session.flush()

    result = aggregate_game_batting(session, "20250401LGSS0")
    assert len(result) == 1
    r = result[0]
    assert r["avg"] == round(1 / 3, 3)  # 0.333
    assert r["obp"] == round(1 / 4, 3)  # 0.250 (SF adds to denominator)
    assert r["avg"] > r["obp"]


def test_aggregate_pitching_high_era_short_outing():
    session = _build_session()
    _seed_game(session)
    session.add(PlayerBasic(player_id=2002, name="투수2"))
    session.add(
        GamePitchingStat(
            game_id="20250401LGSS0",
            team_side="away",
            team_code="LG",
            player_id=2002,
            player_name="투수2",
            is_starting=False,
            appearance_seq=1,
            innings_outs=1,
            earned_runs=5,
            hits_allowed=3,
            walks_allowed=2,
            strikeouts=0,
        )
    )
    session.flush()

    result = aggregate_game_pitching(session, "20250401LGSS0")
    assert len(result) == 1
    r = result[0]
    assert r["innings_outs"] == 1
    assert r["era"] == round(5 * 27 / 1, 2)  # 135.0


def test_aggregate_batting_not_completed_game():
    session = _build_session()
    session.add(
        Game(
            game_id="20250401LGSS0",
            game_date=date(2025, 4, 1),
            away_team="LG",
            home_team="SS",
            game_status=GAME_STATUS_SCHEDULED,
        )
    )
    session.add(PlayerBasic(player_id=1001, name="타자1"))
    session.add(
        GameBattingStat(
            game_id="20250401LGSS0",
            team_side="away",
            team_code="LG",
            player_id=1001,
            player_name="타자1",
            batting_order=1,
            is_starter=True,
            appearance_seq=1,
            plate_appearances=4,
            at_bats=3,
            hits=2,
        )
    )
    session.flush()

    result = aggregate_game_batting(session, "20250401LGSS0")
    assert result == []


def test_aggregate_batting_skips_null_player_id():
    session = _build_session()
    _seed_game(session)
    session.add(
        GameBattingStat(
            game_id="20250401LGSS0",
            team_side="away",
            team_code="LG",
            player_id=None,
            player_name="무명",
            batting_order=1,
            is_starter=True,
            appearance_seq=1,
            plate_appearances=4,
            at_bats=3,
            hits=2,
        )
    )
    session.flush()

    result = aggregate_game_batting(session, "20250401LGSS0")
    assert result == []


def test_aggregate_batting_multiple_players():
    session = _build_session()
    _seed_game(session)
    session.add_all(
        [
            PlayerBasic(player_id=1001, name="타자1"),
            PlayerBasic(player_id=1002, name="타자2"),
        ]
    )
    session.add_all(
        [
            GameBattingStat(
                game_id="20250401LGSS0",
                team_side="away",
                team_code="LG",
                player_id=1001,
                player_name="타자1",
                batting_order=1,
                position="CF",
                is_starter=True,
                appearance_seq=1,
                plate_appearances=4,
                at_bats=3,
                hits=2,
            ),
            GameBattingStat(
                game_id="20250401LGSS0",
                team_side="away",
                team_code="LG",
                player_id=1002,
                player_name="타자2",
                batting_order=2,
                position="SS",
                is_starter=True,
                appearance_seq=1,
                plate_appearances=3,
                at_bats=3,
                hits=0,
            ),
        ]
    )
    session.flush()

    result = aggregate_game_batting(session, "20250401LGSS0")
    assert len(result) == 2
    ids = {r["player_id"] for r in result}
    assert ids == {1001, 1002}
    hits_map = {r["player_id"]: r["hits"] for r in result}
    assert hits_map[1001] == 2
    assert hits_map[1002] == 0


def test_upsert_player_game_batting_overwrite():
    session = _build_session()
    _seed_game(session)
    session.add(PlayerBasic(player_id=1001, name="타자1"))
    session.add(
        GameBattingStat(
            game_id="20250401LGSS0",
            team_side="away",
            team_code="LG",
            player_id=1001,
            player_name="타자1",
            batting_order=1,
            position="CF",
            is_starter=True,
            appearance_seq=1,
            plate_appearances=4,
            at_bats=3,
            hits=2,
        )
    )
    session.flush()

    records = aggregate_game_batting(session, "20250401LGSS0")
    upsert_player_game_batting(session, records)
    first_stored = session.query(PlayerGameBatting).filter_by(game_id="20250401LGSS0").first()
    assert first_stored.hits == 2

    session.query(GameBattingStat).filter_by(game_id="20250401LGSS0").update({"hits": 3})
    session.flush()
    session.expire_all()
    records2 = aggregate_game_batting(session, "20250401LGSS0")
    upsert_player_game_batting(session, records2)
    session.flush()

    stored = session.query(PlayerGameBatting).filter_by(game_id="20250401LGSS0").first()
    assert stored.hits == 3  # should be updated, not duplicated
