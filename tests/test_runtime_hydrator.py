from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.game import Game, GameBattingStat, GamePitchingStat
from src.models.player import PlayerBasic, PlayerMovement, PlayerSeasonBatting, PlayerSeasonPitching
from src.models.team import Team, TeamDailyRoster
from src.sync.runtime_hydrator import RuntimeHydrator


def _build_session_factory():
    engine = create_engine("sqlite:///:memory:")
    for table in (
        Team.__table__,
        PlayerBasic.__table__,
        PlayerSeasonBatting.__table__,
        PlayerSeasonPitching.__table__,
        PlayerMovement.__table__,
        TeamDailyRoster.__table__,
        Game.__table__,
        GameBattingStat.__table__,
        GamePitchingStat.__table__,
    ):
        table.create(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def _seed_source(SessionLocal):
    with SessionLocal() as session:
        session.add(Team(team_id="LG", team_name="LG 트윈스", team_short_name="LG", city="서울"))
        session.add(Team(team_id="SS", team_name="삼성 라이온즈", team_short_name="삼성", city="대구"))
        session.add(PlayerBasic(player_id=1001, name="홍길동", team="LG"))
        session.add(
            PlayerSeasonBatting(
                player_id=1001,
                season=2025,
                league="REGULAR",
                level="KBO1",
                source="ROLLUP",
                team_code="LG",
                games=10,
                hits=12,
                at_bats=40,
            )
        )
        session.add(
            PlayerSeasonPitching(
                player_id=2001,
                season=2025,
                league="REGULAR",
                level="KBO1",
                source="CRAWLER",
                team_code="SS",
                games=3,
                innings_outs=54,
                strikeouts=20,
            )
        )
        session.add(
            PlayerMovement(
                movement_date=date(2025, 4, 1),
                section="등록",
                team_code="LG",
                player_name="홍길동",
            )
        )
        session.add(
            TeamDailyRoster(
                roster_date=date(2025, 4, 1),
                team_code="LG",
                player_id=1001,
                player_name="홍길동",
                position="외야수",
            )
        )
        session.add(
            Game(
                game_id="20250401LGSS0",
                game_date=date(2025, 4, 1),
                away_team="LG",
                home_team="SS",
                away_score=5,
                home_score=3,
                game_status="COMPLETED",
                season_id=2025,
            )
        )
        session.add(
            GameBattingStat(
                game_id="20250401LGSS0",
                team_side="away",
                team_code="LG",
                player_id=1001,
                player_name="홍길동",
                batting_order=1,
                appearance_seq=1,
                standard_position="CF",
                hits=2,
                at_bats=4,
            )
        )
        session.add(
            GamePitchingStat(
                game_id="20250401LGSS0",
                team_side="home",
                team_code="SS",
                player_id=2001,
                player_name="원태인",
                appearance_seq=1,
                standard_position="P",
                is_starting=True,
                innings_outs=18,
                strikeouts=8,
            )
        )
        session.commit()


def test_runtime_hydrator_copies_operational_year_scope():
    source_factory = _build_session_factory()
    target_factory = _build_session_factory()
    _seed_source(source_factory)

    with target_factory() as session:
        session.add(PlayerBasic(player_id=9999, name="stale", team="OLD"))
        session.add(
            Game(
                game_id="20250401LGSS0",
                game_date=date(2025, 4, 1),
                away_team="LG",
                home_team="SS",
                game_status="SCHEDULED",
                season_id=2025,
            )
        )
        session.commit()

    with source_factory() as source_session, target_factory() as target_session:
        summary = RuntimeHydrator(source_session, target_session).hydrate_year(
            2025,
            target_date=date(2025, 4, 2),
        )

    assert summary["player_basic"] == 1
    assert summary["player_season_batting"] == 1
    assert summary["player_season_pitching"] == 1
    assert summary["player_movements"] == 1
    assert summary["team_daily_roster"] == 1
    assert summary["game"] == 1
    assert summary["game_batting_stats"] == 1
    assert summary["game_pitching_stats"] == 1

    with target_factory() as session:
        game = session.query(Game).filter(Game.game_id == "20250401LGSS0").one()
        assert game.game_status == "COMPLETED"
        assert game.away_score == 5
        assert session.query(PlayerBasic).filter(PlayerBasic.player_id == 1001).count() == 1
        assert session.query(PlayerBasic).filter(PlayerBasic.player_id == 9999).count() == 0
        assert session.query(GameBattingStat).filter(GameBattingStat.game_id == "20250401LGSS0").count() == 1
        assert session.query(GamePitchingStat).filter(GamePitchingStat.game_id == "20250401LGSS0").count() == 1
        assert session.query(TeamDailyRoster).count() == 1
        assert session.query(PlayerMovement).count() == 1
