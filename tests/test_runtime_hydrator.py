from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from src.models.game import (
    Game,
    GameBattingStat,
    GameEvent,
    GameIdAlias,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
    GamePlayByPlay,
    GameSummary,
)
from src.models.player import PlayerBasic, PlayerMovement, PlayerSeasonBatting, PlayerSeasonPitching
from src.models.team import Team, TeamDailyRoster
from src.sync.runtime_hydrator import RuntimeHydrator


def _build_session_factory():
    engine = create_engine("sqlite:///:memory:")

    @event.listens_for(engine, "connect")
    def _sqlite_foreign_keys(dbapi_con, _):
        cursor = dbapi_con.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    for table in (
        Team.__table__,
        PlayerBasic.__table__,
        PlayerSeasonBatting.__table__,
        PlayerSeasonPitching.__table__,
        PlayerMovement.__table__,
        TeamDailyRoster.__table__,
        Game.__table__,
        GameIdAlias.__table__,
        GameMetadata.__table__,
        GameInningScore.__table__,
        GameLineup.__table__,
        GameBattingStat.__table__,
        GamePitchingStat.__table__,
        GameEvent.__table__,
        GameSummary.__table__,
        GamePlayByPlay.__table__,
    ):
        table.create(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def _seed_source(SessionLocal):
    with SessionLocal() as session:
        session.add(Team(team_id="LG", team_name="LG 트윈스", team_short_name="LG", city="서울"))
        session.add(Team(team_id="SS", team_name="삼성 라이온즈", team_short_name="삼성", city="대구"))
        session.add(PlayerBasic(player_id=1001, name="홍길동", team="LG"))
        session.add(PlayerBasic(player_id=2001, name="원태인", team="SS"))
        session.flush()
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
        session.flush()
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
        session.add(GameMetadata(game_id="20250401LGSS0", stadium_name="잠실"))
        session.add(
            GameInningScore(
                game_id="20250401LGSS0",
                team_side="away",
                team_code="LG",
                inning=1,
                runs=2,
            )
        )
        session.add(
            GameLineup(
                game_id="20250401LGSS0",
                team_side="away",
                team_code="LG",
                player_id=1001,
                player_name="홍길동",
                batting_order=1,
                appearance_seq=1,
                is_starter=True,
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
        session.add(
            GameEvent(
                game_id="20250401LGSS0",
                event_seq=1,
                inning=1,
                inning_half="top",
                outs=0,
                batter_name="홍길동",
                pitcher_name="원태인",
                description="홍길동 : 좌전 안타",
                event_type="batting",
                result_code="안타",
                bases_before="---",
                bases_after="1--",
                wpa=0.12,
            )
        )
        session.add(
            GameSummary(
                game_id="20250401LGSS0",
                summary_type="결승타",
                player_name="홍길동",
                detail_text="1회 결승타",
            )
        )
        session.add(
            GamePlayByPlay(
                game_id="20250401LGSS0",
                inning=1,
                inning_half="top",
                batter_name="홍길동",
                pitcher_name="원태인",
                play_description="홍길동 : 좌전 안타",
                event_type="batting",
                result="안타",
            )
        )
        session.commit()


def test_runtime_hydrator_copies_operational_year_scope():
    source_factory = _build_session_factory()
    target_factory = _build_session_factory()
    _seed_source(source_factory)

    with target_factory() as session:
        session.add(Team(team_id="LG", team_name="LG 트윈스", team_short_name="LG", city="서울"))
        session.add(Team(team_id="SS", team_name="삼성 라이온즈", team_short_name="삼성", city="대구"))
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
        session.flush()
        session.add(
            GameInningScore(
                game_id="20250401LGSS0",
                team_side="away",
                team_code="OLD",
                inning=1,
                runs=9,
            )
        )
        session.commit()

    with source_factory() as source_session, target_factory() as target_session:
        summary = RuntimeHydrator(source_session, target_session).hydrate_year(
            2025,
            target_date=date(2025, 4, 2),
        )

    assert summary["player_basic"] == 2
    assert summary["player_season_batting"] == 1
    assert summary["player_season_pitching"] == 1
    assert summary["player_movements"] == 1
    assert summary["team_daily_roster"] == 1
    assert summary["game"] == 1
    assert summary["game_metadata"] == 1
    assert summary["game_inning_scores"] == 1
    assert summary["game_lineups"] == 1
    assert summary["game_batting_stats"] == 1
    assert summary["game_pitching_stats"] == 1
    assert summary["game_events"] == 1
    assert summary["game_summary"] == 1
    assert summary["game_play_by_play"] == 1

    with target_factory() as session:
        game = session.query(Game).filter(Game.game_id == "20250401LGSS0").one()
        assert game.game_status == "COMPLETED"
        assert game.away_score == 5
        assert session.query(PlayerBasic).filter(PlayerBasic.player_id == 1001).count() == 1
        assert session.query(PlayerBasic).filter(PlayerBasic.player_id == 9999).count() == 1
        assert session.query(GameBattingStat).filter(GameBattingStat.game_id == "20250401LGSS0").count() == 1
        assert session.query(GamePitchingStat).filter(GamePitchingStat.game_id == "20250401LGSS0").count() == 1
        assert session.query(GameMetadata).filter(GameMetadata.game_id == "20250401LGSS0").count() == 1
        assert session.query(GameInningScore).filter(GameInningScore.game_id == "20250401LGSS0").count() == 1
        assert session.query(GameLineup).filter(GameLineup.game_id == "20250401LGSS0").count() == 1
        assert session.query(GameEvent).filter(GameEvent.game_id == "20250401LGSS0").count() == 1
        assert session.query(GameSummary).filter(GameSummary.game_id == "20250401LGSS0").count() == 1
        assert session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == "20250401LGSS0").count() == 1
        assert session.query(TeamDailyRoster).count() == 1
        assert session.query(PlayerMovement).count() == 1


def test_runtime_hydrator_can_preserve_local_aliases():
    source_factory = _build_session_factory()
    target_factory = _build_session_factory()
    _seed_source(source_factory)

    with target_factory() as session:
        session.add(Team(team_id="LG", team_name="LG 트윈스", team_short_name="LG", city="서울"))
        session.add(Team(team_id="SS", team_name="삼성 라이온즈", team_short_name="삼성", city="대구"))
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
        session.flush()
        session.add(
            GameIdAlias(
                alias_game_id="20250401LGSSG0",
                canonical_game_id="20250401LGSS0",
                source="test",
                reason="preserve",
            )
        )
        session.commit()

    with source_factory() as source_session, target_factory() as target_session:
        summary = RuntimeHydrator(source_session, target_session).hydrate_year(
            2025,
            preserve_aliases=True,
        )

    assert summary["game_id_aliases_preserved"] == 1

    with target_factory() as session:
        alias = session.query(GameIdAlias).filter(GameIdAlias.alias_game_id == "20250401LGSSG0").one()
        game = session.query(Game).filter(Game.game_id == "20250401LGSS0").one()

        assert alias.canonical_game_id == "20250401LGSS0"
        assert game.game_status == "COMPLETED"
