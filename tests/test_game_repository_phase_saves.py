from __future__ import annotations

from datetime import date, time

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import src.repositories.game_repository as game_repository
from src.models.game import (
    Game,
    GameBattingStat,
    GameEvent,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
    GameSummary,
)
from src.models.player import PlayerBasic
from src.utils.game_status import GAME_STATUS_CANCELLED, GAME_STATUS_LIVE, GAME_STATUS_SCHEDULED


class _FakeResolver:
    def __init__(self, session):
        self.session = session

    def resolve_id(self, player_name: str, team_code: str, season: int):
        return {
            ("홍길동", "LG"): 1001,
            ("이승엽", "SS"): 2001,
        }.get((player_name, team_code))


def _build_session_factory():
    engine = create_engine("sqlite:///:memory:")
    for table in (
        Game.__table__,
        PlayerBasic.__table__,
        GameMetadata.__table__,
        GameInningScore.__table__,
        GameLineup.__table__,
        GameBattingStat.__table__,
        GamePitchingStat.__table__,
        GameEvent.__table__,
        GameSummary.__table__,
    ):
        table.create(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def _seed_existing_detail(SessionLocal, game_id: str):
    with SessionLocal() as session:
        session.add(
            Game(
                game_id=game_id,
                game_date=date(2025, 4, 1),
                away_team="LG",
                home_team="SS",
                game_status=GAME_STATUS_SCHEDULED,
            )
        )
        session.add(
            GameMetadata(
                game_id=game_id,
                start_time=time(17, 0),
                stadium_name="잠실",
            )
        )
        session.add(
            GameBattingStat(
                game_id=game_id,
                team_side="away",
                team_code="LG",
                player_id=901,
                player_name="기존타자",
                appearance_seq=1,
                batting_order=1,
                standard_position="CF",
            )
        )
        session.add(
            GamePitchingStat(
                game_id=game_id,
                team_side="home",
                team_code="SS",
                player_id=902,
                player_name="기존투수",
                appearance_seq=1,
                is_starting=True,
                standard_position="P",
            )
        )
        session.add(
            GameEvent(
                game_id=game_id,
                event_seq=1,
                inning=1,
                inning_half="top",
                outs=0,
                batter_name="기존타자",
                pitcher_name="기존투수",
                description="기존 이벤트",
                event_type="batting",
                result_code="안타",
                bases_before="---",
                bases_after="1--",
                wpa=0.12,
                win_expectancy_before=0.5,
                win_expectancy_after=0.62,
                score_diff=0,
                base_state=0,
                home_score=0,
                away_score=0,
            )
        )
        session.add(
            GameInningScore(
                game_id=game_id,
                team_side="away",
                team_code="LG",
                inning=1,
                runs=0,
            )
        )
        session.commit()


def test_save_pregame_lineups_updates_start_time_and_preserves_existing_detail(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(game_repository, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_repository, "PlayerIdResolver", _FakeResolver)
    monkeypatch.setattr(game_repository, "_auto_sync_to_oci", lambda game_id: None)

    _seed_existing_detail(SessionLocal, "20250401LGSS0")

    saved = game_repository.save_pregame_lineups(
        {
            "game_id": "20250401LGSS0",
            "game_date": "20250401",
            "stadium": "잠실",
            "start_time": "18:30",
            "away_starter": "임찬규",
            "home_starter": "원태인",
            "away_lineup": [{"player_name": "홍길동", "batting_order": 1, "position": "중견수"}],
            "home_lineup": [{"player_name": "이승엽", "batting_order": 4, "position": "1루수"}],
        }
    )

    assert saved is True

    with SessionLocal() as session:
        game = session.query(Game).filter(Game.game_id == "20250401LGSS0").one()
        metadata = session.query(GameMetadata).filter(GameMetadata.game_id == "20250401LGSS0").one()
        lineups = session.query(GameLineup).filter(GameLineup.game_id == "20250401LGSS0").order_by(GameLineup.team_side).all()

        assert game.game_status == GAME_STATUS_SCHEDULED
        assert game.away_pitcher == "임찬규"
        assert game.home_pitcher == "원태인"
        assert metadata.start_time == time(18, 30)
        assert session.query(GameBattingStat).filter(GameBattingStat.game_id == "20250401LGSS0").count() == 1
        assert session.query(GamePitchingStat).filter(GamePitchingStat.game_id == "20250401LGSS0").count() == 1
        assert session.query(GameEvent).filter(GameEvent.game_id == "20250401LGSS0").count() == 1
        assert len(lineups) == 2
        assert {(row.team_side, row.player_id, row.canonical_team_code, row.franchise_id) for row in lineups} == {
            ("away", 1001, "LG", 3),
            ("home", 2001, "SS", 1),
        }
        assert session.query(GameSummary).filter(
            GameSummary.game_id == "20250401LGSS0",
            GameSummary.summary_type == "프리뷰",
        ).count() == 1


def test_save_game_snapshot_preserves_detail_rows_and_start_time(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(game_repository, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_repository, "_auto_sync_to_oci", lambda game_id: None)

    _seed_existing_detail(SessionLocal, "20250401LGSS0")

    saved = game_repository.save_game_snapshot(
        {
            "game_id": "20250401LGSS0",
            "game_date": "20250401",
            "metadata": {
                "stadium": "잠실",
                "weather": "맑음",
            },
            "teams": {
                "away": {"code": "LG", "score": 1, "line_score": [1, 0]},
                "home": {"code": "SS", "score": 0, "line_score": [0, 0]},
            },
            "pitchers": {
                "away": [{"player_name": "임찬규", "is_starting": True}],
                "home": [{"player_name": "원태인", "is_starting": True}],
            },
        },
        status=GAME_STATUS_LIVE,
    )

    assert saved is True

    with SessionLocal() as session:
        game = session.query(Game).filter(Game.game_id == "20250401LGSS0").one()
        metadata = session.query(GameMetadata).filter(GameMetadata.game_id == "20250401LGSS0").one()
        inning_rows = session.query(GameInningScore).filter(GameInningScore.game_id == "20250401LGSS0").all()

        assert game.game_status == GAME_STATUS_LIVE
        assert game.away_score == 1
        assert game.home_score == 0
        assert game.away_pitcher == "임찬규"
        assert game.home_pitcher == "원태인"
        assert metadata.start_time == time(17, 0)
        assert metadata.weather == "맑음"
        assert len(inning_rows) == 4
        assert session.query(GameLineup).filter(GameLineup.game_id == "20250401LGSS0").count() == 0
        assert session.query(GameBattingStat).filter(GameBattingStat.game_id == "20250401LGSS0").count() == 1
        assert session.query(GamePitchingStat).filter(GamePitchingStat.game_id == "20250401LGSS0").count() == 1
        assert session.query(GameEvent).filter(GameEvent.game_id == "20250401LGSS0").count() == 1


def test_save_game_snapshot_marks_cancelled_alias_and_sets_season(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(game_repository, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_repository, "_auto_sync_to_oci", lambda game_id: None)

    saved = game_repository.save_game_snapshot(
        {
            "game_id": "20250402LGSS0",
            "game_date": "20250402",
            "metadata": {"is_cancelled": True},
            "teams": {
                "away": {"code": "LG", "score": None, "line_score": []},
                "home": {"code": "SS", "score": None, "line_score": []},
            },
        },
        status="CANCELED",
    )

    assert saved is True

    with SessionLocal() as session:
        game = session.query(Game).filter(Game.game_id == "20250402LGSS0").one()
        metadata = session.query(GameMetadata).filter(GameMetadata.game_id == "20250402LGSS0").one()

        assert game.game_status == GAME_STATUS_CANCELLED
        assert game.season_id == 2025
        assert game.away_team == "LG"
        assert game.home_team == "SS"
        assert metadata.source_payload == {"is_cancelled": True}


def test_save_game_detail_honors_explicit_cancelled_status(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(game_repository, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_repository, "_auto_sync_to_oci", lambda game_id: None)

    saved = game_repository.save_game_detail(
        {
            "game_id": "20250403LGSS0",
            "game_date": "20250403",
            "game_status": "CANCELED",
            "metadata": {"is_cancelled": True},
            "teams": {
                "away": {"code": "LG", "score": None, "line_score": []},
                "home": {"code": "SS", "score": None, "line_score": []},
            },
        }
    )

    assert saved is True

    with SessionLocal() as session:
        game = session.query(Game).filter(Game.game_id == "20250403LGSS0").one()

        assert game.game_status == GAME_STATUS_CANCELLED
        assert game.season_id == 2025


def test_repair_game_parent_from_existing_children_uses_child_scores(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(game_repository, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_repository, "_auto_sync_to_oci", lambda game_id: None)

    with SessionLocal() as session:
        session.add_all(
            [
                GameInningScore(
                    game_id="20250404LGSS0",
                    team_side="away",
                    team_code="LG",
                    inning=1,
                    runs=1,
                ),
                GameInningScore(
                    game_id="20250404LGSS0",
                    team_side="home",
                    team_code="SS",
                    inning=1,
                    runs=3,
                ),
                GameBattingStat(
                    game_id="20250404LGSS0",
                    team_side="away",
                    team_code="LG",
                    player_id=901,
                    player_name="기존타자",
                    appearance_seq=1,
                    runs=1,
                ),
            ]
        )
        session.commit()

    repaired = game_repository.repair_game_parent_from_existing_children("20250404LGSS0")

    assert repaired is True

    with SessionLocal() as session:
        game = session.query(Game).filter(Game.game_id == "20250404LGSS0").one()
        inning = session.query(GameInningScore).filter(
            GameInningScore.game_id == "20250404LGSS0",
            GameInningScore.team_side == "away",
        ).one()

        assert game.away_team == "LG"
        assert game.home_team == "SS"
        assert game.away_score == 1
        assert game.home_score == 3
        assert game.game_status == "COMPLETED"
        assert game.winning_team == "SS"
        assert game.season_id == 2025
        assert inning.franchise_id == 3
        assert inning.canonical_team_code == "LG"
