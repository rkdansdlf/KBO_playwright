from __future__ import annotations

from datetime import date, time

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

import src.repositories.game_helpers as game_helpers_module
import src.repositories.game_relay as game_relay_module
import src.repositories.game_repository as game_repository
import src.repositories.game_save as game_save_module
from src.models.game import (
    Game,
    GameBattingStat,
    GameEvent,
    GameIdAlias,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
    GameSummary,
)
from src.models.player import PlayerBasic
from src.services.game_write_contract import GameWriteContract
from src.utils.game_status import GAME_STATUS_CANCELLED, GAME_STATUS_COMPLETED, GAME_STATUS_LIVE, GAME_STATUS_SCHEDULED


class _FakeResolver:
    def __init__(self, session, **_kwargs):
        self.session = session

    def resolve_id(self, player_name: str, team_code: str, season: int, **kwargs):
        return {
            ("홍길동", "LG"): 1001,
            ("이승엽", "SS"): 2001,
        }.get((player_name, team_code))


def _build_session_factory():
    engine = create_engine("sqlite:///:memory:")
    for table in (
        Game.__table__,
        GameIdAlias.__table__,
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


def _build_fk_session_factory():
    engine = create_engine("sqlite:///:memory:")

    @event.listens_for(engine, "connect")
    def _enable_foreign_keys(dbapi_connection, _connection_record):
        dbapi_connection.execute("PRAGMA foreign_keys=ON")

    for table in (
        Game.__table__,
        GameIdAlias.__table__,
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
    monkeypatch.setattr(game_save_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_save_module, "_auto_sync_to_oci", lambda _game_id: None)
    monkeypatch.setattr(game_helpers_module, "PlayerIdResolver", _FakeResolver)
    monkeypatch.setattr(game_save_module, "_auto_sync_to_oci", lambda game_id: None)
    monkeypatch.setattr(game_relay_module, "_auto_sync_to_oci", lambda game_id: None)

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
        lineups = (
            session.query(GameLineup).filter(GameLineup.game_id == "20250401LGSS0").order_by(GameLineup.team_side).all()
        )

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
        assert (
            session.query(GameSummary)
            .filter(
                GameSummary.game_id == "20250401LGSS0",
                GameSummary.summary_type == "프리뷰",
            )
            .count()
            == 1
        )


def test_save_game_snapshot_preserves_detail_rows_and_start_time(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(game_save_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_relay_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_save_module, "_auto_sync_to_oci", lambda game_id: None)
    monkeypatch.setattr(game_relay_module, "_auto_sync_to_oci", lambda game_id: None)

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

        assert game.game_status == GAME_STATUS_COMPLETED
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
    monkeypatch.setattr(game_save_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_relay_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_save_module, "_auto_sync_to_oci", lambda game_id: None)
    monkeypatch.setattr(game_relay_module, "_auto_sync_to_oci", lambda game_id: None)

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


def test_schedule_write_contract_logs_duplicate_field_skips(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(game_save_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_relay_module, "SessionLocal", SessionLocal)

    logs: list[str] = []
    contract = GameWriteContract(run_label="unit-schedule", log=logs.append, log_duplicate_fields=True)
    payload = {
        "game_id": "20250405LGSS0",
        "game_date": "20250405",
        "home_team_code": "SS",
        "away_team_code": "LG",
        "season_year": 2025,
        "season_type": "regular",
        "game_time": "18:30",
        "stadium": "잠실",
    }

    assert game_repository.save_schedule_game(
        payload,
        write_contract=contract,
        source_reason="monthly_schedule_refresh:2025-04",
    )
    assert game_repository.save_schedule_game(
        payload,
        write_contract=contract,
        source_reason="monthly_schedule_refresh:2025-04",
    )

    assert any("field=game_date" in line and line.startswith("[WRITE]") for line in logs)
    assert any("field=game_date" in line and line.startswith("[SKIP]") for line in logs)
    assert contract.updated_fields > 0
    assert contract.duplicate_fields > 0


def test_save_game_detail_skips_identical_child_rewrites(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(game_save_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_relay_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_save_module, "_auto_sync_to_oci", lambda game_id: None)
    monkeypatch.setattr(game_relay_module, "_auto_sync_to_oci", lambda game_id: None)

    logs: list[str] = []
    contract = GameWriteContract(run_label="unit-detail", log=logs.append)
    payload = {
        "game_id": "20250406LGSS0",
        "game_date": "20250406",
        "metadata": {"stadium": "잠실", "start_time": "18:30"},
        "teams": {
            "away": {"code": "LG", "score": 2, "line_score": [1, 1, 0]},
            "home": {"code": "SS", "score": 1, "line_score": [0, 0, 1]},
        },
        "hitters": {
            "away": [
                {
                    "player_id": 1001,
                    "player_name": "홍길동",
                    "team_code": "LG",
                    "batting_order": 1,
                    "is_starter": True,
                    "appearance_seq": 1,
                    "position": "중견수",
                    "stats": {"plate_appearances": 4, "at_bats": 4, "runs": 1, "hits": 2},
                }
            ],
            "home": [
                {
                    "player_id": 2001,
                    "player_name": "이승엽",
                    "team_code": "SS",
                    "batting_order": 4,
                    "is_starter": True,
                    "appearance_seq": 1,
                    "position": "1루수",
                    "stats": {"plate_appearances": 4, "at_bats": 4, "runs": 1, "hits": 1},
                }
            ],
        },
        "pitchers": {
            "away": [
                {
                    "player_id": 3001,
                    "player_name": "임찬규",
                    "team_code": "LG",
                    "is_starting": True,
                    "appearance_seq": 1,
                    "stats": {"innings_outs": 18, "runs_allowed": 1},
                }
            ],
            "home": [
                {
                    "player_id": 4001,
                    "player_name": "원태인",
                    "team_code": "SS",
                    "is_starting": True,
                    "appearance_seq": 1,
                    "stats": {"innings_outs": 18, "runs_allowed": 2},
                }
            ],
        },
    }

    assert game_repository.save_game_detail(payload, write_contract=contract, source_reason="postgame_finalize")
    assert game_repository.save_game_detail(payload, write_contract=contract, source_reason="postgame_finalize")

    assert any("dataset=game_batting_stats rows=2" in line for line in logs)
    assert any("dataset=game_batting_stats duplicate_rows=2" in line for line in logs)
    assert any("dataset=game_pitching_stats duplicate_rows=2" in line for line in logs)
    assert contract.replaced_datasets > 0
    assert contract.duplicate_datasets > 0


def test_save_game_detail_honors_explicit_cancelled_status(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(game_save_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_relay_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_save_module, "_auto_sync_to_oci", lambda game_id: None)
    monkeypatch.setattr(game_relay_module, "_auto_sync_to_oci", lambda game_id: None)

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


def test_save_game_detail_creates_player_basic_stubs_for_new_payload_ids(monkeypatch):
    SessionLocal = _build_fk_session_factory()
    monkeypatch.setattr(game_save_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_relay_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_save_module, "_auto_sync_to_oci", lambda game_id: None)
    monkeypatch.setattr(game_relay_module, "_auto_sync_to_oci", lambda game_id: None)

    saved = game_repository.save_game_detail(
        {
            "game_id": "20260515HHKT0",
            "game_date": "20260515",
            "metadata": {"stadium": "수원"},
            "teams": {
                "away": {"code": "HH", "score": 5, "line_score": [1, 0, 0, 0, 0, 0, 0, 2, 2]},
                "home": {"code": "KT", "score": 3, "line_score": [0, 1, 0, 0, 0, 0, 0, 2, 0]},
            },
            "hitters": {
                "away": [
                    {
                        "player_id": 56760,
                        "player_name": "쿠싱",
                        "team_code": "HH",
                        "uniform_no": "5",
                        "batting_order": 1,
                        "position": "중",
                        "is_starter": True,
                        "appearance_seq": 1,
                        "stats": {"plate_appearances": 4, "at_bats": 4, "runs": 1, "hits": 2},
                    }
                ],
                "home": [
                    {
                        "player_id": 66606,
                        "player_name": "최원준",
                        "team_code": "KT",
                        "uniform_no": "61",
                        "batting_order": 1,
                        "position": "우",
                        "is_starter": True,
                        "appearance_seq": 1,
                        "stats": {"plate_appearances": 4, "at_bats": 4, "runs": 0, "hits": 1},
                    }
                ],
            },
            "pitchers": {
                "away": [
                    {
                        "player_id": 56761,
                        "player_name": "왕옌청",
                        "team_code": "HH",
                        "is_starting": True,
                        "appearance_seq": 1,
                        "stats": {"innings_outs": 18, "runs_allowed": 3},
                    }
                ],
                "home": [
                    {
                        "player_id": 64001,
                        "player_name": "고영표",
                        "team_code": "KT",
                        "is_starting": True,
                        "appearance_seq": 1,
                        "stats": {"innings_outs": 18, "runs_allowed": 5},
                    }
                ],
            },
        }
    )

    assert saved is True
    with SessionLocal() as session:
        stubs = {
            row.player_id: row
            for row in session.query(PlayerBasic).filter(PlayerBasic.player_id.in_([56760, 66606, 56761, 64001])).all()
        }
        assert set(stubs) == {56760, 66606, 56761, 64001}
        assert stubs[56760].name == "쿠싱"
        assert stubs[66606].team == "KT"
        assert stubs[56761].position == "투수"
        assert all(row.status == "STUB" for row in stubs.values())
        assert session.query(GameBattingStat).filter(GameBattingStat.game_id == "20260515HHKT0").count() == 2
        assert session.query(GamePitchingStat).filter(GamePitchingStat.game_id == "20260515HHKT0").count() == 2


def test_save_game_detail_rejects_same_player_id_on_both_teams(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(game_save_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_relay_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_save_module, "_auto_sync_to_oci", lambda game_id: None)
    monkeypatch.setattr(game_relay_module, "_auto_sync_to_oci", lambda game_id: None)

    saved = game_repository.save_game_detail(
        {
            "game_id": "20010726SSHH0",
            "game_date": "20010726",
            "metadata": {"stadium": "대구"},
            "teams": {
                "away": {"code": "SS", "score": 3, "line_score": [1, 2]},
                "home": {"code": "HH", "score": 2, "line_score": [0, 2]},
            },
            "hitters": {
                "away": [
                    {
                        "player_id": 94415,
                        "player_name": "김태균",
                        "team_code": "SS",
                        "appearance_seq": 1,
                        "stats": {"plate_appearances": 1, "at_bats": 1, "hits": 1},
                    }
                ],
                "home": [
                    {
                        "player_id": 94415,
                        "player_name": "김태균",
                        "team_code": "HH",
                        "appearance_seq": 1,
                        "stats": {"plate_appearances": 1, "at_bats": 1, "hits": 1},
                    }
                ],
            },
            "pitchers": {"away": [], "home": []},
        }
    )

    assert saved is False
    with SessionLocal() as session:
        assert session.query(Game).filter(Game.game_id == "20010726SSHH0").count() == 0
        assert session.query(GameBattingStat).filter(GameBattingStat.game_id == "20010726SSHH0").count() == 0


def test_repair_game_parent_from_existing_children_uses_child_scores(monkeypatch):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(game_save_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_relay_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_save_module, "_auto_sync_to_oci", lambda game_id: None)
    monkeypatch.setattr(game_relay_module, "_auto_sync_to_oci", lambda game_id: None)

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
        inning = (
            session.query(GameInningScore)
            .filter(
                GameInningScore.game_id == "20250404LGSS0",
                GameInningScore.team_side == "away",
            )
            .one()
        )

        assert game.away_team == "LG"
        assert game.home_team == "SS"
        assert game.away_score == 1
        assert game.home_score == 3
        assert game.game_status == "COMPLETED"
        assert game.winning_team == "SS"
        assert game.season_id == 2025
        assert inning.franchise_id == 3
        assert inning.canonical_team_code == "LG"
