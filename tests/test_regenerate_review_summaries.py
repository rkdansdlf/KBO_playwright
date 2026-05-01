from __future__ import annotations

import json
from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.cli import regenerate_review_summaries as cli
from src.models.game import Game, GameEvent, GamePitchingStat, GameSummary
from src.models.player import PlayerMovement, PlayerSeasonPitching
from src.models.team import TeamDailyRoster
from src.utils.game_status import GAME_STATUS_COMPLETED


def _build_session_factory():
    engine = create_engine("sqlite:///:memory:")
    for table in (
        Game.__table__,
        GameEvent.__table__,
        GamePitchingStat.__table__,
        GameSummary.__table__,
        PlayerSeasonPitching.__table__,
        PlayerMovement.__table__,
        TeamDailyRoster.__table__,
    ):
        table.create(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def _seed_review_game(SessionLocal, *, existing_detail: dict | None = None):
    with SessionLocal() as session:
        session.add(
            Game(
                game_id="20250401LGSS0",
                game_date=date(2025, 4, 1),
                away_team="LG",
                home_team="SS",
                away_score=4,
                home_score=2,
                away_pitcher="임찬규",
                home_pitcher="원태인",
                game_status=GAME_STATUS_COMPLETED,
            )
        )
        session.add_all(
            [
                GameEvent(
                    game_id="20250401LGSS0",
                    event_seq=1,
                    inning=8,
                    inning_half="top",
                    description="=====================================",
                    event_type="unknown",
                    wpa=0.9,
                    away_score=4,
                    home_score=2,
                ),
                GameEvent(
                    game_id="20250401LGSS0",
                    event_seq=2,
                    inning=8,
                    inning_half="top",
                    description="홍길동 : 좌중간 2루타",
                    event_type="batting",
                    batter_name="홍길동",
                    pitcher_name="원태인",
                    wpa=0.31,
                    away_score=4,
                    home_score=2,
                ),
            ]
        )
        session.add_all(
            [
                GamePitchingStat(
                    game_id="20250401LGSS0",
                    team_side="away",
                    player_name="임찬규",
                    is_starting=True,
                    appearance_seq=1,
                ),
                GamePitchingStat(
                    game_id="20250401LGSS0",
                    team_side="home",
                    player_name="원태인",
                    is_starting=True,
                    appearance_seq=1,
                ),
            ]
        )
        session.add(
            GameSummary(
                game_id="20250401LGSS0",
                summary_type="리뷰_WPA",
                detail_text=json.dumps(existing_detail or {"old": True}, ensure_ascii=False),
            )
        )
        session.commit()


def test_regenerate_review_summaries_dry_run_does_not_mutate(monkeypatch, tmp_path):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(cli, "SessionLocal", SessionLocal)
    _seed_review_game(SessionLocal)

    rows = cli.regenerate_review_summaries(
        game_ids=["20250401LGSS0"],
        apply=False,
        report_out=tmp_path / "report.csv",
        log=lambda _message: None,
    )

    assert rows[0].status == "DRY_RUN_READY"
    assert rows[0].crucial_moments == 1
    assert rows[0].noise_moments == 0
    with SessionLocal() as session:
        summary = session.query(GameSummary).one()
        assert json.loads(summary.detail_text) == {"old": True}
    assert (tmp_path / "report.csv").exists()


def test_regenerate_review_summaries_apply_updates_review_and_writes_backup(monkeypatch, tmp_path):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(cli, "SessionLocal", SessionLocal)
    _seed_review_game(SessionLocal)

    rows = cli.regenerate_review_summaries(
        game_ids=["20250401LGSS0"],
        apply=True,
        report_out=tmp_path / "report.csv",
        backup_out=tmp_path / "backup.csv",
        log=lambda _message: None,
    )

    assert rows[0].status == "APPLIED"
    assert (tmp_path / "backup.csv").exists()
    with SessionLocal() as session:
        payload = json.loads(session.query(GameSummary).one().detail_text)
    assert payload["game_id"] == "20250401LGSS0"
    assert payload["crucial_moments"][0]["description"] == "홍길동 : 좌중간 2루타"
    assert "old" not in payload


def test_regenerate_review_summaries_skips_noise_payload(monkeypatch, tmp_path):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(cli, "SessionLocal", SessionLocal)
    _seed_review_game(SessionLocal)

    def _fake_review_data(_agg, game):
        return {
            "game_id": game.game_id,
            "crucial_moments": [{"description": "1구 볼", "wpa": 0.8}],
        }

    monkeypatch.setattr(cli, "_build_review_data", _fake_review_data)

    rows = cli.regenerate_review_summaries(
        game_ids=["20250401LGSS0"],
        apply=True,
        report_out=tmp_path / "report.csv",
        backup_out=tmp_path / "backup.csv",
        log=lambda _message: None,
    )

    assert rows[0].status == "SKIPPED_REVIEW_MOMENT_NOISE"
    with SessionLocal() as session:
        summary = session.query(GameSummary).one()
        assert json.loads(summary.detail_text) == {"old": True}


def test_regenerate_review_summaries_syncs_successful_reviews(monkeypatch, tmp_path):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(cli, "SessionLocal", SessionLocal)
    _seed_review_game(SessionLocal, existing_detail=None)
    synced = []

    class _FakeOCISync:
        def __init__(self, _url, _session):
            pass

        def sync_review_summaries_for_games(self, game_ids, *, summary_type):
            synced.extend(game_ids)
            return {"summary": len(game_ids), "games": len(game_ids)}

        def close(self):
            pass

    monkeypatch.setattr(cli, "OCISync", _FakeOCISync)

    rows = cli.regenerate_review_summaries(
        game_ids=["20250401LGSS0"],
        apply=True,
        sync_oci=True,
        oci_url="postgresql://example/test",
        report_out=tmp_path / "report.csv",
        backup_out=tmp_path / "backup.csv",
        log=lambda _message: None,
    )

    assert synced == ["20250401LGSS0"]
    assert rows[0].oci_status == "synced_summary:1"
