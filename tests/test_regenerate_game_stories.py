from __future__ import annotations

import json
from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.cli import regenerate_game_stories as cli
from src.models.game import Game, GameEvent, GameSummary
from src.services.game_story_builder import STORY_SUMMARY_TYPE
from src.utils.game_status import GAME_STATUS_COMPLETED


def _build_session_factory():
    engine = create_engine("sqlite:///:memory:")
    for table in (
        Game.__table__,
        GameEvent.__table__,
        GameSummary.__table__,
    ):
        table.create(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def _seed_story_game(SessionLocal, *, existing_detail: dict | None = None):
    with SessionLocal() as session:
        session.add(
            Game(
                game_id="20250405HHSS0",
                game_date=date(2025, 4, 5),
                away_team="HH",
                home_team="SS",
                away_score=7,
                home_score=6,
                game_status=GAME_STATUS_COMPLETED,
            )
        )
        session.add(
            GameEvent(
                game_id="20250405HHSS0",
                event_seq=1,
                inning=9,
                inning_half="TOP",
                description="우익수 뒤 홈런 (홈런거리:120M)",
                event_type="HIT",
                result_code="HR",
                rbi=1,
                batter_name="문현빈",
                pitcher_name="김재윤",
                wpa=0.5,
                away_score=7,
                home_score=6,
            )
        )
        session.add(
            GameSummary(
                game_id="20250405HHSS0",
                summary_type=STORY_SUMMARY_TYPE,
                detail_text=json.dumps(existing_detail or {"old": True}, ensure_ascii=False),
            )
        )
        session.commit()


def test_regenerate_game_stories_dry_run_does_not_mutate(monkeypatch, tmp_path):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(cli, "SessionLocal", SessionLocal)
    _seed_story_game(SessionLocal)

    rows = cli.regenerate_game_stories(
        game_ids=["20250405HHSS0"],
        apply=False,
        report_out=tmp_path / "report.csv",
        log=lambda _message: None,
    )

    assert rows[0].status == "DRY_RUN_READY"
    assert rows[0].timeline_events == 1
    with SessionLocal() as session:
        summary = session.query(GameSummary).one()
        assert json.loads(summary.detail_text) == {"old": True}
    assert (tmp_path / "report.csv").exists()


def test_regenerate_game_stories_apply_updates_story_and_writes_backup(monkeypatch, tmp_path):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(cli, "SessionLocal", SessionLocal)
    _seed_story_game(SessionLocal)

    rows = cli.regenerate_game_stories(
        game_ids=["20250405HHSS0"],
        apply=True,
        report_out=tmp_path / "report.csv",
        backup_out=tmp_path / "backup.csv",
        log=lambda _message: None,
    )

    assert rows[0].status == "APPLIED"
    assert (tmp_path / "backup.csv").exists()
    with SessionLocal() as session:
        payload = json.loads(session.query(GameSummary).one().detail_text)
    assert payload["game_id"] == "20250405HHSS0"
    assert payload["timeline"][0]["description"] == "우익수 뒤 홈런 (홈런거리:120M)"
    assert "old" not in payload


def test_regenerate_game_stories_syncs_story_summary_type(monkeypatch, tmp_path):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(cli, "SessionLocal", SessionLocal)
    _seed_story_game(SessionLocal)
    synced = []

    class _FakeOCISync:
        def __init__(self, _url, _session):
            pass

        def sync_review_summaries_for_games(self, game_ids, *, summary_type):
            synced.append((list(game_ids), summary_type))
            return {"summary": len(game_ids), "games": len(game_ids)}

        def close(self):
            pass

    monkeypatch.setattr(cli, "OCISync", _FakeOCISync)

    rows = cli.regenerate_game_stories(
        game_ids=["20250405HHSS0"],
        apply=True,
        sync_oci=True,
        oci_url="postgresql://example/test",
        report_out=tmp_path / "report.csv",
        backup_out=tmp_path / "backup.csv",
        log=lambda _message: None,
    )

    assert synced == [(["20250405HHSS0"], STORY_SUMMARY_TYPE)]
    assert rows[0].oci_status == "synced_summary:1"
