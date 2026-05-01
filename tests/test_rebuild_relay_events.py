from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.cli import rebuild_relay_events as cli
from src.models.game import Game, GameEvent


def _build_session_factory():
    engine = create_engine("sqlite:///:memory:")
    Game.__table__.create(bind=engine)
    GameEvent.__table__.create(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def _seed_noisy_game(SessionLocal, *, game_id: str = "20250401LGSS0", game_score=(1, 0)):
    away_score, home_score = game_score
    with SessionLocal() as session:
        session.add(
            Game(
                game_id=game_id,
                game_date=date(2025, 4, 1),
                away_team="LG",
                home_team="SS",
                away_score=away_score,
                home_score=home_score,
                game_status="COMPLETED",
            )
        )
        session.add_all(
            [
                GameEvent(
                    game_id=game_id,
                    event_seq=1,
                    inning=1,
                    inning_half="top",
                    description="1회초 LG 공격",
                    event_type="unknown",
                    wpa=-0.9,
                    away_score=0,
                    home_score=0,
                    outs=0,
                    base_state=0,
                ),
                GameEvent(
                    game_id=game_id,
                    event_seq=2,
                    inning=1,
                    inning_half="top",
                    description="1구 볼",
                    event_type="unknown",
                    wpa=0.8,
                    away_score=0,
                    home_score=0,
                    outs=0,
                    base_state=0,
                ),
                GameEvent(
                    game_id=game_id,
                    event_seq=3,
                    inning=1,
                    inning_half="top",
                    description="홍길동 : 좌전 안타",
                    event_type="batting",
                    result_code="안타",
                    batter_name="홍길동",
                    wpa=0.1,
                    away_score=1,
                    home_score=0,
                    outs=0,
                    base_state=1,
                ),
                GameEvent(
                    game_id=game_id,
                    event_seq=4,
                    inning=1,
                    inning_half="top",
                    description="피치클락 위반 타자 경고 : 삼성 류지혁",
                    event_type="batting",
                    wpa=0.7,
                    away_score=1,
                    home_score=0,
                    outs=0,
                    base_state=1,
                ),
                GameEvent(
                    game_id=game_id,
                    event_seq=5,
                    inning=1,
                    inning_half="top",
                    description="=====================================",
                    event_type="unknown",
                    wpa=-1.0,
                    away_score=1,
                    home_score=0,
                    outs=0,
                    base_state=1,
                ),
            ]
        )
        session.commit()


def test_rebuild_relay_events_dry_run_does_not_mutate(monkeypatch, tmp_path):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(cli, "SessionLocal", SessionLocal)
    _seed_noisy_game(SessionLocal)

    rows = cli.rebuild_relay_events(
        seasons=[2025],
        apply=False,
        min_events=1,
        report_out=tmp_path / "report.csv",
        log=lambda _message: None,
    )

    assert rows[0].status == "DRY_RUN_READY"
    assert rows[0].old_rows == 5
    assert rows[0].new_rows == 1
    with SessionLocal() as session:
        assert session.query(GameEvent).filter(GameEvent.game_id == "20250401LGSS0").count() == 5
    assert (tmp_path / "report.csv").exists()


def test_rebuild_relay_events_apply_replaces_noisy_events(monkeypatch, tmp_path):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(cli, "SessionLocal", SessionLocal)
    _seed_noisy_game(SessionLocal)

    rows = cli.rebuild_relay_events(
        seasons=[2025],
        apply=True,
        min_events=1,
        report_out=tmp_path / "report.csv",
        backup_out=tmp_path / "backup.csv",
        log=lambda _message: None,
    )

    assert rows[0].status == "APPLIED"
    assert (tmp_path / "backup.csv").exists()
    with SessionLocal() as session:
        events = session.query(GameEvent).filter(GameEvent.game_id == "20250401LGSS0").all()
        assert len(events) == 1
        assert events[0].event_seq == 1
        assert events[0].description == "홍길동 : 좌전 안타"
        assert events[0].wpa is not None


def test_rebuild_relay_events_skips_score_mismatch(monkeypatch, tmp_path):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(cli, "SessionLocal", SessionLocal)
    _seed_noisy_game(SessionLocal, game_score=(2, 0))

    rows = cli.rebuild_relay_events(
        seasons=[2025],
        apply=True,
        min_events=1,
        report_out=tmp_path / "report.csv",
        backup_out=tmp_path / "backup.csv",
        log=lambda _message: None,
    )

    assert rows[0].status == "SKIPPED_SCORE_MISMATCH"
    with SessionLocal() as session:
        assert session.query(GameEvent).filter(GameEvent.game_id == "20250401LGSS0").count() == 5


def test_rebuild_relay_events_syncs_applied_games_with_event_batch(monkeypatch, tmp_path):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(cli, "SessionLocal", SessionLocal)
    _seed_noisy_game(SessionLocal)
    synced = []

    def _fake_sync_events(game_ids, report_rows, *, oci_url, log):
        synced.extend(game_ids)
        for row in report_rows:
            if row.game_id in game_ids:
                row.oci_status = "synced_events:1"

    monkeypatch.setattr(cli, "_sync_changed_events", _fake_sync_events)

    rows = cli.rebuild_relay_events(
        seasons=[2025],
        apply=True,
        sync_oci=True,
        oci_url="postgresql://example/test",
        min_events=1,
        report_out=tmp_path / "report.csv",
        backup_out=tmp_path / "backup.csv",
        log=lambda _message: None,
    )

    assert synced == ["20250401LGSS0"]
    assert rows[0].oci_status == "synced_events:1"


def test_rebuild_relay_events_specific_game_sync_mode(monkeypatch, tmp_path):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(cli, "SessionLocal", SessionLocal)
    _seed_noisy_game(SessionLocal)
    synced = []

    class _FakeOCISync:
        def __init__(self, _url, _session):
            pass

        def sync_specific_game(self, game_id):
            synced.append(game_id)
            return {"events": 1}

        def close(self):
            pass

    monkeypatch.setattr(cli, "OCISync", _FakeOCISync)

    rows = cli.rebuild_relay_events(
        seasons=[2025],
        apply=True,
        sync_oci=True,
        oci_sync_mode="specific-game",
        oci_url="postgresql://example/test",
        min_events=1,
        report_out=tmp_path / "report.csv",
        backup_out=tmp_path / "backup.csv",
        log=lambda _message: None,
    )

    assert synced == ["20250401LGSS0"]
    assert rows[0].oci_status == "synced"


def test_rebuild_relay_events_filters_requested_game_ids(monkeypatch, tmp_path):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(cli, "SessionLocal", SessionLocal)
    _seed_noisy_game(SessionLocal, game_id="20250401LGSS0")
    _seed_noisy_game(SessionLocal, game_id="20250402LGSS0")

    rows = cli.rebuild_relay_events(
        seasons=[2025],
        game_ids=["20250402LGSS0"],
        apply=False,
        min_events=1,
        report_out=tmp_path / "report.csv",
        log=lambda _message: None,
    )

    assert [row.game_id for row in rows] == ["20250402LGSS0"]


def test_rebuild_relay_events_reclassifies_other_runner_advance(monkeypatch, tmp_path):
    SessionLocal = _build_session_factory()
    monkeypatch.setattr(cli, "SessionLocal", SessionLocal)
    game_id = "20250511LTKT1"
    with SessionLocal() as session:
        session.add(
            Game(
                game_id=game_id,
                game_date=date(2025, 5, 11),
                away_team="LT",
                home_team="KT",
                away_score=4,
                home_score=1,
                game_status="COMPLETED",
            )
        )
        session.add(
            GameEvent(
                game_id=game_id,
                event_seq=49,
                inning=6,
                inning_half="top",
                description="롯데 윤동희 / 1루주자 이호준 : 폭투로 2루까지 진루",
                event_type="OTHER",
                away_score=4,
                home_score=1,
                outs=1,
                base_state=2,
            )
        )
        session.commit()

    rows = cli.rebuild_relay_events(
        seasons=[2025],
        game_ids=[game_id],
        apply=True,
        min_events=1,
        report_out=tmp_path / "report.csv",
        backup_out=tmp_path / "backup.csv",
        log=lambda _message: None,
    )

    assert rows[0].status == "APPLIED"
    with SessionLocal() as session:
        event = session.query(GameEvent).filter(GameEvent.game_id == game_id).one()
        assert event.event_type == "runner_advance"
        assert event.result_code == "폭투로 2루까지 진루"
