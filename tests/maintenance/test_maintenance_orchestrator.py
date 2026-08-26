"""Unit tests for src.maintenance.orchestrator."""

from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.maintenance.orchestrator import MaintenanceOrchestrator
from src.models.base import Base
from src.models.game import Game, GameBattingStat
from src.models.player import PlayerBasic


def test_maintenance_list_tasks() -> None:
    orch = MaintenanceOrchestrator()
    tasks = orch.list_tasks()
    assert len(tasks) >= 4
    assert any(t.task_name == "pa_formula_audit" for t in tasks)
    assert any(t.task_name == "null_player_ids" for t in tasks)


def test_run_pa_formula_audit_and_fix() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    orch = MaintenanceOrchestrator(engine=engine)
    with Session(engine) as session:
        # Create game parent
        game = Game(
            game_id="20260401OBLG0",
            game_date=date(2026, 4, 1),
            stadium="잠실",
            home_team="LG",
            away_team="OB",
            home_score=5,
            away_score=3,
        )
        session.add(game)
        # Create batting stat with mismatched PA (PA=5 but AB=4, BB=0, HBP=0, SH=0, SF=0 -> should be 4)
        stat = GameBattingStat(
            game_id="20260401OBLG0",
            team_side="home",
            team_code="LG",
            player_id=101,
            player_name="타자A",
            appearance_seq=1,
            plate_appearances=5,
            at_bats=4,
            walks=0,
            hbp=0,
            sacrifice_hits=0,
            sacrifice_flies=0,
        )
        session.add(stat)
        session.commit()

        # Dry run audit
        dry_res = orch.run_pa_formula_audit(session=session, apply=False)
        assert dry_res.status == "DRY_RUN"
        assert dry_res.rows_affected == 1

        # Apply fix
        apply_res = orch.run_pa_formula_audit(session=session, apply=True)
        session.commit()
        assert apply_res.status == "SUCCESS"
        assert apply_res.rows_affected == 1

        # Second audit should be clean (0 rows affected)
        clean_res = orch.run_pa_formula_audit(session=session, apply=False)
        assert clean_res.rows_affected == 0


def test_run_null_player_ids_audit() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    orch = MaintenanceOrchestrator(engine=engine)
    with Session(engine) as session:
        # Register player in PlayerBasic
        player = PlayerBasic(
            player_id=888001,
            name="홍길동",
            team="LG",
            position="외야수",
        )
        session.add(player)

        # Create batting stat with NULL player_id
        stat = GameBattingStat(
            game_id="20260401OBLG0",
            team_side="home",
            team_code="LG",
            player_id=None,
            player_name="홍길동",
            appearance_seq=1,
            plate_appearances=4,
            at_bats=4,
        )
        session.add(stat)
        session.commit()

        # Run resolution with apply
        res = orch.run_null_player_ids_audit(session=session, apply=True)
        session.commit()

        assert res.status == "SUCCESS"
        assert res.rows_affected == 1

        # Verify player_id is updated
        updated_stat = session.query(GameBattingStat).filter_by(player_name="홍길동").first()
        assert updated_stat is not None
        assert updated_stat.player_id == 888001


def test_run_all_report() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    orch = MaintenanceOrchestrator(engine=engine)
    with Session(engine) as session:
        report = orch.run_all(session=session, apply=False)
        assert report.total_tasks == 4
        assert report.failed_tasks == 0
        assert len(report.results) == 4
