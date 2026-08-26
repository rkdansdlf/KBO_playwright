"""Unit tests for src.reporting.engine."""

from __future__ import annotations

import json
from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.models.base import Base
from src.models.game import Game, GameBattingStat
from src.models.player import PlayerBasic
from src.reporting.dto import ReportFormat
from src.reporting.engine import ReportingEngine


def test_generate_quality_report_and_rendering() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    rep_engine = ReportingEngine(engine=engine)
    with Session(engine) as session:
        # Add player and game
        p = PlayerBasic(player_id=1001, name="이승엽", team="SS", position="내야수")
        g = Game(
            game_id="20260401SSLG0",
            game_date=date(2026, 4, 1),
            stadium="대구",
            home_team="SS",
            away_team="LG",
            home_score=6,
            away_score=2,
            game_status="COMPLETED",
        )
        b = GameBattingStat(
            game_id="20260401SSLG0",
            team_side="home",
            team_code="SS",
            player_id=1001,
            player_name="이승엽",
            appearance_seq=1,
            plate_appearances=4,
            at_bats=3,
            walks=1,
            hbp=0,
            sacrifice_hits=0,
            sacrifice_flies=0,
        )
        session.add_all([p, g, b])
        session.commit()

        report = rep_engine.generate_quality_report(session=session)
        assert report.overall_status == "PASS"
        assert len(report.sections) >= 2

        # Test Markdown rendering
        md_text = rep_engine.render_report(report, format_type=ReportFormat.MARKDOWN)
        assert "# KBO Data Quality Intelligence Report" in md_text
        assert "PA Formula Integrity" in md_text

        # Test JSON rendering
        json_text = rep_engine.render_report(report, format_type=ReportFormat.JSON)
        data = json.loads(json_text)
        assert data["overall_status"] == "PASS"

        # Test HTML rendering
        html_text = rep_engine.render_report(report, format_type=ReportFormat.HTML)
        assert "<!DOCTYPE html>" in html_text
        assert "PA Formula Integrity" in html_text


def test_generate_executive_dashboard() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    rep_engine = ReportingEngine(engine=engine)
    with Session(engine) as session:
        report = rep_engine.generate_executive_dashboard(session=session)
        assert report.category == "executive_dashboard"
        assert len(report.sections) >= 3
