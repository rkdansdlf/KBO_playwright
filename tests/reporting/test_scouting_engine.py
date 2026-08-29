"""Tests for KBO Sabermetric Scouting Report Engine and CLI."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.cli.generate_reports import main as report_main
from src.cli.kbo import main as kbo_main
from src.models.base import Base
from src.models.player import PlayerBasic, PlayerSeasonBatting, PlayerSeasonPitching
from src.reporting.engine import ReportingEngine
from src.reporting.scouting_dto import PlayerRole, ScoutingDimension, ScoutingReport
from src.reporting.scouting_engine import ScoutingReportEngine


def test_calc_percentile_logic() -> None:
    """Test percentile calculations for higher-is-better and lower-is-better."""
    vals = [10.0, 20.0, 30.0, 40.0, 50.0]
    p_high = ScoutingReportEngine._calc_percentile(45.0, vals, higher_is_better=True)
    assert p_high == 80.0

    p_low = ScoutingReportEngine._calc_percentile(15.0, vals, higher_is_better=False)
    assert p_low == 80.0


def test_scouting_report_ascii_card_and_markdown() -> None:
    """Test ASCII card rendering and Markdown export."""
    dims = [
        ScoutingDimension("컨택 능력 (Contact)", 95.0, "타율 .347", "S", "최상위 컨택"),
        ScoutingDimension("장타력 (Power)", 90.0, "ISO .263", "S", "파워 우수"),
        ScoutingDimension("선구안 (Discipline)", 80.0, "출루율 .420", "A+", "선구안 우수"),
        ScoutingDimension("기동력 (Speed)", 92.0, "도루 40개", "S", "스피드 탁월"),
        ScoutingDimension("종합 생산력 (Value)", 94.0, "OPS 1.060", "S", "MVP급 공격력"),
    ]
    report = ScoutingReport(
        player_id=12345,
        player_name="김도영",
        team_code="KIA",
        season=2024,
        role=PlayerRole.BATTER,
        overall_grade="S",
        scouting_tier="MVP 후보 / 리그 최정상급 타자",
        dimensions=dims,
        strengths=["컨택 능력", "장타력", "기동력"],
        weaknesses=[],
        classic_stats={"AVG": 0.347, "HR": 38},
        advanced_stats={"wRC+": 175.2, "WAR": 8.1},
    )

    ascii_card = report.to_ascii_card()
    assert "김도영" in ascii_card
    assert "[S]" in ascii_card
    assert "컨택 능력" in ascii_card

    md = report.to_markdown()
    assert "# ⚾ 김도영 (KIA) 2024 시즌 세이버메트릭스 스카우팅 리포트" in md
    assert "5대 핵심 차원 역량 평가" in md

    d = report.to_dict()
    assert d["player_id"] == 12345
    assert d["overall_grade"] == "S"
    assert len(d["dimensions"]) == 5


def test_scouting_engine_evaluate_batter_db() -> None:
    """Test evaluating a batter against database league distribution."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    session = session_factory()

    # Seed players
    p1 = PlayerBasic(player_id=10001, name="김도영")
    p2 = PlayerBasic(player_id=10002, name="비교선수A")
    p3 = PlayerBasic(player_id=10003, name="비교선수B")
    session.add_all([p1, p2, p3])

    # Seed season batting stats
    b1 = PlayerSeasonBatting(
        player_id=10001,
        season=2024,
        team_code="KIA",
        plate_appearances=500,
        at_bats=450,
        hits=150,
        doubles=30,
        triples=5,
        home_runs=35,
        walks=60,
        strikeouts=70,
        stolen_bases=30,
    )
    b2 = PlayerSeasonBatting(
        player_id=10002,
        season=2024,
        team_code="SSG",
        plate_appearances=400,
        at_bats=360,
        hits=90,
        doubles=15,
        triples=1,
        home_runs=10,
        walks=30,
        strikeouts=80,
        stolen_bases=5,
    )
    b3 = PlayerSeasonBatting(
        player_id=10003,
        season=2024,
        team_code="LG",
        plate_appearances=350,
        at_bats=320,
        hits=80,
        doubles=10,
        triples=0,
        home_runs=5,
        walks=20,
        strikeouts=90,
        stolen_bases=2,
    )
    session.add_all([b1, b2, b3])
    session.commit()

    scout_engine = ScoutingReportEngine(session)
    report = scout_engine.generate_scouting_report(player_name_or_id="김도영", year=2024)

    assert report.player_id == 10001
    assert report.role == PlayerRole.BATTER
    assert report.overall_grade in {"S", "A+", "A"}
    assert len(report.dimensions) == 5
    assert report.dimensions[0].score >= 80.0  # Top contact score


def test_scouting_engine_evaluate_pitcher_db() -> None:
    """Test evaluating a pitcher against database league distribution."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    session = session_factory()

    p1 = PlayerBasic(player_id=20001, name="원태인")
    p2 = PlayerBasic(player_id=20002, name="투수비교A")
    session.add_all([p1, p2])

    p_stat1 = PlayerSeasonPitching(
        player_id=20001,
        season=2024,
        team_code="SS",
        games=28,
        innings_outs=480,  # 160 IP
        hits_allowed=140,
        earned_runs=55,
        home_runs_allowed=12,
        walks_allowed=35,
        strikeouts=130,
        wins=15,
        losses=6,
    )
    p_stat2 = PlayerSeasonPitching(
        player_id=20002,
        season=2024,
        team_code="OB",
        games=25,
        innings_outs=300,  # 100 IP
        hits_allowed=120,
        earned_runs=60,
        home_runs_allowed=20,
        walks_allowed=50,
        strikeouts=70,
        wins=5,
        losses=10,
    )
    two_way_batting = PlayerSeasonBatting(
        player_id=20001,
        season=2024,
        team_code="SS",
        plate_appearances=100,
        at_bats=90,
        hits=20,
    )
    session.add_all([p_stat1, p_stat2, two_way_batting])
    session.commit()

    scout_engine = ScoutingReportEngine(session)
    report = scout_engine.generate_scouting_report(player_name_or_id="원태인", year=2024)

    assert report.player_id == 20001
    assert report.role == PlayerRole.PITCHER
    assert len(report.dimensions) == 5
    assert report.dimensions[0].name.startswith("탈삼진")


def test_reporting_engine_generate_scouting_report() -> None:
    """Test ReportingEngine integration for scouting reports."""
    mock_session = MagicMock()
    mock_session.query.return_value.filter.return_value.first.return_value = None
    mock_session.query.return_value.filter_by.return_value.first.return_value = None

    rep_engine = ReportingEngine()
    unified_report = rep_engine.generate_scouting_report(player_name_or_id="김도영", year=2024, session=mock_session)

    assert unified_report.category.value == "scouting"
    assert len(unified_report.sections) == 2
    assert "김도영" in unified_report.title


def test_generate_reports_cli_scouting(capsys) -> None:
    """Test CLI execution for scouting report."""
    exit_code = report_main(["--category", "scouting", "--player", "김도영", "--year", "2024", "--format", "json"])
    assert exit_code == 0

    captured = capsys.readouterr().out
    json_start = captured.find("{")
    assert json_start != -1
    data = json.loads(captured[json_start:])
    assert data["category"] == "scouting"
    assert "김도영" in data["title"]


def test_kbo_master_cli_report_scouting() -> None:
    """Test Master CLI routing kbo report --category scouting."""
    exit_code = kbo_main(["report", "--category", "scouting", "--player", "김도영", "--format", "json"])
    assert exit_code == 0
