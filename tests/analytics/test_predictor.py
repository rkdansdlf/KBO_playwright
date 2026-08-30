"""Tests for KBO Game Matchup Win Predictor and Feature Store Engine."""

from __future__ import annotations

import json

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.analytics.predictor import MatchupPredictor, SabermetricFeatureStore
from src.analytics.predictor_dto import MatchupFeatureVector, MatchupPredictionResult
from src.api.app import app
from src.cli.kbo import main as kbo_master_main
from src.cli.predict_matchups import main as predict_cli_main
from src.models.base import Base
from src.models.game import Game
from src.models.player import PlayerBasic, PlayerSeasonPitching
from src.models.team_stats import TeamSeasonBatting


def test_feature_store_fallback_vector() -> None:
    """Test feature store generates a valid fallback feature vector."""
    store = SabermetricFeatureStore(None)
    vector = store.extract_features_for_game("20240829LGKIA0")

    assert isinstance(vector, MatchupFeatureVector)
    assert vector.home_team == "KIA"
    assert vector.away_team == "LG"
    assert vector.home_starter_fip > 0.0
    assert vector.away_starter_fip > 0.0
    assert vector.home_team_wrc_plus > 0.0


def test_matchup_predictor_calculations() -> None:
    """Test win probabilities, expected runs, and key factors generation."""
    predictor = MatchupPredictor()
    vector = MatchupFeatureVector(
        game_id="20240901LGKIA0",
        game_date="2024-09-01",
        home_team="KIA",
        away_team="LG",
        stadium="Gwangju",
        home_starter_name="양현종",
        home_starter_fip=3.45,
        home_starter_whip=1.18,
        away_starter_name="임찬규",
        away_starter_fip=4.50,
        away_starter_whip=1.42,
        home_team_wrc_plus=115.0,
        home_team_ops=0.820,
        away_team_wrc_plus=102.0,
        away_team_ops=0.760,
        home_bullpen_era=3.80,
        away_bullpen_era=4.40,
        h2h_home_wins=9,
        h2h_away_wins=4,
    )
    result = predictor.predict_matchup(vector)

    assert isinstance(result, MatchupPredictionResult)
    assert result.home_win_prob > 0.50
    assert result.predicted_winner == "KIA"
    assert result.predicted_home_score > result.predicted_away_score
    assert result.predicted_total_runs > 0.0
    assert result.confidence_tier in {"HIGH", "MEDIUM", "TOSS_UP"}
    assert len(result.key_factors) > 0


def test_matchup_prediction_visualizations() -> None:
    """Test ASCII card and Markdown formatting."""
    predictor = MatchupPredictor()
    vector = MatchupFeatureVector(
        game_id="20240809LGKIA0",
        game_date="2024-08-09",
        home_team="KIA",
        away_team="LG",
    )
    result = predictor.predict_matchup(vector)

    ascii_card = result.to_ascii_card()
    assert "KBO MATCHUP WIN PREDICTION" in ascii_card
    assert "WIN PROB" in ascii_card
    assert "STARTERS" in ascii_card

    markdown_doc = result.to_markdown()
    assert "# ⚾ KBO 경기 승부 예측" in markdown_doc
    assert "## 📊 승률 및 예상 스코어 분석" in markdown_doc
    assert "## 🎯 핵심 승부 요인" in markdown_doc

    d = result.to_dict()
    assert d["game_id"] == "20240809LGKIA0"
    assert "home_win_prob" in d
    assert "features" in d


def test_feature_store_db_extraction() -> None:
    """Test feature store extracting stats from database session."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    session = session_factory()

    from datetime import date

    # Seed test data
    game = Game(
        game_id="20240901LGKIA0",
        game_date=date(2024, 9, 1),
        home_team="KIA",
        away_team="LG",
        stadium="광주기아챔피언스필드",
        home_pitcher="양현종",
        away_pitcher="엔스",
        season_id=2024,
    )
    session.add(game)

    pb = PlayerBasic(player_id=60001, name="양현종")
    session.add(pb)

    pitching = PlayerSeasonPitching(
        player_id=60001,
        season=2024,
        team_code="KIA",
        innings_outs=450,
        strikeouts=140,
        walks_allowed=45,
        home_runs_allowed=12,
        earned_runs=60,
    )
    session.add(pitching)

    batting = TeamSeasonBatting(
        team_id="KIA",
        team_name="KIA 타이거즈",
        season=2024,
        ops=0.825,
    )
    session.add(batting)
    session.commit()

    store = SabermetricFeatureStore(session)
    vector = store.extract_features_for_game("20240901LGKIA0")

    assert vector.home_team == "KIA"
    assert vector.home_starter_name == "양현종"
    assert vector.home_starter_fip < 4.0
    assert round(vector.home_team_wrc_plus, 1) == 110.0

    session.close()


def test_predict_matchups_cli_execution(capsys) -> None:
    """Test predict_matchups CLI command with text and JSON formats."""
    # Text format
    code1 = predict_cli_main(["--home", "KIA", "--away", "LG", "--format", "text"])
    assert code1 == 0
    captured1 = capsys.readouterr()
    assert "KBO MATCHUP WIN PREDICTION" in captured1.out

    # JSON format
    code2 = predict_cli_main(["--home", "DOOSAN", "--away", "SSG", "--json"])
    assert code2 == 0
    captured2 = capsys.readouterr()
    json_str = captured2.out[captured2.out.find("{") :]
    data = json.loads(json_str)
    assert data["home_team"] == "DOOSAN"
    assert data["away_team"] == "SSG"
    assert "home_win_prob" in data


def test_kbo_master_cli_predict(capsys) -> None:
    """Test kbo predict master CLI subcommand."""
    code = kbo_master_main(["predict", "--home", "KIA", "--away", "HANWHA", "--format", "markdown"])
    assert code == 0
    captured = capsys.readouterr()
    assert "# ⚾ KBO 경기 승부 예측" in captured.out


def test_fastapi_analytics_predict_endpoint() -> None:
    """Test GET /api/analytics/predict/{game_id} endpoint."""
    client = TestClient(app)
    resp = client.get("/api/analytics/predict/20240829LGKIA0")

    assert resp.status_code == 200
    data = resp.json()
    assert data["game_id"] == "20240829LGKIA0"
    assert "home_win_prob" in data
    assert "predicted_winner" in data
    assert "features" in data
