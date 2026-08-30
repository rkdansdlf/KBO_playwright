"""Unit and integration tests for KBO Player Similarity Search and 1:1 Comparison Engine."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from fastapi.testclient import TestClient

from src.analytics.similarity import PlayerSimilarityEngine
from src.analytics.similarity_dto import (
    HeadToHeadComparisonResult,
    PlayerRole,
    PlayerSimilarityResult,
    PlayerVector,
)
from src.api.app import app
from src.cli.compare_players import main as compare_cli_main
from src.cli.kbo import main as kbo_master_main

if TYPE_CHECKING:
    import pytest


def test_player_vector_and_similarity_math() -> None:
    """Test mathematical accuracy of cosine similarity calculation."""
    vec1 = PlayerVector(
        player_id=1,
        player_name="PlayerA",
        team_code="AAA",
        season=2024,
        role=PlayerRole.BATTER,
        dimensions={"A": 100.0, "B": 100.0, "C": 100.0},
    )
    vec2 = PlayerVector(
        player_id=2,
        player_name="PlayerB",
        team_code="BBB",
        season=2024,
        role=PlayerRole.BATTER,
        dimensions={"A": 100.0, "B": 100.0, "C": 100.0},
    )
    vec3 = PlayerVector(
        player_id=3,
        player_name="PlayerC",
        team_code="CCC",
        season=2024,
        role=PlayerRole.BATTER,
        dimensions={"A": 50.0, "B": 50.0, "C": 50.0},
    )

    # Identical vectors should have similarity 1.0
    sim_1_2 = PlayerSimilarityEngine.compute_cosine_similarity(vec1, vec2)
    assert round(sim_1_2, 4) == 1.0000

    # Parallel vectors with different magnitudes also have cosine similarity 1.0
    sim_1_3 = PlayerSimilarityEngine.compute_cosine_similarity(vec1, vec3)
    assert round(sim_1_3, 4) == 1.0000

    # Orthogonal vectors should have similarity 0.0
    vec_ortho = PlayerVector(
        player_id=4,
        player_name="PlayerOrtho",
        team_code="DDD",
        season=2024,
        role=PlayerRole.BATTER,
        dimensions={"D": 100.0},
    )
    sim_ortho = PlayerSimilarityEngine.compute_cosine_similarity(vec1, vec_ortho)
    assert sim_ortho == 0.0


def test_style_classification() -> None:
    """Test qualitative player archetype style classification."""
    slugger = PlayerVector(
        player_id=1,
        player_name="Slugger",
        team_code="AAA",
        season=2024,
        role=PlayerRole.BATTER,
        dimensions={"장타력 (Power)": 95.0, "기동력 (Speed)": 40.0, "컨택 능력 (Contact)": 70.0},
    )
    style_slugger = PlayerSimilarityEngine.classify_player_style(slugger)
    assert "슬러거" in style_slugger or "거포" in style_slugger

    five_tool = PlayerVector(
        player_id=2,
        player_name="FiveTool",
        team_code="BBB",
        season=2024,
        role=PlayerRole.BATTER,
        dimensions={"장타력 (Power)": 90.0, "기동력 (Speed)": 92.0, "컨택 능력 (Contact)": 88.0},
    )
    style_five = PlayerSimilarityEngine.classify_player_style(five_tool)
    assert "5툴" in style_five or "호타준족" in style_five

    pitcher = PlayerVector(
        player_id=3,
        player_name="Ace",
        team_code="CCC",
        season=2024,
        role=PlayerRole.PITCHER,
        dimensions={"구위 (Stuff)": 92.0, "제구력 (Command)": 88.0, "이닝 소화력 (Workhorse)": 90.0},
    )
    style_ace = PlayerSimilarityEngine.classify_player_style(pitcher)
    assert "에이스" in style_ace or "파이어볼러" in style_ace


def test_find_similar_players_ranking() -> None:
    """Test finding and ranking top-K similar players."""
    engine = PlayerSimilarityEngine()

    result = engine.find_similar_players("김도영", season=2024, top_k=3)
    assert isinstance(result, PlayerSimilarityResult)
    assert result.target_player.player_name == "김도영"
    assert len(result.matches) <= 3
    assert len(result.matches) > 0

    # Ensure rankings are in descending order of similarity
    scores = [m.similarity_score for m in result.matches]
    assert scores == sorted(scores, reverse=True)

    # Top match for Kim Do-young should be high similarity (e.g. Lee Jong-beom or Lee Jung-hoo)
    top_match = result.matches[0]
    assert top_match.similarity_score > 0.90
    assert len(top_match.style_tag) > 0


def test_head_to_head_comparison() -> None:
    """Test 1:1 head-to-head comparison between two players."""
    engine = PlayerSimilarityEngine()

    cmp = engine.compare_players("김도영", "이종범")
    assert isinstance(cmp, HeadToHeadComparisonResult)
    assert cmp.player1.player_name == "김도영"
    assert cmp.player2.player_name == "이종범"
    assert cmp.similarity_score > 0.90
    assert len(cmp.dimension_diffs) >= 5
    assert len(cmp.verdict_summary) > 0


def test_similarity_and_comparison_visualizations() -> None:
    """Test ASCII cards, radar charts, Markdown docs, and JSON serialization."""
    engine = PlayerSimilarityEngine()

    sim_res = engine.find_similar_players("김도영", top_k=3)
    ascii_card = sim_res.to_ascii_card()
    assert "KBO PLAYER SIMILARITY SEARCH" in ascii_card
    assert "김도영" in ascii_card

    md_sim = sim_res.to_markdown()
    assert "# 🔍 KBO 선수 유사도 분석 리포트" in md_sim
    assert "## 🏆 역대 가장 유사한 KBO 선수 랭킹" in md_sim

    cmp_res = engine.compare_players("김도영", "이종범")
    ascii_radar = cmp_res.to_ascii_radar()
    assert "KBO 1:1 COMPARISON" in ascii_radar
    assert "김도영" in ascii_radar
    assert "이종범" in ascii_radar

    md_cmp = cmp_res.to_markdown()
    assert "# ⚔️ KBO 1:1 세이버메트릭스 비교" in md_cmp
    assert "## 📊 5대 역량 세이버메트릭스 비교" in md_cmp

    d_cmp = cmp_res.to_dict()
    assert "player1" in d_cmp
    assert "similarity_score" in d_cmp
    assert "verdict_summary" in d_cmp


def test_compare_players_cli_execution(capsys: pytest.CaptureFixture[str]) -> None:
    """Test compare_players CLI command with 1:1 and find-similar modes."""
    # 1:1 Comparison
    code1 = compare_cli_main(["--player1", "김도영", "--player2", "이종범"])
    assert code1 == 0
    cap1 = capsys.readouterr()
    assert "KBO 1:1 COMPARISON" in cap1.out

    # Find Similar Mode
    code2 = compare_cli_main(["--find-similar", "김도영", "--top-k", "3"])
    assert code2 == 0
    cap2 = capsys.readouterr()
    assert "KBO PLAYER SIMILARITY SEARCH" in cap2.out

    # JSON Format
    code3 = compare_cli_main(["--player1", "김도영", "--player2", "이정후", "--json"])
    assert code3 == 0
    cap3 = capsys.readouterr()
    json_str = cap3.out[cap3.out.find("{") :]
    data = json.loads(json_str)
    assert "player1" in data
    assert data["player1"]["player_name"] == "김도영"
    assert "similarity_score" in data


def test_kbo_master_cli_compare(capsys: pytest.CaptureFixture[str]) -> None:
    """Test kbo master CLI compare subcommand."""
    code = kbo_master_main(["compare", "--player1", "김도영", "--player2", "이종범"])
    assert code == 0
    cap = capsys.readouterr()
    assert "KBO 1:1 COMPARISON" in cap.out


def test_fastapi_analytics_compare_and_similarity_endpoints() -> None:
    """Test FastAPI REST endpoints for player comparison and similarity search."""
    client = TestClient(app)

    # 1. 1:1 Compare endpoint
    res1 = client.get(
        "/api/analytics/compare",
        params={"player1": "김도영", "player2": "이종범"},
        headers={"X-API-Key": "test-api-key"},
    )
    assert res1.status_code == 200
    data1 = res1.json()
    assert "player1" in data1
    assert data1["player1"]["player_name"] == "김도영"
    assert "similarity_score" in data1

    # 2. Similarity endpoint
    res2 = client.get(
        "/api/analytics/similarity/김도영",
        params={"top_k": 3},
        headers={"X-API-Key": "test-api-key"},
    )
    assert res2.status_code == 200
    data2 = res2.json()
    assert "target_player" in data2
    assert data2["target_player"]["player_name"] == "김도영"
    assert len(data2["matches"]) <= 3
