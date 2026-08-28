"""Tests for simulate CLI command and Master CLI integration."""

from __future__ import annotations

import json
from unittest.mock import patch

from src.cli.kbo import main as kbo_main
from src.cli.simulate_game import main as simulate_main


def test_simulate_cli_terminal_output(capsys) -> None:
    """Test simulate CLI terminal output."""
    exit_code = simulate_main(["--innings", "3", "--seed", "42"])
    assert exit_code == 0

    captured = capsys.readouterr().out
    assert "KBO 라이브 경기 시뮬레이션 시작" in captured
    assert "시뮬레이션 경기 종료" in captured
    assert "오늘의 영웅(MVP)" in captured


def test_simulate_cli_json_output(capsys) -> None:
    """Test simulate CLI JSON output format."""
    exit_code = simulate_main(["--innings", "2", "--seed", "42", "--json"])
    assert exit_code == 0

    captured = capsys.readouterr().out
    json_str = captured[captured.find("{") : captured.rfind("}") + 1]
    data = json.loads(json_str)
    assert data["game_id"] == "20260401LGHT0"
    assert data["total_innings"] >= 2
    assert "hero_player" in data
    assert "final_score" in data


def test_kbo_master_cli_simulate_dispatch() -> None:
    """Test Master CLI routing kbo simulate."""
    with patch("src.cli.simulate_game.main", return_value=0) as mock_sim:
        exit_code = kbo_main(["simulate", "--innings", "5", "--speed", "10"])
        assert exit_code == 0
        mock_sim.assert_called_once_with(["--innings", "5", "--speed", "10"])
