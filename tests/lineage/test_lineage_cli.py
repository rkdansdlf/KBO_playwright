"""Tests for Lineage CLI Commands."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, text

from src.cli.kbo import main as kbo_main
from src.cli.lineage import main as lineage_main
from src.models.base import Base

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path


@pytest.fixture(name="lineage_test_engine", scope="module")
def _lineage_test_engine(tmp_path_factory: pytest.TempPathFactory) -> Generator[create_engine, None, None]:
    """Create test SQLite database with seeded records for CLI verification."""
    db_file = tmp_path_factory.mktemp("lineage_cli_db") / "test.db"
    test_engine = create_engine(f"sqlite:///{db_file}")
    Base.metadata.create_all(bind=test_engine)

    with test_engine.begin() as conn:
        conn.execute(
            text("""
            INSERT INTO player_basic (player_id, name) VALUES (75847, '최정');
        """)
        )
        conn.execute(
            text("""
            INSERT INTO game (game_id, season_id, game_date, home_team, away_team, home_score, away_score, game_status, stadium)
            VALUES ('20210523LTOB0', 2021, '2021-05-23', 'DB', 'LT', 4, 0, 'COMPLETED', 'Jamsil'),
                   ('20240401SKLT0', 2024, '2024-04-01', 'SK', 'LT', 5, 2, 'COMPLETED', 'Incheon');
        """)
        )
        conn.execute(
            text("""
            INSERT INTO game_batting_stats (game_id, player_id, player_name, team_side, appearance_seq, runs, hits, at_bats, home_runs)
            VALUES ('20210523LTOB0', 101, 'Batter1', 'home', 1, 1, 2, 4, 0),
                   ('20240401SKLT0', 75847, '최정', 'home', 1, 1, 2, 4, 1);
        """)
        )
        conn.execute(
            text("""
            INSERT INTO game_pitching_stats (game_id, player_id, player_name, team_side, appearance_seq, innings_pitched, earned_runs)
            VALUES ('20210523LTOB0', 201, 'Pitcher1', 'home', 1, '6.0', 0);
        """)
        )
        conn.execute(
            text("""
            INSERT INTO player_season_batting (player_id, season, league, level, source, hits, home_runs, at_bats)
            VALUES (75847, 2024, 'KBO', '1군', 'kbo', 2, 1, 4);
        """)
        )

    yield test_engine
    test_engine.dispose()


@pytest.fixture(autouse=True)
def _patch_lineage_engine(lineage_test_engine: create_engine) -> Generator[None, None, None]:
    """Patch LineageEngine engine resolution to use the seeded test DB."""
    with patch("src.lineage.engine.LineageEngine._resolve_engine", return_value=lineage_test_engine):
        yield


def test_lineage_cli_game_execution(capsys) -> None:
    """Test `python3 -m src.cli.lineage game` execution."""
    exit_code = lineage_main(["game", "20210523LTOB0", "--format", "tree"])
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "Game Lineage: 20210523LTOB0" in captured.out
    assert "DB 4 vs LT 0" in captured.out


def test_lineage_cli_player_execution(capsys) -> None:
    """Test `python3 -m src.cli.lineage player` execution."""
    exit_code = lineage_main(["player", "최정", "--season", "2024", "--metric", "hits", "--format", "json"])
    assert exit_code == 0
    captured = capsys.readouterr()
    assert '"metric_name": "hits"' in captured.out
    assert '"player_name": "최정"' in captured.out


def test_lineage_cli_audit_execution(capsys) -> None:
    """Test `python3 -m src.cli.lineage audit` execution."""
    exit_code = lineage_main(["audit", "--season", "2024"])
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "KBO DATA LINEAGE & PROVENANCE AUDIT" in captured.out


def test_lineage_cli_audit_full_and_artifact_saving(tmp_path: Path, capsys) -> None:
    """Test `python3 -m src.cli.lineage audit --full --save-artifact` execution."""
    out_file = tmp_path / "report.json"
    exit_code = lineage_main(["audit", "--full", "--save-artifact", str(out_file)])
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "MODE: FULL" in captured.out
    assert out_file.exists()
    content = out_file.read_text(encoding="utf-8")
    assert '"audit_mode": "FULL"' in content


def test_kbo_master_cli_lineage_dispatch(capsys) -> None:
    """Test Master CLI `kbo lineage game` dispatching."""
    exit_code = kbo_main(["lineage", "game", "20210523LTOB0", "--json"])
    assert exit_code == 0
    captured = capsys.readouterr()
    assert '"game_id": "20210523LTOB0"' in captured.out
