from __future__ import annotations

from sqlalchemy import create_engine, text

from scripts.legacy.maintenance.repair_historical_team_metadata import run
from src.models.base import Base
from src.models.franchise import Franchise
from src.models.team import Team, TeamCodeMap
from src.models.team_history import TeamHistory


def _build_db(path):
    engine = create_engine(f"sqlite:///{path}")
    Base.metadata.create_all(
        bind=engine,
        tables=[Franchise.__table__, Team.__table__, TeamHistory.__table__, TeamCodeMap.__table__],
    )
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO team_franchises (id, name, original_code, current_code, created_at, updated_at)
                VALUES
                    (6, '키움 히어로즈', 'WO', 'WO', '2026-01-01', '2026-01-01'),
                    (7, '한화 이글스', 'HH', 'HH', '2026-01-01', '2026-01-01'),
                    (8, 'SSG 랜더스', 'SK', 'SSG', '2026-01-01', '2026-01-01')
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO teams (
                    team_id, team_name, team_short_name, city, founded_year, stadium_name,
                    franchise_id, is_active, aliases, created_at, updated_at
                )
                VALUES
                    ('SM', '삼미 슈퍼스타즈', '삼미', '인천', 1982, '인천', 6, 1, NULL, '2026-01-01', '2026-01-01'),
                    ('HH', '한화 이글스', '한화', '대전', 1993, '대전', 7, 1, NULL, '2026-01-01', '2026-01-01')
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO team_history (
                    franchise_id, season, team_name, team_code, created_at, updated_at
                )
                VALUES
                    (6, 1985, '삼미 슈퍼스타즈', 'SM', '2026-01-01', '2026-01-01'),
                    (7, 1993, '한화 이글스', 'HH', '2026-01-01', '2026-01-01')
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE game (
                    game_id TEXT PRIMARY KEY,
                    game_date DATE,
                    away_team TEXT,
                    home_team TEXT,
                    winning_team TEXT,
                    away_franchise_id INTEGER,
                    home_franchise_id INTEGER,
                    winning_franchise_id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE game_batting_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    game_id TEXT,
                    team_code TEXT,
                    franchise_id INTEGER,
                    canonical_team_code TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO game (game_id, game_date, away_team, home_team, winning_team)
                VALUES
                    ('20010405LTHU0', '2001-04-05', 'LT', 'HU', 'HU'),
                    ('20250315LGSK0', '2025-03-15', 'LG', 'SK', 'SK')
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO game_batting_stats (game_id, team_code)
                VALUES
                    ('20010405LTHU0', 'HU'),
                    ('20080401WOLT0', 'WO'),
                    ('20250315LGSK0', 'SK')
                """
            )
        )
    return engine


def test_repair_historical_team_metadata_splits_franchises_and_rebuilds_maps(tmp_path):
    db_path = tmp_path / "metadata.db"
    engine = _build_db(db_path)

    result = run(db_url=f"sqlite:///{db_path}", max_year=2026, apply=True)

    assert result["team_code_map_rows"] > 0
    with engine.connect() as conn:
        franchises = {
            row["id"]: dict(row)
            for row in conn.execute(
                text("SELECT id, name, original_code, current_code FROM team_franchises WHERE id IN (6, 8, 11, 12)")
            ).mappings()
        }
        assert franchises[6]["original_code"] == "SM"
        assert franchises[6]["current_code"] == "HU"
        assert franchises[8]["current_code"] == "SSG"
        assert franchises[11]["current_code"] == "KH"
        assert franchises[12]["current_code"] == "SL"

        teams = {
            row["team_id"]: dict(row)
            for row in conn.execute(
                text(
                    "SELECT team_id, franchise_id, is_active, aliases FROM teams WHERE team_id IN ('HU', 'KH', 'SL', 'SK', 'SSG')"
                )
            ).mappings()
        }
        assert teams["HU"]["franchise_id"] == 6
        assert teams["HU"]["is_active"] == 0
        assert "HD" in (teams["HU"]["aliases"] or "")
        assert teams["KH"]["franchise_id"] == 11
        assert teams["KH"]["is_active"] == 1
        assert teams["SL"]["franchise_id"] == 12
        assert teams["SK"]["franchise_id"] == 8
        assert teams["SSG"]["franchise_id"] == 8

        assert (
            conn.execute(text("SELECT COUNT(*) FROM team_history WHERE season = 1985 AND team_code = 'SM'")).scalar()
            == 0
        )
        assert (
            conn.execute(
                text("SELECT franchise_id FROM team_history WHERE season = 1985 AND team_code = 'CB'")
            ).scalar()
            == 6
        )
        assert (
            conn.execute(
                text("SELECT franchise_id FROM team_history WHERE season = 1993 AND team_code = 'BE'")
            ).scalar()
            == 7
        )
        assert (
            conn.execute(text("SELECT COUNT(*) FROM team_history WHERE season = 1993 AND team_code = 'HH'")).scalar()
            == 0
        )
        assert (
            conn.execute(
                text("SELECT franchise_id FROM team_history WHERE season = 2008 AND team_code = 'WO'")
            ).scalar()
            == 11
        )
        assert (
            conn.execute(
                text("SELECT franchise_id FROM team_history WHERE season = 1999 AND team_code = 'SL'")
            ).scalar()
            == 12
        )

        assert (
            conn.execute(
                text("SELECT canonical_code FROM team_code_map WHERE season = 1985 AND curr_code = 'CB'")
            ).scalar()
            == "HU"
        )
        assert (
            conn.execute(
                text("SELECT canonical_code FROM team_code_map WHERE season = 2008 AND curr_code = 'WO'")
            ).scalar()
            == "KH"
        )
        assert (
            conn.execute(
                text("SELECT canonical_code FROM team_code_map WHERE season = 1999 AND curr_code = 'SL'")
            ).scalar()
            == "SL"
        )

        hu_fact = conn.execute(
            text("SELECT franchise_id, canonical_team_code FROM game_batting_stats WHERE game_id = '20010405LTHU0'")
        ).one()
        wo_fact = conn.execute(
            text("SELECT franchise_id, canonical_team_code FROM game_batting_stats WHERE game_id = '20080401WOLT0'")
        ).one()
        sk_fact = conn.execute(
            text("SELECT franchise_id, canonical_team_code FROM game_batting_stats WHERE game_id = '20250315LGSK0'")
        ).one()
        assert tuple(hu_fact) == (6, "HU")
        assert tuple(wo_fact) == (11, "KH")
        assert tuple(sk_fact) == (8, "SSG")
        assert conn.execute(text("SELECT home_franchise_id FROM game WHERE game_id = '20010405LTHU0'")).scalar() == 6
        assert conn.execute(text("SELECT home_franchise_id FROM game WHERE game_id = '20250315LGSK0'")).scalar() == 8
