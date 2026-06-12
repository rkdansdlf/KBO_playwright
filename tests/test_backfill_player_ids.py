from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from scripts.maintenance.backfill_player_ids import backfill_year


def _make_session():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        for table_name in ("game_batting_stats", "game_pitching_stats", "game_lineups"):
            conn.execute(
                text(
                    f"""
                    CREATE TABLE {table_name} (
                        id INTEGER PRIMARY KEY,
                        game_id TEXT NOT NULL,
                        player_name TEXT,
                        team_code TEXT,
                        uniform_no TEXT,
                        player_id INTEGER
                    )
                    """
                )
            )
    return sessionmaker(bind=engine)()


class FakeResolver:
    def __init__(self):
        self.preloaded = []
        self.calls = []

    def preload_season_index(self, season):
        self.preloaded.append(season)

    def resolve_id(self, player_name, team_code, season, uniform_no=None, is_pitcher=None):
        self.calls.append((player_name, team_code, season, uniform_no, is_pitcher))
        return 7001 if is_pitcher else 8001


def test_backfill_year_processes_pitching_and_lineups_when_batting_has_no_nulls():
    session = _make_session()
    try:
        session.execute(
            text(
                """
                INSERT INTO game_batting_stats(id, game_id, player_name, team_code, uniform_no, player_id)
                VALUES (1, '20260401LGSS0', '이미해결', 'LG', '10', 1001)
                """
            )
        )
        session.execute(
            text(
                """
                INSERT INTO game_pitching_stats(id, game_id, player_name, team_code, uniform_no, player_id)
                VALUES (1, '20260401LGSS0', '투수', 'LG', '45', NULL)
                """
            )
        )
        session.execute(
            text(
                """
                INSERT INTO game_lineups(id, game_id, player_name, team_code, uniform_no, player_id)
                VALUES (1, '20260401LGSS0', '타자', 'LG', '9', NULL)
                """
            )
        )
        session.commit()

        resolver = FakeResolver()
        result = backfill_year(session, resolver, 2026)

        assert result["resolved"] == 2
        assert resolver.preloaded == [2026]
        assert ("투수", "LG", 2026, "45", True) in resolver.calls
        assert ("타자", "LG", 2026, "9", False) in resolver.calls
        assert session.execute(text("SELECT player_id FROM game_pitching_stats")).scalar() == 7001
        assert session.execute(text("SELECT player_id FROM game_lineups")).scalar() == 8001
    finally:
        session.close()
