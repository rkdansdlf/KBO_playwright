from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import scripts.maintenance.cleanup_oci as cleanup_oci
import scripts.maintenance.fix_2024_season_ids as fix_2024


DELETE_ROWCOUNTS = tuple(range(10, 10 + len(cleanup_oci.DELETE_STEPS)))


class _FakeCursor:
    def __init__(self, count_results, delete_rowcounts):
        self.count_results = list(count_results)
        self.delete_rowcounts = list(delete_rowcounts)
        self.statements = []
        self.rowcount = -1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql):
        self.statements.append(sql)
        if sql.startswith("SELECT COUNT"):
            self.rowcount = 1
            return
        self.rowcount = self.delete_rowcounts.pop(0)

    def fetchone(self):
        return (self.count_results.pop(0),)


class _FakeConnection:
    def __init__(self, count_results=(3, 3), delete_rowcounts=(4, 5, 2, 3)):
        self.cursor_obj = _FakeCursor(count_results, delete_rowcounts)
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


def _build_season_db():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE kbo_seasons (
                    season_id INTEGER PRIMARY KEY,
                    season_year INTEGER NOT NULL,
                    league_type_name TEXT NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE game (
                    game_id TEXT PRIMARY KEY,
                    game_date TEXT NOT NULL,
                    season_id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO kbo_seasons (season_id, season_year, league_type_name)
                VALUES
                    (101, 2024, '시범경기'),
                    (102, 2024, '올스타전'),
                    (103, 2024, '와일드카드'),
                    (104, 2024, '준플레이오프'),
                    (105, 2024, '플레이오프'),
                    (106, 2024, '한국시리즈')
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO game (game_id, game_date, season_id)
                VALUES
                    ('EXHIBITION0', '2024-03-10', 1),
                    ('EXHIBITION_NULL', '2024-03-11', NULL),
                    ('REGULAR0', '2024-04-01', 1),
                    ('WILDCARD0', '2024-10-02', 1),
                    ('KOREAN0', '2024-10-25', 106)
                """
            )
        )
    return engine


def _season_rows(engine):
    with engine.connect() as conn:
        return dict(
            conn.execute(
                text("SELECT game_id, season_id FROM game ORDER BY game_id")
            ).fetchall()
        )


def test_cleanup_oci_duplicates_defaults_to_rollback(monkeypatch):
    conn = _FakeConnection(count_results=(3, 3), delete_rowcounts=DELETE_ROWCOUNTS)
    monkeypatch.setattr(cleanup_oci.psycopg2, "connect", lambda database_url: conn)

    counts = cleanup_oci.cleanup_oci_duplicates(database_url="postgresql://example", apply=False)

    assert counts["non_primary_games_before"] == 3
    assert counts["non_primary_games_after"] == 3
    for (label, _sql), rowcount in zip(cleanup_oci.DELETE_STEPS, DELETE_ROWCOUNTS):
        assert counts[label] == rowcount
    assert conn.rolled_back is True
    assert conn.committed is False
    assert conn.closed is True


def test_cleanup_oci_duplicates_commits_only_when_apply_is_set(monkeypatch):
    conn = _FakeConnection(count_results=(3, 0), delete_rowcounts=DELETE_ROWCOUNTS)
    monkeypatch.setattr(cleanup_oci.psycopg2, "connect", lambda database_url: conn)

    counts = cleanup_oci.cleanup_oci_duplicates(database_url="postgresql://example", apply=True)

    assert counts["non_primary_games_after"] == 0
    assert [label for label, _sql in cleanup_oci.DELETE_STEPS][-1] == "game"
    assert conn.committed is True
    assert conn.rolled_back is False
    assert conn.closed is True


def test_fix_2024_seasons_rolls_back_by_default(monkeypatch):
    engine = _build_season_db()
    monkeypatch.setattr(fix_2024, "SessionLocal", sessionmaker(bind=engine))

    updates = fix_2024.fix_2024_seasons(apply=False, log=lambda _message: None)

    assert updates["시범경기"] == 2
    assert updates["와일드카드"] == 1
    assert _season_rows(engine) == {
        "EXHIBITION0": 1,
        "EXHIBITION_NULL": None,
        "KOREAN0": 106,
        "REGULAR0": 1,
        "WILDCARD0": 1,
    }


def test_fix_2024_seasons_commits_when_apply_is_set(monkeypatch):
    engine = _build_season_db()
    monkeypatch.setattr(fix_2024, "SessionLocal", sessionmaker(bind=engine))

    updates = fix_2024.fix_2024_seasons(apply=True, log=lambda _message: None)

    assert updates["시범경기"] == 2
    assert updates["와일드카드"] == 1
    assert updates["한국시리즈"] == 0
    assert _season_rows(engine) == {
        "EXHIBITION0": 101,
        "EXHIBITION_NULL": 101,
        "KOREAN0": 106,
        "REGULAR0": 1,
        "WILDCARD0": 103,
    }
