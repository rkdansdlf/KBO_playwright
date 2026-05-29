import csv
import sqlite3

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import scripts.maintenance.cleanup_oci as cleanup_oci
import scripts.maintenance.fix_2024_season_ids as fix_2024
from scripts.maintenance.apply_event_backed_split_repairs import apply_event_backed_split_repairs
from scripts.maintenance.collect_identity_conflict_evidence import (
    flatten_hitter_evidence,
    load_manifest_targets,
    propose_identity_conflict_updates,
)
from scripts.maintenance.export_identity_conflict_worklist import export_identity_conflict_worklist
from scripts.maintenance.full_audit import collect_audit_metrics, flatten_gate_metrics
from scripts.maintenance.propose_event_backed_split_repairs import propose_event_backed_split_repairs

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


def _build_full_audit_db():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE teams (team_id TEXT PRIMARY KEY)"))
        conn.execute(text("INSERT INTO teams(team_id) VALUES ('LG'), ('SS')"))
        conn.execute(
            text(
                """
                CREATE TABLE player_basic (
                    player_id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    team TEXT
                )
                """
            )
        )
        conn.execute(text("INSERT INTO player_basic(player_id, name) VALUES (1001, '홍길동')"))
        conn.execute(
            text(
                """
                CREATE TABLE game (
                    game_id TEXT PRIMARY KEY,
                    game_date DATE NOT NULL,
                    home_score INTEGER,
                    away_score INTEGER,
                    game_status TEXT
                )
                """
            )
        )
        conn.execute(text("INSERT INTO game(game_id, game_date, game_status) VALUES ('G1', CURRENT_DATE, 'COMPLETED')"))
        conn.execute(
            text(
                """
                CREATE TABLE game_batting_stats (
                    id INTEGER PRIMARY KEY,
                    game_id TEXT,
                    team_side TEXT,
                    team_code TEXT,
                    player_id INTEGER,
                    player_name TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO game_batting_stats(game_id, team_side, team_code, player_id, player_name)
                VALUES
                    ('G1', 'away', 'LG', 1001, '홍길동'),
                    ('G1', 'home', 'SS', 1001, '홍길동')
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE game_pitching_stats (
                    game_id TEXT,
                    team_side TEXT,
                    team_code TEXT,
                    player_id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE game_lineups (
                    game_id TEXT,
                    team_side TEXT,
                    team_code TEXT,
                    player_id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE player_season_batting (
                    player_id INTEGER,
                    season INTEGER,
                    league TEXT,
                    level TEXT,
                    source TEXT,
                    team_code TEXT,
                    hits INTEGER,
                    at_bats INTEGER,
                    plate_appearances INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE player_season_pitching (
                    player_id INTEGER,
                    season INTEGER,
                    league TEXT,
                    level TEXT,
                    source TEXT,
                    team_code TEXT,
                    earned_runs INTEGER,
                    runs_allowed INTEGER
                )
                """
            )
        )
        conn.execute(text("CREATE TABLE players (id INTEGER PRIMARY KEY, kbo_person_id TEXT)"))
        conn.execute(text("CREATE TABLE player_identities (id INTEGER PRIMARY KEY, player_id INTEGER)"))
    return engine


def _season_rows(engine):
    with engine.connect() as conn:
        return dict(conn.execute(text("SELECT game_id, season_id FROM game ORDER BY game_id")).fetchall())


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


def test_full_audit_handles_teams_without_alternate_code():
    engine = _build_full_audit_db()
    with engine.connect() as conn:
        report = collect_audit_metrics(conn)

    flat = flatten_gate_metrics(report)
    assert report["team_code_consistency"]["player_season_batting_invalid_team_codes"] == 0
    assert flat["game_batting_duplicate_player_groups"] == 1
    assert flat["game_batting_player_team_collisions"] == 1


def test_identity_conflict_worklist_is_read_only_and_marks_source_review(tmp_path):
    db_path = tmp_path / "identity_conflict.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE game (
                game_id TEXT PRIMARY KEY,
                game_date TEXT
            );
            CREATE TABLE player_basic (
                player_id INTEGER PRIMARY KEY,
                name TEXT,
                birth_date TEXT,
                debut_year INTEGER,
                team TEXT,
                position TEXT
            );
            CREATE TABLE player_season_batting (
                player_id INTEGER,
                season INTEGER,
                team_code TEXT
            );
            CREATE TABLE game_batting_stats (
                id INTEGER PRIMARY KEY,
                game_id TEXT,
                team_side TEXT,
                team_code TEXT,
                player_id INTEGER,
                player_name TEXT,
                batting_order INTEGER,
                is_starter INTEGER,
                appearance_seq INTEGER,
                position TEXT,
                at_bats INTEGER,
                hits INTEGER
            );
            """
        )
        conn.execute("INSERT INTO game(game_id, game_date) VALUES ('20100701LGSS0', '2010-07-01')")
        conn.executemany(
            """
            INSERT INTO player_basic(player_id, name, birth_date, debut_year, team, position)
            VALUES (?, '이병규', ?, ?, 'LG', '외야수')
            """,
            [(76100, "1983-10-09", 2006), (97109, "1974-10-25", 1997)],
        )
        conn.executemany(
            "INSERT INTO player_season_batting(player_id, season, team_code) VALUES (?, 2010, 'LG')",
            [(76100,), (97109,)],
        )
        conn.executemany(
            """
            INSERT INTO game_batting_stats(
                game_id, team_side, team_code, player_id, player_name,
                batting_order, is_starter, appearance_seq, position, at_bats, hits
            )
            VALUES ('20100701LGSS0', 'away', 'LG', 76100, '이병규', ?, 1, ?, ?, ?, ?)
            """,
            [(2, 1, "LF", 4, 1), (4, 2, "RF", 3, 2)],
        )
        conn.commit()
    finally:
        conn.close()

    result = export_identity_conflict_worklist(
        db_path=db_path,
        output_dir=tmp_path / "reports",
        player_name="이병규",
        team_code="LG",
    )

    assert result["groups"] == 1
    assert result["rows"] == 2
    assert result["source_review_rows"] == 2

    with open(result["rows_csv"], newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))

    assert {row["suggestion_status"] for row in rows} == {"source_review"}
    assert {row["candidate_ids"] for row in rows} == {"76100,97109"}
    assert all(row["suggested_player_id"] == "" for row in rows)

    with open(result["manifest_csv"], newline="", encoding="utf-8") as fh:
        manifest_rows = list(csv.DictReader(fh))

    assert manifest_rows == [
        {
            "game_id": "20100701LGSS0",
            "game_date": "2010-07-01",
            "tables": "game_batting_stats",
            "player_names": "이병규",
            "team_codes": "LG",
            "group_count": "1",
            "row_count": "2",
            "source_review_rows": "2",
        }
    ]

    verify_conn = sqlite3.connect(db_path)
    try:
        count, distinct_player_ids = verify_conn.execute(
            "SELECT COUNT(*), COUNT(DISTINCT player_id) FROM game_batting_stats"
        ).fetchone()
    finally:
        verify_conn.close()
    assert (count, distinct_player_ids) == (2, 1)


def test_identity_conflict_manifest_loader_deduplicates_and_limits(tmp_path):
    manifest_path = tmp_path / "manifest.csv"
    manifest_path.write_text(
        "\n".join(
            [
                "game_id,game_date",
                "20100701LGSS0,2010-07-01",
                "20100701LGSS0,2010-07-01",
                "20100702LGSS0,2010-07-02",
            ]
        ),
        encoding="utf-8",
    )

    targets = load_manifest_targets(manifest_path, limit=2)

    assert [target.game_id for target in targets] == ["20100701LGSS0", "20100702LGSS0"]
    assert [target.game_date for target in targets] == ["20100701", "20100702"]

    offset_targets = load_manifest_targets(manifest_path, limit=1, offset=1)
    assert [target.game_id for target in offset_targets] == ["20100702LGSS0"]


def test_identity_conflict_proposal_uses_exact_source_row_match_without_db_writes(tmp_path):
    db_path = tmp_path / "identity_conflict_proposal.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE game_batting_stats (
                id INTEGER PRIMARY KEY,
                game_id TEXT,
                team_side TEXT,
                team_code TEXT,
                player_id INTEGER,
                player_name TEXT,
                batting_order INTEGER,
                appearance_seq INTEGER,
                position TEXT,
                standard_position TEXT,
                at_bats INTEGER,
                runs INTEGER,
                hits INTEGER,
                doubles INTEGER,
                triples INTEGER,
                home_runs INTEGER,
                rbi INTEGER,
                walks INTEGER,
                intentional_walks INTEGER,
                hbp INTEGER,
                strikeouts INTEGER,
                stolen_bases INTEGER,
                caught_stealing INTEGER,
                sacrifice_hits INTEGER,
                sacrifice_flies INTEGER,
                gdp INTEGER
            );
            CREATE TABLE game_lineups (
                id INTEGER PRIMARY KEY,
                game_id TEXT,
                team_side TEXT,
                team_code TEXT,
                player_id INTEGER,
                player_name TEXT,
                batting_order INTEGER,
                appearance_seq INTEGER,
                position TEXT,
                standard_position TEXT
            );
            """
        )
        conn.executemany(
            """
            INSERT INTO game_batting_stats(
                id, game_id, team_side, team_code, player_id, player_name,
                batting_order, appearance_seq, position, standard_position,
                at_bats, runs, hits, doubles, triples, home_runs, rbi, walks,
                intentional_walks, hbp, strikeouts, stolen_bases, caught_stealing,
                sacrifice_hits, sacrifice_flies, gdp
            )
            VALUES (?, '20100701LGSS0', 'away', 'LG', 76100, '이병규',
                    ?, ?, ?, ?, ?, 0, ?, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
            """,
            [
                (10, 2, 2, "좌", "LF", 4, 1),
                (11, 4, 7, "중", "CF", 3, 2),
            ],
        )
        conn.executemany(
            """
            INSERT INTO game_lineups(
                id, game_id, team_side, team_code, player_id, player_name,
                batting_order, appearance_seq, position, standard_position
            )
            VALUES (?, '20100701LGSS0', 'away', 'LG', 76100, '이병규', ?, ?, ?, ?)
            """,
            [
                (20, 2, 2, "좌", "LF"),
                (21, 4, 7, "중", "CF"),
            ],
        )
        conn.commit()
    finally:
        conn.close()

    evidence_rows = flatten_hitter_evidence(
        [
            {
                "game_id": "20100701LGSS0",
                "game_date": "20100701",
                "hitters": {
                    "away": [
                        {
                            "player_id": 76100,
                            "player_name": "이병규",
                            "team_code": "LG",
                            "batting_order": 2,
                            "appearance_seq": 2,
                            "position": "좌",
                            "stats": {"at_bats": 4, "runs": 0, "hits": 1},
                        },
                        {
                            "player_id": 97109,
                            "player_name": "이병규",
                            "team_code": "LG",
                            "batting_order": 4,
                            "appearance_seq": 7,
                            "position": "중",
                            "stats": {"at_bats": 3, "runs": 0, "hits": 2},
                        },
                    ],
                    "home": [],
                },
            }
        ],
        player_name="이병규",
        team_code="LG",
    )

    proposed, blocked = propose_identity_conflict_updates(
        db_path=db_path,
        evidence_rows=evidence_rows,
        player_name="이병규",
        team_code="LG",
    )

    assert {
        (row["table_name"], row["row_id"], row["current_player_id"], row["proposed_player_id"]) for row in proposed
    } == {
        ("game_batting_stats", 11, 76100, 97109),
        ("game_lineups", 21, 76100, 97109),
    }
    assert {(row["table_name"], row["row_id"], row["reason"], row["source_player_ids"]) for row in blocked} == {
        ("game_batting_stats", 10, "source_matches_current", "76100"),
        ("game_lineups", 20, "source_matches_current", "76100"),
    }

    verify_conn = sqlite3.connect(db_path)
    try:
        assert verify_conn.execute("SELECT COUNT(*) FROM game_batting_stats WHERE player_id = 97109").fetchone()[0] == 0
        assert verify_conn.execute("SELECT COUNT(*) FROM game_lineups WHERE player_id = 97109").fetchone()[0] == 0
    finally:
        verify_conn.close()


def test_event_backed_split_repair_proposes_only_single_matching_event_id(tmp_path):
    db_path = tmp_path / "event_backed_split.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE game_batting_stats (
                id INTEGER PRIMARY KEY,
                game_id TEXT,
                team_side TEXT,
                team_code TEXT,
                player_id INTEGER,
                player_name TEXT,
                plate_appearances INTEGER,
                at_bats INTEGER,
                runs INTEGER,
                hits INTEGER,
                doubles INTEGER,
                triples INTEGER,
                home_runs INTEGER,
                rbi INTEGER,
                walks INTEGER,
                intentional_walks INTEGER,
                hbp INTEGER,
                strikeouts INTEGER,
                stolen_bases INTEGER,
                caught_stealing INTEGER,
                sacrifice_hits INTEGER,
                sacrifice_flies INTEGER,
                gdp INTEGER,
                appearance_seq INTEGER
            );
            CREATE TABLE game_events (
                id INTEGER PRIMARY KEY,
                game_id TEXT,
                event_seq INTEGER,
                inning_half TEXT,
                batter_id INTEGER,
                batter_name TEXT,
                result_code TEXT,
                event_type TEXT
            );
            """
        )
        conn.executemany(
            """
            INSERT INTO game_batting_stats(
                id, game_id, team_side, team_code, player_id, player_name,
                plate_appearances, at_bats, runs, hits, doubles, triples, home_runs,
                rbi, walks, intentional_walks, hbp, strikeouts, stolen_bases,
                caught_stealing, sacrifice_hits, sacrifice_flies, gdp, appearance_seq
            )
            VALUES (?, 'G1', 'home', 'KH', 50167, '이주형',
                    ?, ?, ?, ?, 0, 0, ?, ?, 0, 0, 0, 0, 0, 0, 0, 0, 0, ?)
            """,
            [
                (1, 0, 4, 2, 1, 0, 2, 7),
                (2, 0, 1, 0, 0, 0, 0, 14),
            ],
        )
        conn.executemany(
            """
            INSERT INTO game_events(game_id, event_seq, inning_half, batter_id, batter_name, result_code, event_type)
            VALUES ('G1', ?, 'BOTTOM', 50167, '이주형', 'GO', 'GROUNDOUT')
            """,
            [(idx,) for idx in range(1, 6)],
        )
        conn.executemany(
            """
            INSERT INTO game_batting_stats(
                id, game_id, team_side, team_code, player_id, player_name,
                plate_appearances, at_bats, runs, hits, doubles, triples, home_runs,
                rbi, walks, intentional_walks, hbp, strikeouts, stolen_bases,
                caught_stealing, sacrifice_hits, sacrifice_flies, gdp, appearance_seq
            )
            VALUES (?, 'G2', 'away', 'KH', 50167, '이주형',
                    0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, ?)
            """,
            [(3, 1), (4, 2)],
        )
        conn.executemany(
            """
            INSERT INTO game_events(game_id, event_seq, inning_half, batter_id, batter_name, result_code, event_type)
            VALUES ('G2', ?, 'TOP', ?, '이주형', 'GO', 'GROUNDOUT')
            """,
            [(1, 50167), (2, 51302)],
        )
        conn.commit()
    finally:
        conn.close()

    result = propose_event_backed_split_repairs(
        db_path=db_path,
        output_dir=tmp_path / "reports",
        player_name="이주형",
        team_code="KH",
    )

    assert result["proposed_groups"] == 1
    assert result["blocked_groups"] == 1
    with open(result["proposed_csv"], newline="", encoding="utf-8") as fh:
        proposals = list(csv.DictReader(fh))
    assert proposals[0]["game_id"] == "G1"
    assert proposals[0]["keeper_id"] == "1"
    assert proposals[0]["delete_ids"] == "2"
    assert proposals[0]["merged_at_bats"] == "5"
    assert proposals[0]["merged_hits"] == "1"
    assert proposals[0]["event_batter_ids"] == "50167"

    with open(result["blocked_csv"], newline="", encoding="utf-8") as fh:
        blocked = list(csv.DictReader(fh))
    assert blocked[0]["game_id"] == "G2"
    assert blocked[0]["reason"] == "missing_or_ambiguous_event_batter_id"


def test_apply_event_backed_split_repairs_merges_only_when_apply_is_set(tmp_path):
    db_path = tmp_path / "event_split_apply.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE game_batting_stats (
                id INTEGER PRIMARY KEY,
                game_id TEXT,
                team_side TEXT,
                team_code TEXT,
                player_id INTEGER,
                player_name TEXT,
                plate_appearances INTEGER,
                at_bats INTEGER,
                runs INTEGER,
                hits INTEGER,
                doubles INTEGER,
                triples INTEGER,
                home_runs INTEGER,
                rbi INTEGER,
                walks INTEGER,
                intentional_walks INTEGER,
                hbp INTEGER,
                strikeouts INTEGER,
                stolen_bases INTEGER,
                caught_stealing INTEGER,
                sacrifice_hits INTEGER,
                sacrifice_flies INTEGER,
                gdp INTEGER,
                updated_at TEXT
            );
            INSERT INTO game_batting_stats(
                id, game_id, team_side, team_code, player_id, player_name,
                plate_appearances, at_bats, runs, hits, doubles, triples, home_runs,
                rbi, walks, intentional_walks, hbp, strikeouts, stolen_bases,
                caught_stealing, sacrifice_hits, sacrifice_flies, gdp, updated_at
            )
            VALUES
                (1, 'G1', 'home', 'KH', 50167, '이주형', 0, 4, 2, 1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'old'),
                (2, 'G1', 'home', 'KH', 50167, '이주형', 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'old');
            """
        )
        conn.commit()
    finally:
        conn.close()

    proposals_csv = tmp_path / "proposals.csv"
    proposals_csv.write_text(
        "\n".join(
            [
                "game_id,team_side,team_code,player_name,player_id,row_count,row_ids,event_batter_ids,event_rows,computed_pa,reason,keeper_id,delete_ids,merged_plate_appearances,merged_at_bats,merged_runs,merged_hits,merged_doubles,merged_triples,merged_home_runs,merged_rbi,merged_walks,merged_intentional_walks,merged_hbp,merged_strikeouts,merged_stolen_bases,merged_caught_stealing,merged_sacrifice_hits,merged_sacrifice_flies,merged_gdp",
                'G1,home,KH,이주형,50167,2,"1,2",50167,5,5,single_event_batter_id_matches_current_player_id,1,2,0,5,2,1,0,0,0,2,0,0,0,0,0,0,0,0,0',
            ]
        ),
        encoding="utf-8",
    )

    dry_run = apply_event_backed_split_repairs(
        db_path=db_path,
        proposals_csv=proposals_csv,
        output_dir=tmp_path / "reports",
        apply=False,
    )
    assert dry_run["dry_run"] is True
    assert dry_run["merged_groups"] == 1
    assert dry_run["deleted_rows"] == 1

    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM game_batting_stats").fetchone()[0] == 2
        assert conn.execute("SELECT at_bats FROM game_batting_stats WHERE id = 1").fetchone()[0] == 4
    finally:
        conn.close()

    applied = apply_event_backed_split_repairs(
        db_path=db_path,
        proposals_csv=proposals_csv,
        output_dir=tmp_path / "reports",
        apply=True,
    )
    assert applied["dry_run"] is False
    assert applied["merged_groups"] == 1
    assert applied["deleted_rows"] == 1
    assert applied["backup_path"]

    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM game_batting_stats").fetchone()[0] == 1
        row = conn.execute(
            "SELECT at_bats, runs, hits, rbi, updated_at FROM game_batting_stats WHERE id = 1"
        ).fetchone()
        assert row[:4] == (5, 2, 1, 2)
        assert row[4] != "old"
    finally:
        conn.close()
