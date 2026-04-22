from __future__ import annotations

from sqlalchemy import create_engine, text

from scripts.maintenance.repair_game_id_integrity import (
    _load_tables,
    apply_duplicate_group,
    collect_conflicts,
    collect_2024_backfill_candidates,
    collect_duplicate_groups,
    is_actionable_backfill_candidate,
    standardize_game_franchise_ids,
)


def _build_db(
    *,
    conflicting: bool = False,
    mergeable_enrichment: bool = False,
    metadata_drift: bool = False,
    summary_player_id_drift: bool = False,
    summary_generated_player_id_drift: bool = False,
):
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE game (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    game_id VARCHAR(20) NOT NULL UNIQUE,
                    game_date DATE NOT NULL,
                    away_team VARCHAR(20),
                    home_team VARCHAR(20),
                    away_franchise_id INTEGER,
                    home_franchise_id INTEGER,
                    season_id INTEGER,
                    game_status VARCHAR(32),
                    is_primary BOOLEAN DEFAULT 1,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE game_batting_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    game_id VARCHAR(20) NOT NULL,
                    player_id INTEGER,
                    appearance_seq INTEGER,
                    hits INTEGER,
                    extra_stats TEXT,
                    uniform_no VARCHAR(10),
                    franchise_id INTEGER,
                    canonical_team_code VARCHAR(10),
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE game_metadata (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    game_id VARCHAR(20) NOT NULL UNIQUE,
                    stadium_name VARCHAR(100),
                    attendance INTEGER,
                    start_time VARCHAR(20),
                    end_time VARCHAR(20),
                    game_time_minutes INTEGER,
                    source_payload TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE game_summary (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    game_id VARCHAR(20) NOT NULL,
                    summary_type VARCHAR(50),
                    player_id INTEGER,
                    player_name VARCHAR(50),
                    detail_text TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (game_id, summary_type, player_name, detail_text)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE game_id_aliases (
                    alias_game_id VARCHAR(20) PRIMARY KEY,
                    canonical_game_id VARCHAR(20) NOT NULL,
                    source VARCHAR(50),
                    reason VARCHAR(120),
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO game (game_id, game_date, away_team, home_team, away_franchise_id, home_franchise_id, season_id, game_status)
                VALUES
                    ('20250315LGSK0', '2025-03-15', 'LG', 'SSG', 3, 8, 259, 'COMPLETED'),
                    ('20250315LGSSG0', '2025-03-15', 'LG', 'SSG', 3, 8, 259, 'COMPLETED')
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO game_batting_stats (game_id, player_id, appearance_seq, hits, extra_stats, uniform_no, franchise_id, canonical_team_code)
                VALUES
                    ('20250315LGSK0', 1001, 1, 2, NULL, NULL, :target_franchise_id, :target_canonical_team_code),
                    ('20250315LGSSG0', 1001, 1, :source_hits, :source_extra_stats, :source_uniform_no, NULL, NULL)
                """
            ),
            {
                "source_hits": 3 if conflicting else 2,
                "source_extra_stats": '{"xr": -0.09}' if mergeable_enrichment else None,
                "source_uniform_no": "8" if mergeable_enrichment else None,
                "target_franchise_id": 8 if mergeable_enrichment else None,
                "target_canonical_team_code": "SSG" if mergeable_enrichment else None,
            },
        )
        if metadata_drift:
            conn.execute(
                text(
                    """
                    INSERT INTO game_metadata (
                        game_id, stadium_name, attendance, start_time, end_time, game_time_minutes, source_payload
                    )
                    VALUES
                        (
                            '20250315LGSK0',
                            '문학',
                            12000,
                            '18:31',
                            '21:42',
                            191,
                            '{"game_id": "20250315LGSK0", "start_time": "18:31"}'
                        ),
                        (
                            '20250315LGSSG0',
                            '인천 SSG 랜더스필드',
                            12000,
                            '18:30',
                            '21:42',
                            191,
                            '{"game_id": "20250315LGSSG0", "start_time": "18:30"}'
                        )
                    """
                )
            )
        if summary_player_id_drift:
            conn.execute(
                text(
                    """
                    INSERT INTO game_summary (game_id, summary_type, player_id, player_name, detail_text)
                    VALUES
                        ('20250315LGSK0', '실책', NULL, '김테스트', '김테스트(1회)'),
                        ('20250315LGSSG0', '실책', 7001, '김테스트', '김테스트(1회)')
                    """
                )
            )
        if summary_generated_player_id_drift:
            conn.execute(
                text(
                    """
                    INSERT INTO game_summary (game_id, summary_type, player_id, player_name, detail_text)
                    VALUES
                        ('20250315LGSK0', '폭투', 900517, '데이비슨', '데이비슨(7회)'),
                        ('20250315LGSSG0', '폭투', 900333, '데이비슨', '데이비슨(7회)')
                    """
                )
            )
    return engine


def test_duplicate_repair_merges_identical_child_rows_and_records_alias():
    engine = _build_db()
    with engine.begin() as conn:
        tables = _load_tables(conn)
        groups = collect_duplicate_groups(conn, tables, [2025])
        conflicts = collect_conflicts(conn, tables, groups)

        assert len(groups) == 1
        assert groups[0]["primary_game_id"] == "20250315LGSK0"
        assert conflicts == []

        apply_duplicate_group(conn, tables, groups[0])

        remaining_games = conn.execute(text("SELECT game_id FROM game ORDER BY game_id")).fetchall()
        batting_count = conn.execute(text("SELECT COUNT(*) FROM game_batting_stats")).scalar()
        alias = conn.execute(text("SELECT canonical_game_id FROM game_id_aliases WHERE alias_game_id = '20250315LGSSG0'")).scalar()

        assert remaining_games == [("20250315LGSK0",)]
        assert batting_count == 1
        assert alias == "20250315LGSK0"


def test_duplicate_repair_reports_conflicting_child_rows():
    engine = _build_db(conflicting=True)
    with engine.begin() as conn:
        tables = _load_tables(conn)
        groups = collect_duplicate_groups(conn, tables, [2025])
        conflicts = collect_conflicts(conn, tables, groups)

        assert len(conflicts) == 1
        assert conflicts[0]["table_name"] == "game_batting_stats"
        assert conflicts[0]["reason"] == "conflicting_child_row"


def test_duplicate_repair_merges_enrichment_without_losing_identity_columns():
    engine = _build_db(mergeable_enrichment=True)
    with engine.begin() as conn:
        tables = _load_tables(conn)
        groups = collect_duplicate_groups(conn, tables, [2025])
        conflicts = collect_conflicts(conn, tables, groups)

        assert conflicts == []

        apply_duplicate_group(conn, tables, groups[0])

        row = conn.execute(
            text(
                """
                SELECT extra_stats, uniform_no, franchise_id, canonical_team_code
                FROM game_batting_stats
                WHERE game_id = '20250315LGSK0'
                """
            )
        ).mappings().one()

        assert row["extra_stats"] == '{"xr": -0.09}'
        assert row["uniform_no"] == "8"
        assert row["franchise_id"] == 8
        assert row["canonical_team_code"] == "SSG"


def test_duplicate_repair_derives_logical_key_when_franchise_ids_are_missing():
    engine = _build_db()
    with engine.begin() as conn:
        conn.execute(text("UPDATE game SET away_franchise_id = NULL, home_franchise_id = NULL"))
        tables = _load_tables(conn)

        groups = collect_duplicate_groups(conn, tables, [2025])
        updated = standardize_game_franchise_ids(conn, tables, [2025])

        assert len(groups) == 1
        assert groups[0]["primary_game_id"] == "20250315LGSK0"
        assert updated == 2

        franchises = conn.execute(
            text("SELECT game_id, away_franchise_id, home_franchise_id FROM game ORDER BY game_id")
        ).fetchall()
        assert franchises == [
            ("20250315LGSK0", 3, 8),
            ("20250315LGSSG0", 3, 8),
        ]


def test_duplicate_repair_resolves_metadata_stadium_and_start_time_drift():
    engine = _build_db(metadata_drift=True)
    with engine.begin() as conn:
        tables = _load_tables(conn)
        groups = collect_duplicate_groups(conn, tables, [2025])
        conflicts = collect_conflicts(conn, tables, groups)

        assert conflicts == []

        apply_duplicate_group(conn, tables, groups[0])

        metadata = conn.execute(
            text(
                """
                SELECT game_id, stadium_name, start_time, attendance, game_time_minutes
                FROM game_metadata
                """
            )
        ).mappings().one()

        assert metadata["game_id"] == "20250315LGSK0"
        assert metadata["stadium_name"] == "인천 SSG 랜더스필드"
        assert metadata["start_time"] == "18:30"
        assert metadata["attendance"] == 12000
        assert metadata["game_time_minutes"] == 191


def test_duplicate_repair_uses_actual_game_summary_unique_key():
    engine = _build_db(summary_player_id_drift=True)
    with engine.begin() as conn:
        tables = _load_tables(conn)
        groups = collect_duplicate_groups(conn, tables, [2025])
        conflicts = collect_conflicts(conn, tables, groups)

        assert conflicts == []

        apply_duplicate_group(conn, tables, groups[0])

        summary_rows = conn.execute(
            text(
                """
                SELECT game_id, summary_type, player_id, player_name, detail_text
                FROM game_summary
                """
            )
        ).fetchall()

        assert summary_rows == [
            ("20250315LGSK0", "실책", 7001, "김테스트", "김테스트(1회)"),
        ]


def test_duplicate_repair_keeps_canonical_summary_when_generated_player_ids_differ():
    engine = _build_db(summary_generated_player_id_drift=True)
    with engine.begin() as conn:
        tables = _load_tables(conn)
        groups = collect_duplicate_groups(conn, tables, [2025])
        conflicts = collect_conflicts(conn, tables, groups)

        assert conflicts == []

        apply_duplicate_group(conn, tables, groups[0])

        summary_rows = conn.execute(
            text(
                """
                SELECT game_id, summary_type, player_id, player_name, detail_text
                FROM game_summary
                """
            )
        ).fetchall()

        assert summary_rows == [
            ("20250315LGSK0", "폭투", 900517, "데이비슨", "데이비슨(7회)"),
        ]


def test_2024_backfill_manifest_classifies_international_games_as_non_actionable():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE game (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    game_id VARCHAR(20) NOT NULL UNIQUE,
                    game_date DATE NOT NULL,
                    away_team VARCHAR(20),
                    home_team VARCHAR(20),
                    season_id INTEGER,
                    game_status VARCHAR(32),
                    is_primary BOOLEAN DEFAULT 1
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE kbo_seasons (
                    season_id INTEGER PRIMARY KEY,
                    season_year INTEGER NOT NULL,
                    league_type_code INTEGER NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE game_batting_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    game_id VARCHAR(20) NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE game_pitching_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    game_id VARCHAR(20) NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO kbo_seasons (season_id, season_year, league_type_code)
                VALUES (259, 2024, 0), (202490, 2024, 9)
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO game (game_id, game_date, away_team, home_team, season_id, game_status)
                VALUES
                    ('20240501LGOB0', '2024-05-01', 'LG', '두산', 259, 'COMPLETED'),
                    ('20241113AUJP0', '2024-11-13', '호주', '일본', 202490, 'COMPLETED')
                """
            )
        )

        tables = _load_tables(conn)
        candidates = collect_2024_backfill_candidates(conn, tables)

    by_game_id = {row["game_id"]: row for row in candidates}

    assert by_game_id["20240501LGOB0"]["classification"] == "pending_recrawl"
    assert by_game_id["20240501LGOB0"]["league_type_code"] == 0
    assert is_actionable_backfill_candidate(by_game_id["20240501LGOB0"])

    assert by_game_id["20241113AUJP0"]["classification"] == "site_detail_unavailable_international_league"
    assert by_game_id["20241113AUJP0"]["league_type_code"] == 9
    assert not is_actionable_backfill_candidate(by_game_id["20241113AUJP0"])
