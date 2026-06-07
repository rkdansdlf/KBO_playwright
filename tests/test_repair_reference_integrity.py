from sqlalchemy import create_engine, text

from scripts.legacy.maintenance.repair_reference_integrity import run


def _build_db(path):
    engine = create_engine(f"sqlite:///{path}")
    with engine.begin() as conn:
        conn.execute(text("PRAGMA foreign_keys = OFF"))
        conn.execute(text("CREATE TABLE teams (team_id TEXT PRIMARY KEY)"))
        conn.execute(
            text(
                """
                CREATE TABLE game (
                    game_id TEXT PRIMARY KEY,
                    game_date DATE NOT NULL,
                    away_team TEXT REFERENCES teams(team_id),
                    home_team TEXT REFERENCES teams(team_id),
                    winning_team TEXT REFERENCES teams(team_id),
                    game_status TEXT,
                    is_primary BOOLEAN DEFAULT 1,
                    created_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE game_metadata (
                    game_id TEXT PRIMARY KEY REFERENCES game(game_id),
                    stadium_name TEXT,
                    start_time TEXT,
                    source_payload TEXT,
                    created_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE game_lineups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    game_id TEXT REFERENCES game(game_id),
                    team_code TEXT REFERENCES teams(team_id),
                    player_id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE game_id_aliases (
                    alias_game_id TEXT PRIMARY KEY,
                    canonical_game_id TEXT NOT NULL REFERENCES game(game_id),
                    source TEXT,
                    reason TEXT,
                    created_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO teams (team_id)
                VALUES ('HU'), ('LG'), ('SK'), ('KT'), ('SS')
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO game (
                    game_id, game_date, away_team, home_team, winning_team, game_status, is_primary, created_at, updated_at
                )
                VALUES
                    ('20010405LTHD0', '2001-04-05', 'LG', 'HD', 'HD', 'COMPLETED', 1, '2026-01-01', '2026-01-01'),
                    ('20250315LGSK0', '2025-03-15', 'LG', 'SK', NULL, 'CANCELLED', 1, '2026-01-01', '2026-01-01')
                """
            )
        )
        conn.execute(text("INSERT INTO game_lineups (game_id, team_code, player_id) VALUES ('20010405LTHD0', 'HD', 1)"))
        conn.execute(
            text(
                """
                INSERT INTO game_metadata (game_id, stadium_name, start_time, source_payload, created_at, updated_at)
                VALUES
                    ('20250315LGSSG0', '문학', '18:30', '{"stadium": "문학"}', '2026-01-01', '2026-01-01'),
                    ('20250316SSKT0', '수원', '14:00', '{"stadium": "수원"}', '2026-01-01', '2026-01-01')
                """
            )
        )
    return engine


def test_repair_reference_integrity_normalizes_teams_and_repairs_metadata(tmp_path):
    db_path = tmp_path / "repair.db"
    engine = _build_db(db_path)

    result = run(
        db_url=f"sqlite:///{db_path}",
        apply=True,
        only="all",
        output_dir=tmp_path / "reports",
    )

    assert result["applied"] >= 4
    assert result["skipped"] == 0
    with engine.connect() as conn:
        assert (
            conn.execute(
                text("SELECT COUNT(*) FROM game WHERE home_team='HD' OR away_team='HD' OR winning_team='HD'")
            ).scalar()
            == 0
        )
        assert conn.execute(text("SELECT COUNT(*) FROM game_lineups WHERE team_code='HD'")).scalar() == 0
        assert (
            conn.execute(text("SELECT stadium_name FROM game_metadata WHERE game_id='20250315LGSK0'")).scalar()
            == "문학"
        )
        assert conn.execute(text("SELECT COUNT(*) FROM game_metadata WHERE game_id='20250315LGSSG0'")).scalar() == 0
        assert (
            conn.execute(
                text("SELECT canonical_game_id FROM game_id_aliases WHERE alias_game_id='20250315LGSSG0'")
            ).scalar()
            == "20250315LGSK0"
        )
        created = conn.execute(
            text("SELECT away_team, home_team, game_status FROM game WHERE game_id='20250316SSKT0'")
        ).fetchone()
        assert tuple(created) == ("SS", "KT", "CANCELLED")
        assert conn.execute(text("PRAGMA foreign_key_check")).fetchall() == []


def test_repair_reference_integrity_dry_run_does_not_mutate(tmp_path):
    db_path = tmp_path / "repair.db"
    engine = _build_db(db_path)

    result = run(
        db_url=f"sqlite:///{db_path}",
        apply=False,
        only="all",
        output_dir=tmp_path / "reports",
    )

    assert result["dry_run"] > 0
    with engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM game WHERE home_team='HD' OR winning_team='HD'")).scalar() == 1
        assert conn.execute(text("SELECT COUNT(*) FROM game WHERE game_id='20250316SSKT0'")).scalar() == 0
