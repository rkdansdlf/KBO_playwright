from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from scripts.maintenance.resolve_null_player_ids_conservative import (
    OverrideEntry,
    choose_candidate_ids,
    is_group_resolvable,
    update_null_player_ids_for_group,
)


def _make_session():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE player_basic (
                    player_id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    uniform_no TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE player_season_batting (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    player_id INTEGER NOT NULL,
                    season INTEGER NOT NULL,
                    team_code TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE player_season_pitching (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    player_id INTEGER NOT NULL,
                    season INTEGER NOT NULL,
                    team_code TEXT
                )
                """
            )
        )
        for table in ("game_batting_stats", "game_pitching_stats", "game_lineups"):
            conn.execute(
                text(
                    f"""
                    CREATE TABLE {table} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        game_id TEXT NOT NULL,
                        team_code TEXT,
                        player_name TEXT NOT NULL,
                        uniform_no TEXT,
                        player_id INTEGER
                    )
                    """
                )
            )
            conn.execute(
                text(
                    f"""
                    INSERT INTO {table} (game_id, team_code, player_name, uniform_no, player_id)
                    VALUES
                        ('20240201KIAHH0', 'KIA', '테스트선수', '10', NULL),
                        ('20240202KIAHH0', 'KIA', '테스트선수', '10', NULL),
                        ('20240202KIAHH0', 'KIA', '다른선수', '99', NULL)
                    """
                )
            )
    return sessionmaker(bind=engine)()


def test_only_single_candidate_is_resolvable():
    assert is_group_resolvable([10001]) is True
    assert is_group_resolvable([]) is False
    assert is_group_resolvable([10001, 10002]) is False


def test_same_update_rule_applies_to_all_three_tables():
    session = _make_session()
    try:
        for table in ("game_batting_stats", "game_pitching_stats", "game_lineups"):
            updated = update_null_player_ids_for_group(
                session,
                table_name=table,
                year=2024,
                team_code="KIA",
                player_name="테스트선수",
                player_id=12345,
                dry_run=False,
            )
            assert updated == 2

            cnt = session.execute(
                text(
                    f"""
                    SELECT COUNT(*)
                    FROM {table}
                    WHERE player_name = '테스트선수'
                      AND player_id = 12345
                    """
                )
            ).scalar()
            assert cnt == 2
    finally:
        session.close()


def test_override_applies_before_automatic_matching():
    session = _make_session()
    try:
        session.execute(text("INSERT INTO player_basic(player_id, name, uniform_no) VALUES (777, '임의선수', '22')"))
        session.commit()

        overrides = {
            ("game_batting_stats", 2024, "KIA", "테스트선수"): OverrideEntry(
                source_table="game_batting_stats",
                year=2024,
                team_code="KIA",
                player_name="테스트선수",
                resolved_player_id=777,
                reason="manual",
                evidence_source="unit-test",
            )
        }
        result = choose_candidate_ids(
            session,
            table_name="game_batting_stats",
            season=2024,
            team_code="KIA",
            player_name="테스트선수",
            uniform_nos=["10"],
            alias_map={},
            overrides=overrides,
        )
        assert result["candidate_ids"] == [777]
        assert result["resolution_method"] == "override_exact_group"
    finally:
        session.close()


def test_override_is_rejected_when_player_basic_missing():
    session = _make_session()
    try:
        overrides = {
            ("game_pitching_stats", 2024, "KIA", "테스트선수"): OverrideEntry(
                source_table="game_pitching_stats",
                year=2024,
                team_code="KIA",
                player_name="테스트선수",
                resolved_player_id=999999,
                reason="manual",
                evidence_source="unit-test",
            )
        }
        result = choose_candidate_ids(
            session,
            table_name="game_pitching_stats",
            season=2024,
            team_code="KIA",
            player_name="테스트선수",
            uniform_nos=["10"],
            alias_map={},
            overrides=overrides,
        )
        assert result["candidate_ids"] == []
        assert result["resolution_reason"] == "override_player_id_not_found_in_player_basic"
    finally:
        session.close()


def test_role_and_uniform_filter_narrows_to_single_candidate():
    session = _make_session()
    try:
        session.execute(
            text(
                """
                INSERT INTO player_basic(player_id, name, uniform_no)
                VALUES
                    (1001, '동명이인', '11'),
                    (1002, '동명이인', '22')
                """
            )
        )
        session.execute(
            text(
                """
                INSERT INTO player_season_batting(player_id, season, team_code)
                VALUES
                    (1001, 2024, 'KIA'),
                    (1002, 2024, 'KIA')
                """
            )
        )
        session.commit()

        result = choose_candidate_ids(
            session,
            table_name="game_batting_stats",
            season=2024,
            team_code="KIA",
            player_name="동명이인",
            uniform_nos=["22"],
            alias_map={},
            overrides={},
        )
        assert result["candidate_ids"] == [1002]
        assert result["resolution_method"] == "uniform_filter"
    finally:
        session.close()
