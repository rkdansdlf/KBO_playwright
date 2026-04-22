from sqlalchemy import create_engine, text

from scripts.maintenance.fill_null_player_ids_from_oci import _choose_candidate_id, _remote_candidate_map


def test_choose_candidate_id_prefers_single_real_id_over_generated_placeholder():
    assert _choose_candidate_id([50167, 900605]) == 50167


def test_choose_candidate_id_leaves_multiple_real_candidates_unresolved():
    assert _choose_candidate_id([50167, 68700, 900605]) is None


def test_remote_candidate_map_batches_by_game_id_and_match_key():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE game_batting_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    game_id VARCHAR(20),
                    team_side VARCHAR(10),
                    team_code VARCHAR(10),
                    player_name VARCHAR(50),
                    appearance_seq INTEGER,
                    player_id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO game_batting_stats (
                    game_id, team_side, team_code, player_name, appearance_seq, player_id
                )
                VALUES
                    ('20250510LGSS1', 'away', 'LG', '김정율', 8, 50101),
                    ('20250510LGSS1', 'away', 'LG', '김정율', 8, 900101),
                    ('20250510LGSS1', 'home', 'SS', '이승현', 1, NULL),
                    ('20250511LGSS0', 'away', 'LG', '다른선수', 1, 50102)
                """
            )
        )
        candidates = _remote_candidate_map(
            conn,
            "game_batting_stats",
            [
                {
                    "game_id": "20250510LGSS1",
                    "team_side": "away",
                    "team_code": "LG",
                    "player_name": "김정율",
                    "appearance_seq": 8,
                }
            ],
        )

    assert candidates == {
        ("20250510LGSS1", "away", "LG", "김정율", 8): [50101, 900101],
    }
