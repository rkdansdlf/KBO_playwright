from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import scripts.maintenance.fill_oci_null_player_ids_from_local as fill_local_to_oci
from scripts.maintenance.fill_oci_null_player_ids_from_local import (
    _choose_candidate_id,
    _local_candidate_map,
    fill_oci_from_local,
)


def _create_stats_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE game_batting_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    game_id TEXT NOT NULL,
                    team_side TEXT,
                    team_code TEXT,
                    player_name TEXT NOT NULL,
                    appearance_seq INTEGER NOT NULL,
                    player_id INTEGER
                )
                """
            )
        )


def _seed_local(engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO game_batting_stats (
                    game_id, team_side, team_code, player_name, appearance_seq, player_id
                )
                VALUES
                    ('20250510LGSS1', 'away', 'LG', '김정율', 8, 50101),
                    ('20250510LGSS1', 'away', 'LG', '김정율', 8, 900101),
                    ('20250511LGSS0', 'home', 'SS', '동명이인', 1, 50102),
                    ('20250511LGSS0', 'home', 'SS', '동명이인', 1, 50103)
                """
            )
        )


def _seed_remote(engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO game_batting_stats (
                    game_id, team_side, team_code, player_name, appearance_seq, player_id
                )
                VALUES
                    ('20250510LGSS1', 'away', 'LG', '김정율', 8, NULL),
                    ('20250511LGSS0', 'home', 'SS', '동명이인', 1, NULL),
                    ('20250512LGSS0', 'away', 'LG', '미해결', 1, NULL)
                """
            )
        )


def _build_databases(tmp_path: Path):
    local_engine = create_engine("sqlite:///:memory:")
    remote_path = tmp_path / "remote.db"
    remote_url = f"sqlite:///{remote_path}"
    remote_engine = create_engine(remote_url)

    _create_stats_table(local_engine)
    _create_stats_table(remote_engine)
    _seed_local(local_engine)
    _seed_remote(remote_engine)

    return local_engine, remote_url


def _remote_player_ids(remote_url: str) -> list[int | None]:
    engine = create_engine(remote_url)
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT player_id FROM game_batting_stats ORDER BY id")
        ).fetchall()
    return [row[0] for row in rows]


def test_choose_candidate_id_prefers_single_real_id_over_generated_placeholder():
    assert _choose_candidate_id([50101, 900101]) == 50101


def test_choose_candidate_id_leaves_multiple_real_candidates_unresolved():
    assert _choose_candidate_id([50102, 50103, 900101]) is None


def test_local_candidate_map_batches_by_game_id_and_match_key(tmp_path):
    local_engine, _remote_url = _build_databases(tmp_path)
    SessionLocal = sessionmaker(bind=local_engine)

    with SessionLocal() as session:
        candidates = _local_candidate_map(
            session,
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


def test_fill_oci_from_local_dry_run_reports_without_mutating_remote(tmp_path, monkeypatch):
    local_engine, remote_url = _build_databases(tmp_path)
    monkeypatch.setattr(fill_local_to_oci, "SessionLocal", sessionmaker(bind=local_engine))

    result = fill_oci_from_local(
        oci_url=remote_url,
        years=(2025,),
        tables=("game_batting_stats",),
        output_dir=tmp_path / "reports",
        apply=False,
    )

    assert result["dry_run"] is True
    assert result["resolved_rows"] == 1
    assert result["unresolved_rows"] == 2
    assert result["updated_rows"] == 1
    assert Path(result["resolved_csv"]).exists()
    assert Path(result["unresolved_csv"]).exists()
    assert _remote_player_ids(remote_url) == [None, None, None]


def test_fill_oci_from_local_apply_updates_only_resolved_remote_rows(tmp_path, monkeypatch):
    local_engine, remote_url = _build_databases(tmp_path)
    monkeypatch.setattr(fill_local_to_oci, "SessionLocal", sessionmaker(bind=local_engine))

    result = fill_oci_from_local(
        oci_url=remote_url,
        years=(2025,),
        tables=("game_batting_stats",),
        output_dir=tmp_path / "reports",
        apply=True,
    )

    assert result["dry_run"] is False
    assert result["resolved_rows"] == 1
    assert result["unresolved_rows"] == 2
    assert result["updated_rows"] == 1
    assert _remote_player_ids(remote_url) == [50101, None, None]
