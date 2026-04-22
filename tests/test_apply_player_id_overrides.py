import csv
from pathlib import Path

from sqlalchemy import create_engine, text

from scripts.maintenance.apply_player_id_overrides import apply_overrides


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _make_db(tmp_path: Path) -> str:
    db_path = tmp_path / "kbo_test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        for table in ("game_batting_stats", "game_pitching_stats"):
            conn.execute(
                text(
                    f"""
                    CREATE TABLE {table} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        game_id TEXT NOT NULL,
                        appearance_seq INTEGER NOT NULL,
                        team_code TEXT,
                        player_name TEXT NOT NULL,
                        player_id INTEGER
                    )
                    """
                )
            )
        conn.execute(
            text(
                """
                CREATE TABLE game_lineups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    game_id TEXT NOT NULL,
                    appearance_seq INTEGER NOT NULL,
                    team_code TEXT,
                    player_name TEXT NOT NULL,
                    player_id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO game_batting_stats
                    (game_id, appearance_seq, team_code, player_name, player_id)
                VALUES
                    ('20260101HHSS0', 1, 'HH', '테스트선수', NULL),
                    ('20260102HHSS0', 1, 'HH', '테스트선수', 900001),
                    ('20260103HHSS0', 1, 'HH', '다른선수', NULL)
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO game_pitching_stats
                    (game_id, appearance_seq, team_code, player_name, player_id)
                VALUES
                    ('20260101HHSS0', 1, 'HH', '행오버라이드', 900002),
                    ('20260101HHSS0', 2, 'HH', '행오버라이드', 77777)
                """
            )
        )
    return f"sqlite:///{db_path}"


def _group_overrides_csv(tmp_path: Path) -> Path:
    path = tmp_path / "group_overrides.csv"
    _write_csv(
        path,
        [
            "source_table",
            "year",
            "team_code",
            "player_name",
            "resolved_player_id",
            "reason",
            "evidence_source",
        ],
        [
            {
                "source_table": "game_batting_stats",
                "year": 2026,
                "team_code": "HH",
                "player_name": "테스트선수",
                "resolved_player_id": 12345,
                "reason": "unit",
                "evidence_source": "unit-test",
            }
        ],
    )
    return path


def _row_overrides_csv(tmp_path: Path) -> Path:
    path = tmp_path / "row_overrides.csv"
    _write_csv(
        path,
        [
            "source_table",
            "game_id",
            "appearance_seq",
            "team_code",
            "player_name",
            "resolved_player_id",
            "reason",
            "evidence_source",
        ],
        [
            {
                "source_table": "game_pitching_stats",
                "game_id": "20260101HHSS0",
                "appearance_seq": 1,
                "team_code": "HH",
                "player_name": "행오버라이드",
                "resolved_player_id": 60146,
                "reason": "unit",
                "evidence_source": "unit-test",
            }
        ],
    )
    return path


def _player_ids(db_url: str, table: str, player_name: str) -> list[int | None]:
    engine = create_engine(db_url)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT player_id
                FROM {table}
                WHERE player_name = :player_name
                ORDER BY game_id, appearance_seq
                """
            ),
            {"player_name": player_name},
        ).fetchall()
    return [row[0] for row in rows]


def test_apply_overrides_dry_run_writes_report_without_mutating_db(tmp_path):
    db_url = _make_db(tmp_path)

    result = apply_overrides(
        db_url=db_url,
        group_overrides_csv=_group_overrides_csv(tmp_path),
        row_overrides_csv=_row_overrides_csv(tmp_path),
        output_dir=tmp_path / "reports",
        years={2026},
        tables=None,
        include_generated=False,
        apply=False,
        backup=True,
    )

    assert result["dry_run"] is True
    assert result["group_overrides"] == 1
    assert result["row_overrides"] == 1
    assert result["matched_rows"] == 2
    assert result["updated_rows"] == 0
    assert Path(result["report_csv"]).exists()
    assert result["backup_path"] == ""
    assert _player_ids(db_url, "game_batting_stats", "테스트선수") == [None, 900001]
    assert _player_ids(db_url, "game_pitching_stats", "행오버라이드") == [900002, 77777]


def test_apply_overrides_updates_nulls_and_exact_rows(tmp_path):
    db_url = _make_db(tmp_path)

    result = apply_overrides(
        db_url=db_url,
        group_overrides_csv=_group_overrides_csv(tmp_path),
        row_overrides_csv=_row_overrides_csv(tmp_path),
        output_dir=tmp_path / "reports",
        years={2026},
        tables=None,
        include_generated=False,
        apply=True,
        backup=False,
    )

    assert result["dry_run"] is False
    assert result["matched_rows"] == 2
    assert result["updated_rows"] == 2
    assert _player_ids(db_url, "game_batting_stats", "테스트선수") == [12345, 900001]
    assert _player_ids(db_url, "game_pitching_stats", "행오버라이드") == [60146, 77777]


def test_apply_overrides_can_include_generated_group_ids(tmp_path):
    db_url = _make_db(tmp_path)

    result = apply_overrides(
        db_url=db_url,
        group_overrides_csv=_group_overrides_csv(tmp_path),
        row_overrides_csv=tmp_path / "missing_row_overrides.csv",
        output_dir=tmp_path / "reports",
        years={2026},
        tables={"game_batting_stats"},
        include_generated=True,
        apply=True,
        backup=False,
    )

    assert result["group_overrides"] == 1
    assert result["row_overrides"] == 0
    assert result["matched_rows"] == 2
    assert result["updated_rows"] == 2
    assert _player_ids(db_url, "game_batting_stats", "테스트선수") == [12345, 12345]
