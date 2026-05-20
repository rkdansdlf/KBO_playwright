import csv
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from scripts.maintenance.apply_player_id_overrides import OverrideApplyError, apply_overrides


def _write(path: Path, content: str) -> Path:
    path.write_text(content, encoding="utf-8")
    return path


def _build_db(tmp_path: Path) -> str:
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE game_pitching_stats (
                    id INTEGER PRIMARY KEY,
                    game_id TEXT NOT NULL,
                    team_code TEXT,
                    player_name TEXT NOT NULL,
                    appearance_seq INTEGER NOT NULL,
                    player_id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO game_pitching_stats
                    (id, game_id, team_code, player_name, appearance_seq, player_id)
                VALUES
                    (1, '20260314SKHH0', 'SSG', '화이트', 1, NULL),
                    (2, '20260421SKSS0', 'SS', '이승현', 4, 901403),
                    (3, '20260319HTHH0', 'HH', '화이트', 1, 55855),
                    (4, '20260320SSNC0', 'SS', '이승현', 3, 60146),
                    (5, '20260315SKHH0', 'SSG', '화이트', 2, 900003)
                """
            )
        )
    return f"sqlite:///{db_path}"


def _empty_group_csv(tmp_path: Path) -> Path:
    return _write(
        tmp_path / "group.csv",
        "source_table,year,team_code,player_name,resolved_player_id,reason,evidence_source\n",
    )


def _build_lg_2010_db(tmp_path: Path) -> str:
    db_path = tmp_path / "lg_2010.db"
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        for table_name in ("game_batting_stats", "game_lineups"):
            conn.execute(
                text(
                    f"""
                    CREATE TABLE {table_name} (
                        id INTEGER PRIMARY KEY,
                        game_id TEXT NOT NULL,
                        team_code TEXT,
                        player_name TEXT NOT NULL,
                        appearance_seq INTEGER NOT NULL,
                        player_id INTEGER,
                        batting_order INTEGER,
                        position TEXT,
                        standard_position TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    f"""
                    INSERT INTO {table_name}
                        (id, game_id, team_code, player_name, appearance_seq,
                         player_id, batting_order, position, standard_position)
                    VALUES
                        (1, '20100420LGKH0', 'LG', '이병규', 4, 76100, 4, '우', 'RF'),
                        (2, '20100420LGKH0', 'LG', '이병규', 8, 76100, 8, '좌', 'LF'),
                        (3, '20100422LGKH0', 'LG', '이병규', 2, 76100, 2, '좌', 'LF'),
                        (4, '20100422LGKH0', 'LG', '이병규', 6, 76100, 4, '우', 'RF')
                    """
                )
            )
    return f"sqlite:///{db_path}"


def _duplicate_group_count(conn, table_name: str) -> int:
    group_columns = "game_id, player_id, team_code" if table_name == "game_lineups" else "game_id, player_id"
    return int(
        conn.execute(
            text(
                f"""
                SELECT COUNT(*)
                FROM (
                    SELECT {group_columns}
                    FROM {table_name}
                    WHERE game_id IN ('20100420LGKH0', '20100422LGKH0')
                      AND player_id IS NOT NULL
                    GROUP BY {group_columns}
                    HAVING COUNT(*) > 1
                )
                """
            )
        ).scalar()
        or 0
    )


def _report_statuses(result: dict) -> list[str]:
    with Path(result["report_csv"]).open(newline="", encoding="utf-8") as fh:
        return [row["status"] for row in csv.DictReader(fh) if row["override_type"] == "row"]


def test_group_overrides_update_null_and_generated_only(tmp_path):
    db_url = _build_db(tmp_path)
    group_csv = _write(
        tmp_path / "group.csv",
        "\n".join(
            [
                "source_table,year,team_code,player_name,resolved_player_id,reason,evidence_source",
                "game_pitching_stats,2026,SSG,화이트,55855,test,fixture",
            ]
        ),
    )
    row_csv = _write(
        tmp_path / "rows.csv",
        "source_table,game_id,appearance_seq,team_code,player_name,resolved_player_id,reason,evidence_source\n",
    )

    result = apply_overrides(
        db_url=db_url,
        group_overrides_csv=group_csv,
        row_overrides_csv=row_csv,
        output_dir=tmp_path,
        years={2026},
        tables={"game_pitching_stats"},
        include_generated=True,
        apply=True,
        backup=False,
    )

    assert result["updated_rows"] == 2
    engine = create_engine(db_url)
    with engine.connect() as conn:
        player_ids = conn.execute(
            text("SELECT player_id FROM game_pitching_stats WHERE id IN (1, 5) ORDER BY id")
        ).fetchall()
        untouched = conn.execute(text("SELECT player_id FROM game_pitching_stats WHERE id = 3")).scalar_one()
    assert player_ids == [(55855,), (55855,)]
    assert untouched == 55855


def test_row_overrides_can_correct_exact_non_null_rows(tmp_path):
    db_url = _build_db(tmp_path)
    group_csv = _empty_group_csv(tmp_path)
    row_csv = _write(
        tmp_path / "rows.csv",
        "\n".join(
            [
                "source_table,game_id,appearance_seq,team_code,player_name,resolved_player_id,reason,evidence_source",
                "game_pitching_stats,20260319HTHH0,1,HH,화이트,56724,test,fixture",
                "game_pitching_stats,20260421SKSS0,4,SS,이승현,60146,test,fixture",
            ]
        ),
    )

    result = apply_overrides(
        db_url=db_url,
        group_overrides_csv=group_csv,
        row_overrides_csv=row_csv,
        output_dir=tmp_path,
        years={2026},
        tables={"game_pitching_stats"},
        include_generated=True,
        apply=True,
        backup=False,
    )

    assert result["updated_rows"] == 2
    engine = create_engine(db_url)
    with engine.connect() as conn:
        white_id = conn.execute(text("SELECT player_id FROM game_pitching_stats WHERE id = 3")).scalar_one()
        lee_id = conn.execute(text("SELECT player_id FROM game_pitching_stats WHERE id = 2")).scalar_one()
        already_correct = conn.execute(text("SELECT player_id FROM game_pitching_stats WHERE id = 4")).scalar_one()
    assert white_id == 56724
    assert lee_id == 60146
    assert already_correct == 60146


def test_apply_overrides_dry_run_writes_report_without_mutating_rows(tmp_path):
    db_url = _build_db(tmp_path)
    group_csv = _write(
        tmp_path / "group.csv",
        "\n".join(
            [
                "source_table,year,team_code,player_name,resolved_player_id,reason,evidence_source",
                "game_pitching_stats,2026,SSG,화이트,55855,test,fixture",
            ]
        ),
    )
    row_csv = _write(
        tmp_path / "rows.csv",
        "\n".join(
            [
                "source_table,game_id,appearance_seq,team_code,player_name,resolved_player_id,reason,evidence_source",
                "game_pitching_stats,20260319HTHH0,1,HH,화이트,56724,test,fixture",
                "game_pitching_stats,20260421SKSS0,4,SS,이승현,60146,test,fixture",
            ]
        ),
    )

    result = apply_overrides(
        db_url=db_url,
        group_overrides_csv=group_csv,
        row_overrides_csv=row_csv,
        output_dir=tmp_path,
        years={2026},
        tables={"game_pitching_stats"},
        include_generated=True,
        apply=False,
        backup=True,
    )

    assert result["dry_run"] is True
    assert result["matched_rows"] == 4
    assert result["updated_rows"] == 0
    assert Path(result["report_csv"]).exists()
    assert result["backup_path"] == ""

    engine = create_engine(db_url)
    with engine.connect() as conn:
        ids = conn.execute(
            text("SELECT id, player_id FROM game_pitching_stats ORDER BY id")
        ).fetchall()
    assert ids == [(1, None), (2, 901403), (3, 55855), (4, 60146), (5, 900003)]


def test_lg_2010_row_overrides_split_homonym_groups_idempotently(tmp_path):
    db_url = _build_lg_2010_db(tmp_path)
    row_csv = _write(
        tmp_path / "rows.csv",
        "\n".join(
            [
                "source_table,game_id,appearance_seq,team_code,player_name,resolved_player_id,reason,evidence_source",
                "game_batting_stats,20100420LGKH0,8,LG,이병규,97109,split_lg_2010_homonym,fixture",
                "game_lineups,20100420LGKH0,8,LG,이병규,97109,split_lg_2010_homonym,fixture",
                "game_batting_stats,20100422LGKH0,2,LG,이병규,97109,split_lg_2010_homonym,fixture",
                "game_lineups,20100422LGKH0,2,LG,이병규,97109,split_lg_2010_homonym,fixture",
            ]
        ),
    )

    result = apply_overrides(
        db_url=db_url,
        group_overrides_csv=_empty_group_csv(tmp_path),
        row_overrides_csv=row_csv,
        output_dir=tmp_path,
        years={2010},
        tables={"game_batting_stats", "game_lineups"},
        include_generated=False,
        apply=True,
        backup=False,
    )

    assert result["updated_rows"] == 4
    assert result["row_status_counts"] == {"needs_update": 4}
    assert _report_statuses(result) == ["needs_update"] * 4

    engine = create_engine(db_url)
    with engine.connect() as conn:
        for table_name in ("game_batting_stats", "game_lineups"):
            rows = conn.execute(
                text(
                    f"""
                    SELECT game_id, appearance_seq, player_id
                    FROM {table_name}
                    WHERE game_id IN ('20100420LGKH0', '20100422LGKH0')
                      AND team_code = 'LG'
                      AND player_name = '이병규'
                    ORDER BY game_id, appearance_seq
                    """
                )
            ).fetchall()
            assert rows == [
                ("20100420LGKH0", 4, 76100),
                ("20100420LGKH0", 8, 97109),
                ("20100422LGKH0", 2, 97109),
                ("20100422LGKH0", 6, 76100),
            ]
            assert _duplicate_group_count(conn, table_name) == 0

    second = apply_overrides(
        db_url=db_url,
        group_overrides_csv=_empty_group_csv(tmp_path),
        row_overrides_csv=row_csv,
        output_dir=tmp_path,
        years={2010},
        tables={"game_batting_stats", "game_lineups"},
        include_generated=False,
        apply=True,
        backup=False,
    )

    assert second["updated_rows"] == 0
    assert second["row_status_counts"] == {"already_correct": 4}
    assert _report_statuses(second) == ["already_correct"] * 4
    with engine.connect() as conn:
        assert _duplicate_group_count(conn, "game_batting_stats") == 0
        assert _duplicate_group_count(conn, "game_lineups") == 0


def test_row_override_conflict_aborts_apply_without_partial_mutation(tmp_path):
    db_path = tmp_path / "conflict.db"
    db_url = f"sqlite:///{db_path}"
    engine = create_engine(db_url)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE game_batting_stats (
                    id INTEGER PRIMARY KEY,
                    game_id TEXT NOT NULL,
                    team_code TEXT,
                    player_name TEXT NOT NULL,
                    appearance_seq INTEGER NOT NULL,
                    player_id INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO game_batting_stats
                    (id, game_id, team_code, player_name, appearance_seq, player_id)
                VALUES
                    (1, '20100420LGKH0', 'LG', '이병규', 4, 76100),
                    (2, '20100420LGKH0', 'LG', '이병규', 8, 97109)
                """
            )
        )
    row_csv = _write(
        tmp_path / "rows.csv",
        "\n".join(
            [
                "source_table,game_id,appearance_seq,team_code,player_name,resolved_player_id,reason,evidence_source",
                "game_batting_stats,20100420LGKH0,4,LG,이병규,97109,conflict_fixture,fixture",
            ]
        ),
    )

    with pytest.raises(OverrideApplyError) as exc_info:
        apply_overrides(
            db_url=db_url,
            group_overrides_csv=_empty_group_csv(tmp_path),
            row_overrides_csv=row_csv,
            output_dir=tmp_path,
            years={2010},
            tables={"game_batting_stats"},
            include_generated=False,
            apply=True,
            backup=False,
        )

    result = exc_info.value.result
    assert result["invalid_row_overrides"] == 1
    assert result["row_status_counts"] == {"conflict": 1}
    assert _report_statuses(result) == ["conflict"]
    with engine.connect() as conn:
        ids = conn.execute(
            text("SELECT id, player_id FROM game_batting_stats ORDER BY id")
        ).fetchall()
    assert ids == [(1, 76100), (2, 97109)]
