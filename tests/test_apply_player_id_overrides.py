from pathlib import Path

from sqlalchemy import create_engine, text

from scripts.maintenance.apply_player_id_overrides import apply_overrides


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
    group_csv = _write(
        tmp_path / "group.csv",
        "source_table,year,team_code,player_name,resolved_player_id,reason,evidence_source\n",
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
