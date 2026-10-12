from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, text

from src.cli.data_quality_regression_pack import main


def _create_quality_tables(conn) -> None:
    """A season table with the shape the pack queries, nothing else."""
    conn.execute(
        text(
            """
            CREATE TABLE player_season_batting (
                player_id INTEGER,
                season INTEGER,
                plate_appearances INTEGER,
                at_bats INTEGER,
                walks INTEGER,
                hbp INTEGER,
                sacrifice_hits INTEGER,
                sacrifice_flies INTEGER,
                league TEXT,
                level TEXT,
                source TEXT
            )
            """,
        ),
    )


def _make_report(*, ok: bool = True) -> MagicMock:
    report = MagicMock()
    report.ok = ok
    report.to_dict.return_value = {"ok": ok}
    return report


class TestDataQualityRegressionPackCLI:
    def test_requires_db_url(self):
        with (
            patch("src.cli.data_quality_regression_pack.os.getenv", return_value=None),
        ):
            try:
                main([])
            except SystemExit as exc:
                assert exc.code == 2

    def test_success_exit_code(self):
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        with (
            patch("src.cli.data_quality_regression_pack.create_engine", return_value=mock_engine),
            patch("src.cli.data_quality_regression_pack.os.getenv", return_value="sqlite:///:memory:"),
            patch("src.cli.data_quality_regression_pack.run_regression_pack", return_value=_make_report(ok=True)),
        ):
            result = main([])
            assert result == 0

    def test_failure_exit_code(self):
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        with (
            patch("src.cli.data_quality_regression_pack.create_engine", return_value=mock_engine),
            patch("src.cli.data_quality_regression_pack.os.getenv", return_value="sqlite:///:memory:"),
            patch("src.cli.data_quality_regression_pack.run_regression_pack", return_value=_make_report(ok=False)),
        ):
            result = main([])
            assert result == 1

    def test_json_output(self):
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        with (
            patch("src.cli.data_quality_regression_pack.create_engine", return_value=mock_engine),
            patch("src.cli.data_quality_regression_pack.os.getenv", return_value="sqlite:///:memory:"),
            patch("src.cli.data_quality_regression_pack.run_regression_pack", return_value=_make_report(ok=True)),
        ):
            result = main(["--json"])
            assert result == 0

    def test_main_success_stdout(self):
        with (
            patch("src.cli.data_quality_regression_pack.create_engine") as mock_create_engine,
            patch("src.cli.data_quality_regression_pack.run_regression_pack") as mock_run,
            patch("src.cli.data_quality_regression_pack.render_regression_report") as mock_render,
        ):
            mock_conn = MagicMock()
            mock_engine = MagicMock()
            mock_engine.connect.return_value.__enter__.return_value = mock_conn
            mock_create_engine.return_value = mock_engine

            mock_report = MagicMock()
            mock_report.ok = True
            mock_run.return_value = mock_report
            mock_render.return_value = "rendered regression report"

            result = main(["--database-url", "sqlite:///:memory:"])

            assert result == 0
            mock_create_engine.assert_called_once_with("sqlite:///:memory:")
            mock_run.assert_called_once_with(
                mock_conn,
                target_date=None,
                season=None,
                require_schema=False,
            )
            mock_render.assert_called_once_with(mock_report)

    def test_main_runs_with_env_vars(self):
        with (
            patch("src.cli.data_quality_regression_pack.create_engine") as mock_create_engine,
            patch("src.cli.data_quality_regression_pack.run_regression_pack") as mock_run,
            patch("src.cli.data_quality_regression_pack.render_regression_report") as mock_render,
            patch.dict("os.environ", {"DATABASE_URL": "sqlite:///from_env"}, clear=True),
        ):
            mock_conn = MagicMock()
            mock_engine = MagicMock()
            mock_engine.connect.return_value.__enter__.return_value = mock_conn
            mock_create_engine.return_value = mock_engine

            mock_report = MagicMock()
            mock_report.ok = True
            mock_run.return_value = mock_report
            mock_render.return_value = "rendered"

            result = main([])

            assert result == 0
            mock_create_engine.assert_called_once_with("sqlite:///from_env")

    def test_main_with_json_and_failure(self, capsys):
        with (
            patch("src.cli.data_quality_regression_pack.create_engine") as mock_create_engine,
            patch("src.cli.data_quality_regression_pack.run_regression_pack") as mock_run,
            patch("src.cli.data_quality_regression_pack.report_to_json") as mock_json,
        ):
            mock_conn = MagicMock()
            mock_engine = MagicMock()
            mock_engine.connect.return_value.__enter__.return_value = mock_conn
            mock_create_engine.return_value = mock_engine

            mock_report = MagicMock()
            mock_report.ok = False
            mock_run.return_value = mock_report
            mock_json.return_value = '{"ok": false}'

            result = main(["--database-url", "sqlite:///:memory:", "--json"])

            assert result == 1
            captured = capsys.readouterr()
            assert '{"ok": false}' in captured.out
            mock_json.assert_called_once_with(mock_report)

    def test_main_scopes_checks_and_writes_json_artifact(self, tmp_path):
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        output_path = tmp_path / "regression.json"

        with (
            patch("src.cli.data_quality_regression_pack.create_engine", return_value=mock_engine),
            patch(
                "src.cli.data_quality_regression_pack.run_regression_pack", return_value=_make_report(ok=True)
            ) as mock_run,
            patch("src.cli.data_quality_regression_pack.render_regression_report", return_value="ok"),
            patch("src.cli.data_quality_regression_pack.report_to_json", return_value='{"ok": true}'),
        ):
            result = main(
                [
                    "--database-url",
                    "sqlite:///:memory:",
                    "--date",
                    "2026-06-24",
                    "--require-schema",
                    "--output",
                    str(output_path),
                ],
            )

        assert result == 0
        mock_run.assert_called_once_with(
            mock_conn,
            target_date="20260624",
            season=2026,
            require_schema=True,
        )
        assert output_path.read_text(encoding="utf-8") == '{"ok": true}\n'


class TestRecordingABaselineFromTheCommandLine:
    """The daily gate's remediation hint names this command, so it has to exist.

    A hint telling an operator to run a flag the tool does not have is worse than
    no hint: it reads as an instruction and produces nothing. Writing is also the
    only step between "the gate compares against nothing" and "the gate can say
    whether the backlog grew".
    """

    def test_writing_a_baseline_creates_the_file_the_gate_reads(self, tmp_path, capsys) -> None:
        db_path = tmp_path / "pa.db"
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            _create_quality_tables(conn)
            conn.execute(
                text(
                    """
                    INSERT INTO player_season_batting
                        (player_id, season, plate_appearances, at_bats, walks, hbp,
                         sacrifice_hits, sacrifice_flies, league, level, source)
                    VALUES (5001, 2026, 15, 13, 1, 0, 0, 0, 'REGULAR', 'KBO1', 'LEGACY_CRAWLER')
                    """,
                ),
            )
        baseline = tmp_path / "baseline.json"

        exit_code = main(
            [
                "--database-url",
                f"sqlite:///{db_path}",
                "--year",
                "2026",
                "--write-baseline",
                str(baseline),
            ],
        )

        out = capsys.readouterr().out
        assert baseline.exists(), "the command wrote no file"
        body = json.loads(baseline.read_text())
        assert body["checks"]["player_season_batting_unaccounted_pa"]["player_ids"] == ["5001"]
        assert "recorded 1 violating row(s)" in out
        # The exit code still reports the pack's own verdict. Recording a baseline
        # is extra work done alongside the run, not a reason to call a failing
        # pack a success -- the whole point is that the backlog stays visible.
        assert exit_code == 1

    def test_writing_records_the_scope_so_a_season_run_can_read_it(self, tmp_path, capsys) -> None:
        db_path = tmp_path / "pa.db"
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            _create_quality_tables(conn)
        baseline = tmp_path / "baseline.json"

        main(["--database-url", f"sqlite:///{db_path}", "--year", "2026", "--write-baseline", str(baseline)])

        capsys.readouterr()
        assert json.loads(baseline.read_text())["scope"] == {"season": 2026}

    def test_the_unfixable_record_is_written_alongside_it(self, tmp_path, capsys) -> None:
        """The two files are read together, so one without the other is a half answer."""
        db_path = tmp_path / "pa.db"
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.begin() as conn:
            _create_quality_tables(conn)
        baseline = tmp_path / "baseline.json"

        main(["--database-url", f"sqlite:///{db_path}", "--write-baseline", str(baseline)])

        capsys.readouterr()
        assert (tmp_path / "unrecoverable.json").exists()
