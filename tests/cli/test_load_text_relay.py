"""Tests for load_text_relay CLI."""

from __future__ import annotations

import csv
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.cli.load_text_relay import (
    _find_csv_files,
    _load_csv_file,
    build_arg_parser,
    load_text_relays,
    main,
)


class TestFindCsvFiles:
    def test_finds_text_relay_csvs(self, tmp_path):
        (tmp_path / "20250615_KT_LG_text_relay.csv").write_text("inning,half\n1,top\n")
        (tmp_path / "20250616_SS_HH_text_relay.csv").write_text("inning,half\n1,bot\n")
        result = _find_csv_files(tmp_path)
        assert len(result) == 2

    def test_ignores_non_matching_files(self, tmp_path):
        (tmp_path / "other.csv").write_text("a,b\n")
        (tmp_path / "readme.txt").write_text("hello")
        result = _find_csv_files(tmp_path)
        assert result == []

    def test_empty_directory(self, tmp_path):
        result = _find_csv_files(tmp_path)
        assert result == []

    def test_returns_sorted(self, tmp_path):
        (tmp_path / "z_text_relay.csv").write_text("a\n")
        (tmp_path / "a_text_relay.csv").write_text("a\n")
        result = _find_csv_files(tmp_path)
        assert result[0].name == "a_text_relay.csv"
        assert result[1].name == "z_text_relay.csv"


def _write_csv(path: Path, headers: list[str], rows: list[list[str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)


class TestLoadCsvFile:
    def test_loads_valid_csv(self, tmp_path):
        csv_path = tmp_path / "20250615_KT_LG_text_relay.csv"
        _write_csv(csv_path, ["inning", "inning_half", "batter_name"], [["1", "top", "김현수"]])

        mock_session = MagicMock()
        result = _load_csv_file(csv_path, mock_session)
        assert result == 1
        mock_session.add.assert_called_once()
        mock_session.flush.assert_called_once()

    def test_loads_multiple_rows(self, tmp_path):
        csv_path = tmp_path / "G1_text_relay.csv"
        _write_csv(csv_path, ["inning", "batter_name"], [["1", "A"], ["2", "B"], ["3", "C"]])

        mock_session = MagicMock()
        result = _load_csv_file(csv_path, mock_session)
        assert result == 3

    def test_game_id_from_stem(self, tmp_path):
        csv_path = tmp_path / "20250615_KT_LG_text_relay.csv"
        _write_csv(csv_path, ["inning"], [["1"]])

        mock_session = MagicMock()
        _load_csv_file(csv_path, mock_session)

        added_play = mock_session.add.call_args[0][0]
        assert added_play.game_id == "20250615_KT_LG"

    def test_parse_error_returns_zero(self, tmp_path):
        csv_path = tmp_path / "G1_text_relay.csv"
        csv_path.write_text("inning\n1\n", encoding="utf-8")

        mock_session = MagicMock()
        with patch("csv.DictReader", side_effect=csv.Error("bad csv")):
            result = _load_csv_file(csv_path, mock_session)
        assert result == 0
        mock_session.rollback.assert_called_once()

    def test_missing_file_returns_zero(self, tmp_path):
        csv_path = tmp_path / "nonexistent_text_relay.csv"
        mock_session = MagicMock()
        result = _load_csv_file(csv_path, mock_session)
        assert result == 0


class TestLoadTextRelays:
    def test_loads_multiple_files(self, tmp_path):
        _write_csv(tmp_path / "G1_text_relay.csv", ["inning"], [["1"], ["2"]])
        _write_csv(tmp_path / "G2_text_relay.csv", ["inning"], [["1"]])

        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        with patch("src.cli.load_text_relay.SessionLocal", return_value=mock_session):
            results = load_text_relays(tmp_path, dry_run=False)

        assert results == {"G1": 2, "G2": 1}
        mock_session.commit.assert_called_once()

    def test_dry_run_rolls_back(self, tmp_path):
        _write_csv(tmp_path / "G1_text_relay.csv", ["inning"], [["1"]])

        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        with patch("src.cli.load_text_relay.SessionLocal", return_value=mock_session):
            results = load_text_relays(tmp_path, dry_run=True)

        assert results == {"G1": 1}
        mock_session.rollback.assert_called_once()
        mock_session.commit.assert_not_called()

    def test_no_files_returns_empty(self, tmp_path):
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        with patch("src.cli.load_text_relay.SessionLocal", return_value=mock_session):
            results = load_text_relays(tmp_path, dry_run=False)

        assert results == {}

    def test_commit_failure_returns_empty(self, tmp_path):
        _write_csv(tmp_path / "G1_text_relay.csv", ["inning"], [["1"]])

        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_session.commit.side_effect = Exception("DB error")

        with patch("src.cli.load_text_relay.SessionLocal", return_value=mock_session):
            with patch("src.cli.load_text_relay.SQLAlchemyError", (Exception,)):
                results = load_text_relays(tmp_path, dry_run=False)

        assert results == {}


class TestBuildArgParser:
    def test_default_input_dir(self):
        parser = build_arg_parser()
        args = parser.parse_args([])
        assert args.input_dir == Path("data")
        assert args.dry_run is False

    def test_custom_input_dir(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--input-dir", "/tmp/csv"])
        assert args.input_dir == Path("/tmp/csv")

    def test_dry_run_flag(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--dry-run"])
        assert args.dry_run is True


class TestMain:
    def test_main_with_files(self, tmp_path, caplog):
        _write_csv(tmp_path / "G1_text_relay.csv", ["inning"], [["1"]])

        with patch("src.cli.load_text_relay.load_text_relays", return_value={"G1": 1}):
            with patch("sys.argv", ["load_text_relay", "--input-dir", str(tmp_path)]):
                with caplog.at_level(logging.INFO):
                    main()

    def test_main_no_files(self, tmp_path, caplog):
        with patch("src.cli.load_text_relay.load_text_relays", return_value={}):
            with patch("sys.argv", ["load_text_relay", "--input-dir", str(tmp_path)]):
                with caplog.at_level(logging.INFO):
                    main()


import logging
