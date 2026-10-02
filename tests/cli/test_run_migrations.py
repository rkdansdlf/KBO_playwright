"""Unit tests for src.cli.run_migrations."""

from __future__ import annotations

import json
import pytest

from src.cli.run_migrations import build_arg_parser, main


def test_build_arg_parser() -> None:
    parser = build_arg_parser()
    args = parser.parse_args(["--dialect", "oracle", "--dry-run", "--json"])
    assert args.dialect == "oracle"
    assert args.dry_run is True
    assert args.json is True


def test_main_cli_execution_status_json(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(["--dialect", "oracle", "--status", "--json", "--db-url", "sqlite:///:memory:"])
    captured = capsys.readouterr()

    assert exit_code == 0
    data = json.loads(captured.out)
    assert data["dialect"] == "oracle"
    assert "total_available" in data
    assert data["total_available"] >= 50


def test_the_dialect_must_be_named() -> None:
    """A bare invocation used to silently pick the Oracle chain.

    The chains are separate files, so the default was a guess about which
    database was about to be changed -- and a wrong guess applies one chain's
    schema changes to another database. Refusing is the only safe default.
    """
    parser = build_arg_parser()

    with pytest.raises(SystemExit) as excinfo:
        parser.parse_args(["--status"])

    assert excinfo.value.code == 2


def test_an_unknown_dialect_is_rejected() -> None:
    parser = build_arg_parser()

    with pytest.raises(SystemExit) as excinfo:
        parser.parse_args(["--dialect", "mysql"])

    assert excinfo.value.code == 2
