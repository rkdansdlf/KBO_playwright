"""Tests for CLI entrypoint argument parsing."""

from __future__ import annotations

import pytest

from src.cli.seed_p1_data import build_arg_parser, run_seat, run_parking, run_food, run_all


class TestSeedP1DataCLI:
    def test_parser_type_choices(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--type", "seat"])
        assert args.type == "seat"

    def test_parser_default_dry_run(self):
        parser = build_arg_parser()
        args = parser.parse_args([])
        assert args.dry_run is False

    def test_parser_dry_run_flag(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--dry-run"])
        assert args.dry_run is True

    def test_parser_invalid_type(self):
        parser = build_arg_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--type", "invalid"])

    @pytest.mark.parametrize("run_func", [run_seat, run_parking, run_food, run_all])
    def test_run_functions_accept_dry_run(self, run_func):
        # Just verify the function signature accepts dry_run
        import inspect

        sig = inspect.signature(run_func)
        assert "dry_run" in sig.parameters
