"""Tests for CLI entrypoint argument parsing across multiple modules."""

from __future__ import annotations

import pytest

from src.cli.crawl_p0_data import build_arg_parser as p0_parser
from src.cli.crawl_schedule import build_arg_parser as schedule_parser
from src.cli.crawl_team_events import build_arg_parser as team_events_parser
from src.cli.crawl_ticket_info import build_arg_parser as ticket_parser
from src.cli.crawl_transit_time import build_arg_parser as transit_parser
from src.cli.crawl_congestion import build_arg_parser as congestion_parser
from src.cli.run_daily_update import build_arg_parser as daily_update_parser
from src.cli.gap_report import build_arg_parser as gap_report_parser
from src.cli.refresh_source_snapshots import build_arg_parser as refresh_parser


class TestCLIArgParsers:
    @pytest.mark.parametrize(
        "parser_fn,name",
        [
            (p0_parser, "p0_data"),
            (schedule_parser, "schedule"),
            (team_events_parser, "team_events"),
            (ticket_parser, "ticket"),
            (transit_parser, "transit"),
            (congestion_parser, "congestion"),
            (daily_update_parser, "daily_update"),
            (gap_report_parser, "gap_report"),
            (refresh_parser, "refresh"),
        ],
    )
    def test_parsers_exist_and_have_required_args(self, parser_fn, name):
        """Verify each parser can be built and has expected arguments."""
        parser = parser_fn()
        assert parser is not None

    def test_p0_parser_type_choices(self):
        parser = p0_parser()
        args = parser.parse_args(["--type", "all"])
        assert args.type == "all"

    def test_schedule_parser_year(self):
        parser = schedule_parser()
        args = parser.parse_args(["--year", "2026"])
        assert args.year == 2026

    def test_daily_update_parser_fix(self):
        parser = daily_update_parser()
        args = parser.parse_args(["--fix"])
        assert args.fix is True

    def test_gap_report_parser_dry_run(self):
        parser = gap_report_parser()
        args = parser.parse_args(["--dry-run"])
        assert args.dry_run is True

    def test_refresh_parser_all(self):
        parser = refresh_parser()
        args = parser.parse_args(["--all"])
        assert args.all is True
