from __future__ import annotations

from src.cli.crawl_futures import build_arg_parser


class TestCrawlFuturesArgs:
    def test_parser_defaults(self):
        parser = build_arg_parser()
        args = parser.parse_args([])
        assert args.season is not None
        assert args.concurrency == 3
        assert args.delay == 2.0
        assert args.limit is None
        assert args.json_summary is False
        assert args.changed_since is None

    def test_parser_json_summary(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--json-summary"])
        assert args.json_summary is True

    def test_parser_season(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--season", "2025"])
        assert args.season == 2025

    def test_parser_concurrency(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--concurrency", "5"])
        assert args.concurrency == 5

    def test_parser_limit(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--limit", "10"])
        assert args.limit == 10

    def test_parser_player_ids(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--player-ids", "123,456"])
        assert args.player_ids == "123,456"

    def test_parser_changed_since(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--changed-since", "2026-06-08"])
        assert args.changed_since == "2026-06-08"
