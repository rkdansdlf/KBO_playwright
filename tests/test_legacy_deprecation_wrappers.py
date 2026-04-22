from __future__ import annotations

import argparse

from scripts.crawling import collect_detailed_data, crawl_and_save


def test_crawl_and_save_default_stops_with_guidance(capsys):
    result = crawl_and_save.main([])

    out = capsys.readouterr().out
    assert result == 2
    assert "DEPRECATED" in out
    assert "src.cli.crawl_schedule" in out
    assert "src.cli.collect_games" in out


def test_crawl_and_save_games_only_delegates_schedule(monkeypatch, capsys):
    captured: dict[str, argparse.Namespace] = {}

    async def _fake_run_schedule_collection(args: argparse.Namespace) -> None:
        captured["args"] = args

    monkeypatch.setattr(crawl_and_save, "_run_schedule_collection", _fake_run_schedule_collection)

    result = crawl_and_save.main(
        ["--games-only", "--year", "2025", "--months", "3,4", "--delay", "2.5"]
    )

    out = capsys.readouterr().out
    assert result == 0
    assert "DEPRECATED" in out
    assert captured["args"].year == 2025
    assert captured["args"].months == "3,4"
    assert captured["args"].delay == 2.5


def test_collect_detailed_data_stops_with_guidance(capsys):
    result = collect_detailed_data.main(["--games", "--limit", "5", "--sync"])

    out = capsys.readouterr().out
    assert result == 2
    assert "DEPRECATED" in out
    assert "src.cli.crawl_game_details" in out
    assert "src.cli.run_daily_update" in out
