from __future__ import annotations

import asyncio
import json

import src.cli.crawler_live_smoke as live_smoke


class _FakeScheduleCrawler:
    async def crawl_schedule(self, year: int, month: int):
        assert (year, month) == (2025, 1)
        return [
            {
                "game_id": "20250101LGSS0",
                "game_date": "20250101",
                "game_status": "COMPLETED",
            },
            {
                "game_id": "20250102KTSS0",
                "game_date": "20250102",
                "game_status": "COMPLETED",
            },
            {
                "game_id": "20250101NCLG0",
                "game_date": "20250101",
                "game_status": "COMPLETED",
            },
        ]


class _FakeDetailCrawlerComplete:
    async def crawl_game(self, game_id: str, game_date: str, lightweight: bool = False):
        return {
            "game_id": game_id,
            "game_date": game_date,
            "hitters": {"away": [{"player_id": 1}], "home": [{"player_id": 2}]},
            "pitchers": {"away": [{"player_id": 3}], "home": [{"player_id": 4}]},
        }

    def get_last_failure_reason(self, game_id: str):
        return None


class _FakeDetailCrawlerIncomplete:
    async def crawl_game(self, game_id: str, game_date: str, lightweight: bool = False):
        return {
            "game_id": game_id,
            "game_date": game_date,
            "hitters": {"away": [], "home": [{"player_id": 2}]},
            "pitchers": {"away": [{"player_id": 3}], "home": [{"player_id": 4}]},
        }

    def get_last_failure_reason(self, game_id: str):
        return "incomplete_detail"


class _FakeRelayCrawlerComplete:
    async def crawl_game_relay(self, game_id: str):
        return {"events": [], "raw_pbp_rows": [{"description": "play"}]}

    def get_last_failure_reason(self, game_id: str):
        return None


class _FakeRelayCrawlerEmpty:
    async def crawl_game_relay(self, game_id: str):
        return {"events": [], "raw_pbp_rows": []}

    def get_last_failure_reason(self, game_id: str):
        return "relay_empty"


def test_main_requires_network_opt_in(monkeypatch, capsys):
    monkeypatch.delenv("KBO_LIVE_SMOKE", raising=False)

    code = live_smoke.main(["--date", "20250101"])

    assert code == 2
    assert "requires --allow-network" in capsys.readouterr().out


def test_schedule_scope_selects_matching_detail_candidates_with_limit():
    result = asyncio.run(
        live_smoke.run_smoke(
            target_date="20250101",
            scope="schedule",
            limit=1,
            schedule_crawler=_FakeScheduleCrawler(),
        )
    )

    assert result["ok"] is True
    assert result["candidates"] == ["20250101LGSS0"]
    assert result["results"] == []
    assert result["failure_reasons"] == {}


def test_detail_scope_requires_both_team_hitters_and_pitchers():
    complete = asyncio.run(
        live_smoke.run_smoke(
            target_date="20250101",
            game_id="20250101LGSS0",
            scope="detail",
            detail_crawler=_FakeDetailCrawlerComplete(),
        )
    )
    incomplete = asyncio.run(
        live_smoke.run_smoke(
            target_date="20250101",
            game_id="20250101LGSS0",
            scope="detail",
            detail_crawler=_FakeDetailCrawlerIncomplete(),
        )
    )

    assert complete["ok"] is True
    assert complete["results"][0]["detail"]["hitters"] == {"away": 1, "home": 1}
    assert incomplete["ok"] is False
    assert incomplete["failure_reasons"] == {"20250101LGSS0": ["incomplete_detail"]}


def test_relay_scope_accepts_events_or_raw_pbp_rows():
    complete = asyncio.run(
        live_smoke.run_smoke(
            target_date="20250101",
            game_id="20250101LGSS0",
            scope="relay",
            relay_crawler=_FakeRelayCrawlerComplete(),
        )
    )
    empty = asyncio.run(
        live_smoke.run_smoke(
            target_date="20250101",
            game_id="20250101LGSS0",
            scope="relay",
            relay_crawler=_FakeRelayCrawlerEmpty(),
        )
    )

    assert complete["ok"] is True
    assert complete["results"][0]["relay"]["raw_pbp_rows"] == 1
    assert empty["ok"] is False
    assert empty["failure_reasons"] == {"20250101LGSS0": ["relay_empty"]}


def test_json_output_shape(monkeypatch, capsys):
    monkeypatch.setenv("KBO_LIVE_SMOKE", "1")
    monkeypatch.setattr(live_smoke, "GameDetailCrawler", lambda: _FakeDetailCrawlerComplete())

    code = live_smoke.main(
        [
            "--date",
            "20250101",
            "--game-id",
            "20250101LGSS0",
            "--scope",
            "detail",
            "--json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert code == 0
    assert set(payload) == {"ok", "target_date", "scope", "candidates", "results", "failure_reasons"}
    assert payload["ok"] is True
    assert payload["target_date"] == "20250101"
    assert payload["scope"] == "detail"
    assert payload["candidates"] == ["20250101LGSS0"]
