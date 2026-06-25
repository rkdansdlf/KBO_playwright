from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.cli.crawler_live_smoke import (
    SCOPES,
    _base_result,
    _candidate,
    _detail_complete,
    _network_allowed,
    _print_human_summary,
    _resolve_candidates,
    _row_count,
    _select_schedule_candidates,
    build_arg_parser,
    main,
    run_smoke,
)


def _run_and_close(coro, result):
    coro.close()
    return result


class TestCrawlerLiveSmoke:
    def test_network_not_allowed(self):
        with patch("src.cli.crawler_live_smoke._network_allowed", return_value=False):
            result = main(["--date", "20250101"])
            assert result == 2

    def test_network_allowed_schedule_scope(self):
        with patch("src.cli.crawler_live_smoke._network_allowed", return_value=True):
            with patch("src.cli.crawler_live_smoke.asyncio.run") as mock_run:
                mock_run.side_effect = lambda coro: _run_and_close(coro, {"ok": True})
                result = main(["--date", "20250101", "--allow-network", "--scope", "schedule"])
                assert result == 0

    def test_network_allowed_fail(self):
        with patch("src.cli.crawler_live_smoke._network_allowed", return_value=True):
            with patch("src.cli.crawler_live_smoke.asyncio.run") as mock_run:
                mock_run.side_effect = lambda coro: _run_and_close(coro, {"ok": False})
                result = main(["--date", "20250101", "--allow-network"])
                assert result == 1


class TestNetworkAllowed:
    def test_env_set(self, monkeypatch):
        monkeypatch.setenv("KBO_LIVE_SMOKE", "1")
        assert _network_allowed(allow_network=False) is True

    def test_env_not_set(self, monkeypatch):
        monkeypatch.setenv("KBO_LIVE_SMOKE", "")
        assert _network_allowed(allow_network=False) is False

    def test_allow_network_flag(self):
        assert _network_allowed(allow_network=True) is True


class TestBaseResult:
    def test_structure(self):
        result = _base_result("20250101", "all")
        assert result["ok"] is False
        assert result["target_date"] == "20250101"
        assert result["scope"] == "all"
        assert result["candidates"] == []
        assert result["results"] == []
        assert result["failure_reasons"] == {}


class TestCandidate:
    def test_normalizes_game_id(self):
        c = _candidate("20250615LGSS0", "20250615")
        assert c["game_id"] == "20250615LGSS0"
        assert c["game_date"] == "20250615"


class TestRowCount:
    def test_counts_list_rows(self):
        assert _row_count({"hitters": {"away": [1, 2, 3]}}, "hitters", "away") == 3

    def test_empty_payload(self):
        assert _row_count(None, "hitters", "away") == 0

    def test_missing_section(self):
        assert _row_count({}, "hitters", "away") == 0

    def test_non_list_returns_zero(self):
        assert _row_count({"hitters": {"away": "not_a_list"}}, "hitters", "away") == 0


class TestDetailComplete:
    def test_complete(self):
        payload = {
            "hitters": {"away": [1], "home": [1]},
            "pitchers": {"away": [1], "home": [1]},
        }
        assert _detail_complete(payload) is True

    def test_incomplete(self):
        payload = {
            "hitters": {"away": [], "home": [1]},
            "pitchers": {"away": [1], "home": [1]},
        }
        assert _detail_complete(payload) is False

    def test_empty_payload(self):
        assert _detail_complete(None) is False


class TestSelectScheduleCandidates:
    def test_selects_matching_date(self):
        games = [
            {"game_date": "2025-06-15", "game_id": "G1"},
            {"game_date": "2025-06-16", "game_id": "G2"},
        ]
        with patch("src.cli.crawler_live_smoke.is_detail_candidate_game", return_value=True):
            result = _select_schedule_candidates(games, target_date="20250615", limit=10)
        assert len(result) == 1
        assert result[0]["game_id"] == "G1"

    def test_respects_limit(self):
        games = [{"game_date": "2025-06-15", "game_id": f"G{i}"} for i in range(5)]
        with patch("src.cli.crawler_live_smoke.is_detail_candidate_game", return_value=True):
            result = _select_schedule_candidates(games, target_date="20250615", limit=2)
        assert len(result) == 2

    def test_skips_non_candidates(self):
        games = [{"game_date": "2025-06-15", "game_id": "G1"}]
        with patch("src.cli.crawler_live_smoke.is_detail_candidate_game", return_value=False):
            result = _select_schedule_candidates(games, target_date="20250615", limit=10)
        assert result == []

    def test_skips_empty_game_id(self):
        games = [{"game_date": "2025-06-15", "game_id": ""}]
        with patch("src.cli.crawler_live_smoke.is_detail_candidate_game", return_value=True):
            result = _select_schedule_candidates(games, target_date="20250615", limit=10)
        assert result == []


class TestResolveCandidates:
    @pytest.mark.asyncio
    async def test_with_game_id(self):
        result = await _resolve_candidates(
            target_date="20250615",
            game_id="20250615LGSS0",
            limit=1,
            schedule_crawler=MagicMock(),
        )
        assert len(result) == 1
        assert result[0]["game_id"] == "20250615LGSS0"

    @pytest.mark.asyncio
    async def test_with_game_id_mismatched_date(self):
        with pytest.raises(ValueError, match="date prefix"):
            await _resolve_candidates(
                target_date="20250615",
                game_id="20250616LGSS0",
                limit=1,
                schedule_crawler=MagicMock(),
            )

    @pytest.mark.asyncio
    async def test_without_game_id(self):
        mock_crawler = MagicMock()
        mock_crawler.crawl_schedule = AsyncMock(
            return_value=[{"game_date": "2025-06-15", "game_id": "G1"}]
        )
        with patch("src.cli.crawler_live_smoke.is_detail_candidate_game", return_value=True):
            result = await _resolve_candidates(
                target_date="20250615",
                game_id=None,
                limit=10,
                schedule_crawler=mock_crawler,
            )
        assert len(result) == 1


class TestRunSmoke:
    @pytest.mark.asyncio
    async def test_schedule_scope_returns_ok(self):
        mock_schedule = MagicMock()
        mock_schedule.crawl_schedule = AsyncMock(
            return_value=[{"game_date": "2025-06-15", "game_id": "G1"}]
        )
        result = await run_smoke(
            target_date="20250615",
            scope="schedule",
            schedule_crawler=mock_schedule,
        )
        assert result["ok"] is True

    @pytest.mark.asyncio
    async def test_invalid_scope_raises(self):
        with pytest.raises(ValueError, match="Unsupported scope"):
            await run_smoke(target_date="20250615", scope="invalid")

    @pytest.mark.asyncio
    async def test_invalid_limit_raises(self):
        with pytest.raises(ValueError, match="at least 1"):
            await run_smoke(target_date="20250615", scope="all", limit=0)

    @pytest.mark.asyncio
    async def test_no_candidates(self):
        mock_schedule = MagicMock()
        mock_schedule.crawl_schedule = AsyncMock(return_value=[])
        result = await run_smoke(
            target_date="20250615",
            scope="all",
            schedule_crawler=mock_schedule,
        )
        assert result["ok"] is False
        assert "no_detail_candidates" in result["failure_reasons"]["schedule"]

    @pytest.mark.asyncio
    async def test_detail_scope_success(self):
        mock_schedule = MagicMock()
        mock_schedule.crawl_schedule = AsyncMock(
            return_value=[{"game_date": "2025-06-15", "game_id": "G1"}]
        )
        mock_detail = MagicMock()
        mock_detail.crawl_game = AsyncMock(
            return_value={
                "hitters": {"away": [1], "home": [1]},
                "pitchers": {"away": [1], "home": [1]},
            }
        )
        result = await run_smoke(
            target_date="20250615",
            scope="detail",
            schedule_crawler=mock_schedule,
            detail_crawler=mock_detail,
        )
        assert result["ok"] is True

    @pytest.mark.asyncio
    async def test_detail_scope_failure(self):
        mock_schedule = MagicMock()
        mock_schedule.crawl_schedule = AsyncMock(
            return_value=[{"game_date": "2025-06-15", "game_id": "G1"}]
        )
        mock_detail = MagicMock()
        mock_detail.crawl_game = AsyncMock(return_value=None)
        mock_detail.get_last_failure_reason = MagicMock(return_value="timeout")
        result = await run_smoke(
            target_date="20250615",
            scope="detail",
            schedule_crawler=mock_schedule,
            detail_crawler=mock_detail,
        )
        assert result["ok"] is False
        assert "G1" in result["failure_reasons"]


class TestBuildArgParser:
    def test_default_scope(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--date", "20250101"])
        assert args.scope == "all"
        assert args.limit == 1

    def test_custom_scope(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--date", "20250101", "--scope", "relay"])
        assert args.scope == "relay"

    def test_json_flag(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--date", "20250101", "--json"])
        assert args.json is True


class TestPrintHumanSummary:
    def test_passed(self, caplog):
        result = {"ok": True, "target_date": "20250101", "scope": "all", "candidates": ["G1"]}
        with caplog.at_level(logging.INFO):
            _print_human_summary(result)
        assert "passed" in caplog.text

    def test_failed(self, caplog):
        result = {"ok": False, "target_date": "20250101", "scope": "all", "candidates": []}
        with caplog.at_level(logging.INFO):
            _print_human_summary(result)
        assert "failed" in caplog.text


import logging
