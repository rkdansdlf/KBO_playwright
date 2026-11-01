from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.cli.crawl_text_relay import (
    _kbo_game_id_from_naver,
    _payload_to_relay_rows,
    build_arg_parser,
    main,
    run_from_args,
    run_season,
    run_single_game,
)


class TestCrawlTextRelayCLI:
    def test_main_single_game(self):
        with patch("src.cli.crawl_text_relay.run_single_game", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = 10
            result = main(["--game-id", "20260412SKLG0"])

            assert result == 0
            mock_run.assert_called_once_with(
                game_id="20260412SKLG0",
                save=False,
                output_dir="data",
            )

    def test_main_single_game_save(self):
        with patch("src.cli.crawl_text_relay.run_single_game", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = 5
            result = main(["--game-id", "20260412SKLG0", "--save"])

            assert result == 0
            mock_run.assert_called_once_with(
                game_id="20260412SKLG0",
                save=True,
                output_dir="data",
            )

    def test_main_single_game_custom_output_dir(self):
        with patch("src.cli.crawl_text_relay.run_single_game", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = 3
            result = main(["--game-id", "20260412SKLG0", "--output-dir", "/tmp/relay"])

            assert result == 0
            mock_run.assert_called_once_with(
                game_id="20260412SKLG0",
                save=False,
                output_dir="/tmp/relay",
            )

    def test_main_season(self):
        with patch("src.cli.crawl_text_relay.run_season", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = {"total": 5, "success": 4, "failed": 1}
            result = main(["--season", "2026"])

            assert result == 0
            call_kwargs = mock_run.call_args
            assert call_kwargs.kwargs["season"] == 2026
            assert call_kwargs.kwargs["month"] is None
            assert call_kwargs.kwargs["save"] is False

    def test_main_season_with_month(self):
        with patch("src.cli.crawl_text_relay.run_season", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = {"total": 2, "success": 2, "failed": 0}
            result = main(["--season", "2026", "--month", "4", "--save"])

            assert result == 0
            call_kwargs = mock_run.call_args
            assert call_kwargs.kwargs["season"] == 2026
            assert call_kwargs.kwargs["month"] == 4
            assert call_kwargs.kwargs["save"] is True


class TestBuildArgParser:
    def test_parser_creation(self):
        parser = build_arg_parser()
        assert parser is not None

    def test_parser_season_flag(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--season", "2026"])
        assert args.season == 2026

    def test_parser_game_id_flag(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--game-id", "20260412SKLG0"])
        assert args.game_id == "20260412SKLG0"


class TestRunFromArgs:
    async def test_run_single_game(self):
        args = MagicMock()
        args.game_id = "G1"
        args.season = None
        args.month = None
        args.save = False
        args.output_dir = "data"
        with patch("src.cli.crawl_text_relay.run_single_game", new_callable=AsyncMock) as mock:
            mock.return_value = 5
            result = await run_from_args(args)
            assert result["rows"] == 5

    async def test_run_season(self):
        args = MagicMock()
        args.game_id = None
        args.season = 2026
        args.month = None
        args.save = False
        args.output_dir = "data"
        with patch("src.cli.crawl_text_relay.run_season", new_callable=AsyncMock) as mock:
            mock.return_value = {"total": 10}
            result = await run_from_args(args)
            assert result["total"] == 10


class TestNaverGameId:
    def test_builds_kbo_game_id(self):
        game = {"gameDate": "2026-08-19", "awayTeamCode": "HT", "homeTeamCode": "HH"}
        assert _kbo_game_id_from_naver(game) == "20260819HTHH0"

    def test_skips_cancelled_games(self):
        game = {"gameDate": "2026-08-19", "awayTeamCode": "HT", "homeTeamCode": "HH", "cancel": True}
        assert _kbo_game_id_from_naver(game) is None

    def test_skips_suspended_games(self):
        game = {"gameDate": "2026-08-19", "awayTeamCode": "HT", "homeTeamCode": "HH", "suspended": True}
        assert _kbo_game_id_from_naver(game) is None

    def test_skips_missing_fields(self):
        assert _kbo_game_id_from_naver({"gameDate": "2026-08-19"}) is None


class TestPayloadToRows:
    def test_converts_raw_pbp_rows(self):
        payload = {
            "raw_pbp_rows": [
                {
                    "inning": 3,
                    "inning_half": "top",
                    "pitcher_name": "A",
                    "batter_name": "B",
                    "play_description": "B:볼넷",
                    "result": "볼넷",
                }
            ]
        }
        rows = _payload_to_relay_rows(payload)
        assert len(rows) == 1
        assert rows[0].inning == 3
        assert rows[0].inning_half == "top"
        assert rows[0].pitcher_name == "A"
        assert rows[0].batter_name == "B"
        assert rows[0].result == "볼넷"
        assert rows[0].description == "B:볼넷"

    def test_empty_payload(self):
        assert _payload_to_relay_rows({"raw_pbp_rows": []}) == []


class TestCrawlerExecution:
    @pytest.mark.asyncio
    async def test_run_single_game_returns_row_count(self):
        crawler = MagicMock()
        crawler.crawl_game_relay = AsyncMock(return_value={"raw_pbp_rows": [{"inning": 1, "play_description": "x"}]})
        crawler.close = AsyncMock()
        crawler.get_last_failure_reason = MagicMock(return_value=None)

        with patch("src.crawlers.relay_crawler.RelayCrawler", return_value=crawler):
            rows = await run_single_game(game_id="20260412SKLG0", save=False, output_dir="relay")

        assert rows == 1
        crawler.crawl_game_relay.assert_awaited_once_with("20260412SKLG0")
        crawler.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_run_single_game_no_payload_returns_zero(self):
        crawler = MagicMock()
        crawler.crawl_game_relay = AsyncMock(return_value=None)
        crawler.close = AsyncMock()
        crawler.get_last_failure_reason = MagicMock(return_value="relay_not_found")

        with patch("src.crawlers.relay_crawler.RelayCrawler", return_value=crawler):
            rows = await run_single_game(game_id="20260412SKLG0")

        assert rows == 0

    @pytest.mark.asyncio
    async def test_run_season_uses_db_game_ids_and_counts_failures(self):
        relay = MagicMock()
        relay.crawl_game_relay = AsyncMock(
            side_effect=[
                {"raw_pbp_rows": [{"inning": 1}]},
                None,
            ]
        )
        relay.close = AsyncMock()

        with (
            patch("src.cli.crawl_text_relay._load_game_ids_from_db", return_value=["G1", "G2"]),
            patch("src.crawlers.relay_crawler.RelayCrawler", return_value=relay),
        ):
            result = await run_season(season=2026, month=None, save=False, output_dir="relay")

        assert result == {"total": 2, "success": 1, "failed": 1}
        relay.crawl_game_relay.assert_any_await("G1")
        relay.crawl_game_relay.assert_any_await("G2")
        relay.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_run_season_falls_back_to_naver_when_db_empty(self):
        relay = MagicMock()
        relay.crawl_game_relay = AsyncMock(return_value=None)
        relay.close = AsyncMock()

        with (
            patch("src.cli.crawl_text_relay._load_game_ids_from_db", return_value=[]),
            patch(
                "src.cli.crawl_text_relay._fetch_schedule_game_ids_from_naver",
                new_callable=AsyncMock,
                return_value=["20260819HTHH0"],
            ) as mock_naver,
            patch("src.crawlers.relay_crawler.RelayCrawler", return_value=relay),
        ):
            result = await run_season(season=2026, month=8, save=False, output_dir="relay")

        mock_naver.assert_awaited_once_with(2026, 8)
        assert result == {"total": 1, "success": 0, "failed": 1}

    @pytest.mark.asyncio
    async def test_run_season_saves_csv(self, tmp_path):
        relay = MagicMock()
        relay.crawl_game_relay = AsyncMock(return_value={"raw_pbp_rows": [{"inning": 2, "play_description": "y"}]})
        relay.close = AsyncMock()

        with (
            patch("src.cli.crawl_text_relay._load_game_ids_from_db", return_value=["20260819HTHH0"]),
            patch("src.crawlers.relay_crawler.RelayCrawler", return_value=relay),
        ):
            result = await run_season(
                season=2026,
                month=8,
                save=True,
                output_dir=str(tmp_path),
            )

        assert result == {"total": 1, "success": 1, "failed": 0}
        csv_files = list(tmp_path.glob("*_text_relay.csv"))
        assert len(csv_files) == 1
        assert csv_files[0].name == "20260819HTHH0_text_relay.csv"


class TestScheduleSourceFailureHandling:
    """A schedule timeout must not be reported as a quiet "0 games" success."""

    @pytest.mark.asyncio
    async def test_all_dates_failing_raises(self):
        from src.cli.crawl_text_relay import ScheduleSourceUnavailableError, _fetch_schedule_game_ids_from_naver

        with (
            patch("src.cli.crawl_text_relay._fetch_naver_schedule_games", new_callable=AsyncMock, return_value=None),
            pytest.raises(ScheduleSourceUnavailableError),
        ):
            await _fetch_schedule_game_ids_from_naver(2026, 10)

    @pytest.mark.asyncio
    async def test_genuine_no_game_month_returns_empty(self):
        from src.cli.crawl_text_relay import _fetch_schedule_game_ids_from_naver

        with patch("src.cli.crawl_text_relay._fetch_naver_schedule_games", new_callable=AsyncMock, return_value=[]):
            assert await _fetch_schedule_game_ids_from_naver(2026, 12) == []

    @pytest.mark.asyncio
    async def test_partial_failure_keeps_found_games(self):
        from src.cli.crawl_text_relay import _fetch_schedule_game_ids_from_naver

        game = {"gameId": "20261026LGSS02026", "gameDate": "2026-10-26"}
        calls = {"n": 0}

        async def fake(_client, _season, _date):
            calls["n"] += 1
            return None if calls["n"] % 3 == 0 else [game]

        with (
            patch("src.cli.crawl_text_relay._fetch_naver_schedule_games", side_effect=fake),
            patch("src.cli.crawl_text_relay._kbo_game_id_from_naver", return_value="20261026LGSS0"),
        ):
            assert await _fetch_schedule_game_ids_from_naver(2026, 10) == ["20261026LGSS0"]

    def test_main_returns_nonzero_when_schedule_unavailable(self):
        from src.cli.crawl_text_relay import ScheduleSourceUnavailableError

        with patch(
            "src.cli.crawl_text_relay.run_season",
            new_callable=AsyncMock,
            side_effect=ScheduleSourceUnavailableError("down"),
        ):
            assert main(["--season", "2026", "--month", "10"]) == 1

    def test_main_returns_zero_for_no_game_window(self):
        with patch("src.cli.crawl_text_relay.run_season", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = {"total": 0, "success": 0, "failed": 0}
            assert main(["--season", "2026", "--month", "12"]) == 0

    @pytest.mark.asyncio
    async def test_majority_failure_raises(self):
        from src.cli.crawl_text_relay import ScheduleSourceUnavailableError, _fetch_schedule_game_ids_from_naver

        calls = {"n": 0}

        async def fake(_client, _season, _date):
            calls["n"] += 1
            return [] if calls["n"] <= 10 else None  # 21/31 failed

        with (
            patch("src.cli.crawl_text_relay._fetch_naver_schedule_games", side_effect=fake),
            pytest.raises(ScheduleSourceUnavailableError),
        ):
            await _fetch_schedule_game_ids_from_naver(2026, 10)

    def test_main_returns_nonzero_when_every_crawl_failed(self):
        with patch("src.cli.crawl_text_relay.run_season", new_callable=AsyncMock) as mock_run:
            mock_run.return_value = {"total": 3, "success": 0, "failed": 3}
            assert main(["--season", "2026", "--month", "10"]) == 1


class TestFetchNaverScheduleGames:
    @staticmethod
    def _client(response=None, exc=None):
        client = MagicMock()
        client.get = AsyncMock(return_value=response, side_effect=exc)
        return client

    @pytest.mark.asyncio
    async def test_read_timeout_returns_none(self):
        import httpx

        from src.cli.crawl_text_relay import _fetch_naver_schedule_games

        client = self._client(exc=httpx.ReadTimeout("t"))
        assert await _fetch_naver_schedule_games(client, 2026, "2026-10-26") is None

    @pytest.mark.asyncio
    async def test_non_200_returns_none(self):
        from src.cli.crawl_text_relay import _fetch_naver_schedule_games

        client = self._client(response=MagicMock(status_code=503))
        assert await _fetch_naver_schedule_games(client, 2026, "2026-10-26") is None

    @pytest.mark.asyncio
    async def test_invalid_json_returns_none(self):
        from src.cli.crawl_text_relay import _fetch_naver_schedule_games

        response = MagicMock(status_code=200)
        response.json.side_effect = ValueError("bad json")
        assert await _fetch_naver_schedule_games(self._client(response=response), 2026, "2026-10-26") is None

    @pytest.mark.asyncio
    async def test_empty_games_returns_empty_list(self):
        from src.cli.crawl_text_relay import _fetch_naver_schedule_games

        response = MagicMock(status_code=200)
        response.json.return_value = {"result": {"games": []}}
        assert await _fetch_naver_schedule_games(self._client(response=response), 2026, "2026-12-01") == []
