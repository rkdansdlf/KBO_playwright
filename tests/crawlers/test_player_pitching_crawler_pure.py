"""Unit tests for player_pitching_all_series_crawler pure functions."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from src.crawlers.player_pitching_all_series_crawler import (
    PitcherStats,
    _map_pitcher_basic1_stats,
    _update_pitcher_basic2_stats,
    build_pitching_crawl_summary,
)


class TestMapPitcherBasic1Stats:
    def test_new_player(self) -> None:
        with patch(
            "src.crawlers.player_pitching_all_series_crawler.resolve_team_code",
            return_value="LG",
        ):
            pitchers: dict[int, PitcherStats] = {}
            row = {
                "player_id": 100,
                "player_name": "Kelly",
                "team_name": "LG",
                "raw": {
                    "G": "20",
                    "GS": "20",
                    "W": "12",
                    "L": "5",
                    "IP": "140.1",
                    "H": "110",
                    "HR": "10",
                    "BB": "30",
                    "SO": "120",
                    "ERA": "3.50",
                    "WHIP": "1.10",
                    "CG": "2",
                    "SHO": "1",
                },
            }
            result = _map_pitcher_basic1_stats(row, 2026, "REGULAR", pitchers, max_players=None)
            assert result is True
            assert 100 in pitchers
            stats = pitchers[100]
            assert stats.player_name == "Kelly"
            assert stats.team_code == "LG"
            assert stats.games == 20
            assert stats.games_started == 20
            assert stats.wins == 12
            assert stats.losses == 5
            assert stats.innings_pitched == 140.1
            assert stats.hits_allowed == 110
            assert stats.home_runs_allowed == 10
            assert stats.walks_allowed == 30
            assert stats.strikeouts == 120
            assert stats.era == 3.50
            assert stats.whip == 1.10

    def test_existing_player_updates(self) -> None:
        with patch(
            "src.crawlers.player_pitching_all_series_crawler.resolve_team_code",
            return_value="SSG",
        ):
            pitchers: dict[int, PitcherStats] = {1: PitcherStats(player_id=1, season=2026, league="REGULAR")}
            row = {
                "player_id": 1,
                "player_name": "Kim",
                "team_name": "SSG",
                "raw": {"G": "30", "W": "15", "L": "8", "ERA": "2.80"},
            }
            _map_pitcher_basic1_stats(row, 2026, "REGULAR", pitchers, max_players=None)
            stats = pitchers[1]
            assert stats.player_name == "Kim"
            assert stats.games == 30
            assert stats.wins == 15
            assert stats.losses == 8
            assert stats.era == 2.80
            # Not in raw, should remain None
            assert stats.hits_allowed is None

    def test_max_players_limit(self) -> None:
        pitchers: dict[int, PitcherStats] = {1: PitcherStats(player_id=1, season=2026, league="REGULAR")}
        row = {"player_id": 2, "player_name": "Park", "team_name": "KT", "raw": {}}
        result = _map_pitcher_basic1_stats(row, 2026, "REGULAR", pitchers, max_players=1)
        assert result is False
        assert 2 not in pitchers

    def test_max_players_allow_existing(self) -> None:
        pitchers: dict[int, PitcherStats] = {1: PitcherStats(player_id=1, season=2026, league="REGULAR")}
        row = {"player_id": 1, "player_name": "Park", "team_name": "KT", "raw": {"G": "10"}}
        with patch(
            "src.crawlers.player_pitching_all_series_crawler.resolve_team_code",
            return_value="KT",
        ):
            result = _map_pitcher_basic1_stats(row, 2026, "REGULAR", pitchers, max_players=1)
        assert result is True

    def test_empty_raw(self) -> None:
        with patch(
            "src.crawlers.player_pitching_all_series_crawler.resolve_team_code",
            return_value="LG",
        ):
            pitchers: dict[int, PitcherStats] = {}
            row = {
                "player_id": 5,
                "player_name": "Choi",
                "team_name": "LG",
                "raw": {},
            }
            _map_pitcher_basic1_stats(row, 2026, "REGULAR", pitchers, max_players=None)
            stats = pitchers[5]
            assert stats.games is None
            assert stats.innings_pitched is None

    def test_korean_game_started_key(self) -> None:
        with patch(
            "src.crawlers.player_pitching_all_series_crawler.resolve_team_code",
            return_value="LG",
        ):
            pitchers: dict[int, PitcherStats] = {}
            row = {
                "player_id": 10,
                "player_name": "Yoo",
                "team_name": "LG",
                "raw": {"선발": "15"},
            }
            _map_pitcher_basic1_stats(row, 2026, "REGULAR", pitchers, max_players=None)
            assert pitchers[10].games_started == 15

    def test_extra_stats_metrics_and_rankings(self) -> None:
        with patch(
            "src.crawlers.player_pitching_all_series_crawler.resolve_team_code",
            return_value="LG",
        ):
            pitchers: dict[int, PitcherStats] = {}
            row = {
                "player_id": 20,
                "player_name": "Kim",
                "team_name": "LG",
                "raw": {
                    "CG": "3",
                    "SHO": "2",
                    "TBF": "500",
                    "순위": "5",
                    "WPCT": "0.750",
                },
            }
            _map_pitcher_basic1_stats(row, 2026, "REGULAR", pitchers, max_players=None)
            stats = pitchers[20]
            metrics = stats.extra_stats.get("metrics", {})
            assert metrics.get("complete_games") == 3
            assert metrics.get("shutouts") == 2
            assert metrics.get("tbf") == 500
            rankings = stats.extra_stats.get("rankings", {})
            assert rankings.get("basic1") == 5

    def test_no_resolve_team_code(self) -> None:
        with patch(
            "src.crawlers.player_pitching_all_series_crawler.resolve_team_code",
            return_value="TestTeam",
        ):
            pitchers: dict[int, PitcherStats] = {}
            row = {
                "player_id": 30,
                "player_name": "Park",
                "team_name": "TestTeam",
                "raw": {},
            }
            _map_pitcher_basic1_stats(row, 2026, "REGULAR", pitchers, max_players=None)
            assert pitchers[30].team_code == "TestTeam"


class TestPitcherStatsToRepositoryPayload:
    def test_basic_payload(self) -> None:
        stats = PitcherStats(player_id=1, season=2026, league="REGULAR")
        payload = stats.to_repository_payload()
        assert payload["player_id"] == 1
        assert payload["season"] == 2026
        assert payload["league"] == "REGULAR"
        assert payload["level"] == "KBO1"
        assert payload["source"] == "CRAWLER"

    def test_full_stats(self) -> None:
        stats = PitcherStats(
            player_id=2,
            season=2026,
            league="REGULAR",
            player_name="Kelly",
            team_code="LG",
            games=25,
            wins=15,
            losses=5,
            era=3.20,
            whip=1.05,
        )
        payload = stats.to_repository_payload()
        assert payload["player_name"] == "Kelly"
        assert payload["team_code"] == "LG"
        assert payload["games"] == 25
        assert payload["wins"] == 15
        assert payload["losses"] == 5
        assert payload["era"] == 3.20
        assert payload["whip"] == 1.05

    def test_extra_stats_in_payload(self) -> None:
        stats = PitcherStats(player_id=3, season=2026, league="REGULAR")
        stats.extra_stats["metrics"] = {"complete_games": 3}
        stats.extra_stats["rankings"] = {"basic1": 1}
        payload = stats.to_repository_payload()
        assert payload["extra_stats"]["metrics"]["complete_games"] == 3
        assert payload["extra_stats"]["rankings"]["basic1"] == 1


class TestUpdatePitcherBasic2Stats:
    def test_updates_metrics(self) -> None:
        stats = PitcherStats(player_id=1, season=2026, league="REGULAR")
        header_index = {"CG": 0, "QS": 1, "NP": 2}
        cell_text_fn = lambda idx: {0: "5", 1: "10", 2: "2500"}.get(idx)
        _update_pitcher_basic2_stats(stats, header_index, cell_text_fn, "basic2")
        metrics = stats.extra_stats.get("metrics", {})
        assert metrics["complete_games"] == 5
        assert metrics["quality_starts"] == 10
        assert metrics["np"] == 2500

    def test_updates_single_fields(self) -> None:
        stats = PitcherStats(player_id=1, season=2026, league="REGULAR")
        header_index = {"IBB": 0, "WP": 1, "BK": 2}
        cell_text_fn = lambda idx: {0: "3", 1: "2", 2: "1"}.get(idx)
        _update_pitcher_basic2_stats(stats, header_index, cell_text_fn, "basic2")
        assert stats.intentional_walks == 3
        assert stats.wild_pitches == 2
        assert stats.balks == 1

    def test_updates_rankings(self) -> None:
        stats = PitcherStats(player_id=1, season=2026, league="REGULAR")
        header_index = {"순위": 0}
        cell_text_fn = lambda idx: "3"
        _update_pitcher_basic2_stats(stats, header_index, cell_text_fn, "basic2")
        assert stats.extra_stats["rankings"]["basic2"] == 3

    def test_unknown_headers_ignored(self) -> None:
        stats = PitcherStats(player_id=1, season=2026, league="REGULAR")
        header_index = {"UNKNOWN": 0}
        cell_text_fn = lambda idx: "42"
        _update_pitcher_basic2_stats(stats, header_index, cell_text_fn, "basic2")
        # Should not crash, no fields updated
        assert stats.intentional_walks is None
        assert stats.extra_stats.get("rankings", {}) == {}


class TestBuildPitchingCrawlSummary:
    def test_empty_list(self) -> None:
        with patch(
            "src.crawlers.player_pitching_all_series_crawler.filter_valid_season_stat_payloads",
            return_value=([], {}),
        ):
            summary, valid = build_pitching_crawl_summary([])
        assert summary["processed_rows"] == 0
        assert len(valid) == 0

    def test_with_valid_stats(self) -> None:
        s1 = PitcherStats(player_id=1, season=2026, league="REGULAR")
        s2 = PitcherStats(player_id=2, season=2026, league="REGULAR")
        valid_payloads = [
            s1.to_repository_payload(),
            s2.to_repository_payload(),
        ]
        with patch(
            "src.crawlers.player_pitching_all_series_crawler.filter_valid_season_stat_payloads",
            return_value=(valid_payloads, {}),
        ):
            summary, valid = build_pitching_crawl_summary([s1, s2])
        assert summary["processed_rows"] == 2
        assert summary["valid_rows"] == 2
        assert len(valid) == 2
