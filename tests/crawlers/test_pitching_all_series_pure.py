from __future__ import annotations

from unittest.mock import patch

from src.crawlers.player_pitching_all_series_crawler import (
    PitcherStats,
    _map_pitcher_basic1_stats,
    _update_pitcher_basic2_stats,
    build_pitching_crawl_summary,
    extract_player_id,
    normalize_header,
)


class TestNormalizeHeader:
    def test_basic(self):
        assert normalize_header("ERA") == "ERA"

    def test_none(self):
        assert normalize_header(None) == ""

    def test_whitespace(self):
        assert normalize_header("  ERA  ") == "ERA"

    def test_newline(self):
        assert normalize_header("ERA\n2.50") == "ERA"

    def test_multiple_words(self):
        assert normalize_header("Home Runs") == "Home"

    def test_nbsp(self):
        assert normalize_header("ERA\xa0") == "ERA"

    def test_korean(self):
        assert normalize_header("평균자책") == "평균자책"

    def test_empty(self):
        assert normalize_header("") == ""


class TestExtractPlayerId:
    def test_basic(self):
        assert extract_player_id("/Player.aspx?playerId=12345") == 12345

    def test_with_extra_params(self):
        assert extract_player_id("/Player.aspx?playerId=67890&season=2023") == 67890

    def test_none(self):
        assert extract_player_id(None) is None

    def test_empty(self):
        assert extract_player_id("") is None

    def test_no_player_id(self):
        assert extract_player_id("/Player.aspx?other=value") is None

    def test_large_id(self):
        assert extract_player_id("?playerId=999999") == 999999


class TestUpdatePitcherBasic2Stats:
    def test_complete_games(self):
        stats = PitcherStats(player_id=1, season=2023, league="REGULAR")
        header_index = {"CG": 0}
        cell_text_fn = lambda idx: "5"
        _update_pitcher_basic2_stats(stats, header_index, cell_text_fn, "sort1")
        assert stats.extra_stats["metrics"]["complete_games"] == 5

    def test_shutouts(self):
        stats = PitcherStats(player_id=1, season=2023, league="REGULAR")
        header_index = {"SHO": 0}
        cell_text_fn = lambda idx: "2"
        _update_pitcher_basic2_stats(stats, header_index, cell_text_fn, "sort1")
        assert stats.extra_stats["metrics"]["shutouts"] == 2

    def test_avg_against(self):
        stats = PitcherStats(player_id=1, season=2023, league="REGULAR")
        header_index = {"AVG": 0}
        cell_text_fn = lambda idx: "0.250"
        _update_pitcher_basic2_stats(stats, header_index, cell_text_fn, "sort1")
        assert stats.extra_stats["metrics"]["avg_against"] == 0.25

    def test_wild_pitches(self):
        stats = PitcherStats(player_id=1, season=2023, league="REGULAR")
        header_index = {"WP": 0}
        cell_text_fn = lambda idx: "3"
        _update_pitcher_basic2_stats(stats, header_index, cell_text_fn, "sort1")
        assert stats.wild_pitches == 3

    def test_intentional_walks(self):
        stats = PitcherStats(player_id=1, season=2023, league="REGULAR")
        header_index = {"IBB": 0}
        cell_text_fn = lambda idx: "2"
        _update_pitcher_basic2_stats(stats, header_index, cell_text_fn, "sort1")
        assert stats.intentional_walks == 2

    def test_balks(self):
        stats = PitcherStats(player_id=1, season=2023, league="REGULAR")
        header_index = {"BK": 0}
        cell_text_fn = lambda idx: "1"
        _update_pitcher_basic2_stats(stats, header_index, cell_text_fn, "sort1")
        assert stats.balks == 1

    def test_ranking(self):
        stats = PitcherStats(player_id=1, season=2023, league="REGULAR")
        header_index = {"순위": 0}
        cell_text_fn = lambda idx: "5"
        _update_pitcher_basic2_stats(stats, header_index, cell_text_fn, "era")
        assert stats.extra_stats["rankings"]["era"] == 5

    def test_none_value_skipped(self):
        stats = PitcherStats(player_id=1, season=2023, league="REGULAR")
        header_index = {"CG": 0}
        cell_text_fn = lambda idx: None
        _update_pitcher_basic2_stats(stats, header_index, cell_text_fn, "sort1")
        assert "complete_games" not in stats.extra_stats.get("metrics", {})

    def test_multiple_metrics(self):
        stats = PitcherStats(player_id=1, season=2023, league="REGULAR")
        header_index = {"CG": 0, "SHO": 1, "TBF": 2}
        cell_text_fn = lambda idx: ["5", "2", "100"][idx]
        _update_pitcher_basic2_stats(stats, header_index, cell_text_fn, "sort1")
        assert stats.extra_stats["metrics"]["complete_games"] == 5
        assert stats.extra_stats["metrics"]["shutouts"] == 2
        assert stats.extra_stats["metrics"]["tbf"] == 100


class TestMapPitcherBasic1Stats:
    def test_basic_mapping(self):
        row = {
            "player_id": 12345,
            "player_name": "홍길동",
            "team_name": "LG",
            "raw": {"G": "25", "W": "10", "L": "5", "ERA": "3.50"},
        }
        pitchers = {}
        result = _map_pitcher_basic1_stats(row, 2023, "REGULAR", pitchers, None)
        assert result is True
        assert 12345 in pitchers
        stats = pitchers[12345]
        assert stats.player_name == "홍길동"
        assert stats.games == 25
        assert stats.wins == 10
        assert stats.losses == 5
        assert stats.era == 3.5

    def test_max_players_limit(self):
        row = {
            "player_id": 99999,
            "player_name": "New Player",
            "team_name": "LG",
            "raw": {"G": "10"},
        }
        pitchers = {1: PitcherStats(player_id=1, season=2023, league="REGULAR")}
        result = _map_pitcher_basic1_stats(row, 2023, "REGULAR", pitchers, max_players=1)
        assert result is False
        assert 99999 not in pitchers

    def test_existing_player_updated(self):
        row = {
            "player_id": 12345,
            "player_name": "홍길동",
            "team_name": "LG",
            "raw": {"G": "30", "SO": "150"},
        }
        pitchers = {12345: PitcherStats(player_id=12345, season=2023, league="REGULAR", games=25)}
        result = _map_pitcher_basic1_stats(row, 2023, "REGULAR", pitchers, None)
        assert result is True
        assert pitchers[12345].games == 30
        assert pitchers[12345].strikeouts == 150

    @patch("src.crawlers.player_pitching_all_series_crawler.resolve_team_code")
    def test_team_code_resolution(self, mock_resolve):
        mock_resolve.return_value = "LG"
        row = {
            "player_id": 12345,
            "player_name": "홍길동",
            "team_name": "LG Twins",
            "raw": {"G": "10"},
        }
        pitchers = {}
        _map_pitcher_basic1_stats(row, 2023, "REGULAR", pitchers, None)
        assert pitchers[12345].team_code == "LG"

    def test_innings_pitched(self):
        row = {
            "player_id": 12345,
            "player_name": "홍길동",
            "team_name": "LG",
            "raw": {"IP": "50.1"},
        }
        pitchers = {}
        _map_pitcher_basic1_stats(row, 2023, "REGULAR", pitchers, None)
        assert pitchers[12345].innings_outs is not None

    def test_missing_fields_preserved(self):
        row = {
            "player_id": 12345,
            "player_name": "홍길동",
            "team_name": "LG",
            "raw": {"G": "10"},
        }
        pitchers = {12345: PitcherStats(player_id=12345, season=2023, league="REGULAR", wins=5)}
        _map_pitcher_basic1_stats(row, 2023, "REGULAR", pitchers, None)
        assert pitchers[12345].wins == 5


class TestBuildPitchingCrawlSummary:
    def test_all_valid(self):
        stats_list = [
            PitcherStats(
                player_id=1, season=2023, league="REGULAR", player_name="A", team_code="LG", games=25, wins=10
            ),
            PitcherStats(player_id=2, season=2023, league="REGULAR", player_name="B", team_code="SS", games=20, wins=8),
        ]
        summary, valid = build_pitching_crawl_summary(stats_list)
        assert summary["processed_rows"] == 2
        assert summary["valid_rows"] == 2
        assert len(valid) == 2

    def test_empty_list(self):
        summary, valid = build_pitching_crawl_summary([])
        assert summary["processed_rows"] == 0
        assert summary["valid_rows"] == 0
        assert len(valid) == 0


class TestPitcherStats:
    def test_default_values(self):
        stats = PitcherStats(player_id=1, season=2023, league="REGULAR")
        assert stats.level == "KBO1"
        assert stats.source == "CRAWLER"
        assert stats.games is None
        assert stats.extra_stats == {"rankings": {}}

    def test_to_repository_payload(self):
        stats = PitcherStats(
            player_id=1,
            season=2023,
            league="REGULAR",
            player_name="홍길동",
            team_code="LG",
            games=25,
            wins=10,
        )
        payload = stats.to_repository_payload()
        assert payload["player_id"] == 1
        assert payload["player_name"] == "홍길동"
        assert payload["season"] == 2023
        assert payload["games"] == 25
        assert payload["wins"] == 10

    def test_to_repository_payload_with_innings_outs(self):
        stats = PitcherStats(player_id=1, season=2023, league="REGULAR", innings_outs=150)
        payload = stats.to_repository_payload()
        assert payload["innings_outs"] == 150
        assert payload["extra_stats"]["innings_outs"] == 150
