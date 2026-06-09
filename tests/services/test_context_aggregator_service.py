from datetime import date
from unittest.mock import MagicMock

import pytest

from src.services.context_aggregator import ContextAggregator


class StubRow:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


class TestPitchingHelpers:
    def test_pitching_outs_from_value_none(self):
        assert ContextAggregator._pitching_outs_from_value(None) is None

    def test_pitching_outs_from_value_integer(self):
        assert ContextAggregator._pitching_outs_from_value(5) == 15

    def test_pitching_outs_from_value_one_out(self):
        assert ContextAggregator._pitching_outs_from_value(5.1) == 16

    def test_pitching_outs_from_value_two_outs(self):
        assert ContextAggregator._pitching_outs_from_value(5.2) == 17

    def test_pitching_outs_from_value_zero(self):
        assert ContextAggregator._pitching_outs_from_value(0) == 0

    def test_pitching_outs_uses_innings_outs_when_present(self):
        row = StubRow(innings_outs=12, innings_pitched=5.0)
        assert ContextAggregator._pitching_outs(row) == 12

    def test_pitching_outs_falls_back_to_value(self):
        row = StubRow(innings_outs=None, innings_pitched=5.0)
        assert ContextAggregator._pitching_outs(row) == 15

    def test_innings_display_from_outs(self):
        assert ContextAggregator._innings_display_from_outs(15) == "5.0"
        assert ContextAggregator._innings_display_from_outs(16) == "5.1"

    def test_pitching_game_line(self):
        row = StubRow(
            innings_pitched=5.0, innings_outs=15, batters_faced=20, pitches=80,
            hits_allowed=4, runs_allowed=2, earned_runs=2, home_runs_allowed=1,
            walks_allowed=2, strikeouts=5, decision="W", wins=1, losses=0,
            saves=0, holds=0, era=3.60, whip=1.20,
        )
        line = ContextAggregator._pitching_game_line(row)
        assert line["innings_outs"] == 15
        assert line["innings_pitched"] == "5.0"
        assert line["batters_faced"] == 20
        assert line["era"] == 3.60

    def test_pitching_season_line_none(self):
        assert ContextAggregator._pitching_season_line(None) is None

    def test_pitching_season_line_with_data(self):
        row = StubRow(season=2024, league="REGULAR", team_code="LG",
                      games=10, games_started=10, wins=5, losses=2,
                      saves=0, holds=0, innings_pitched="60.0", innings_outs=180,
                      quality_starts=5, era=3.00, whip=1.10, fip=3.20, kbb=3.5)
        line = ContextAggregator._pitching_season_line(row)
        assert line["season"] == 2024
        assert line["era"] == 3.00
        assert line["wins"] == 5


class TestEmptyBullpenPayload:
    def test_empty_bullpen_structure(self):
        payload = ContextAggregator._empty_bullpen_payload()
        assert payload["pitchers"] == []
        assert payload["totals"]["innings_pitched"] == "0.0"
        assert payload["totals"]["pitchers"] == 0


class TestGetTeamL10Summary:
    def test_no_games(self):
        session = MagicMock()
        session.query.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = []
        agg = ContextAggregator(session)
        result = agg.get_team_l10_summary("LG", date(2024, 6, 1))
        assert result["wins"] == 0
        assert result["losses"] == 0
        assert result["streak"] == "-"

    def test_mixed_results(self):
        mock_games = [
            StubRow(home_team="LG", away_team="SS", home_score=5, away_score=3,
                    game_status="completed", game_date=date(2024, 5, 30)),
            StubRow(home_team="SS", away_team="LG", home_score=2, away_score=1,
                    game_status="completed", game_date=date(2024, 5, 29)),
            StubRow(home_team="LG", away_team="KT", home_score=4, away_score=4,
                    game_status="draw", game_date=date(2024, 5, 28)),
        ]
        session = MagicMock()
        session.query.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = mock_games
        agg = ContextAggregator(session)
        result = agg.get_team_l10_summary("LG", date(2024, 6, 1))
        assert result["wins"] == 1
        assert result["losses"] == 1
        assert result["draws"] == 1
        assert result["l10_text"] == "1승 1패 1무"

    def test_streak_calculation(self):
        mock_games = [
            StubRow(home_team="LG", away_team="SS", home_score=5, away_score=3,
                    game_status="completed", game_date=date(2024, 5, 30)),
            StubRow(home_team="KT", away_team="LG", home_score=2, away_score=1,
                    game_status="completed", game_date=date(2024, 5, 29)),
        ]
        session = MagicMock()
        session.query.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = mock_games
        agg = ContextAggregator(session)
        # Most recent game is a win, so streak should be 1 win
        result = agg.get_team_l10_summary("LG", date(2024, 6, 1))
        assert "1" in result["streak"]


class TestGetHeadToHeadSummary:
    def test_no_matchups(self):
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = []
        agg = ContextAggregator(session)
        result = agg.get_head_to_head_summary("LG", "KT", 2024, date(2024, 6, 1))
        assert result["a_wins"] == 0
        assert result["b_wins"] == 0

    def test_a_wins_more(self):
        mock_games = [
            StubRow(home_team="LG", away_team="KT", home_score=5, away_score=3,
                    game_status="completed", game_date=date(2024, 5, 1)),
        ]
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = mock_games
        agg = ContextAggregator(session)
        result = agg.get_head_to_head_summary("LG", "KT", 2024, date(2024, 6, 1))
        assert result["a_wins"] == 1
        assert result["b_wins"] == 0

    def test_b_wins_more(self):
        mock_games = [
            StubRow(home_team="KT", away_team="LG", home_score=4, away_score=1,
                    game_status="completed", game_date=date(2024, 5, 1)),
        ]
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = mock_games
        agg = ContextAggregator(session)
        result = agg.get_head_to_head_summary("LG", "KT", 2024, date(2024, 6, 1))
        assert result["b_wins"] == 1
        assert result["a_wins"] == 0


class TestGetCrucialMoments:
    def test_no_events(self):
        session = MagicMock()
        session.query.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = []
        agg = ContextAggregator(session)
        result = agg.get_crucial_moments("20240501LGSS0")
        assert result == []

    def test_filters_noise_events(self):
        event = StubRow(
            description="파울", wpa=0.5, event_type="hit", inning=9,
            inning_half="bottom", away_score=1, home_score=2,
            batter_name="Kim", pitcher_name="Park",
        )
        session = MagicMock()
        session.query.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = [event]
        with pytest.MonkeyPatch().context() as mp:
            mp.setattr("src.services.context_aggregator.is_relay_noise_text", lambda x: True)
            agg = ContextAggregator(session)
            result = agg.get_crucial_moments("20240501LGSS0")
            assert result == []

    def test_returns_limited_moments(self):
        events = [
            StubRow(description="홈런", wpa=0.5, event_type="home_run", inning=9,
                    inning_half="bottom", away_score=2, home_score=3,
                    batter_name="A", pitcher_name="B"),
            StubRow(description="안타", wpa=0.3, event_type="hit", inning=7,
                    inning_half="top", away_score=1, home_score=2,
                    batter_name="C", pitcher_name="D"),
        ]
        session = MagicMock()
        session.query.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = events
        agg = ContextAggregator(session)
        result = agg.get_crucial_moments("20240501LGSS0", limit=1)
        assert len(result) == 1
        assert result[0]["description"] == "홈런"


class TestTeamRecentMetrics:
    def test_no_game_ids(self):
        session = MagicMock()
        session.query.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = []
        agg = ContextAggregator(session)
        result = agg.get_team_recent_metrics("LG", date(2024, 6, 1))
        assert result == {}
