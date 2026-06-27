from __future__ import annotations

from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest

from src.services.context_aggregator import ContextAggregator


class StubRow:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


@pytest.fixture
def agg():
    session = MagicMock()
    return ContextAggregator(session)


class TestPitchingPayloadRow:
    def test_starter_role(self, agg):
        row = StubRow(
            team_side="away",
            team_code="LG",
            player_id=123,
            player_name="Kim",
            appearance_seq=1,
            is_starting=True,
            innings_outs=15,
            innings_pitched=5.0,
            batters_faced=20,
            pitches=80,
            hits_allowed=4,
            runs_allowed=2,
            earned_runs=2,
            home_runs_allowed=1,
            walks_allowed=2,
            strikeouts=5,
            decision="W",
            wins=1,
            losses=0,
            saves=0,
            holds=0,
            era=3.60,
            whip=1.20,
        )
        result = agg._pitching_payload_row(row, None)
        assert result["role"] == "starter"
        assert result["is_starting"] is True

    def test_bullpen_role(self, agg):
        row = StubRow(
            team_side="home",
            team_code="SS",
            player_id=456,
            player_name="Park",
            appearance_seq=2,
            is_starting=False,
            innings_outs=6,
            innings_pitched=2.0,
            batters_faced=8,
            pitches=30,
            hits_allowed=1,
            runs_allowed=0,
            earned_runs=0,
            home_runs_allowed=0,
            walks_allowed=1,
            strikeouts=3,
            decision=None,
            wins=0,
            losses=0,
            saves=1,
            holds=0,
            era=1.50,
            whip=1.00,
        )
        result = agg._pitching_payload_row(row, None)
        assert result["role"] == "bullpen"
        assert result["is_starting"] is False

    def test_with_season_row(self, agg):
        season_row = StubRow(
            season=2024,
            league="REGULAR",
            team_code="LG",
            games=10,
            games_started=10,
            wins=5,
            losses=2,
            saves=0,
            holds=0,
            innings_pitched="60.0",
            innings_outs=180,
            quality_starts=5,
            era=3.00,
            whip=1.10,
            fip=3.20,
            kbb=3.5,
        )
        row = StubRow(
            team_side="away",
            team_code="LG",
            player_id=123,
            player_name="Kim",
            appearance_seq=1,
            is_starting=True,
            innings_outs=15,
            innings_pitched=5.0,
            batters_faced=20,
            pitches=80,
            hits_allowed=4,
            runs_allowed=2,
            earned_runs=2,
            home_runs_allowed=1,
            walks_allowed=2,
            strikeouts=5,
            decision="W",
            wins=1,
            losses=0,
            saves=0,
            holds=0,
            era=3.60,
            whip=1.20,
        )
        result = agg._pitching_payload_row(row, season_row)
        assert result["season_stats_found"] is True
        assert result["season_line"]["era"] == 3.00


class TestGetCompletedGamePitchingBreakdown:
    def test_no_game(self, agg):
        agg.session.query.return_value.filter.return_value.first.return_value = None
        result = agg.get_completed_game_pitching_breakdown("20240101LGSS0")
        assert result["game_id"] == "20240101LGSS0"
        assert result["starters"] == {"away": None, "home": None}

    def test_with_game_date(self, agg):
        mock_game = StubRow(game_id="20240315LGSS0", game_date=date(2024, 3, 15))
        agg.session.query.return_value.filter.return_value.first.return_value = mock_game
        agg.session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
        result = agg.get_completed_game_pitching_breakdown("20240315LGSS0")
        assert result["season_year"] == 2024

    def test_with_pitchers(self, agg):
        mock_game = StubRow(game_id="20240315LGSS0", game_date=date(2024, 3, 15))
        agg.session.query.return_value.filter.return_value.first.return_value = mock_game

        starter_row = StubRow(
            game_id="20240315LGSS0",
            team_side="away",
            team_code="LG",
            player_id=123,
            player_name="Kim",
            appearance_seq=1,
            is_starting=True,
            id=1,
            innings_outs=21,
            innings_pitched=7.0,
            batters_faced=25,
            pitches=90,
            hits_allowed=3,
            runs_allowed=1,
            earned_runs=1,
            home_runs_allowed=0,
            walks_allowed=2,
            strikeouts=8,
            decision="W",
            wins=1,
            losses=0,
            saves=0,
            holds=0,
            era=1.29,
            whip=1.14,
        )
        agg.session.query.return_value.filter.return_value.order_by.return_value.all.return_value = [starter_row]
        agg.session.query.return_value.filter.return_value.all.return_value = []
        result = agg.get_completed_game_pitching_breakdown("20240315LGSS0")
        assert result["starters"]["away"] is not None
        assert result["starters"]["away"]["player_name"] == "Kim"


class TestSeasonPitchingRows:
    def test_empty_player_ids(self, agg):
        result = agg._season_pitching_rows([], 2024)
        assert result == {}

    def test_no_season_year(self, agg):
        rows = [StubRow(player_id=123)]
        result = agg._season_pitching_rows(rows, None)
        assert result == {}

    def test_returns_season_rows(self, agg):
        pitch_row = StubRow(player_id=123)
        agg.session.query.return_value.filter.return_value.all.return_value = [pitch_row]
        result = agg._season_pitching_rows([StubRow(player_id=123)], 2024)
        assert 123 in result


class TestAssignPitchingRole:
    def test_starter_assignment(self, agg):
        row = StubRow(is_starting=True, team_side="away")
        payload = {"role": "starter"}
        starters = {"away": None, "home": None}
        bullpen = {"away": MagicMock(), "home": MagicMock()}
        result = agg._assign_pitching_role(row, payload, "away", starters, bullpen)
        assert result is True
        assert starters["away"] == payload

    def test_starter_already_filled(self, agg):
        row = StubRow(is_starting=True, team_side="away")
        payload = {"role": "starter"}
        existing = {"role": "starter"}
        starters = {"away": existing, "home": None}
        bullpen = {"away": MagicMock(), "home": MagicMock()}
        result = agg._assign_pitching_role(row, payload, "away", starters, bullpen)
        assert result is True
        assert starters["away"] == existing

    def test_bullpen_assignment(self, agg):
        row = StubRow(is_starting=False, team_side="away")
        payload = {"role": "bullpen", "game_line": {"innings_outs": 6}}
        starters = {"away": None, "home": None}
        bullpen = {"away": MagicMock(), "home": MagicMock()}
        result = agg._assign_pitching_role(row, payload, "away", starters, bullpen)
        assert result is False

    def test_unknown_side_skips_bullpen(self, agg):
        row = StubRow(is_starting=False, team_side="unknown")
        payload = {"role": "bullpen"}
        starters = {"away": None, "home": None}
        bullpen = {"away": MagicMock(), "home": MagicMock()}
        result = agg._assign_pitching_role(row, payload, "unknown", starters, bullpen)
        assert result is True


class TestAppendBullpenPitcher:
    def test_appends_and_updates_totals(self, agg):
        side_payload = {
            "pitchers": [],
            "totals": {
                "pitchers": 0,
                "innings_outs": 0,
                "innings_pitched": "0.0",
                "pitches": 0,
                "hits_allowed": 0,
                "runs_allowed": 0,
                "earned_runs": 0,
                "walks_allowed": 0,
                "strikeouts": 0,
            },
        }
        payload_row = {
            "game_line": {
                "innings_outs": 6,
                "pitches": 30,
                "hits_allowed": 1,
                "runs_allowed": 0,
                "earned_runs": 0,
                "walks_allowed": 1,
                "strikeouts": 3,
            }
        }
        agg._append_bullpen_pitcher(side_payload, payload_row)
        assert len(side_payload["pitchers"]) == 1
        assert side_payload["totals"]["pitchers"] == 1
        assert side_payload["totals"]["innings_outs"] == 6
        assert side_payload["totals"]["strikeouts"] == 3


class TestDiagnoseCompletedGameCoachPitching:
    def test_no_summaries(self, agg):
        agg.session.query.return_value.filter.return_value.all.return_value = []
        agg.get_completed_game_pitching_breakdown = MagicMock(
            return_value={
                "raw_counts": {
                    "game_pitching_rows": 0,
                    "starter_rows": 0,
                    "bullpen_rows": 0,
                    "player_id_missing_rows": 0,
                    "season_pitching_matches": 0,
                },
                "starters": {},
                "bullpen": {},
                "unmatched_season_stats": [],
            }
        )
        result = agg.diagnose_completed_game_coach_pitching("20240101LGSS0")
        assert result["drop_stage"] == "raw_game_pitching_stats_missing"

    def test_with_summaries(self, agg):
        mock_summary = StubRow(
            detail_text='{"pitching_breakdown": {"starters": {"away": {"name": "Kim"}}, "bullpen": {"away": {"pitchers": [{"name": "A"}]}}}}'
        )
        agg.session.query.return_value.filter.return_value.all.return_value = [mock_summary]
        breakdown = {
            "raw_counts": {
                "game_pitching_rows": 5,
                "starter_rows": 1,
                "bullpen_rows": 4,
                "player_id_missing_rows": 0,
                "season_pitching_matches": 5,
            },
            "starters": {"away": {"player_name": "Kim"}, "home": {"player_name": "Park"}},
            "bullpen": {"away": {"pitchers": [{"player_name": "A"}]}, "home": {"pitchers": [{"player_name": "B"}]}},
            "unmatched_season_stats": [],
        }
        agg.get_completed_game_pitching_breakdown = MagicMock(return_value=breakdown)
        result = agg.diagnose_completed_game_coach_pitching("20240101LGSS0")
        assert result["drop_stage"] == "ok"


class TestDiagnoseFinalPitchingPayload:
    def test_no_detail_text(self, agg):
        summary = StubRow(detail_text=None)
        result = agg._diagnose_final_pitching_payload([summary])
        assert result["found"] is False

    def test_invalid_json(self, agg):
        summary = StubRow(detail_text="invalid")
        result = agg._diagnose_final_pitching_payload([summary])
        assert result["found"] is True
        assert result["has_pitching"] is False

    def test_valid_json_no_pitching(self, agg):
        summary = StubRow(detail_text='{"other": "data"}')
        result = agg._diagnose_final_pitching_payload([summary])
        assert result["found"] is True
        assert result["has_pitching"] is False

    def test_with_pitching(self, agg):
        summary = StubRow(detail_text='{"pitching_breakdown": {"starters": {"away": {}}, "bullpen": {}}}')
        result = agg._diagnose_final_pitching_payload([summary])
        assert result["has_pitching"] is True


class TestSummaryPayload:
    def test_none_detail(self, agg):
        summary = StubRow(detail_text=None)
        assert agg._summary_payload(summary) is None

    def test_invalid_json(self, agg):
        summary = StubRow(detail_text="invalid")
        assert agg._summary_payload(summary) == {}

    def test_valid_dict(self, agg):
        summary = StubRow(detail_text='{"key": "value"}')
        assert agg._summary_payload(summary) == {"key": "value"}

    def test_non_dict_json(self, agg):
        summary = StubRow(detail_text='["list"]')
        assert agg._summary_payload(summary) is None


class TestCountFinalPitchingRows:
    def test_counts_starters_and_bullpen(self, agg):
        final_pitching = {
            "starters": {"away": {"name": "Kim"}, "home": {"name": "Park"}},
            "bullpen": {"away": {"pitchers": [{"name": "A"}]}, "home": {"pitchers": []}},
        }
        starter_rows, bullpen_rows = agg._count_final_pitching_rows(final_pitching)
        assert starter_rows == 2
        assert bullpen_rows == 1

    def test_empty_pitching(self, agg):
        starter_rows, bullpen_rows = agg._count_final_pitching_rows({})
        assert starter_rows == 0
        assert bullpen_rows == 0


class TestCoachPitchingDropStage:
    def test_no_pitching_rows(self, agg):
        raw = {"game_pitching_rows": 0, "starter_rows": 0, "bullpen_rows": 0}
        result = agg._coach_pitching_drop_stage(raw, 0, 0, {"found": False})
        assert result == "raw_game_pitching_stats_missing"

    def test_no_starters(self, agg):
        raw = {"game_pitching_rows": 5, "starter_rows": 0, "bullpen_rows": 5}
        result = agg._coach_pitching_drop_stage(raw, 0, 0, {"found": False})
        assert result == "raw_starter_flags_missing"

    def test_repository_missing(self, agg):
        raw = {"game_pitching_rows": 5, "starter_rows": 1, "bullpen_rows": 5}
        result = agg._coach_pitching_drop_stage(raw, 0, 0, {"found": False})
        assert result == "repository_pitching_rows_missing"

    def test_final_payload_missing(self, agg):
        raw = {"game_pitching_rows": 5, "starter_rows": 1, "bullpen_rows": 0}
        final_payload = {"found": False, "has_pitching": False, "starter_rows": 0, "bullpen_rows": 0}
        result = agg._coach_pitching_drop_stage(raw, 1, 0, final_payload)
        assert result == "final_review_payload_missing"


class TestGetTeamL10Summary:
    def test_null_scores_skipped(self, agg):
        mock_games = [
            StubRow(
                home_team="LG",
                away_team="SS",
                home_score=None,
                away_score=3,
                game_status="completed",
                game_date=date(2024, 5, 30),
            ),
        ]
        agg.session.query.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = (
            mock_games
        )
        result = agg.get_team_l10_summary("LG", date(2024, 6, 1))
        assert result["wins"] == 0
        assert result["losses"] == 0


class TestGetHeadToHeadSummary:
    def test_draw(self, agg):
        mock_games = [
            StubRow(
                home_team="LG",
                away_team="KT",
                home_score=3,
                away_score=3,
                game_status="completed",
                game_date=date(2024, 5, 1),
            ),
        ]
        agg.session.query.return_value.filter.return_value.all.return_value = mock_games
        result = agg.get_head_to_head_summary("LG", "KT", 2024, date(2024, 6, 1))
        assert result["draws"] == 1
        assert result["a_wins"] == 0

    def test_away_team_a_wins(self, agg):
        mock_games = [
            StubRow(
                home_team="KT",
                away_team="LG",
                home_score=1,
                away_score=4,
                game_status="completed",
                game_date=date(2024, 5, 1),
            ),
        ]
        agg.session.query.return_value.filter.return_value.all.return_value = mock_games
        result = agg.get_head_to_head_summary("LG", "KT", 2024, date(2024, 6, 1))
        assert result["a_wins"] == 1


class TestGetCrucialMoments:
    def test_filters_unknown_type(self, agg):
        event = StubRow(
            description="Unknown event",
            wpa=0.5,
            event_type="hit",
            inning=9,
            inning_half="bottom",
            away_score=1,
            home_score=2,
            batter_name="Kim",
            pitcher_name="Park",
        )
        agg.session.query.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = [
            event
        ]
        with patch("src.services.context_aggregator.is_relay_noise_text", return_value=True):
            result = agg.get_crucial_moments("20240501LGSS0")
            assert result == []

    def test_filters_other_type(self, agg):
        event = StubRow(
            description="Substitution",
            wpa=0.5,
            event_type="hit",
            inning=5,
            inning_half="top",
            away_score=1,
            home_score=1,
            batter_name="Kim",
            pitcher_name="Park",
        )
        agg.session.query.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = [
            event
        ]
        with patch("src.services.context_aggregator.is_relay_noise_text", return_value=True):
            result = agg.get_crucial_moments("20240501LGSS0")
            assert result == []


class TestGetTeamRecentMetrics:
    def test_empty_game_ids_returns_empty(self, agg):
        agg.session.query.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = []
        result = agg.get_team_recent_metrics("LG", date(2024, 6, 1))
        assert result == {}


class TestGetPostseasonSeriesSummary:
    def test_no_sample_game(self, agg):
        agg.session.query.return_value.filter.return_value.first.return_value = None
        result = agg.get_postseason_series_summary("LG", "SS", 2024, date(2024, 11, 1))
        assert result is None

    def test_no_season_id(self, agg):
        mock_game = StubRow(season_id=None)
        agg.session.query.return_value.filter.return_value.first.return_value = mock_game
        result = agg.get_postseason_series_summary("LG", "SS", 2024, date(2024, 11, 1))
        assert result is None

    def test_no_games_in_series(self, agg):
        mock_game = StubRow(season_id=20240)
        agg.session.query.return_value.filter.return_value.first.return_value = mock_game
        agg.session.query.return_value.filter.return_value.all.return_value = []
        result = agg.get_postseason_series_summary("LG", "SS", 2024, date(2024, 11, 1))
        assert result is None

    def test_with_series_games(self, agg):
        mock_game = StubRow(season_id=20240)
        agg.session.query.return_value.filter.return_value.first.return_value = mock_game

        series_games = [
            StubRow(
                home_team="LG",
                away_team="SS",
                home_score=5,
                away_score=3,
                game_status="completed",
                game_date=date(2024, 11, 1),
            ),
            StubRow(
                home_team="SS",
                away_team="LG",
                home_score=4,
                away_score=2,
                game_status="completed",
                game_date=date(2024, 11, 2),
            ),
        ]
        agg.session.query.return_value.filter.return_value.all.return_value = series_games
        result = agg.get_postseason_series_summary("LG", "SS", 2024, date(2024, 11, 3))
        assert result["a_wins"] == 1
        assert result["b_wins"] == 1


class TestGetPitcherSeasonStats:
    def test_no_player_id(self, agg):
        result = agg.get_pitcher_season_stats(0, 2024)
        assert result is None

    def test_no_stats(self, agg):
        agg.session.query.return_value.filter.return_value.first.return_value = None
        result = agg.get_pitcher_season_stats(123, 2024)
        assert result is None

    def test_with_stats(self, agg):
        stats = StubRow(
            player_id=123,
            season=2024,
            era=3.50,
            wins=10,
            losses=5,
            saves=0,
            holds=2,
            games=25,
            innings_pitched="150.0",
        )
        agg.session.query.return_value.filter.return_value.first.return_value = stats
        result = agg.get_pitcher_season_stats(123, 2024)
        assert result["era"] == 3.50
        assert result["summary_text"] == "10승 5패 3.5ERA"


class TestGetRecentPlayerMovements:
    def test_string_date(self, agg):
        movement = StubRow(
            movement_date=date(2024, 5, 30),
            section="부상",
            player_name="Kim",
            remarks="왼쪽 통증",
            canonical_team_id="KIA",
            team_code="KIA",
        )
        agg.session.query.return_value.filter.return_value.order_by.return_value.all.return_value = [movement]
        result = agg.get_recent_player_movements("HT", "2024-06-01")
        assert len(result) == 1
        assert result[0]["player"] == "Kim"

    def test_datetime_date(self, agg):
        movement = StubRow(
            movement_date=date(2024, 5, 30),
            section="트레이드",
            player_name="Park",
            remarks="트레이드",
            canonical_team_id="LT",
            team_code="LT",
        )
        agg.session.query.return_value.filter.return_value.order_by.return_value.all.return_value = [movement]
        result = agg.get_recent_player_movements("LT", datetime(2024, 6, 1))
        assert len(result) == 1

    def test_no_movements(self, agg):
        agg.session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
        result = agg.get_recent_player_movements("LG", date(2024, 6, 1))
        assert result == []


class TestGetDailyRosterChanges:
    def test_no_curr_roster(self, agg):
        agg.session.query.return_value.filter.return_value.all.side_effect = [[], []]
        result = agg.get_daily_roster_changes("LG", date(2024, 6, 1))
        assert result == {"added": [], "removed": []}

    def test_no_prev_roster(self, agg):
        curr = [StubRow(player_id=1, player_name="Kim")]
        agg.session.query.return_value.filter.return_value.all.side_effect = [curr, []]
        result = agg.get_daily_roster_changes("LG", date(2024, 6, 1))
        assert result == {"added": [], "removed": []}

    def test_with_changes(self, agg):
        curr = [StubRow(player_id=1, player_name="Kim"), StubRow(player_id=2, player_name="Park")]
        prev = [StubRow(player_id=1, player_name="Kim"), StubRow(player_id=3, player_name="Lee")]
        agg.session.query.return_value.filter.return_value.all.side_effect = [curr, prev]
        result = agg.get_daily_roster_changes("LG", date(2024, 6, 1))
        assert "Park" in result["added"]
        assert "Lee" in result["removed"]


class TestGetTeamErrorGames:
    def test_no_errors(self, agg):
        agg.session.query.return_value.join.return_value.join.return_value.filter.return_value.order_by.return_value.all.return_value = []
        result = agg.get_team_error_games("LG", 2024)
        assert result == []

    def test_with_errors(self, agg):
        row = MagicMock()
        row.game_id = "20240501LGSS0"
        row.game_date = date(2024, 5, 1)
        row.home_team = "LG"
        row.away_team = "SS"
        row.home_score = 5
        row.away_score = 3
        row.player_name = "Kim"
        row.detail_text = "실책"
        agg.session.query.return_value.join.return_value.join.return_value.filter.return_value.order_by.return_value.all.return_value = [
            row
        ]
        result = agg.get_team_error_games("LG", 2024)
        assert len(result) == 1
        assert result[0]["errors"][0]["player"] == "Kim"

    def test_with_target_date_string(self, agg):
        agg.session.query.return_value.join.return_value.join.return_value.filter.return_value.order_by.return_value.all.return_value = []
        result = agg.get_team_error_games("LG", 2024, "2024-06-01")
        assert result == []


class TestGetToughestOpponents:
    def test_no_games(self, agg):
        agg.session.query.return_value.filter.return_value.all.return_value = []
        result = agg.get_toughest_opponents("LG", 2024)
        assert result == []

    def test_with_opponents(self, agg):
        games = [
            StubRow(
                home_team="LG",
                away_team="SS",
                home_score=5,
                away_score=3,
                game_status="completed",
                game_date=date(2024, 5, 1),
            ),
            StubRow(
                home_team="LG",
                away_team="SS",
                home_score=2,
                away_score=4,
                game_status="completed",
                game_date=date(2024, 5, 2),
            ),
        ]
        agg.session.query.return_value.filter.return_value.all.return_value = games
        result = agg.get_toughest_opponents("LG", 2024)
        assert result[0]["opponent"] == "SS"
        assert result[0]["win_rate"] == 0.5


class TestGetPositionAvgComparison:
    def test_no_player_stat(self, agg):
        agg.session.query.return_value.filter.return_value.first.return_value = None
        agg.session.query.return_value.join.return_value.filter.return_value.first.return_value = None
        result = agg.get_position_avg_comparison(123, "투수", 2024)
        assert result == {}
