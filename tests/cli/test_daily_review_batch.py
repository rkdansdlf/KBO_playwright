from __future__ import annotations

import asyncio
import json
import logging
from unittest.mock import MagicMock, patch

import pytest

from src.cli.daily_review_batch import (
    _build_review_data,
    _trusted_relay_game_ids,
    _upsert_review_summary,
    main,
    run_review_batch,
)


class TestDailyReviewBatchCLI:
    def test_main_no_games(self):
        with (
            patch("src.cli.daily_review_batch.refresh_game_status_for_date") as mock_status,
            patch("src.cli.daily_review_batch.SessionLocal") as mock_sesh,
            patch("src.cli.daily_review_batch.write_refresh_manifest") as mock_manifest,
        ):
            mock_status.return_value = {"updated": 0}
            mock_session = MagicMock()
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
            mock_sesh.return_value.__enter__.return_value = mock_session

            result = main(["--date", "20251015"])
            assert result == 0
            mock_manifest.assert_called_once()

    def test_main_with_games(self):
        with (
            patch("src.cli.daily_review_batch.refresh_game_status_for_date") as mock_status,
            patch("src.cli.daily_review_batch.SessionLocal") as mock_sesh,
            patch("src.cli.daily_review_batch.ContextAggregator") as MockAgg,
            patch("src.cli.daily_review_batch.write_refresh_manifest"),
        ):
            mock_status.return_value = {"updated": 0}
            mock_game = MagicMock()
            mock_game.game_id = "20251015LGHH0"
            mock_game.away_team = "LG"
            mock_game.home_team = "SS"
            mock_game.game_date = MagicMock()
            mock_game.game_date.strftime.return_value = "20251015"
            mock_game.game_date.year = 2025
            mock_session = MagicMock()
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = [mock_game]
            mock_sesh.return_value.__enter__.return_value = mock_session
            mock_agg = MagicMock()
            mock_agg.get_crucial_moments.return_value = [{"event": "HR"}]
            MockAgg.return_value = mock_agg

            result = main(["--date", "20251015"])
            assert result == 0


class TestUpsertReviewSummary:
    def test_creates_new_summary(self):
        from src.models.game import GameSummary

        mock_session = MagicMock()
        mock_session.query.return_value.filter.return_value.all.return_value = []

        with patch("src.cli.daily_review_batch.GameSummary") as mock_cls:
            _upsert_review_summary(mock_session, "GAME1", '{"data": 1}')
            mock_cls.assert_called_once_with(
                game_id="GAME1",
                summary_type="리뷰_WPA",
                detail_text='{"data": 1}',
            )
            mock_session.add.assert_called_once()

    def test_updates_existing_summary(self):
        mock_session = MagicMock()
        existing = MagicMock()
        mock_session.query.return_value.filter.return_value.all.return_value = [existing]

        _upsert_review_summary(mock_session, "GAME1", '{"data": 2}')
        assert existing.detail_text == '{"data": 2}'
        mock_session.add.assert_not_called()


class TestTrustedRelayGameIds:
    def test_empty_game_ids(self):
        mock_session = MagicMock()
        result = _trusted_relay_game_ids(mock_session, [])
        assert result == set()

    def test_trusted_by_status(self):
        mock_session = MagicMock()
        row1 = MagicMock()
        row1.game_id = "G1"
        row1.validation_status = "verified"
        row2 = MagicMock()
        row2.game_id = "G2"
        row2.validation_status = "unverified"
        mock_session.query.return_value.filter.return_value.all.return_value = [row1, row2]

        result = _trusted_relay_game_ids(mock_session, ["G1", "G2"])
        assert result == {"G1"}

    def test_trusted_by_wpa(self):
        mock_session = MagicMock()
        mock_session.query.return_value.filter.return_value.all.return_value = []
        wpa_row = MagicMock()
        wpa_row.__getitem__ = lambda self, i: "G3"
        mock_session.query.return_value.filter.return_value.distinct.return_value.all.return_value = [wpa_row]

        result = _trusted_relay_game_ids(mock_session, ["G3"])
        assert "G3" in result


class TestBuildReviewData:
    def test_builds_with_team_movements(self):
        agg = MagicMock()
        agg.get_crucial_moments.return_value = [{"event": "HR"}]
        agg.get_completed_game_pitching_breakdown.return_value = {}
        agg.get_recent_player_movements.return_value = []
        agg.get_daily_roster_changes.return_value = []

        game = MagicMock()
        game.game_id = "G1"
        game.game_date.strftime.return_value = "20251015"
        game.game_date.year = 2025
        game.away_team = "LG"
        game.home_team = "SS"
        game.away_score = 5
        game.home_score = 3

        with patch("src.cli.daily_review_batch.team_code_from_game_id_segment") as mock_tc:
            mock_tc.side_effect = lambda code, year: code.lower()
            result = _build_review_data(agg, game)

        assert result["game_id"] == "G1"
        assert result["final_score"] == "LG 5 : 3 SS"
        assert "away_movements" in result
        assert "home_movements" in result

    def test_skips_movements_when_no_team_code(self):
        agg = MagicMock()
        agg.get_crucial_moments.return_value = []
        agg.get_completed_game_pitching_breakdown.return_value = {}

        game = MagicMock()
        game.game_id = "G1"
        game.game_date.strftime.return_value = "20251015"
        game.game_date.year = 2025
        game.away_team = "LG"
        game.home_team = "SS"
        game.away_score = 5
        game.home_score = 3

        with patch("src.cli.daily_review_batch.team_code_from_game_id_segment") as mock_tc:
            mock_tc.return_value = None
            result = _build_review_data(agg, game)

        assert "away_movements" not in result
