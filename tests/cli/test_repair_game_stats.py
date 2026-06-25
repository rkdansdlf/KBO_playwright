import logging
from unittest.mock import MagicMock, patch

import pytest

from src.cli.repair_game_stats import _repair_batting, _repair_pitching, main


class TestRepairBatting:
    def test_no_missing_stats(self, caplog):
        mock_session = MagicMock()
        query_chain = MagicMock()
        query_chain.count.return_value = 0
        mock_session.query.return_value.filter.return_value = query_chain

        with patch("src.cli.repair_game_stats.SessionLocal") as mock_sf:
            mock_sf.return_value.__enter__.return_value = mock_session
            with caplog.at_level(logging.INFO):
                _repair_batting()

        assert "No missing batting stats found" in caplog.text

    def test_updates_batting_stats(self, caplog):
        mock_session = MagicMock()

        class FakeStat:
            def __init__(self):
                self.at_bats = 100
                self.hits = 30
                self.walks = 10
                self.hbp = 2
                self.sacrifice_flies = 1
                self.doubles = 5
                self.triples = 1
                self.home_runs = 3
                self.strikeouts = 20
                self.plate_appearances = 110
                self.intentional_walks = 2
                self.stolen_bases = 5
                self.caught_stealing = 1
                self.gdp = 2
                self.sacrifice_hits = 0
                self.extra_stats = None
                self.avg = 0.0
                self.obp = None
                self.slg = None
                self.ops = None
                self.iso = None
                self.babip = None

        mock_stat = FakeStat()

        query_chain = MagicMock()
        query_chain.count.return_value = 1
        query_chain.all.return_value = [mock_stat]
        mock_session.query.return_value.filter.return_value = query_chain

        with (
            patch("src.cli.repair_game_stats.SessionLocal") as mock_sf,
            patch("src.cli.repair_game_stats.BattingStatCalculator") as mock_calc,
        ):
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_calc.calculate_ratios.return_value = {
                "avg": 0.273,
                "obp": 0.345,
                "slg": 0.450,
                "ops": 0.795,
                "iso": 0.177,
                "babip": 0.300,
                "xr": 42.0,
            }

            with caplog.at_level(logging.INFO):
                _repair_batting()

        assert mock_stat.avg == 0.273
        assert mock_stat.obp == 0.345
        assert mock_stat.extra_stats == {"xr": 42.0}
        assert "Updated 1 rows" in caplog.text

    def test_commits_every_500(self):
        mock_session = MagicMock()

        class FakeStat:
            def __init__(self):
                self.at_bats = 10
                self.hits = 3
                self.walks = 1
                self.hbp = 0
                self.sacrifice_flies = 0
                self.doubles = 0
                self.triples = 0
                self.home_runs = 0
                self.strikeouts = 2
                self.plate_appearances = 11
                self.intentional_walks = 0
                self.stolen_bases = 0
                self.caught_stealing = 0
                self.gdp = 0
                self.sacrifice_hits = 0
                self.extra_stats = None
                self.avg = None
                self.obp = None
                self.slg = None
                self.ops = None
                self.iso = None
                self.babip = None

        stats = [FakeStat() for _ in range(501)]

        query_chain = MagicMock()
        query_chain.count.return_value = 501
        query_chain.all.return_value = stats
        mock_session.query.return_value.filter.return_value = query_chain

        with (
            patch("src.cli.repair_game_stats.SessionLocal") as mock_sf,
            patch("src.cli.repair_game_stats.BattingStatCalculator") as mock_calc,
        ):
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_calc.calculate_ratios.return_value = {
                "avg": 0.3,
                "obp": 0.35,
                "slg": 0.4,
                "ops": 0.75,
                "iso": 0.1,
                "babip": 0.28,
                "xr": 5.0,
            }

            _repair_batting()
            assert mock_session.commit.call_count >= 2


class TestRepairPitching:
    def test_no_missing_stats(self, caplog):
        mock_session = MagicMock()
        query_chain = MagicMock()
        query_chain.count.return_value = 0
        mock_session.query.return_value.filter.return_value = query_chain

        with patch("src.cli.repair_game_stats.SessionLocal") as mock_sf:
            mock_sf.return_value.__enter__.return_value = mock_session
            with caplog.at_level(logging.INFO):
                _repair_pitching()

        assert "No missing pitching stats found" in caplog.text

    def test_updates_pitching_stats(self):
        mock_session = MagicMock()

        class FakePitchingStat:
            def __init__(self):
                self.innings_outs = 100
                self.earned_runs = 20
                self.hits_allowed = 50
                self.walks_allowed = 15
                self.strikeouts = 60
                self.home_runs_allowed = 5
                self.hit_batters = 2
                self.batters_faced = 200
                self.era = 0.0
                self.whip = None
                self.k_per_nine = None
                self.bb_per_nine = None
                self.kbb = None
                self.extra_stats = None

        mock_stat = FakePitchingStat()

        query_chain = MagicMock()
        query_chain.count.return_value = 1
        query_chain.all.return_value = [mock_stat]
        mock_session.query.return_value.filter.return_value = query_chain

        with (
            patch("src.cli.repair_game_stats.SessionLocal") as mock_sf,
            patch("src.cli.repair_game_stats.PitchingStatCalculator") as mock_calc,
        ):
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_calc.calculate_ratios.return_value = {
                "era": 1.8,
                "whip": 1.15,
                "k_per_nine": 5.4,
                "bb_per_nine": 1.35,
                "kbb": 4.0,
                "fip": 2.1,
            }

            _repair_pitching()

        assert mock_stat.era == 1.8
        assert mock_stat.whip == 1.15
        assert mock_stat.extra_stats == {"fip": 2.1}

    def test_handles_fip_attribute(self):
        mock_session = MagicMock()

        class FakePitchingStatWithFip:
            def __init__(self):
                self.innings_outs = 100
                self.earned_runs = 20
                self.hits_allowed = 50
                self.walks_allowed = 15
                self.strikeouts = 60
                self.home_runs_allowed = 5
                self.hit_batters = 2
                self.batters_faced = 200
                self.era = 0.0
                self.whip = None
                self.k_per_nine = None
                self.bb_per_nine = None
                self.kbb = None
                self.fip = None
                self.extra_stats = None

        mock_stat = FakePitchingStatWithFip()

        query_chain = MagicMock()
        query_chain.count.return_value = 1
        query_chain.all.return_value = [mock_stat]
        mock_session.query.return_value.filter.return_value = query_chain

        with (
            patch("src.cli.repair_game_stats.SessionLocal") as mock_sf,
            patch("src.cli.repair_game_stats.PitchingStatCalculator") as mock_calc,
        ):
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_calc.calculate_ratios.return_value = {
                "era": 1.8,
                "whip": 1.15,
                "k_per_nine": 5.4,
                "bb_per_nine": 1.35,
                "kbb": 4.0,
                "fip": 2.1,
            }

            _repair_pitching()
            assert mock_stat.fip == 2.1


class TestRepairGameStatsCLI:
    def test_default_all(self):
        with patch("src.cli.repair_game_stats.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            query_chain = MagicMock()
            query_chain.count.return_value = 0
            mock_session.query.return_value.filter.return_value = query_chain
            result = main([])
            assert result is None

    def test_batting_only(self):
        with patch("src.cli.repair_game_stats.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            query_chain = MagicMock()
            query_chain.count.return_value = 0
            mock_session.query.return_value.filter.return_value = query_chain
            result = main(["--type", "batting"])
            assert result is None

    def test_pitching_only(self):
        with patch("src.cli.repair_game_stats.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            query_chain = MagicMock()
            query_chain.count.return_value = 0
            mock_session.query.return_value.filter.return_value = query_chain
            result = main(["--type", "pitching"])
            assert result is None
