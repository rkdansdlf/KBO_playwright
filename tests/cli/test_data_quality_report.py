from __future__ import annotations

import json
import pathlib
from unittest.mock import MagicMock, patch

import pytest

from src.cli.data_quality_report import (
    _audit_batting_consistency,
    _build_report_data,
    _empty_year_data,
    _populate_source_summaries,
    _report_path,
    _source_counts,
    _write_csv_report,
    _write_json_report,
    generate_report,
    main,
)


class TestEmptyYearData:
    def test_returns_expected_structure(self):
        result = _empty_year_data()
        assert "batting" in result
        assert "pitching" in result
        assert "fielding" in result
        assert "baserunning" in result
        assert "discrepancies" in result
        assert result["batting"]["total"] == 0
        assert result["batting"]["sources"] == {}
        assert result["batting"]["consistency_rate"] == 0.0


class TestSourceCounts:
    def test_returns_count_and_sources(self):
        from src.models.player import PlayerSeasonBatting

        mock_session = MagicMock()
        mock_row1 = MagicMock()
        mock_row1.source = "kbo_api"
        mock_row1.id = 1

        mock_query = MagicMock()
        mock_query.filter.return_value.group_by.return_value.all.return_value = [
            ("kbo_api", 2),
            (None, 1),
        ]
        mock_session.query.return_value.filter.return_value.group_by.return_value = (
            mock_query.filter.return_value.group_by.return_value
        )

        total, sources = _source_counts(mock_session, PlayerSeasonBatting, PlayerSeasonBatting.season, 2025)
        assert total == 3
        assert sources == {"kbo_api": 2, "UNKNOWN": 1}


class TestPopulateSourceSummaries:
    def test_populates_all_categories(self):
        mock_session = MagicMock()
        year_data = _empty_year_data()

        with patch("src.cli.data_quality_report._source_counts") as mock_counts:
            mock_counts.return_value = (10, {"kbo_api": 8, "crawler": 2})
            _populate_source_summaries(mock_session, year_data, 2025)

        for category in ["batting", "pitching", "fielding", "baserunning"]:
            assert year_data[category]["total"] == 10
            assert year_data[category]["sources"] == {"kbo_api": 8, "crawler": 2}


class TestAuditBattingConsistency:
    def test_no_officials(self):
        mock_session = MagicMock()
        mock_session.query.return_value.filter.return_value.limit.return_value.all.return_value = []
        year_data = _empty_year_data()

        _audit_batting_consistency(mock_session, year_data, 2025)
        assert year_data["batting"]["consistency_rate"] == 0.0

    def test_all_match(self):
        mock_session = MagicMock()
        official = MagicMock()
        official.player_id = 1
        official.at_bats = 100
        official.hits = 30
        mock_session.query.return_value.filter.return_value.limit.return_value.all.return_value = [official]

        with patch("src.aggregators.season_stat_aggregator.SeasonStatAggregator.aggregate_batting_season") as mock_agg:
            mock_agg.return_value = {"at_bats": 100, "hits": 30}
            year_data = _empty_year_data()
            _audit_batting_consistency(mock_session, year_data, 2025)
            assert year_data["batting"]["consistency_rate"] == 100.0

    def test_mismatch_appended(self):
        mock_session = MagicMock()
        official = MagicMock()
        official.player_id = 1
        official.at_bats = 100
        official.hits = 30
        mock_session.query.return_value.filter.return_value.limit.return_value.all.return_value = [official]

        player = MagicMock()
        player.name = "Kim"
        mock_session.query.return_value.filter_by.return_value.first.return_value = player

        with patch("src.aggregators.season_stat_aggregator.SeasonStatAggregator.aggregate_batting_season") as mock_agg:
            mock_agg.return_value = {"at_bats": 90, "hits": 25}
            year_data = _empty_year_data()
            _audit_batting_consistency(mock_session, year_data, 2025)
            assert year_data["batting"]["consistency_rate"] == 0.0
            assert len(year_data["discrepancies"]) == 1
            assert year_data["discrepancies"][0]["type"] == "BATTING"
            assert year_data["discrepancies"][0]["player_id"] == 1

    def test_none_calc_skipped(self):
        mock_session = MagicMock()
        official = MagicMock()
        official.player_id = 1
        official.at_bats = 100
        official.hits = 30
        mock_session.query.return_value.filter.return_value.limit.return_value.all.return_value = [official]

        with patch("src.aggregators.season_stat_aggregator.SeasonStatAggregator.aggregate_batting_season") as mock_agg:
            mock_agg.return_value = None
            year_data = _empty_year_data()
            _audit_batting_consistency(mock_session, year_data, 2025)
            assert year_data["batting"]["consistency_rate"] == 0.0


class TestReportPath:
    def test_local_json_path(self, tmp_path):
        result = _report_path(str(tmp_path), None, "json")
        assert result.endswith(".json")
        assert "data_quality_report_local_" in result

    def test_remote_csv_path(self, tmp_path):
        result = _report_path(str(tmp_path), "postgresql://test", "csv")
        assert result.endswith(".csv")
        assert "data_quality_report_remote_" in result


class TestWriteJsonReport:
    def test_writes_valid_json(self, tmp_path):
        data = {"year": 2025, "data": [1, 2, 3]}
        path = str(tmp_path / "test.json")
        _write_json_report(data, path)
        loaded = json.loads(pathlib.Path(path).read_text())
        assert loaded == data


class TestWriteCsvReport:
    def test_writes_csv_rows(self, tmp_path):
        data = {
            "years": {
                2025: {
                    "batting": {"total": 10, "consistency_rate": 95.0, "sources": {"kbo": 10}},
                    "pitching": {"total": 0, "consistency_rate": "N/A", "sources": {}},
                    "fielding": {"total": 0, "consistency_rate": "N/A", "sources": {}},
                    "baserunning": {"total": 0, "consistency_rate": "N/A", "sources": {}},
                }
            }
        }
        path = str(tmp_path / "test.csv")
        _write_csv_report(data, path)
        content = pathlib.Path(path).read_text()
        assert "Year" in content
        assert "BATTING" in content
        assert "2025" in content


class TestGenerateReport:
    def test_calls_build_and_write(self, tmp_path):
        with patch("src.cli.data_quality_report._build_report_data") as mock_build:
            mock_build.return_value = {"years": {}}
            generate_report([2025], "json", str(tmp_path))
            mock_build.assert_called_once()

    def test_csv_format(self, tmp_path):
        with patch("src.cli.data_quality_report._build_report_data") as mock_build:
            mock_build.return_value = {"years": {}}
            with patch("src.cli.data_quality_report._write_csv_report") as mock_write:
                generate_report([2025], "csv", str(tmp_path))
                mock_write.assert_called_once()


class TestDataQualityReportCLI:
    def test_default_args(self):
        with patch("src.cli.data_quality_report.generate_report") as mock:
            result = main([])
            assert result == 0
            mock.assert_called_once()

    def test_specific_years(self):
        with patch("src.cli.data_quality_report.generate_report") as mock:
            result = main(["--years", "2025"])
            assert result == 0
            args, _ = mock.call_args
            assert args[0] == [2025]

    def test_csv_format(self):
        with patch("src.cli.data_quality_report.generate_report") as mock:
            result = main(["--format", "csv"])
            assert result == 0
            args, _ = mock.call_args
            assert args[1] == "csv"

    def test_with_db_url(self):
        with patch("src.cli.data_quality_report.generate_report") as mock:
            result = main(["--db-url", "postgresql://localhost/test"])
            assert result == 0
            args, _ = mock.call_args
            assert args[3] == "postgresql://localhost/test"

    def test_year_range(self):
        with patch("src.cli.data_quality_report.generate_report") as mock:
            result = main(["--years", "2023-2025"])
            assert result == 0
            args, _ = mock.call_args
            assert args[0] == [2023, 2024, 2025]
