from unittest.mock import MagicMock, patch

from scripts.crawling.crawl_all_historical import (
    determine_crawling_strategy,
    get_year_range_validation,
)


class TestGetYearRangeValidation:
    def test_valid_range(self):
        assert get_year_range_validation(1982, 2025) == (1982, 2025)

    def test_before_1982(self):
        import pytest

        with pytest.raises(ValueError, match="1982"):
            get_year_range_validation(1980, 2025)

    def test_start_gt_end(self):
        import pytest

        with pytest.raises(ValueError, match="시작"):
            get_year_range_validation(2020, 2019)


class TestDetermineCrawlingStrategy:
    def test_legacy(self):
        assert determine_crawling_strategy(2001) == "legacy"
        assert determine_crawling_strategy(1982) == "legacy"

    def test_modern(self):
        assert determine_crawling_strategy(2002) == "modern"
        assert determine_crawling_strategy(2025) == "modern"


class TestRunLegacyCrawling:
    @patch("scripts.crawling.crawl_all_historical.subprocess.run")
    def test_batting_success(self, mock_run):
        from scripts.crawling.crawl_all_historical import run_legacy_crawling

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "크롤링 완료"
        mock_run.return_value = mock_result

        success, output = run_legacy_crawling(2000, "regular", "batting")
        assert success is True

    @patch("scripts.crawling.crawl_all_historical.subprocess.run")
    def test_unknown_type(self, mock_run):
        from scripts.crawling.crawl_all_historical import run_legacy_crawling

        success, output = run_legacy_crawling(2000, "regular", "fielding")
        assert success is False
        assert "Unknown" in output


class TestRunModernCrawling:
    @patch("scripts.crawling.crawl_all_historical.subprocess.run")
    def test_pitching_success(self, mock_run):
        from scripts.crawling.crawl_all_historical import run_modern_crawling

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "크롤링 완료"
        mock_run.return_value = mock_result

        success, output = run_modern_crawling(2025, "regular", "pitching")
        assert success is True
