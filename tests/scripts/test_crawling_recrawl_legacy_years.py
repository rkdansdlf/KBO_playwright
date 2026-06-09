from unittest.mock import MagicMock, patch

from scripts.crawling.recrawl_legacy_years import recrawl_legacy_years


class TestRecrawlLegacyYears:
    @patch("scripts.crawling.recrawl_legacy_years.subprocess.run")
    @patch("scripts.crawling.recrawl_legacy_years.get_available_series_by_year")
    def test_empty_series(self, mock_get_series, mock_run):
        mock_get_series.return_value = []
        result = recrawl_legacy_years(1982, 1982, reset_first=False)
        assert result is False

    @patch("scripts.crawling.recrawl_legacy_years.subprocess.run")
    @patch("scripts.crawling.recrawl_legacy_years.get_available_series_by_year")
    def test_crawl_tasks_created(self, mock_get_series, mock_run):
        mock_get_series.return_value = ["regular"]
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "크롤링 완료"
        mock_run.return_value = mock_result

        result = recrawl_legacy_years(1982, 1982, reset_first=False)
        assert result is True

    @patch("scripts.crawling.recrawl_legacy_years.subprocess.run")
    @patch("scripts.crawling.recrawl_legacy_years.get_available_series_by_year")
    def test_reset_failure(self, mock_get_series, mock_run):
        import subprocess

        mock_run.side_effect = subprocess.CalledProcessError(1, ["reset_sqlite.py"])
        result = recrawl_legacy_years(1982, 1982, reset_first=True)
        assert result is False
