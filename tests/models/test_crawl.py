from __future__ import annotations

from datetime import UTC, datetime

from src.models.crawl import CrawlRun


class TestCrawlRun:
    def test_tablename(self):
        assert CrawlRun.__tablename__ == "crawl_runs"

    def test_has_id(self):
        assert hasattr(CrawlRun, "id")

    def test_has_label(self):
        assert hasattr(CrawlRun, "label")

    def test_has_started_at(self):
        assert hasattr(CrawlRun, "started_at")

    def test_has_finished_at(self):
        assert hasattr(CrawlRun, "finished_at")

    def test_has_active_count(self):
        assert hasattr(CrawlRun, "active_count")

    def test_has_retired_count(self):
        assert hasattr(CrawlRun, "retired_count")

    def test_has_staff_count(self):
        assert hasattr(CrawlRun, "staff_count")

    def test_has_confirmed_profiles(self):
        assert hasattr(CrawlRun, "confirmed_profiles")

    def test_has_heuristic_only(self):
        assert hasattr(CrawlRun, "heuristic_only")

    def test_has_created_at(self):
        assert hasattr(CrawlRun, "created_at")

    def test_default_values_in_table(self):
        col = CrawlRun.__table__.c
        assert col.active_count.default.arg == 0
        assert col.retired_count.default.arg == 0
        assert col.staff_count.default.arg == 0
        assert col.confirmed_profiles.default.arg == 0
        assert col.heuristic_only.default.arg == 0
