"""Unit tests for src.crawlers.dto."""

from __future__ import annotations

from src.crawlers.dto import (
    CrawlExecutionStats,
    CrawlRequest,
    CrawlResponse,
    ExtractorResult,
)


def test_crawl_request_to_dict() -> None:
    req = CrawlRequest(
        url="https://www.koreabaseball.com/Schedule/Schedule.aspx",
        params={"year": "2025"},
        timeout_seconds=15.0,
    )
    d = req.to_dict()
    assert d["url"] == "https://www.koreabaseball.com/Schedule/Schedule.aspx"
    assert d["params"] == {"year": "2025"}
    assert d["timeout_seconds"] == 15.0


def test_crawl_response_to_dict() -> None:
    res = CrawlResponse(
        url="https://www.koreabaseball.com",
        status_code=200,
        text="<html><body>Hello</body></html>",
        elapsed_seconds=0.123,
    )
    d = res.to_dict()
    assert d["url"] == "https://www.koreabaseball.com"
    assert d["status_code"] == 200
    assert d["text_length"] == len("<html><body>Hello</body></html>")
    assert d["elapsed_seconds"] == 0.123


def test_crawl_execution_stats() -> None:
    stats = CrawlExecutionStats()
    stats.record_request(success=True)
    stats.record_request(success=False, retried=True)
    stats.record_throttle(0.5)
    stats.total_duration_seconds = 2.5

    d = stats.to_dict()
    assert d["requests_count"] == 2
    assert d["success_count"] == 1
    assert d["failed_count"] == 1
    assert d["retried_count"] == 1
    assert d["throttled_seconds"] == 0.5


def test_extractor_result_to_dict() -> None:
    result = ExtractorResult(
        records=[{"player_id": 101, "name": "Player 1"}],
        validation_errors=["minor warning"],
    )
    d = result.to_dict()
    assert d["total_records"] == 1
    assert len(d["records"]) == 1
    assert d["validation_errors"] == ["minor warning"]
