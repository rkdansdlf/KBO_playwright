from __future__ import annotations

from src.crawlers.kbo_event_crawler import (
    KBO_EVENT_DEFAULT_URLS,
    KboEventCrawler,
    extract_kbo_event_links,
    extract_kbo_event_page,
)


def test_extract_kbo_event_links_filters_event_candidates():
    html = """
    <html><body>
      <a href="/Event/Detail.aspx?id=1">팬 이벤트 안내</a>
      <a href="/Schedule/Schedule.aspx">경기 일정</a>
      <a href="https://example.com/promotion">프로모션 안내</a>
    </body></html>
    """

    result = extract_kbo_event_links(html, "https://www.koreabaseball.com")

    assert [item["title"] for item in result] == ["팬 이벤트 안내", "프로모션 안내"]
    assert result[0]["source_url"] == "https://www.koreabaseball.com/Event/Detail.aspx?id=1"
    assert result[1]["source_url"] == "https://example.com/promotion"


def test_extract_kbo_event_page_uses_business_event_title():
    html = """
    <html><head><title>신청하기 | 미디어데이&팬페스트 입장권 | 주요 사업/행사 | KBO</title></head></html>
    """

    result = extract_kbo_event_page(html, "https://www.koreabaseball.com/Kbo/BusinessAndEvent/MediaDay.aspx")

    assert result is not None
    assert result["title"] == "미디어데이&팬페스트 입장권"
    assert result["source_url"] == "https://www.koreabaseball.com/Kbo/BusinessAndEvent/MediaDay.aspx"


def test_kbo_event_crawler_defaults_to_business_event_urls():
    crawler = KboEventCrawler()

    assert crawler.urls == KBO_EVENT_DEFAULT_URLS
