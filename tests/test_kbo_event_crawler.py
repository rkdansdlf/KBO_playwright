from __future__ import annotations

from src.crawlers.kbo_event_crawler import extract_kbo_event_links


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
