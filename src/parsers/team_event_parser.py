"""
Parser for team event/news HTML pages. Extracts event title, date, type, and source URL.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

EVENT_KEYWORDS = [
    "이벤트",
    "시구",
    "증정",
    "팬",
    "클래스",
    "신청",
    "모집",
    "프로모션",
    "할인",
    "페스티벌",
    "공연",
    "사인회",
    "포토",
    "굿즈",
    "기념품",
    "경품",
    "추첨",
    "당첨",
    "안내",
    "개막",
    "마감",
    "투표",
]

TEAM_CODE_FROM_SOURCE_KEY = {
    "lg_twins_events": "LG",
    "hanwha_eagles_events": "HH",
    "doosan_bears_events": "OB",
    "ssg_landers_events": "SK",
    "nc_dinos_events": "NC",
    "kia_tigers_events": "HT",
    "lotte_giants_events": "LT",
    "samsung_lions_events": "SS",
    "kt_wiz_events": "KT",
    "kiwoom_heroes_events": "WO",
}

EVENT_TEAM_NAME_MAP = {
    "LG": "LG",
    "HH": "HH",
    "SS": "SS",
    "KT": "KT",
    "OB": "OB",
    "LT": "LT",
    "HT": "HT",
    "NC": "NC",
    "SK": "SK",
    "WO": "WO",
}

SOURCE_CONFIG_MAP: dict[str, dict[str, Any]] = {
    "lg_twins_events": {
        "link_prefix": "https://www.lgtwins.com",
        "title_sel": "a.subject",
        "date_sel": "span.date",
    },
    "hanwha_eagles_events": {
        "link_prefix": "",
        "title_sel": "td.tit a",
        "date_sel": "td.date",
    },
    "doosan_bears_events": {
        "link_prefix": "https://www.doosanbears.com",
        "title_sel": "td.title a",
        "date_sel": "td.date",
    },
    "ssg_landers_events": {
        "link_prefix": "https://www.ssglanders.com",
        "title_sel": "td.title a",
        "date_sel": "td.date",
    },
    "nc_dinos_events": {
        "link_prefix": "https://www.ncdinos.com",
        "title_sel": "td.subject a",
        "date_sel": "td.date",
    },
    "kia_tigers_events": {
        "link_prefix": "https://www.kiatigers.com",
        "title_sel": "td.tit a",
        "date_sel": "td.date",
    },
    "lotte_giants_events": {
        "link_prefix": "https://www.giantsclub.com",
        "title_sel": "td.tit a",
        "date_sel": "td.date",
    },
    "samsung_lions_events": {
        "link_prefix": "https://www.samsunglions.com",
        "title_sel": "td.title a",
        "date_sel": "td.date",
    },
    "kt_wiz_events": {
        "link_prefix": "https://www.ktwiz.co.kr",
        "title_sel": "td.title a",
        "date_sel": "td.date",
    },
    "kiwoom_heroes_events": {
        "link_prefix": "https://www.heroesbaseball.co.kr",
        "title_sel": "td.title a",
        "date_sel": "td.date",
    },
}

CUTOFF_DAYS = 60


def _classify_event(title: str) -> str:
    if any(kw in title for kw in ["증정", "경품", "굿즈", "기념품"]):
        return "giveaway"
    if "시구" in title:
        return "first_pitch"
    if any(kw in title for kw in ["할인", "프로모션"]):
        return "discount"
    if any(kw in title for kw in ["사인회", "팬"]):
        return "fan_participation"
    if any(kw in title for kw in ["공연", "페스티벌", "축제"]):
        return "festival"
    if any(kw in title for kw in ["신청", "모집", "클래스"]):
        return "promotion"
    if any(kw in title for kw in ["개막", "안내", "공지"]):
        return "notice"
    return "promotion"


def parse_team_events(html: str, source_key: str, metadata: dict | None = None) -> list[dict]:
    team_code = TEAM_CODE_FROM_SOURCE_KEY.get(source_key, "UNKNOWN")
    config = SOURCE_CONFIG_MAP.get(source_key, {"link_prefix": ""})
    page_url = (metadata or {}).get("url", "")
    cutoff_days = int((metadata or {}).get("cutoff_days", CUTOFF_DAYS))
    fetched_at_str = (metadata or {}).get("fetched_at", "")
    if fetched_at_str:
        try:
            fetched_at = datetime.fromisoformat(fetched_at_str)
        except (ValueError, TypeError):
            fetched_at = datetime.now()
    else:
        fetched_at = datetime.now()
    cutoff_date = fetched_at - timedelta(days=cutoff_days)

    soup = BeautifulSoup(html, "html.parser")
    title_sel = config.get("title_sel", "a")
    date_sel = config.get("date_sel", "")

    events = []
    seen_titles = set()

    for a_tag in soup.select(title_sel):
        title = a_tag.get_text(strip=True)
        if len(title) < 4:
            continue
        if not any(kw in title for kw in EVENT_KEYWORDS):
            continue
        if title in seen_titles:
            continue
        seen_titles.add(title)

        href = a_tag.get("href", "")
        if href.startswith("http"):
            source_url = href
        elif config.get("link_prefix"):
            prefix = config["link_prefix"]
            source_url = prefix + ("/" if not href.startswith("/") else "") + href
        else:
            source_url = href

        published_at = None
        row = a_tag.find_parent(["tr", "li", "div", "dl", "table"])
        if row and date_sel:
            date_el = row.select_one(date_sel)
            if date_el:
                date_text = date_el.get_text(strip=True)
                try:
                    parsed = datetime.strptime(date_text.replace(".", "-").replace("/", "-"), "%Y-%m-%d")
                    if parsed >= cutoff_date:
                        published_at = parsed
                except ValueError:
                    pass

        if not published_at:
            continue

        events.append(
            {
                "event_scope": "team",
                "team_id": EVENT_TEAM_NAME_MAP.get(team_code, team_code),
                "title": title[:300],
                "event_type": _classify_event(title),
                "published_at": published_at,
                "source_url": source_url or page_url,
                "last_seen_at": datetime.utcnow(),
                "status": "unknown",
            }
        )

    return events


if __name__ == "__main__":
    import sys
    html = sys.stdin.read() if not sys.stdin.isatty() else (
        '<html><body><table><tr>'
        '<td><a class="subject" href="/notice/1">2025 시즌 이벤트 안내</a></td>'
        '<td><span class="date">2025-03-15</span></td>'
        '</tr></table></body></html>'
    )
    result = parse_team_events(html, "lg_twins_events", {"cutoff_days": 90, "fetched_at": "2025-06-01T00:00:00"})
    for item in result:
        print(item)
