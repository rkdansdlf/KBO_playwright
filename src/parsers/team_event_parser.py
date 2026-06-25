"""Parser for team event/news HTML pages. Extracts event title, date, type, and source URL."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from src.constants import KST

if TYPE_CHECKING:
    from bs4.element import Tag

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
    "공지",
    "시타",
    "데이",
    "DAY",
    "투어",
    "하이파이브",
    "브랜드데이",
    "파트너데이",
    "스페셜",
    "MATCH",
    "매치",
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
        "title_sel": "ul.news_list.event span.title, a.subject",
        "date_sel": ".date, span.date",
    },
    "hanwha_eagles_events": {
        "link_prefix": "",
        "title_sel": "td.tit a",
        "date_sel": "td.date",
    },
    "doosan_bears_events": {
        "link_prefix": "https://www.doosanbears.com",
        "title_sel": "p.tit, td.title a",
        "date_sel": "p.txt, td.date",
    },
    "ssg_landers_events": {
        "link_prefix": "https://www.ssglanders.com",
        "title_sel": "h4.text-dotdotdot, td.title a",
        "date_sel": "td.date",
    },
    "nc_dinos_events": {
        "link_prefix": "https://www.ncdinos.com",
        "title_sel": "#board_list_event a.title, td.subject a",
        "date_sel": ".date, td.date",
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
        "link_prefix": "https://www.heroesbaseball.co.kr/story/heroesNews/",
        "title_sel": ".headNotice a, ul.teamNews h4 a, td.title a",
        "date_sel": "span, td.date",
    },
}

CUTOFF_DAYS = 60
DATE_PATTERN = re.compile(r"(20\d{2})[.\-/](\d{1,2})[.\-/](\d{1,2})")
ONCLICK_HREF_PATTERN = re.compile(r"""location\.href\s*=\s*["']([^"']+)["']""")
EVENT_ONLY_URL_MARKERS = (
    "/feed/events",
    "/doorun/events",
    "newstype=event",
)


_EVENT_KEYWORDS: dict[str, tuple[str, ...]] = {
    "giveaway": ("증정", "경품", "굿즈", "기념품"),
    "first_pitch": ("시구",),
    "discount": ("할인", "프로모션"),
    "fan_participation": ("사인회", "팬"),
    "festival": ("공연", "페스티벌", "축제"),
    "promotion": ("신청", "모집", "클래스"),
    "notice": ("개막", "안내", "공지"),
}


def _classify_event(title: str) -> str:
    for event_type, keywords in _EVENT_KEYWORDS.items():
        if any(kw in title for kw in keywords):
            return event_type
    return "promotion"


def _parse_fetched_at(metadata: dict | None) -> datetime:
    fetched_at_str = (metadata or {}).get("fetched_at", "")
    if not fetched_at_str:
        return datetime.now(KST)
    try:
        fetched_at = datetime.fromisoformat(fetched_at_str)
    except (ValueError, TypeError):
        logger.debug("Invalid fetched_at string: %s", fetched_at_str)
        return datetime.now(KST)
    if fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=KST)
    return fetched_at


def _parse_date_text(text: str) -> datetime | None:
    match = DATE_PATTERN.search(text or "")
    if not match:
        return None
    year, month, day = (int(part) for part in match.groups())
    try:
        return datetime(year, month, day, tzinfo=KST)
    except ValueError:
        return None


def _is_event_only_page(page_url: str) -> bool:
    lowered = (page_url or "").lower()
    return any(marker in lowered for marker in EVENT_ONLY_URL_MARKERS)


def _is_event_title(title: str, page_url: str) -> bool:
    if _is_event_only_page(page_url):
        return True
    return any(kw in title for kw in EVENT_KEYWORDS)


def _filter_dict_rows(items: object) -> list[dict]:
    if isinstance(items, list):
        return [item for item in items if isinstance(item, dict)]
    return []


def _iter_json_rows(payload: object) -> list[dict]:
    if isinstance(payload, list):
        return _filter_dict_rows(payload)
    if not isinstance(payload, dict):
        return []
    content = payload.get("content")
    if isinstance(content, list):
        return _filter_dict_rows(content)
    return _extract_rows_from_dict_payload(payload)


def _extract_rows_from_dict_payload(payload: dict[str, Any]) -> list[dict]:
    result = payload.get("result")
    if isinstance(result, dict):
        for key in ("data", "content"):
            value = result.get(key)
            if isinstance(value, list):
                return _filter_dict_rows(value)
    data = payload.get("data")
    if isinstance(data, list):
        return _filter_dict_rows(data)
    if isinstance(data, dict):
        inner = data.get("content")
        if isinstance(inner, list):
            return _filter_dict_rows(inner)
    return []


def _extract_source_url(tag: Tag, config: dict[str, Any], page_url: str) -> str:
    href = str(tag.get("href", "") or "").strip()
    if not href:
        link_parent = tag.find_parent("a", href=True)
        if link_parent:
            href = str(link_parent.get("href", "") or "").strip()
    if not href:
        href = _extract_onclick_href(tag)

    if not href or href.startswith(("#", "javascript:")):
        return page_url
    if href.startswith("http"):
        return href

    return urljoin(config.get("link_prefix") or page_url, href)


def _extract_onclick_href(tag: Tag) -> str:
    ancestors = [tag, *tag.find_parents(["tr", "li", "div", "dl", "section", "article"])]
    for parent in ancestors[:6]:
        href = _match_onclick_href(str(parent.get("onclick", "") or ""))
        if href:
            return href
        for clickable in parent.select("[onclick]"):
            href = _match_onclick_href(str(clickable.get("onclick", "") or ""))
            if href:
                return href
    return ""


def _match_onclick_href(onclick: str) -> str:
    match = ONCLICK_HREF_PATTERN.search(onclick)
    return match.group(1).strip() if match else ""


def _extract_published_at(tag: Tag, date_sel: str, cutoff_date: datetime) -> datetime | None:
    ancestors = [tag, *tag.find_parents(["tr", "li", "div", "dl", "table", "section", "article"])]
    for row in ancestors[:10]:
        if date_sel:
            date_el = row.select_one(date_sel)
            if date_el:
                parsed = _parse_date_text(date_el.get_text(" ", strip=True))
                if parsed:
                    return parsed if parsed >= cutoff_date else None

        parsed = _parse_date_text(row.get_text(" ", strip=True))
        if parsed:
            return parsed if parsed >= cutoff_date else None

    return None


def _parse_json_team_events(
    html: str,
    source_key: str,
    metadata: dict | None,
    cutoff_date: datetime,
    fetched_at: datetime,
) -> list[dict]:
    try:
        payload = json.loads(html)
    except (TypeError, ValueError):
        return []

    team_code = TEAM_CODE_FROM_SOURCE_KEY.get(source_key)
    if not team_code:
        return []

    page_url = (metadata or {}).get("url", "")
    events = []
    seen_titles = set()
    for row in _iter_json_rows(payload):
        title = str(row.get("TITLE") or row.get("title") or "").strip()
        if len(title) < 4:
            continue
        if not _is_event_title(title, page_url):
            continue
        if title in seen_titles:
            continue

        published_at = _parse_date_text(
            str(
                row.get("PUB_DATE")
                or row.get("pubDate")
                or row.get("createdDate")
                or row.get("showDate")
                or row.get("created_date")
                or row.get("show_date")
                or row.get("date")
                or "",
            ),
        )
        if not published_at or published_at < cutoff_date:
            continue

        seen_titles.add(title)
        row_id = row.get("ID") or row.get("id")
        source_url = page_url
        if row_id:
            source_url = f"{page_url}#id={row_id}" if page_url else str(row_id)

        events.append(
            {
                "event_scope": "team",
                "team_id": EVENT_TEAM_NAME_MAP.get(team_code, team_code),
                "title": title[:300],
                "event_type": _classify_event(title),
                "published_at": published_at,
                "source_url": source_url,
                "last_seen_at": fetched_at,
                "status": "unknown",
            },
        )

    return events


def parse_team_events(html: str, source_key: str, metadata: dict | None = None) -> list[dict]:
    """Parses team events.

    Args:
        html: Html.
        source_key: Source Key.
        metadata: Metadata.

    Returns:
        List of results.

    """
    team_code = TEAM_CODE_FROM_SOURCE_KEY.get(source_key, "UNKNOWN")
    if team_code == "UNKNOWN":
        return []
    config = SOURCE_CONFIG_MAP.get(source_key, {"link_prefix": ""})
    page_url = (metadata or {}).get("url", "")
    cutoff_days = int((metadata or {}).get("cutoff_days", CUTOFF_DAYS))
    fetched_at = _parse_fetched_at(metadata)
    cutoff_date = fetched_at - timedelta(days=cutoff_days)

    json_events = _parse_json_team_events(html, source_key, metadata, cutoff_date, fetched_at)
    if json_events:
        return json_events

    soup = BeautifulSoup(html, "html.parser")
    title_sel = config.get("title_sel", "a")
    date_sel = config.get("date_sel", "")

    events = []
    seen_titles = set()
    title_tags = list(soup.select(title_sel))
    fallback_tags = [] if title_tags else list(soup.select("a[href]"))

    for title_tag in title_tags + fallback_tags:
        title = title_tag.get_text(" ", strip=True)
        if len(title) < 4:
            continue
        if not _is_event_title(title, page_url):
            continue
        if title in seen_titles:
            continue
        seen_titles.add(title)

        source_url = _extract_source_url(title_tag, config, page_url)
        published_at = _extract_published_at(title_tag, date_sel, cutoff_date)

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
                "last_seen_at": fetched_at,
                "status": "unknown",
            },
        )

    return events


if __name__ == "__main__":
    import sys

    html = (
        sys.stdin.read()
        if not sys.stdin.isatty()
        else (
            "<html><body><table><tr>"
            '<td><a class="subject" href="/notice/1">2025 시즌 이벤트 안내</a></td>'
            '<td><span class="date">2025-03-15</span></td>'
            "</tr></table></body></html>"
        )
    )
    result = parse_team_events(html, "lg_twins_events", {"cutoff_days": 90, "fetched_at": "2025-06-01T00:00:00"})
    for item in result:
        logger.info(item)
