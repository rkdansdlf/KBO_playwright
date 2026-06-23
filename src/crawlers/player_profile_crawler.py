"""
KBO Player Profile Crawler (Enhanced)
Collects extended player profile: photo_url, bats, throws, salary, draft info, debut_year.
Source: KBO HitterDetail/PitcherDetail Basic.aspx
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import re
from datetime import datetime
from typing import Any

from playwright.async_api import Error as PlaywrightError
from playwright.async_api import Page

from src.constants import KST

logger = logging.getLogger(__name__)

from src.urls import HITTER_DETAIL, PITCHER_DETAIL
from src.utils.compliance import compliance
from src.utils.player_validation import validate_player_payload
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.utils.playwright_retry import NAV_TIMEOUT, SEL_TIMEOUT
from src.utils.request_policy import RequestPolicy

# KBO profile page selectors (common across Hitter/Pitcher detail pages)
_PROFILE_PREFIXES = [
    "cphContents_cphContents_cphContents_playerProfile",
    "cphContents_cphContents_cphContents_ucPlayerProfile",
    "cphContents_cphContents_cphContents_ucRetireInfo",
]

_EXTRACT_JS = f"""
() => {{
    const prefixes = {list(_PROFILE_PREFIXES)};
    let prefix = null;
    let name = "";

    for (const p of prefixes) {{
        const el = document.getElementById(p + "_lblName");
        if (el && el.innerText.trim()) {{
            prefix = p;
            name = el.innerText.trim();
            break;
        }}
    }}

    // Fallback for some retired pages where name is in a different place
    if (!name) {{
        const nameEl = document.querySelector('.player_basic .list02 li span, .player_info .name');
        if (nameEl) name = nameEl.innerText.trim();
    }}

    // Last resort: Title (format: "Name | Position | ...")
    if (!name) {{
        const title = document.title;
        if (title && title.includes('|')) {{
            name = title.split('|')[0].trim();
        }}
    }}

    if (!name && !prefix) return {{ error: "NO_PROFILE_ELEMENT" }};
    if (!prefix) prefix = prefixes[0]; // fallback prefix for getVal

    const getVal = (suffix) => {{
        const el = document.getElementById(prefix + "_" + suffix);
        return el ? el.innerText.trim() : null;
    }};

    // Wait for img profile element to have a real source
    const photoSelector = '#' + prefix + '_imgProfile, #' + prefix + '_imgProgile';
    let photoEl = document.querySelector(photoSelector);

    const infoEl = document.querySelector('.player-info, .playerInfo, #' + prefix);
    const rawText = infoEl ? infoEl.innerText : document.body.innerText;

    let photoUrl = photoEl ? (photoEl.src || photoEl.getAttribute('src')) : null;

    // Final check for the specific person image pattern if the ID-based one is not found or is default
    if (!photoUrl || photoUrl.includes('no-Image.png') || photoUrl.includes('emblem')) {{
        const personImg = document.querySelector('.photo img[src*="person"], .photo img[src*="player"]');
        if (personImg) photoUrl = personImg.src || personImg.getAttribute('src');
    }}

    // If still emblem, null it out
    if (photoUrl && photoUrl.includes('emblem')) photoUrl = null;

    return {{
        name:         name,
        photo_url:    photoUrl,
        photo_attr:   photoEl ? photoEl.getAttribute('src') : null,
        salary:       getVal('lblSalary'),
        signing:      getVal('lblPayment'),
        draft:        getVal('lblDraft'),
        debut:        getVal('lblJoinInfo') || getVal('lblEntryYear') || getVal('lblDebutYear'),
        height_weight: getVal('lblHeightWeight'),
        raw_text:     rawText,
    }};
}}
"""

# Pitcher positions
PITCHER_POSITIONS = {"P", "투수"}
# CDN no-image sentinel
NO_IMAGE_SENTINEL = "no-Image.png"

HAND_MAP = {"우": "R", "좌": "L", "양": "S"}
PROFILE_CRAWL_EXCEPTIONS = (
    PlaywrightError,
    TimeoutError,
    RuntimeError,
    ValueError,
    TypeError,
    KeyError,
    IndexError,
    OSError,
)


def _parse_hands(text: str) -> dict[str, str | None]:
    """Parse throwing/batting hand from 포지션 텍스트 like '투수(우투우타)'."""
    result = {"bats": None, "throws": None}
    m = re.search(r"\((.)[투](.)타\)", text)
    if m:
        result["throws"] = HAND_MAP.get(m.group(1))
        result["bats"] = HAND_MAP.get(m.group(2))
    return result


def _parse_debut_year(text: str | None) -> int | None:
    """Extract 4-digit year from a text like '2015 두산' or '2015년'."""
    if not text:
        return None
    # Extract digits (2 to 4 digits)
    m = re.search(r"(\d{2,4})", text)
    if not m:
        return None

    year = int(m.group(1))
    if year < 100:
        # Assume 2000s for KBO entrants (founded 1982)
        return 2000 + year if year < 50 else 1900 + year
    return year


def _parse_height_weight(text: str | None) -> dict[str, int | None]:
    """Parse height and weight from '185cm/92kg' format."""
    result = {"height_cm": None, "weight_kg": None}
    if not text:
        return result
    m = re.search(r"(\d+)\s*cm\s*/\s*(\d+)\s*kg", text)
    if m:
        result["height_cm"] = int(m.group(1))
        result["weight_kg"] = int(m.group(2))
    return result


def _clean_photo_url(raw: str | None) -> str | None:
    """Return None for missing/default images."""
    if not raw or NO_IMAGE_SENTINEL in raw:
        return None
    # Avoid local about:blank or data urls
    if not raw.startswith("http") and not raw.startswith("//"):
        return None
    # Ensure absolute URL
    if raw.startswith("//"):
        return "https:" + raw
    return raw


class PlayerProfileCrawler:
    """
    선수 고유 ID를 사용하여 KBO 공식 사이트에서 상세 프로필을 크롤링.
    타자/투수 페이지를 포지션 기준으로 자동 선택.
    """

    HITTER_URL = HITTER_DETAIL
    PITCHER_URL = PITCHER_DETAIL
    RETIRE_HITTER_URL = "https://www.koreabaseball.com/Record/Retire/Hitter.aspx"
    RETIRE_PITCHER_URL = "https://www.koreabaseball.com/Record/Retire/Pitcher.aspx"
    FUTURES_HITTER_URL = "https://www.koreabaseball.com/Futures/Player/HitterDetail.aspx"
    FUTURES_PITCHER_URL = "https://www.koreabaseball.com/Futures/Player/PitcherDetail.aspx"

    def __init__(self, request_delay: float = 1.2, pool: AsyncPlaywrightPool | None = None) -> None:
        self.request_delay = request_delay
        self.pool = pool
        self.policy = RequestPolicy.with_delay(request_delay, request_delay)
        self._last_failure_reason: dict[str, str] = {}

    def get_last_failure_reason(self, player_id: str) -> str | None:
        return self._last_failure_reason.get(str(player_id))

    def _select_urls(self, player_id: str, position: str | None) -> list[str]:
        """순차적으로 시도할 URL 후보 목록을 반환"""
        is_pitcher = position and (
            position.strip() in PITCHER_POSITIONS or any(p in (position or "") for p in ["투수", "P"])
        )

        candidates = []
        if is_pitcher:
            candidates = [
                f"{self.PITCHER_URL}?playerId={player_id}",
                f"{self.HITTER_URL}?playerId={player_id}",
                f"{self.RETIRE_PITCHER_URL}?playerId={player_id}",
                f"{self.RETIRE_HITTER_URL}?playerId={player_id}",
                f"{self.FUTURES_PITCHER_URL}?playerId={player_id}",
                f"{self.FUTURES_HITTER_URL}?playerId={player_id}",
            ]
        else:
            candidates = [
                f"{self.HITTER_URL}?playerId={player_id}",
                f"{self.PITCHER_URL}?playerId={player_id}",
                f"{self.RETIRE_HITTER_URL}?playerId={player_id}",
                f"{self.RETIRE_PITCHER_URL}?playerId={player_id}",
                f"{self.FUTURES_HITTER_URL}?playerId={player_id}",
                f"{self.FUTURES_PITCHER_URL}?playerId={player_id}",
            ]
        return candidates

    async def crawl_player_profile(
        self,
        player_id: str,
        *,
        position: str | None = None,
    ) -> dict[str, Any] | None:
        """
        Crawl the profile detail page for player_id.
        Returns a dict with photo_url, bats, throws, debut_year,
        salary_original, signing_bonus_original, draft_info.
        """
        pool = self.pool or AsyncPlaywrightPool(max_pages=1)
        owns_pool = self.pool is None
        await pool.start()
        try:
            page = await pool.acquire()
            try:
                return await self._fetch_profile(page, player_id, position)
            except PROFILE_CRAWL_EXCEPTIONS:
                logger.exception("❌ Profile crawl failed for %s", player_id)
                return None
            finally:
                await pool.release(page)
        finally:
            if owns_pool:
                await pool.close()

    async def _fetch_profile(self, page: Page, player_id: str, position: str | None) -> dict[str, Any] | None:
        urls = self._select_urls(player_id, position)
        last_reason = "profile_not_found"

        for url in urls:
            logger.info("📡 Attempting profile [%s]: %s", player_id, url)
            if not await compliance.is_allowed(url):
                logger.warning("⚠️  BLOCKED by compliance: %s", url)
                last_reason = "blocked"
                continue

            try:
                raw = await self.policy.run_with_retry_async(
                    self._load_profile_page,
                    page,
                    url,
                )

                if raw.get("error"):
                    last_reason = "profile_element_missing"
                    continue

                # If name is empty, this is a stub page. Try next URL if possible,
                # but usually stub means no data on any of them.
                ok, reason = validate_player_payload({"player_id": player_id, "name": raw.get("name")})
                if not ok:
                    logger.warning("⚠️  Stub profile detected at %s", url)
                    last_reason = (
                        "profile_stub"
                        if reason in {"missing_player_name", "unknown_player_name"}
                        else (reason or "profile_stub")
                    )
                    continue

                # Success found data
                from src.parsers.player_profile_parser import parse_profile

                parsed = parse_profile(_profile_raw_text(raw))
                result = _build_profile_result(player_id, raw, parsed)
                self._last_failure_reason.pop(str(player_id), None)
            except PROFILE_CRAWL_EXCEPTIONS:
                logger.exception("   (Failed attempt at %s)", url)
                last_reason = "selector_timeout"
                continue
            else:
                return result

        self._last_failure_reason[str(player_id)] = last_reason
        return None

    async def _load_profile_page(self, page: Page, url: str) -> dict[str, Any]:
        await self.policy.delay_async(host="www.koreabaseball.com")
        # domcontentloaded avoids networkidle timeout on KBO pages
        await page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)

        # Wait for any name element to be attached
        with contextlib.suppress(PlaywrightError, TimeoutError):
            await page.wait_for_selector('[id$="lblName"], .player_basic, .player_info', timeout=SEL_TIMEOUT)

        # Wait for potential AJAX content or image source update
        try:
            # Look for non-placeholder images in the photo div
            await page.wait_for_function(
                """() => {
                    const img = document.querySelector('.photo img');
                    return img && img.src && !img.src.includes('no-Image.png') && !img.src.includes('about:blank');
                }""",
                timeout=2000,
            )
        except (PlaywrightError, TimeoutError):
            logger.info("No real image found for player (expected for some players)")

        return await page.evaluate(_EXTRACT_JS)


def _profile_raw_text(raw: dict[str, Any]) -> str:
    raw_text = raw.get("raw_text") or ""
    field_labels = {
        "name": "선수명",
        "salary": "연봉",
        "signing": "입단 계약금",
        "draft": "지명순위",
        "debut": "입단년도",
        "height_weight": "신장/체중",
    }
    for key, label in field_labels.items():
        if label not in raw_text and raw.get(key):
            raw_text += f" {label}: {raw[key]}"
    return raw_text


def _profile_photo_url(raw: dict[str, Any], player_id: str) -> str | None:
    photo_url = raw.get("photo_url") or raw.get("photo_attr")
    if photo_url and "no-Image.png" not in photo_url:
        return str(photo_url)
    curr_year = datetime.now(KST).year
    return f"https://6ptotvmi5753.edge.naverncp.com/KBO_IMAGE/person/middle/{curr_year}/{player_id}.jpg"


def _build_profile_result(player_id: str, raw: dict[str, Any], parsed: dict[str, Any]) -> dict[str, Any]:
    hands = _parse_hands(raw.get("raw_text") or "")
    height_weight = _parse_height_weight(raw.get("height_weight"))
    return {
        "player_id": player_id,
        "name": raw.get("name") or parsed.get("player_name"),
        "photo_url": _clean_photo_url(_profile_photo_url(raw, player_id)),
        "bats": parsed.get("batting_hand") or hands["bats"],
        "throws": parsed.get("throwing_hand") or hands["throws"],
        "height_cm": parsed.get("height_cm") or height_weight["height_cm"],
        "weight_kg": parsed.get("weight_kg") or height_weight["weight_kg"],
        "debut_year": parsed.get("entry_year") or _parse_debut_year(raw.get("debut")),
        "salary_original": parsed.get("salary_original") or (raw.get("salary") or "").strip() or None,
        "signing_bonus_original": parsed.get("signing_bonus_original") or (raw.get("signing") or "").strip() or None,
        "draft_info": parsed.get("draft_info") or (raw.get("draft") or "").strip() or None,
        "salary_amount": parsed.get("salary_amount"),
        "salary_currency": parsed.get("salary_currency"),
        "signing_bonus_amount": parsed.get("signing_bonus_amount"),
        "signing_bonus_currency": parsed.get("signing_bonus_currency"),
        "draft_year": parsed.get("draft_year"),
        "draft_round": parsed.get("draft_round"),
        "draft_pick_overall": parsed.get("draft_pick_overall"),
        "draft_type": parsed.get("draft_type"),
        "education_path": parsed.get("education_path"),
    }


async def main() -> None:
    """Quick test for a known problematic ID (900076)"""
    crawler = PlayerProfileCrawler()
    result = await crawler.crawl_player_profile("900076")
    if result:
        logger.info("✅ Success:")
        for k, v in result.items():
            logger.info("  %s: %s", k, v)
    else:
        logger.error("❌ No result (Expected for stub/empty profiles)")


if __name__ == "__main__":
    asyncio.run(main())
