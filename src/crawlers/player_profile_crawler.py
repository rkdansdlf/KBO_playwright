"""
KBO Player Profile Crawler (Enhanced)
Collects extended player profile: photo_url, bats, throws, salary, draft info, debut_year.
Source: KBO HitterDetail/PitcherDetail Basic.aspx
"""
import asyncio
import re
from typing import Dict, Optional

from playwright.async_api import Page
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.utils.safe_print import safe_print as print

# KBO profile page selectors (common across Hitter/Pitcher detail pages)
_PROFILE_ID = "cphContents_cphContents_cphContents_playerProfile"
_EXTRACT_JS = f"""
() => {{
    const $ = (id) => {{
        const el = document.getElementById(id);
        return el ? el.innerText.trim() : null;
    }};
    const photoEl = document.getElementById('{_PROFILE_ID}_imgProgile') || document.querySelector('.photo img');

    // Detect position/hand from profile text block
    const infoEl = document.querySelector('.player-info, .playerInfo, #cphContents_cphContents_cphContents_playerProfile');
    const rawText = infoEl ? infoEl.innerText : document.body.innerText;

    return {{
        photo_url:    photoEl ? (photoEl.src || photoEl.getAttribute('src')) : null,
        salary:       $('{_PROFILE_ID}_lblSalary'),
        signing:      $('{_PROFILE_ID}_lblPayment'),
        draft:        $('{_PROFILE_ID}_lblDraft'),
        debut:        $('{_PROFILE_ID}_lblJoinInfo') || $('{_PROFILE_ID}_lblEntryYear') || $('{_PROFILE_ID}_lblDebutYear'),
        raw_text:     rawText,
    }};
}}
"""

# Pitcher positions
PITCHER_POSITIONS = {"P", "투수"}
# CDN no-image sentinel
NO_IMAGE_SENTINEL = "no-Image.png"

HAND_MAP = {"우": "R", "좌": "L", "양": "S"}


def _parse_hands(text: str) -> Dict[str, Optional[str]]:
    """Parse throwing/batting hand from 포지션 텍스트 like '투수(우투우타)'."""
    result = {"bats": None, "throws": None}
    m = re.search(r'\((.)[투](.)타\)', text)
    if m:
        result["throws"] = HAND_MAP.get(m.group(1))
        result["bats"] = HAND_MAP.get(m.group(2))
    return result


def _parse_debut_year(text: Optional[str]) -> Optional[int]:
    """Extract 4-digit year from a text like '2015 두산' or '2015년'."""
    if not text:
        return None
    # Extract digits (2 to 4 digits)
    m = re.search(r'(\d{2,4})', text)
    if not m:
        return None
    
    year = int(m.group(1))
    if year < 100:
        # Assume 2000s for KBO entrants (founded 1982)
        return 2000 + year if year < 50 else 1900 + year
    return year


def _clean_photo_url(raw: Optional[str]) -> Optional[str]:
    """Return None for missing/default images."""
    if not raw or NO_IMAGE_SENTINEL in raw:
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

    HITTER_URL = "https://www.koreabaseball.com/Record/Player/HitterDetail/Basic.aspx"
    PITCHER_URL = "https://www.koreabaseball.com/Record/Player/PitcherDetail/Basic.aspx"

    def __init__(self, request_delay: float = 1.5, pool: Optional[AsyncPlaywrightPool] = None):
        self.request_delay = request_delay
        self.pool = pool

    def _select_url(self, player_id: str, position: Optional[str]) -> str:
        if position and (position.strip() in PITCHER_POSITIONS or
                         any(p in (position or "") for p in ["투수", "P"])):
            return f"{self.PITCHER_URL}?playerId={player_id}"
        return f"{self.HITTER_URL}?playerId={player_id}"

    async def crawl_player_profile(
        self,
        player_id: str,
        *,
        position: Optional[str] = None,
    ) -> Optional[Dict]:
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
            except Exception as e:
                print(f"❌ Profile crawl failed for {player_id}: {e}")
                return None
            finally:
                await pool.release(page)
        finally:
            if owns_pool:
                await pool.close()

    async def _fetch_profile(
        self, page: Page, player_id: str, position: Optional[str]
    ) -> Dict:
        url = self._select_url(player_id, position)
        print(f"📡 Fetching profile [{player_id}]: {url}")

        # domcontentloaded avoids networkidle timeout on KBO pages
        await page.goto(url, wait_until="domcontentloaded", timeout=20000)
        await page.wait_for_timeout(1500)
        await page.wait_for_selector(f"#{_PROFILE_ID}_lblName", timeout=5000)

        raw = await page.evaluate(_EXTRACT_JS)

        # If hitter page shows no data (wrong type), try pitcher URL instead
        if not raw.get("raw_text") or "선수명" not in (raw.get("raw_text") or ""):
            fallback = (
                f"{self.PITCHER_URL}?playerId={player_id}"
                if "HitterDetail" in url
                else f"{self.HITTER_URL}?playerId={player_id}"
            )
            await page.goto(fallback, wait_until="domcontentloaded", timeout=20000)
            await page.wait_for_timeout(1500)
            # Ensure the profile container is actually loaded
            await page.wait_for_selector(f"#{_PROFILE_ID}_lblName", timeout=5000)
            raw = await page.evaluate(_EXTRACT_JS)
        
        hands = _parse_hands(raw.get("raw_text") or "")

        return {
            "player_id": player_id,
            "photo_url": _clean_photo_url(raw.get("photo_url")),
            "bats": hands["bats"],
            "throws": hands["throws"],
            "debut_year": _parse_debut_year(raw.get("debut")),
            "salary_original": (raw.get("salary") or "").strip() or None,
            "signing_bonus_original": (raw.get("signing") or "").strip() or None,
            "draft_info": (raw.get("draft") or "").strip() or None,
        }


async def main():
    """Quick test for a known pitcher (임찬규 - LG, ID: 79171)"""
    crawler = PlayerProfileCrawler()
    result = await crawler.crawl_player_profile("79171", position="투수")
    if result:
        for k, v in result.items():
            print(f"  {k}: {v}")
    else:
        print("❌ No result")

if __name__ == "__main__":
    asyncio.run(main())
