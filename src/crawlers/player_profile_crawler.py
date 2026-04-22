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
from src.utils.throttle import throttle
from src.utils.compliance import compliance

# KBO profile page selectors (common across Hitter/Pitcher detail pages)
_PROFILE_ID_REG = "cphContents_cphContents_cphContents_playerProfile"
_PROFILE_ID_FUT = "cphContents_cphContents_cphContents_ucPlayerProfile"

_EXTRACT_JS = f"""
() => {{
    const prefixes = ['{_PROFILE_ID_REG}', '{_PROFILE_ID_FUT}'];
    let prefix = null;
    let name = "";
    
    for (const p of prefixes) {{
        const el = document.getElementById(p + "_lblName");
        if (el) {{
            prefix = p;
            name = el.innerText.trim();
            break;
        }}
    }}

    if (!prefix) return {{ error: "NO_PROFILE_ELEMENT" }};

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

    HITTER_URL = "https://www.koreabaseball.com/Record/Player/HitterDetail/Basic.aspx"
    PITCHER_URL = "https://www.koreabaseball.com/Record/Player/PitcherDetail/Basic.aspx"
    FUTURES_HITTER_URL = "https://www.koreabaseball.com/Futures/Player/HitterDetail.aspx"
    FUTURES_PITCHER_URL = "https://www.koreabaseball.com/Futures/Player/PitcherDetail.aspx"

    def __init__(self, request_delay: float = 1.2, pool: Optional[AsyncPlaywrightPool] = None):
        self.request_delay = request_delay
        self.pool = pool

    def _select_urls(self, player_id: str, position: Optional[str]) -> list[str]:
        """순차적으로 시도할 URL 후보 목록을 반환"""
        is_pitcher = position and (position.strip() in PITCHER_POSITIONS or
                                   any(p in (position or "") for p in ["투수", "P"]))
        
        if is_pitcher:
            return [
                f"{self.PITCHER_URL}?playerId={player_id}",
                f"{self.HITTER_URL}?playerId={player_id}",
                f"{self.FUTURES_PITCHER_URL}?playerId={player_id}",
                f"{self.FUTURES_HITTER_URL}?playerId={player_id}",
            ]
        else:
            return [
                f"{self.HITTER_URL}?playerId={player_id}",
                f"{self.PITCHER_URL}?playerId={player_id}",
                f"{self.FUTURES_HITTER_URL}?playerId={player_id}",
                f"{self.FUTURES_PITCHER_URL}?playerId={player_id}",
            ]

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
    ) -> Optional[Dict]:
        urls = self._select_urls(player_id, position)
        
        for url in urls:
            print(f"📡 Attempting profile [{player_id}]: {url}")
            if not await compliance.is_allowed(url):
                print(f"⚠️  BLOCKED by compliance: {url}")
                continue

            try:
                await throttle.wait()
                # domcontentloaded avoids networkidle timeout on KBO pages
                await page.goto(url, wait_until="domcontentloaded", timeout=15000)
                await page.wait_for_timeout(500)
                
                # Anchor on Name label to ensure profile is loaded
                await page.wait_for_selector(f'[id$="lblName"]', state="attached", timeout=5000)
                try:
                    # Wait for any image in the photo container to have a valid src
                    await page.wait_for_function(
                        """() => {
                            const img = document.querySelector('.player_info img, .playerInfo img, .photo img, [id*="imgPro"]');
                            return img && img.src && !img.src.includes('about:blank');
                        }""",
                        timeout=3000
                    )
                except:
                    pass
                
                raw = await page.evaluate(_EXTRACT_JS)
                
                if raw.get("error"):
                    continue
                    
                # If name is empty, this is a stub page. Try next URL if possible, 
                # but usually stub means no data on any of them.
                if not raw.get("name"):
                    print(f"⚠️  Stub profile detected at {url}")
                    continue
                
                # Success found data
                hands = _parse_hands(raw.get("raw_text") or "")
                photo_url = raw.get("photo_url") or raw.get("photo_attr")

                # If still emblem or no-Image, try heuristic CDN URL for newer players
                if not photo_url or "no-Image.png" in photo_url:
                    from datetime import datetime
                    year = datetime.now().year
                    # Standard pattern: https://6ptotvmi5753.edge.naverncp.com/KBO_IMAGE/person/middle/<year>/<player_id>.jpg
                    photo_url = f"https://6ptotvmi5753.edge.naverncp.com/KBO_IMAGE/person/middle/{year}/{player_id}.jpg"

                return {
                    "player_id": player_id,
                    "photo_url": _clean_photo_url(photo_url),
                    "bats": hands["bats"],
                    "throws": hands["throws"],
                    "debut_year": _parse_debut_year(raw.get("debut")),
                    "salary_original": (raw.get("salary") or "").strip() or None,
                    "signing_bonus_original": (raw.get("signing") or "").strip() or None,
                    "draft_info": (raw.get("draft") or "").strip() or None,
                }
            except Exception as e:
                print(f"   (Failed attempt at {url}: {e})")
                continue
        
        return None


async def main():
    """Quick test for a known problematic ID (900076)"""
    crawler = PlayerProfileCrawler()
    result = await crawler.crawl_player_profile("900076")
    if result:
        print("✅ Success:")
        for k, v in result.items():
            print(f"  {k}: {v}")
    else:
        print("❌ No result (Expected for stub/empty profiles)")

if __name__ == "__main__":
    asyncio.run(main())
