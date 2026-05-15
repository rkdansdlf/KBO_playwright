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
from src.utils.compliance import compliance
from src.utils.player_validation import validate_player_payload
from src.utils.request_policy import RequestPolicy

# KBO profile page selectors (common across Hitter/Pitcher detail pages)
_PROFILE_PREFIXES = [
    'cphContents_cphContents_cphContents_playerProfile',
    'cphContents_cphContents_cphContents_ucPlayerProfile',
    'cphContents_cphContents_cphContents_ucRetireInfo',
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


def _parse_height_weight(text: Optional[str]) -> Dict[str, Optional[int]]:
    """Parse height and weight from '185cm/92kg' format."""
    result = {"height_cm": None, "weight_kg": None}
    if not text:
        return result
    m = re.search(r"(\d+)\s*cm\s*/\s*(\d+)\s*kg", text)
    if m:
        result["height_cm"] = int(m.group(1))
        result["weight_kg"] = int(m.group(2))
    return result


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
    RETIRE_HITTER_URL = "https://www.koreabaseball.com/Record/Retire/Hitter.aspx"
    RETIRE_PITCHER_URL = "https://www.koreabaseball.com/Record/Retire/Pitcher.aspx"
    FUTURES_HITTER_URL = "https://www.koreabaseball.com/Futures/Player/HitterDetail.aspx"
    FUTURES_PITCHER_URL = "https://www.koreabaseball.com/Futures/Player/PitcherDetail.aspx"

    def __init__(self, request_delay: float = 1.2, pool: Optional[AsyncPlaywrightPool] = None):
        self.request_delay = request_delay
        self.pool = pool
        self.policy = RequestPolicy(min_delay=request_delay, max_delay=request_delay)
        self._last_failure_reason: dict[str, str] = {}

    def get_last_failure_reason(self, player_id: str) -> Optional[str]:
        return self._last_failure_reason.get(str(player_id))

    def _select_urls(self, player_id: str, position: Optional[str]) -> list[str]:
        """순차적으로 시도할 URL 후보 목록을 반환"""
        is_pitcher = position and (position.strip() in PITCHER_POSITIONS or
                                   any(p in (position or "") for p in ["투수", "P"]))

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
        last_reason = "profile_not_found"
        
        for url in urls:
            print(f"📡 Attempting profile [{player_id}]: {url}")
            if not await compliance.is_allowed(url):
                print(f"⚠️  BLOCKED by compliance: {url}")
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
                    print(f"⚠️  Stub profile detected at {url}")
                    last_reason = "profile_stub" if reason in {"missing_player_name", "unknown_player_name"} else (reason or "profile_stub")
                    continue
                
                # Success found data
                hands = _parse_hands(raw.get("raw_text") or "")
                hw = _parse_height_weight(raw.get("height_weight"))
                photo_url = raw.get("photo_url") or raw.get("photo_attr")

                # If still emblem or no-Image, try heuristic CDN URL for newer players
                if not photo_url or "no-Image.png" in photo_url:
                    from datetime import datetime
                    year = datetime.now().year
                    # Standard pattern: https://6ptotvmi5753.edge.naverncp.com/KBO_IMAGE/person/middle/<year>/<player_id>.jpg
                    photo_url = f"https://6ptotvmi5753.edge.naverncp.com/KBO_IMAGE/person/middle/{year}/{player_id}.jpg"

                result = {
                    "player_id": player_id,
                    "name": raw.get("name"),
                    "photo_url": _clean_photo_url(photo_url),
                    "bats": hands["bats"],
                    "throws": hands["throws"],
                    "height_cm": hw["height_cm"],
                    "weight_kg": hw["weight_kg"],
                    "debut_year": _parse_debut_year(raw.get("debut")),
                    "salary_original": (raw.get("salary") or "").strip() or None,
                    "signing_bonus_original": (raw.get("signing") or "").strip() or None,
                    "draft_info": (raw.get("draft") or "").strip() or None,
                }
                self._last_failure_reason.pop(str(player_id), None)
                return result
            except Exception as e:
                print(f"   (Failed attempt at {url}: {e})")
                last_reason = "selector_timeout"
                continue
        
        self._last_failure_reason[str(player_id)] = last_reason
        return None

    async def _load_profile_page(self, page: Page, url: str) -> Dict:
        await self.policy.delay_async(host="www.koreabaseball.com")
        # domcontentloaded avoids networkidle timeout on KBO pages
        await page.goto(url, wait_until="domcontentloaded", timeout=15000)

        # Wait for any name element to be attached
        try:
            await page.wait_for_selector('[id$="lblName"], .player_basic, .player_info', timeout=5000)
        except Exception:
            pass

        # Wait for potential AJAX content (especially for retired players)
        try:
            await page.wait_for_function(
                """() => {
                    const el = document.querySelector('[id$="lblName"], .player_basic .list02 li span, .player_info .name');
                    return el && el.innerText.trim().length > 0;
                }""",
                timeout=3000
            )
        except Exception:
            # Fallback to a small timeout if JS check fails
            await page.wait_for_timeout(500)

        return await page.evaluate(_EXTRACT_JS)


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
