"""
KBO Player Profile Crawler (Step 2)
Collects player profile information (basic info, physical stats, position)
"""
import asyncio
import time
from typing import Dict, Optional
from playwright.async_api import async_playwright, Page


class PlayerProfileCrawler:
    """Crawls detailed player profile information"""

    def __init__(self, request_delay: float = 1.5):
        self.base_url = "https://www.koreabaseball.com/Record/Player/HitterDetail/Basic.aspx"
        self.request_delay = request_delay

    async def crawl_player_profile(self, player_id: str) -> Optional[Dict]:
        """
        Crawl player profile for given player ID

        Args:
            player_id: KBO player ID

        Returns:
            Dictionary containing player profile data
        """
        print(f"\n🔍 Crawling profile for player ID: {player_id}")

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            try:
                profile_data = await self._fetch_profile(page, player_id)
                return profile_data

            except Exception as e:
                print(f"❌ Error crawling profile for {player_id}: {e}")
                return None
            finally:
                await browser.close()

    async def _fetch_profile(self, page: Page, player_id: str) -> Dict:
        """Fetch player profile page and extract data"""
        url = f"{self.base_url}?playerId={player_id}"
        print(f"📡 Fetching: {url}")

        await page.goto(url, wait_until="networkidle", timeout=30000)
        await asyncio.sleep(self.request_delay)

        profile = {
            'player_id': player_id,
            'basic_info': await self._extract_basic_info(page),
            'physical_info': await self._extract_physical_info(page),
            'career_info': await self._extract_career_info(page)
        }

        return profile

    async def _extract_basic_info(self, page: Page) -> Dict:
        """Extract basic player information (name, team, position, etc.)"""
        info = {
            'name': None,
            'team': None,
            'back_number': None,
            'position': None,
            'birth_date': None
        }

        try:
            # Look for player info section
            # KBO typically uses div.player-info or similar
            info_area = await page.query_selector('.player-info, .playerInfo, #cphContents_cphContents_cphContents_playerProfile')

            if info_area:
                text = await info_area.inner_text()
                lines = text.split('\n')

                # Parse player info from text
                for line in lines:
                    line = line.strip()
                    if '이름' in line or '선수명' in line:
                        info['name'] = line.split(':')[-1].strip() if ':' in line else None
                    elif '팀' in line or 'Team' in line:
                        info['team'] = line.split(':')[-1].strip() if ':' in line else None
                    elif '등번호' in line or '번호' in line:
                        info['back_number'] = line.split(':')[-1].strip() if ':' in line else None
                    elif '포지션' in line or 'Position' in line:
                        info['position'] = line.split(':')[-1].strip() if ':' in line else None
                    elif '생년월일' in line or '출생' in line:
                        info['birth_date'] = line.split(':')[-1].strip() if ':' in line else None

                print(f"✅ Extracted basic info: {info['name']}")

        except Exception as e:
            print(f"⚠️  Error extracting basic info: {e}")

        return info

    async def _extract_physical_info(self, page: Page) -> Dict:
        """Extract physical information (height, weight, bat/throw)"""
        info = {
            'height': None,
            'weight': None,
            'bat_hand': None,  # 타격 (우/좌/양)
            'throw_hand': None  # 투구 (우/좌)
        }

        try:
            # Physical info is usually in the same area as basic info
            info_area = await page.query_selector('.player-info, .playerInfo, #cphContents_cphContents_cphContents_playerProfile')

            if info_area:
                text = await info_area.inner_text()
                lines = text.split('\n')

                for line in lines:
                    line = line.strip()
                    if '신장' in line or '키' in line or 'Height' in line:
                        info['height'] = line.split(':')[-1].strip() if ':' in line else None
                    elif '체중' in line or '몸무게' in line or 'Weight' in line:
                        info['weight'] = line.split(':')[-1].strip() if ':' in line else None
                    elif '투타' in line:
                        # Format: "투타: 우투우타" or "좌투좌타"
                        value = line.split(':')[-1].strip() if ':' in line else None
                        if value:
                            if '투' in value and '타' in value:
                                parts = value.split('타')
                                info['throw_hand'] = parts[0].replace('투', '').strip()
                                info['bat_hand'] = parts[1].strip() if len(parts) > 1 else None

                print(f"✅ Extracted physical info")

        except Exception as e:
            print(f"⚠️  Error extracting physical info: {e}")

        return info

    async def _extract_career_info(self, page: Page) -> Dict:
        """Extract career information (debut, draft, etc.)"""
        info = {
            'debut_year': None,
            'draft_year': None,
            'draft_round': None,
            'career_summary': None
        }

        try:
            # Career info section
            career_area = await page.query_selector('.career-info, .careerInfo, #cphContents_cphContents_cphContents_playerProfile')

            if career_area:
                text = await career_area.inner_text()
                lines = text.split('\n')

                for line in lines:
                    line = line.strip()
                    if '입단' in line or '데뷔' in line or 'Debut' in line:
                        info['debut_year'] = line.split(':')[-1].strip() if ':' in line else None
                    elif '드래프트' in line or 'Draft' in line:
                        value = line.split(':')[-1].strip() if ':' in line else None
                        if value:
                            info['career_summary'] = value

                print(f"✅ Extracted career info")

        except Exception as e:
            print(f"⚠️  Error extracting career info: {e}")

        return info


async def main():
    """Test the player profile crawler"""
    crawler = PlayerProfileCrawler()

    # Test with a known player ID
    # Example: 79171 (임찬규 - LG)
    test_player_id = "79171"

    profile = await crawler.crawl_player_profile(test_player_id)

    if profile:
        print(f"\n📊 Player Profile:")
        print(f"  Player ID: {profile['player_id']}")
        print(f"\n  Basic Info:")
        for key, value in profile['basic_info'].items():
            print(f"    {key}: {value}")
        print(f"\n  Physical Info:")
        for key, value in profile['physical_info'].items():
            print(f"    {key}: {value}")
        print(f"\n  Career Info:")
        for key, value in profile['career_info'].items():
            print(f"    {key}: {value}")


if __name__ == "__main__":
    asyncio.run(main())
