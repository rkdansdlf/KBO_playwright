"""
KBO Player Profile Crawler (Step 2)
Collects player profile information (basic info, physical stats, position)
"""
import asyncio
import time
from typing import Dict, Optional
from playwright.async_api import Page

from src.utils.status_parser import parse_status_from_text
from src.utils.playwright_pool import AsyncPlaywrightPool


class PlayerProfileCrawler:
    """선수 고유 ID(player_id)를 사용하여 KBO 공식 사이트에서
    선수의 상세 프로필 정보(기본 정보, 신체 정보, 경력 등)를 크롤링하는 클래스.
    """

    def __init__(self, request_delay: float = 1.5, pool: Optional[AsyncPlaywrightPool] = None):
        self.base_url = "https://www.koreabaseball.com/Record/Player/HitterDetail/Basic.aspx"
        self.request_delay = request_delay
        self.pool = pool

    async def crawl_player_profile(self, player_id: str) -> Optional[Dict]:
        """
        주어진 선수 ID에 대한 프로필 정보를 크롤링하는 메인 메서드.

        Args:
            player_id: KBO 선수 고유 ID

        Returns:
            선수 프로필 데이터가 담긴 딕셔너리. 오류 발생 시 None을 반환.
        """
        print(f"\n🔍 Crawling profile for player ID: {player_id}")

        pool = self.pool or AsyncPlaywrightPool(max_pages=1)
        owns_pool = self.pool is None
        await pool.start()
        try:
            page = await pool.acquire()
            try:
                profile_data = await self._fetch_profile(page, player_id)
                return profile_data
            except Exception as e:
                print(f"❌ Error crawling profile for {player_id}: {e}")
                return None
            finally:
                await pool.release(page)
        finally:
            if owns_pool:
                await pool.close()

    async def _fetch_profile(self, page: Page, player_id: str) -> Dict:
        """선수 프로필 페이지에 접속하여 데이터를 추출하는 내부 메서드."""
        url = f"{self.base_url}?playerId={player_id}"
        print(f"📡 Fetching: {url}")

        await page.goto(url, wait_until="networkidle", timeout=30000)
        # await asyncio.sleep(self.request_delay) # removed or minimized if networkidle is enough

        # Single DOM Read Optimization
        extraction_script = """
        () => {
            const profileArea = document.querySelector('.player-info, .playerInfo, #cphContents_cphContents_cphContents_playerProfile');
            return {
                profile_text: profileArea ? profileArea.innerText : "",
                body_text: document.body.innerText
            };
        }
        """
        data = await page.evaluate(extraction_script)
        profile_text = data['profile_text']
        body_text = data['body_text']

        # 각 섹션(기본, 신체, 경력)에서 정보를 추출하여 종합합니다.
        profile = {
            'player_id': player_id,
            'basic_info': self._parse_basic_info(profile_text),
            'physical_info': self._parse_physical_info(profile_text),
            'career_info': self._parse_career_info(profile_text),
            'status': None,
            'staff_role': None,
            'status_source': None,
        }

        parsed = parse_status_from_text(body_text)
        if parsed:
            status, staff_role = parsed
            profile['status'] = status
            profile['staff_role'] = staff_role
            profile['status_source'] = "profile"

        return profile

    def _parse_basic_info(self, text: str) -> Dict:
        """선수의 기본 정보(이름, 팀, 등번호, 포지션, 생년월일)를 추출합니다."""
        info = {
            'name': None,
            'team': None,
            'back_number': None,
            'position': None,
            'birth_date': None
        }

        if not text:
            return info

        lines = text.split('\n')
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

        return info

    def _parse_physical_info(self, text: str) -> Dict:
        """선수의 신체 정보(키, 몸무게, 투타유형)를 추출합니다."""
        info = {
            'height': None,
            'weight': None,
            'bat_hand': None,  # 타격 (우/좌/양)
            'throw_hand': None  # 투구 (우/좌)
        }

        if not text:
            return info

        lines = text.split('\n')
        for line in lines:
            line = line.strip()
            if '신장' in line or '키' in line or 'Height' in line:
                info['height'] = line.split(':')[-1].strip() if ':' in line else None
            elif '체중' in line or '몸무게' in line or 'Weight' in line:
                info['weight'] = line.split(':')[-1].strip() if ':' in line else None
            elif '투타' in line:
                value = line.split(':')[-1].strip() if ':' in line else None
                if value:
                    if '투' in value and '타' in value:
                        parts = value.split('타')
                        info['throw_hand'] = parts[0].replace('투', '').strip()
                        info['bat_hand'] = parts[1].strip() if len(parts) > 1 else None
        return info

    def _parse_career_info(self, text: str) -> Dict:
        """선수의 경력 정보(데뷔, 드래프트 정보 등)를 추출합니다."""
        info = {
            'debut_year': None,
            'draft_year': None,
            'draft_round': None,
            'career_summary': None
        }

        if not text:
            return info

        lines = text.split('\n')
        for line in lines:
            line = line.strip()
            if '입단' in line or '데뷔' in line or 'Debut' in line:
                info['debut_year'] = line.split(':')[-1].strip() if ':' in line else None
            elif '드래프트' in line or 'Draft' in line:
                value = line.split(':')[-1].strip() if ':' in line else None
                if value:
                    info['career_summary'] = value
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
