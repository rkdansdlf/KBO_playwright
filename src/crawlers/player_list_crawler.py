"""
KBO Player List Crawler (Step 1)
Collects list of all players by team (hitters and pitchers)
"""
import asyncio
import time
from typing import List, Dict
from playwright.async_api import async_playwright, Page
from src.utils.safe_print import safe_print as print


class PlayerListCrawler:
    """KBO 공식 기록실에서 특정 시즌의 모든 타자와 투수 목록을 크롤링하는 클래스.
    
    주요 기능:
    - 지정된 시즌의 타자 및 투수 순위 페이지에 접근합니다.
    - 각 페이지의 선수 표에서 선수 이름, 팀, 고유 ID(playerId) 등의 기본 정보를 추출합니다.
    - 모든 선수 정보를 수집하여 딕셔너리 형태로 반환합니다.
    """

    def __init__(self, request_delay: float = 1.5):
        self.base_url = "https://www.koreabaseball.com/Record/Player/HitterBasic/Basic1.aspx"
        self.pitcher_url = "https://www.koreabaseball.com/Record/Player/PitcherBasic/Basic1.aspx"
        self.request_delay = request_delay

        # KBO team codes
        self.teams = {
            'LG': 'LG 트윈스',
            'KT': 'KT 위즈',
            'SK': 'SSG 랜더스',
            'NC': 'NC 다이노스',
            'OB': '두산 베어스',
            'HH': '한화 이글스',
            'LT': '롯데 자이언츠',
            'SK': 'SK 와이번스',
            'HT': 'KIA 타이거즈',
            'SS': '삼성 라이온즈'
        }

    async def crawl_all_players(self, season_year: int = 2024) -> Dict[str, List[Dict]]:
        """
        지정된 시즌의 모든 타자와 투수 정보를 크롤링하는 메인 메서드.

        Args:
            season_year: 크롤링할 시즌 연도 (기본값: 2024)

        Returns:
            타자(hitters)와 투수(pitchers) 목록이 담긴 딕셔너리.
        """
        print(f"\n🔍 Crawling all players for {season_year} season...")

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            try:
                # 타자와 투수 정보를 순차적으로 크롤링합니다.
                all_hitters = await self._crawl_hitters(page, season_year)
                all_pitchers = await self._crawl_pitchers(page, season_year)

                return {
                    'hitters': all_hitters,
                    'pitchers': all_pitchers,
                    'season_year': season_year
                }

            except Exception as e:
                print(f"❌ Error crawling players: {e}")
                return {'hitters': [], 'pitchers': [], 'season_year': season_year}
            finally:
                await browser.close()

    async def _crawl_hitters(self, page: Page, season_year: int) -> List[Dict]:
        """모든 타자 목록을 크롤링합니다."""
        print(f"\n📊 Crawling hitters...")
        url = f"{self.base_url}?gyear={season_year}"

        await page.goto(url, wait_until="networkidle", timeout=30000)
        await asyncio.sleep(self.request_delay)

        hitters = await self._extract_player_table(page, 'hitter')
        print(f"✅ Found {len(hitters)} hitters")

        return hitters

    async def _crawl_pitchers(self, page: Page, season_year: int) -> List[Dict]:
        """모든 투수 목록을 크롤링합니다."""
        print(f"\n📊 Crawling pitchers...")
        url = f"{self.pitcher_url}?gyear={season_year}"

        await page.goto(url, wait_until="networkidle", timeout=30000)
        await asyncio.sleep(self.request_delay)

        pitchers = await self._extract_player_table(page, 'pitcher')
        print(f"✅ Found {len(pitchers)} pitchers")

        return pitchers

    async def _extract_player_table(self, page: Page, player_type: str) -> List[Dict]:
        """페이지 내의 선수 정보 테이블에서 데이터를 추출합니다.

        Args:
            page: Playwright의 Page 객체.
            player_type: 선수 유형 ('hitter' 또는 'pitcher').

        Returns:
            추출된 선수 정보 딕셔너리의 리스트.
        """
        players = []

        try:
            # 선수 정보가 담긴 메인 테이블을 찾습니다.
            # KBO 사이트는 `tData01`, `tData02` 등 여러 클래스 이름을 사용하므로,
            # `div.record_result table`과 같이 더 신뢰성 있는 선택자를 사용합니다.
            tables = await page.query_selector_all('div.record_result table, table[summary*="선수"], table[class*="tData"]')

            if not tables:
                print(f"⚠️  No tables found for {player_type}")
                return players

            # 일반적으로 첫 번째 테이블이 메인 선수 목록입니다.
            main_table = tables[0]
            rows = await main_table.query_selector_all('tbody tr')

            for row in rows:
                try:
                    cells = await row.query_selector_all('td')
                    if len(cells) < 3:
                        continue

                    # 선수 프로필 링크에서 고유 ID(playerId)를 추출합니다.
                    player_link = await row.query_selector('a[href*="playerId"]')
                    player_id = None
                    if player_link:
                        href = await player_link.get_attribute('href')
                        if href and 'playerId=' in href:
                            player_id = href.split('playerId=')[1].split('&')[0]

                    # 각 셀의 텍스트 값을 추출합니다.
                    cell_values = []
                    for cell in cells:
                        text = await cell.inner_text()
                        cell_values.append(text.strip())

                    # 선수 이름이 없는 행은 건너뜁니다.
                    if not cell_values or len(cell_values) < 2:
                        continue

                    # 추출된 정보를 바탕으로 선수 딕셔너리를 생성합니다.
                    # 컬럼 순서: [순위, 선수명, 팀, ...]
                    player = {
                        'player_id': player_id,
                        'player_name': cell_values[1] if len(cell_values) > 1 else '',
                        'team': cell_values[2] if len(cell_values) > 2 else '',
                        'player_type': player_type,
                        'raw_data': cell_values
                    }

                    if player['player_name']:
                        players.append(player)

                except Exception as e:
                    print(f"⚠️  Error parsing player row: {e}")
                    continue

        except Exception as e:
            print(f"❌ Error extracting {player_type} table: {e}")

        return players


async def main():
    """Test the player list crawler"""
    crawler = PlayerListCrawler()

    # Crawl all players for 2024 season
    result = await crawler.crawl_all_players(season_year=2024)

    print(f"\n📊 Player List Summary:")
    print(f"  Total Hitters: {len(result['hitters'])}")
    print(f"  Total Pitchers: {len(result['pitchers'])}")

    if result['hitters']:
        print(f"\n  Sample Hitters:")
        for player in result['hitters'][:5]:
            print(f"    - {player['player_name']} ({player['team']}) [ID: {player['player_id']}]")

    if result['pitchers']:
        print(f"\n  Sample Pitchers:")
        for player in result['pitchers'][:5]:
            print(f"    - {player['player_name']} ({player['team']}) [ID: {player['player_id']}]")


if __name__ == "__main__":
    asyncio.run(main())
