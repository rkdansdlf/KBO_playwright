"""
KBO Schedule Crawler POC
Collects game IDs from the KBO schedule page
"""
import asyncio
import time
from datetime import datetime
from typing import List, Dict, Optional
from playwright.async_api import async_playwright, Page

from src.utils.team_codes import team_code_from_game_id_segment


class ScheduleCrawler:
    """KBO 공식 사이트의 월별 경기 일정 페이지에서 경기 정보를 크롤링하는 클래스.

    주요 기능:
    - 특정 연도와 월에 해당하는 경기 일정 페이지에 접근합니다.
    - 페이지 내의 모든 경기 링크를 분석하여 고유 ID(gameId)를 추출합니다.
    - gameId를 바탕으로 경기 날짜, 홈/어웨이 팀 코드 등의 상세 정보를 파싱합니다.
    - 수집된 경기 정보 리스트를 반환합니다.
    """

    def __init__(self, request_delay: float = 1.5):
        self.base_url = "https://www.koreabaseball.com/Schedule/Schedule.aspx"
        self.request_delay = request_delay

    async def crawl_schedule(self, year: int, month: int, series_id: str = None) -> List[Dict]:
        """
        지정된 연도와 월의 경기 일정을 크롤링하는 메인 메서드.

        Args:
            year: 시즌 연도 (예: 2024)
            month: 월 (1-12)
            series_id: 시리즈 ID (옵션)

        Returns:
            경기 정보 딕셔너리가 담긴 리스트.
        """
        print(f"🔍 Crawling schedule for {year}-{month:02d} (Series: {series_id})...")

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            try:
                games = await self._crawl_month(page, year, month, series_id=series_id)
                print(f"✅ Found {len(games)} games")
                return games
            except Exception as e:
                print(f"❌ Error crawling schedule: {e}")
                return []
            finally:
                await browser.close()

    async def crawl_season(self, year: int, months: Optional[List[int]] = None) -> List[Dict]:
        """
        주어진 시즌의 여러 달에 걸쳐 경기 일정을 크롤링합니다.

        Args:
            year: 시즌 연도
            months: 크롤링할 월 목록 (기본값: 3월-10월)
        """
        months = months or list(range(3, 11))
        all_games: List[Dict] = []

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            try:
                for month in months:
                    month_games = await self._crawl_month(page, year, month)
                    all_games.extend(month_games)
                return all_games
            finally:
                await browser.close()


    async def _crawl_month(self, page: Page, year: int, month: int, series_id: str = None) -> List[Dict]:
        """특정 월의 경기 일정 페이지에 접속하여 게임 정보를 추출합니다."""
        # 기본 페이지로 이동 (파라미터 없이)
        if page.url != self.base_url:
            await page.goto(self.base_url, wait_until="networkidle", timeout=30000)
        
        print(f"[NAV] Selecting Year: {year}, Month: {month}, Series: {series_id}")

        # 1. 연도 선택
        await page.select_option('#ddlYear', str(year))
        await asyncio.sleep(0.5)

        # 2. 월 선택 
        # (월 선택 -> 포스트백)
        await page.select_option('#ddlMonth', f"{month:02d}")
        try:
            await page.wait_for_timeout(500)
            await page.wait_for_load_state("networkidle", timeout=5000)
        except:
            pass
            
        # 3. 리그(Series) 선택 (옵션이 있는 경우에만)
        # series_id가 제공되면 선택. (예: "0,9,6" for Regular, "1" for Exhibition)
        if series_id:
            try:
                # 해당 값이 옵션에 있는지 확인
                option_exists = await page.eval_on_selector(f'#ddlSeries option[value="{series_id}"]', 'e => !!e')
                if option_exists:
                    await page.select_option('#ddlSeries', series_id)
                    # 시리즈 선택 -> 포스트백
                    try:
                        await page.wait_for_timeout(500)
                        await page.wait_for_load_state("networkidle", timeout=5000)
                    except:
                        pass
                else:
                    print(f"[WARN] Series option '{series_id}' not found for {year}-{month:02d}. Skipping series selection.")
            except Exception as e:
                print(f"[WARN] Error selecting series {series_id}: {e}")

        await asyncio.sleep(self.request_delay)
        
        return await self._extract_games(page, year, month)

    async def _extract_games(self, page: Page, year: int, month: int) -> List[Dict]:
        """페이지에서 경기 관련 데이터를 추출합니다.

        `gameId`가 포함된 모든 링크를 찾아, 각 링크에서 경기 ID, 날짜, 팀 정보 등을 파싱합니다.
        """
        games = []

        # `gameId` 파라미터가 포함된 모든 경기 링크를 찾습니다.
        game_links = await page.query_selector_all('a[href*="gameId="]')

        for link in game_links:
            try:
                href = await link.get_attribute('href')
                if not href or 'gameId=' not in href:
                    continue

                # URL에서 game_id를 추출합니다.
                game_id = self._extract_game_id(href)
                if not game_id:
                    continue

                # game_id 형식(YYYYMMDD...)을 바탕으로 날짜를 추출합니다.
                game_date = game_id[:8]

                # game_id에서 홈/어웨이 팀 코드를 추출합니다.
                away_segment = game_id[8:10] if len(game_id) >= 10 else None
                home_segment = game_id[10:12] if len(game_id) >= 12 else None

                games.append({
                    'game_id': game_id,
                    'game_date': game_date,
                    'season_year': year,
                    'season_type': 'regular', # 시즌 유형 (정규, 포스트시즌 등)
                    'away_team_code': team_code_from_game_id_segment(away_segment, year),
                    'home_team_code': team_code_from_game_id_segment(home_segment, year),
                    'doubleheader_no': int(game_id[-1]) if game_id[-1].isdigit() else 0, # 더블헤더 여부
                    'game_status': 'scheduled', # 경기 상태 (예정, 종료 등)
                    'crawl_status': 'pending', # 크롤링 상태
                    'url': f"https://www.koreabaseball.com{href}" if href.startswith('/') else href
                })

            except Exception as e:
                print(f"[WARN] Error extracting game: {e}")
                continue

        # game_id를 기준으로 중복된 경기 정보를 제거합니다.
        unique_games = {g['game_id']: g for g in games}
        return list(unique_games.values())

    def _extract_game_id(self, href: str) -> str:
        """URL(href)에서 game_id를 안전하게 추출합니다."""
        try:
            if 'gameId=' in href:
                game_id = href.split('gameId=')[1].split('&')[0]
                return game_id
        except:
            pass
        return ""


async def main():
    """Test the schedule crawler"""
    crawler = ScheduleCrawler()

    # Crawl current month schedule
    now = datetime.now()
    games = await crawler.crawl_schedule(now.year, now.month)

    print(f"\n📊 Schedule Summary:")
    print(f"Total games found: {len(games)}")

    if games:
        print(f"\n📝 First 5 games:")
        for game in games[:5]:
            print(f"  - {game['game_id']} | {game['game_date']}")


if __name__ == "__main__":
    asyncio.run(main())
