"""
KBO Schedule Crawler POC
Collects game IDs from the KBO schedule page
"""
import asyncio
import time
from datetime import datetime
from typing import List, Dict, Optional
from playwright.async_api import Page

from src.utils.team_codes import team_code_from_game_id_segment, resolve_team_code
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.utils.compliance import compliance


class ScheduleCrawler:
    """KBO 공식 사이트의 월별 경기 일정 페이지에서 경기 정보를 크롤링하는 클래스.

    주요 기능:
    - 특정 연도와 월에 해당하는 경기 일정 페이지에 접근합니다.
    - 페이지 내의 모든 경기 링크를 분석하여 고유 ID(gameId)를 추출합니다.
    - gameId를 바탕으로 경기 날짜, 홈/어웨이 팀 코드 등의 상세 정보를 파싱합니다.
    - 수집된 경기 정보 리스트를 반환합니다.
    """

    def __init__(self, request_delay: float = 1.5, pool: Optional[AsyncPlaywrightPool] = None):
        self.base_url = "https://www.koreabaseball.com/Schedule/Schedule.aspx"
        self.request_delay = request_delay
        self.pool = pool

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

        pool = self.pool or AsyncPlaywrightPool(max_pages=1)
        owns_pool = self.pool is None
        await pool.start()
        try:
            page = await pool.acquire()
            try:
                games = await self._crawl_month(page, year, month, series_id=series_id)
                print(f"✅ Found {len(games)} games")
                return games
            except Exception as e:
                print(f"❌ Error crawling schedule: {e}")
                return []
            finally:
                await pool.release(page)
        finally:
            if owns_pool:
                await pool.close()

    async def crawl_season(self, year: int, months: Optional[List[int]] = None, series_id: str = None) -> List[Dict]:
        """
        주어진 시즌의 여러 달에 걸쳐 경기 일정을 크롤링합니다.

        Args:
            year: 시즌 연도
            months: 크롤링할 월 목록 (기본값: 3월-10월)
            series_id: 시리즈 ID (옵션)
        """
        months = months or list(range(3, 11))
        all_games: List[Dict] = []

        pool = self.pool or AsyncPlaywrightPool(max_pages=1)
        owns_pool = self.pool is None
        await pool.start()
        try:
            page = await pool.acquire()
            try:
                for month in months:
                    month_games = await self._crawl_month(page, year, month, series_id=series_id)
                    all_games.extend(month_games)
                return all_games
            finally:
                await pool.release(page)
        finally:
            if owns_pool:
                await pool.close()


    async def _crawl_month(self, page: Page, year: int, month: int, series_id: str = None) -> List[Dict]:
        """특정 월의 경기 일정 페이지에서 정보를 추출합니다.
        series_id가 지정되지 않은 경우 전 시리즈(시범/정규/포스트)를 순회합니다.
        """
        if page.url != self.base_url:
            if not await compliance.is_allowed(self.base_url):
                print(f"[COMPLIANCE] Navigation to {self.base_url} aborted.")
                return []
            await page.goto(self.base_url, wait_until="networkidle", timeout=30000)
        
        # 1. 연도 및 월 선택 (Postback 발생 가능)
        await self._select_year_month(page, year, month)
        
        # 2. 시리즈 목록 확인
        all_series_options = await page.eval_on_selector_all(
            '#ddlSeries option', 
            'elements => elements.map(el => ({text: el.innerText, value: el.value}))'
        )
        
        target_series = [series_id] if series_id else [opt['value'] for opt in all_series_options if opt['value']]
        
        all_games = []
        seen_game_ids = set()
        
        for sid in target_series:
            print(f"[NAV] Selecting Series: {sid} for {year}-{month:02d}")
            try:
                await page.select_option('#ddlSeries', sid)
                await page.wait_for_load_state("networkidle", timeout=5000)
                await page.wait_for_timeout(500)
                
                month_games = await self._extract_games(page, year, month)
                for g in month_games:
                    gid = g.get('game_id')
                    if gid and gid not in seen_game_ids:
                        all_games.append(g)
                        seen_game_ids.add(gid)
            except Exception as e:
                print(f"[WARN] Error crawling series {sid}: {e}")
                
        return all_games

    async def _select_year_month(self, page: Page, year: int, month: int):
        """연도와 월 드롭다운을 선택하고 페이지 갱신을 기다립니다."""
        current_year = await page.eval_on_selector('#ddlYear', 'el => el.value')
        if current_year != str(year):
            await page.select_option('#ddlYear', str(year))
            await page.wait_for_load_state("networkidle", timeout=5000)
            await page.wait_for_timeout(500)
            
        current_month = await page.eval_on_selector('#ddlMonth', 'el => el.value')
        target_month_str = f"{month:02d}"
        if current_month != target_month_str:
            await page.select_option('#ddlMonth', target_month_str)
            await page.wait_for_load_state("networkidle", timeout=5000)
            await page.wait_for_timeout(500)

    async def _extract_games(self, page: Page, year: int, month: int) -> List[Dict]:
        """페이지에서 경기 관련 데이터를 추출합니다. (JS Fast Path)

        `gameId`가 포함된 모든 링크를 찾아, 각 링크에서 경기 ID, 날짜, 팀 정보 등을 파싱합니다.
        """
        
        # JS를 사용하여 모든 게임 정보를 한 번에 추출
        extraction_script = """
        (year) => {
            const results = [];
            const rows = document.querySelectorAll('.tbl tbody tr');
            let currentDateString = ""; // To handle rowspan or implicit date

            rows.forEach(tr => {
                // If it's a "No Game" row, skip
                if (tr.innerText.includes("데이터가 없습니다")) return;

                const cells = Array.from(tr.querySelectorAll('td'));
                if (cells.length < 3) return;

                // 1. Identify Date
                // Sometimes the date is in the first cell: "03.28(Sat)"
                // Or proper class might be used. 
                // Let's check the first cell's text.
                let firstCellText = cells[0].innerText.trim();
                let timeCellIndex = 1;
                let matchCellIndex = 2;
                let stadiumCellIndex = 7; // Default assumption based on sample

                // heuristic: Date like "03.28" or "03.28(토)"
                // If first cell matches date pattern, update currentDateString
                // Regex: DD.MM or MM.DD? KBO is usually MM.DD(Day)
                const dateMatch = firstCellText.match(/(\d{2})\.(\d{2})/);
                if (dateMatch) {
                    currentDateString = dateMatch[0]; // "03.28"
                    // If date cell exists, time is next
                } else {
                    // If no date in first cell, it might be rowspan'd from previous.
                    // But effectively in DOM traversal, if a cell is rowspan'd, it doesn't appear in subsequent rows?
                    // Actually, in `querySelectorAll` of TRs, if a TD has rowspan=5, it only appears in the FIRST TR.
                    // subsequent TRs will have fewer cells.
                    // So we must rely on `currentDateString` persistence.
                    // AND adjust indices. If date cell is missing, Time is likely at index 0.
                    
                    // Check if first cell is Time
                    if (/^\d{1,2}:\d{2}$/.test(firstCellText)) {
                        timeCellIndex = 0;
                        matchCellIndex = 1;
                        stadiumCellIndex = 6; // Shifted by 1?
                    }
                }
                
                if (!currentDateString) return; // Can't parse without date

                // 2. Extract Time
                const timeText = cells[timeCellIndex] ? cells[timeCellIndex].innerText.trim() : "";
                if (!/^\d{1,2}:\d{2}$/.test(timeText)) return; // Not a game row?

                // 3. Extract Matchup (Away vs Home)
                // Text: "KTvsLG" or "KT vs LG"
                const matchText = cells[matchCellIndex] ? cells[matchCellIndex].innerText.trim() : "";
                if (!matchText.includes("vs")) return;

                const teams = matchText.split("vs");
                if (teams.length !== 2) return;
                
                const awayName = teams[0].trim();
                const homeName = teams[1].trim();

                // 4. Extract Stadium (heuristic index)
                // Based on sample: ['03.28(토)\t14:00\tKTvsLG\t\t\t\t\t잠실\t-']
                // Split by tab shows many empty cells. 
                // Let's just find the cell that is NOT Time, NOT Match, and looks like Stadium.
                // Stadiums: 잠실, 문학, 대구, 창원, 대전, 고척, 광주, 사직, 수원
                let stadium = "";
                for (let i = matchCellIndex + 1; i < cells.length; i++) {
                    const txt = cells[i].innerText.trim();
                    if (txt.length >= 2 && txt.length <= 5 && !txt.includes("-") && !txt.includes("취소")) {
                         // Very rough heuristic
                         stadium = txt;
                         break;
                    }
                }
                
                // 5. Construct Game ID
                // Need standard team codes. The site uses Names (KT, LG, etc.).
                // We need to map Name -> Code. 
                // Ideally this mapping happens in Python, but we need Code to form GameID if we want it in JS.
                // OR we pass raw names to Python and Python constructs ID.
                // Let's pass raw names and let Python handle ID construction if ID is missing.
                
                // Let's check if there is a link
                const link = tr.querySelector('a[href*="gameId="]');
                if (link) {
                    // We already handled this in previous logic? 
                    // No, we are replacing the logic or augmenting it?
                    // The instruction says "Update the JS... to fallback".
                    // So I should keep the link logic or merge it.
                    // If link exists, we prefer it.
                    return; 
                }

                // If no link, we construct data
                // Date extraction: "03.28" -> "20260328"
                const [mm, dd] = currentDateString.split(".");
                const fullDate = `${year}${mm}${dd}`;

                results.push({
                    game_id: null,
                    game_date: fullDate,
                    season_year: year,
                    season_type: 'regular',
                    away_name: awayName,
                    home_name: homeName,
                    doubleheader_no: 0,
                    game_status: 'scheduled',
                    crawl_status: 'text_parsed',
                    url_suffix: '', 
                    game_time: timeText,
                    stadium: stadium
                });
            });

            // Original Link-based Logic (keep it for existing games)
            const linkSet = new Set();
            const links = document.querySelectorAll('a[href*="gameId="]');
            links.forEach(link => {
                const href = link.getAttribute('href');
                const match = href.match(/gameId=([^&]+)/);
                if (!match) return;
                const gameId = match[1];
                if (linkSet.has(gameId)) return;
                linkSet.add(gameId);
                
                // ... same parsing ...
                const gameDate = gameId.substring(0, 8);
                const awaySegment = gameId.length >= 10 ? gameId.substring(8, 10) : "";
                const homeSegment = gameId.length >= 12 ? gameId.substring(10, 12) : "";
                const doubleHeader = (!isNaN(parseInt(gameId.slice(-1)))) ? parseInt(gameId.slice(-1)) : 0;
                
                // Time/Stadium extraction from DOM (link parent)
                let gameTime = null;
                let stadium = null;
                try {
                    const cell = link.closest('td');
                    if (cell) {
                         const row = cell.parentElement; 
                         // Try to find time in the row
                         const cells = Array.from(row.querySelectorAll('td'));
                         cells.forEach(c => {
                            const t = c.innerText.trim();
                            if (/^\\d{1,2}:\\d{2}$/.test(t)) gameTime = t;
                         });
                         // Stadium...
                         // Reuse the loop or index logic?
                         // It's consistent with text-only logic.
                    }
                } catch(e) {}

                results.push({
                    game_id: gameId,
                    game_date: gameDate,
                    season_year: year,
                    season_type: 'regular',
                    away_segment: awaySegment, 
                    home_segment: homeSegment,
                    doubleheader_no: doubleHeader,
                    game_status: 'scheduled',
                    crawl_status: 'link_parsed',
                    url_suffix: href,
                    game_time: gameTime,
                    stadium: stadium
                });
            });

            return results;
        }
        """

        try:
            raw_games = await page.evaluate(extraction_script, year)
            games = []

            for g in raw_games:
                away_code = team_code_from_game_id_segment(g.get('away_segment'), year)
                home_code = team_code_from_game_id_segment(g.get('home_segment'), year)
                
                # Fallback Construction if game_id is missing (future games or link not found)
                if not g.get('game_id'):
                    away_name = g.get('away_name')
                    home_name = g.get('home_name')
                    # Pass 'year' to ensure history-aware resolution
                    away_code = resolve_team_code(away_name, year) or away_name
                    home_code = resolve_team_code(home_name, year) or home_name
                    
                    # KBO Website uses LEGACY codes in Game IDs.
                    # We must map our canonical codes (KH, DB, SSG, KIA) to KBO legacy (WO, OB, SK, HT).
                    KBO_LEGACY_CODES = {
                        "KH": "WO",  # Kiwoom -> Woori
                        "DB": "OB",  # Doosan -> OB
                        "SSG": "SK", # SSG -> SK (Wyverns)
                        "KIA": "HT", # KIA -> Haitai
                        "LT": "LT",
                        "LG": "LG",
                        "NC": "NC",
                        "HH": "HH",
                        "KT": "KT",
                        "SS": "SS"
                    }
                    
                    kbo_away_code = KBO_LEGACY_CODES.get(away_code, away_code)
                    kbo_home_code = KBO_LEGACY_CODES.get(home_code, home_code)

                    if g.get('game_date') and kbo_away_code and kbo_home_code:
                        # Construct ID: YYYYMMDD + AWAY + HOME + DH
                        dh = g.get('doubleheader_no', 0)
                        constructed_id = f"{g['game_date']}{kbo_away_code}{kbo_home_code}{dh}"
                        g['game_id'] = constructed_id                
                
                games.append({
                    'game_id': g['game_id'],
                    'game_date': g['game_date'],
                    'season_year': g['season_year'],
                    'season_type': g['season_type'],
                    'away_team_code': away_code,
                    'home_team_code': home_code,
                    'doubleheader_no': g['doubleheader_no'],
                    'game_status': g['game_status'],
                    'crawl_status': g['crawl_status'],
                    'game_time': g.get('game_time'),
                    'stadium': g.get('stadium'),
                    'url': f"https://www.koreabaseball.com{g['url_suffix']}" if g.get('url_suffix') and g['url_suffix'].startswith('/') else g.get('url_suffix')
                })
            
            
        except Exception as e:
            print(f"[WARN] Error extracting game (JS): {e}")
            return []
            
        if not games:
             # Debugging: Check if table exists or content
             content = await page.content()
             print(f"[DEBUG] No games found. Page content len: {len(content)}")
             if "gameId=" in content:
                 print("[DEBUG] 'gameId=' string FOUND in HTML but extraction failed.")
             else:
                 print("[DEBUG] 'gameId=' string NOT found in HTML.")
                 # Dump first few rows of the table to see structure
                 debug_script = """
                 () => {
                     const rows = document.querySelectorAll('.tbl tbody tr');
                     const data = [];
                     for(let i=0; i<Math.min(rows.length, 5); i++) {
                         data.push(rows[i].innerText);
                     }
                     return data;
                 }
                 """
                 try:
                     rows_text = await page.evaluate(debug_script)
                     print(f"[DEBUG] Table Rows Sample: {rows_text}")
                 except:
                     pass
                 
        return games

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
