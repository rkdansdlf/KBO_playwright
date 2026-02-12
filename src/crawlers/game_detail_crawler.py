"""GameCenter box score crawler with structured outputs."""
from __future__ import annotations

import asyncio
import os
import re
from typing import Dict, List, Optional, Any
from urllib.parse import urljoin, urlparse, parse_qs

from playwright.async_api import Page

from src.utils.team_codes import resolve_team_code, team_code_from_game_id_segment
from src.utils.playwright_pool import AsyncPlaywrightPool


HITTER_HEADER_MAP = {
    "타석": "plate_appearances",
    "타수": "at_bats",
    "득점": "runs",
    "안타": "hits",
    "2루타": "doubles",
    "3루타": "triples",
    "홈런": "home_runs",
    "타점": "rbi",
    "볼넷": "walks",
    "고의4구": "intentional_walks",
    "사구": "hbp",
    "삼진": "strikeouts",
    "도루": "stolen_bases",
    "도실": "caught_stealing",
    "희타": "sacrifice_hits",
    "희비": "sacrifice_flies",
    "병살": "gdp",
    "타율": "avg",
    "출루율": "obp",
    "장타율": "slg",
    "OPS": "ops",
    "ISO": "iso",
    "BABIP": "babip",
}


PITCHER_HEADER_MAP = {
    "이닝": "innings",
    "타자": "batters_faced",
    "투구수": "pitches",
    "피안타": "hits_allowed",
    "실점": "runs_allowed",
    "자책": "earned_runs",
    "피홈런": "home_runs_allowed",
    "볼넷": "walks_allowed",
    "삼진": "strikeouts",
    "사구": "hit_batters",
    "폭투": "wild_pitches",
    "보크": "balks",
    "승": "wins",
    "패": "losses",
    "세": "saves",
    "홀드": "holds",
    "ERA": "era",
    "WHIP": "whip",
    "K/9": "k_per_nine",
    "BB/9": "bb_per_nine",
    "K/BB": "kbb",
}


HITTER_FLOAT_KEYS = {"avg", "obp", "slg", "ops", "iso", "babip"}
PITCHER_FLOAT_KEYS = {"era", "whip", "fip", "k_per_nine", "bb_per_nine", "kbb"}


class GameDetailCrawler:
    """Crawl KBO GameCenter review pages and return structured box score data."""

    def __init__(
        self,
        request_delay: float = 1.5,
        resolver: Optional[Any] = None,
        pool: Optional[AsyncPlaywrightPool] = None,
    ):
        self.base_url = "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx"
        self.request_delay = request_delay
        self.resolver = resolver
        self.pool = pool

    async def crawl_game(self, game_id: str, game_date: str) -> Optional[Dict[str, Any]]:
        result = await self.crawl_games([{"game_id": game_id, "game_date": game_date}])
        return result[0] if result else None

    async def crawl_games(
        self,
        games: List[Dict[str, str]],
        concurrency: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        if not games:
            return []

        max_concurrency = concurrency or int(os.getenv("KBO_GAME_DETAIL_CONCURRENCY", "3"))
        max_concurrency = max(1, min(max_concurrency, len(games)))

        pool = self.pool or AsyncPlaywrightPool(max_pages=max_concurrency)
        owns_pool = self.pool is None
        if self.pool:
            max_concurrency = min(max_concurrency, self.pool.max_pages)

        results: List[Optional[Dict[str, Any]]] = [None] * len(games)
        await pool.start()
        try:
            queue: asyncio.Queue[Optional[tuple[int, Dict[str, str]]]] = asyncio.Queue()
            for idx, entry in enumerate(games):
                queue.put_nowait((idx, entry))
            for _ in range(max_concurrency):
                queue.put_nowait(None)

            async def worker() -> None:
                page = await pool.acquire()
                try:
                    while True:
                        item = await queue.get()
                        if item is None:
                            queue.task_done()
                            break
                        idx, entry = item
                        game_id = entry["game_id"]
                        game_date = entry["game_date"]
                        try:
                            payload = await self._crawl_single(page, game_id, game_date)
                            results[idx] = payload
                        except Exception as exc:  # pragma: no cover - resilience path
                            print(f"❌ Error crawling {game_id}: {exc}")
                        finally:
                            queue.task_done()
                finally:
                    await pool.release(page)

            workers = [asyncio.create_task(worker()) for _ in range(max_concurrency)]
            await queue.join()
            await asyncio.gather(*workers, return_exceptions=True)
        finally:
            if owns_pool:
                await pool.close()

        return [payload for payload in results if payload]

    async def _crawl_single(self, page: Page, game_id: str, game_date: str) -> Optional[Dict[str, Any]]:
        # 1. Fetch Lineup to get Roster Map (Player Name -> ID, Uniform)
        lineup_url = f"{self.base_url}?gameId={game_id}&gameDate={game_date}&section=LINEUP"
        print(f"📡 Fetching Lineup: {lineup_url}")
        
        roster_map = {}
        try:
            await page.goto(lineup_url, wait_until="domcontentloaded", timeout=30000)
            # Wait for lineup to ensure it's loaded (some async loading might happen)
            # We assume if domcontentloaded is done, we can try extracting. 
            # If KBO loads lineup via AJAX, we might need a wait.
            # Let's try waiting for a known element, or just a small sleep if unsure.
            await asyncio.sleep(0.5) 
            roster_map = await self._extract_roster_from_lineup(page)
            print(f"   ✅ Extracted {len(roster_map)} players from Lineup")
        except Exception as e:
            print(f"⚠️  Error fetching lineup for {game_id}: {e}")

        # 2. Fetch Review (Box Score)
        url = f"{self.base_url}?gameId={game_id}&gameDate={game_date}&section=REVIEW"
        print(f"📡 Fetching BoxScore: {url}")

        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await self._wait_for_boxscore(page)
        await asyncio.sleep(self.request_delay)

        season_year = self._parse_season_year(game_date)
        team_info = await self._extract_team_info(page, game_id, season_year)
        metadata = await self._extract_metadata(page)
        
        # New: Extract Game Summary
        game_summary = await self._extract_game_summary(page)

        hitters = {
            'away': await self._extract_hitters(page, 'away', team_info['away']['code'], season_year, roster_map),
            'home': await self._extract_hitters(page, 'home', team_info['home']['code'], season_year, roster_map),
        }
        pitchers = {
            'away': await self._extract_pitchers(page, 'away', team_info['away']['code'], season_year, roster_map),
            'home': await self._extract_pitchers(page, 'home', team_info['home']['code'], season_year, roster_map),
        }

        game_data = {
            'game_id': game_id,
            'game_date': game_date,
            'metadata': metadata,
            'summary': game_summary, # Add to payload
            'teams': team_info,
            'home_team_code': team_info['home']['code'],
            'away_team_code': team_info['away']['code'],
            'hitters': hitters,
            'pitchers': pitchers,
        }

        return game_data

    async def _wait_for_boxscore(self, page: Page) -> None:
        await page.wait_for_selector('#tblAwayHitter1, #tblHomeHitter1', timeout=10000)

    async def _extract_metadata(self, page: Page) -> Dict[str, Any]:
        metadata = {
            'stadium': None,
            'attendance': None,
            'start_time': None,
            'end_time': None,
            'game_time': None,
            'duration_minutes': None,
        }

        try:
            info_area = await page.query_selector('.box-score-area, .game-info, .score-board')
            if not info_area:
                return metadata

            text = (await info_area.inner_text()).replace('\n', ' ')

            stadium_match = re.search(r'구장\s*[:：]\s*([^\s]+)', text)
            if stadium_match:
                metadata['stadium'] = stadium_match.group(1).strip()

            attendance_match = re.search(r'관중\s*[:：]\s*([\d,]+)', text)
            if attendance_match:
                try:
                    metadata['attendance'] = int(attendance_match.group(1).replace(',', '').strip())
                except ValueError:
                    pass

            start_match = re.search(r'개시\s*[:：]\s*([\d:]+)', text)
            if start_match:
                metadata['start_time'] = start_match.group(1).strip()

            end_match = re.search(r'종료\s*[:：]\s*([\d:]+)', text)
            if end_match:
                metadata['end_time'] = end_match.group(1).strip()

            duration_match = re.search(r'경기시간\s*[:：]\s*([\d:]+)', text)
            if duration_match:
                metadata['game_time'] = duration_match.group(1).strip()
                metadata['duration_minutes'] = self._parse_duration_minutes(metadata['game_time'])

        except Exception as exc:  # pragma: no cover - resilience path
            print(f"⚠️  Error extracting metadata: {exc}")

        return metadata

    async def _extract_team_info(self, page: Page, game_id: str, season_year: Optional[int]) -> Dict[str, Dict[str, Any]]:
        script = """
        () => {
            const tables = Array.from(document.querySelectorAll('table'));
            let teamTable, inningTable, totalTable;
            
            for (const table of tables) {
                const headers = Array.from(table.querySelectorAll('thead th')).map(th => th.innerText.trim().toUpperCase());
                
                if (!teamTable && (headers.includes('TEAM') || headers.includes('팀') || (headers.length === 2 && headers[1] === 'TEAM'))) teamTable = table;
                // Only identify as inning table if we haven't found one, and it looks like 1, 2, 3...
                if (!inningTable && headers.includes('1') && headers.includes('2') && headers.includes('3')) inningTable = table;
                if (!totalTable && headers.includes('R') && headers.includes('H')) totalTable = table;
            }
            
            if (!teamTable || !inningTable || !totalTable) return null;
            
            const getRows = (t) => Array.from(t.querySelectorAll('tbody tr')).map(tr => 
                Array.from(tr.querySelectorAll('td')).map(td => td.innerText.trim())
            );
            
            const teamRows = getRows(teamTable);
            const inningRows = getRows(inningTable);
            const totalRows = getRows(totalTable); // [R, H, E, B]
            
            if (teamRows.length >= 2 && inningRows.length >= 2 && totalRows.length >= 2) {
                const headers = ["TEAM", ...Array.from({length: inningRows[0].length}, (_,k)=>String(k+1)), "R", "H", "E"];
                const rows = [];
                for (let i=0; i<2; i++) {
                    const teamName = teamRows[i][0] || "Unknown";
                    const innings = inningRows[i];
                    // totalRows has R, H, E, B. We need R, H, E for Python parser (last 3).
                    const totals = totalRows[i].slice(0, 3); 
                    rows.push([teamName, ...innings, ...totals]);
                }
                return { headers, rows };
            }
            return null;
        }
        """

        result = await page.evaluate(script)
        away_info: Dict[str, Any]
        home_info: Dict[str, Any]

        if result:
            print(f"DEBUG_JS_RESULT: {result}")
        else:
            print("DEBUG_JS_RESULT: None (No matching table found)")

        if result and len(result['rows']) >= 2:
            headers = result['headers']
            rows = result['rows']
            away_info = self._parse_scoreboard_row(headers, rows[0], season_year)
            home_info = self._parse_scoreboard_row(headers, rows[1], season_year)
        else:
            # Fallback to gameId decoding
            away_segment = game_id[8:10] if len(game_id) >= 10 else None
            home_segment = game_id[10:12] if len(game_id) >= 12 else None
            away_info = {
                'name': away_segment,
                'code': team_code_from_game_id_segment(away_segment, season_year),
                'score': None,
                'hits': None,
                'errors': None,
                'line_score': [],
            }
            home_info = {
                'name': home_segment,
                'code': team_code_from_game_id_segment(home_segment, season_year),
                'score': None,
                'hits': None,
                'errors': None,
                'line_score': [],
            }

        # Ensure codes resolve via team names if available
        for info in (away_info, home_info):
            if info.get('name'):
                resolved = resolve_team_code(info['name'], season_year)
                if resolved:
                    info['code'] = resolved

        if not away_info.get('code'):
            segment = game_id[8:10] if len(game_id) >= 10 else None
            away_info['code'] = team_code_from_game_id_segment(segment, season_year)
        if not home_info.get('code'):
            segment = game_id[10:12] if len(game_id) >= 12 else None
            home_info['code'] = team_code_from_game_id_segment(segment, season_year)

        return {'away': away_info, 'home': home_info}

    async def _extract_hitters(self, page: Page, team_side: str, team_code: Optional[str], season_year: Optional[int], roster_map: Optional[Dict[str, List[Dict[str, Any]]]] = None) -> List[Dict[str, Any]]:
        selectors = ['#tblAwayHitter1', '#tblAwayHitter3'] if team_side == 'away' else ['#tblHomeHitter1', '#tblHomeHitter3']
        tables = []
        for selector in selectors:
            table_rows = await self._extract_table_rows(page, selector)
            if table_rows:
                tables.append(table_rows)

        base_rows = tables[0] if tables else []
        extra_rows = tables[1] if len(tables) > 1 else []
        
        # KEY FIX: Legacy tables (e.g. 2018) might not have player name in the extra table (Table 3).
        # In that case, we must merge by INDEX, assuming 1:1 correspondence.
        # Modern tables (Table 2 in some views) might have names.
        # We check if extra_rows have names.
        extra_has_names = any(r.get('playerName') for r in extra_rows)
        
        extra_map = {}
        if extra_has_names:
            extra_map = {row['playerName']: row for row in extra_rows if row['playerName']}

        results: List[Dict[str, Any]] = []
        for idx, row in enumerate(base_rows, start=1):
            player_name = row['playerName']
            if not player_name or player_name in {'합계', '팀합계'}:
                continue

            stats = {}
            extras = {}
            self._populate_hitter_stats(stats, extras, row['cells'])

            # Merge Strategy: Name-based OR Index-based
            if extra_has_names:
                extra_row = extra_map.get(player_name)
            else:
                base_idx = idx - 1 # idx starts at 1
                if base_idx < len(extra_rows):
                    extra_row = extra_rows[base_idx]
                else:
                    extra_row = None

            if extra_row:
                 self._populate_hitter_stats(stats, extras, extra_row['cells'])

            batting_order = self._parse_batting_order(row['cells'])
            position = self._parse_position(row['cells'])
            is_starter = batting_order is not None and batting_order <= 9

            player_id = row['playerId']
            uniform_no = row.get('uniformNo')
            
            # Optimization: Check roster_map if ID is missing
            if not player_id and roster_map and player_name in roster_map:
                candidates = roster_map[player_name]
                if len(candidates) == 1:
                    player_id = candidates[0]['id']
                    # Use roster uniform if available and row uniform is missing
                    if not uniform_no:
                        uniform_no = candidates[0]['uniform']
                elif len(candidates) > 1:
                    # Ambiguity! If we have uniform_no in row, use it to match
                    if uniform_no:
                        for c in candidates:
                            if c['uniform'] == str(uniform_no):
                                player_id = c['id']
                                break
                    # If still ambiguous, we might fall back to resolver or heuristics
            
            if not player_id and self.resolver and team_code and season_year:
                player_id = self.resolver.resolve_id(player_name, team_code, season_year, uniform_no=uniform_no)
                # If resolved, ensure it's a string as crawler usually expects string IDs?
                # DB stores as int in models (PlayerBasic.player_id is int).
                # But here we pass it through.
                # Let's keep it as is (int or str), DB layer handles type.

            payload = {
                'player_id': player_id,
                'player_name': player_name,
                'uniform_no': uniform_no,
                'team_code': team_code,
                'team_side': team_side,
                'batting_order': batting_order,
                'position': position,
                'is_starter': is_starter,
                'appearance_seq': idx,
                'stats': stats,
                'extras': extras or None,
            }
            results.append(payload)

        return results

    async def _extract_pitchers(self, page: Page, team_side: str, team_code: Optional[str], season_year: Optional[int], roster_map: Optional[Dict[str, List[Dict[str, Any]]]] = None) -> List[Dict[str, Any]]:
        selector = '#tblAwayPitcher' if team_side == 'away' else '#tblHomePitcher'
        rows = await self._extract_table_rows(page, selector)
        results: List[Dict[str, Any]] = []

        for idx, row in enumerate(rows, start=1):
            player_name = row['playerName']
            if not player_name or player_name in {'합계', '팀합계'}:
                continue

            stats = {}
            extras = {}
            self._populate_pitcher_stats(stats, extras, row['cells'])

            innings_text = row['cells'].get('이닝') or row['cells'].get('IP')
            innings_outs = self._parse_innings_to_outs(innings_text)

            result_text = row['cells'].get('결과') or row['cells'].get('결')
            decision = self._parse_decision(result_text)
            if decision:
                stats['decision'] = decision

            player_id = row['playerId']
            uniform_no = row.get('uniformNo')
            
            # Optimization: Check roster_map if ID is missing
            if not player_id and roster_map and player_name in roster_map:
                candidates = roster_map[player_name]
                if len(candidates) == 1:
                    player_id = candidates[0]['id']
                    if not uniform_no:
                        uniform_no = candidates[0]['uniform']
                elif len(candidates) > 1:
                    if uniform_no:
                        for c in candidates:
                            if c['uniform'] == str(uniform_no):
                                player_id = c['id']
                                break
                    else:
                        # Ambiguous case: Multiple candidates, no uniform in table.
                        # We'll let the resolver handle it or pick the first one?
                        # Using resolver is safer as it might have DB knowledge.
                        pass

            if not player_id and self.resolver and team_code and season_year:
                player_id = self.resolver.resolve_id(player_name, team_code, season_year, uniform_no=uniform_no)

            payload = {
                'player_id': player_id,
                'player_name': player_name,
                'uniform_no': uniform_no,
                'team_code': team_code,
                'team_side': team_side,
                'is_starting': idx == 1,
                'appearance_seq': idx,
                'stats': {**stats, 'innings_outs': innings_outs},
                'extras': extras or None,
            }
            results.append(payload)

        return results

    async def _extract_table_rows(self, page: Page, selector: str) -> List[Dict[str, Any]]:
        if not selector:
            return []

        script = """
        (sel) => {
            const table = document.querySelector(sel);
            if (!table) return [];
            const headers = Array.from(table.querySelectorAll('thead th')).map(th => th.innerText.trim());
            
            // Find '선수명' index
            let nameIndex = -1;
            for (let i = 0; i < headers.length; i++) {
                if (headers[i] === '선수명') {
                    nameIndex = i;
                    break;
                }
            }
            
            return Array.from(table.querySelectorAll('tbody tr')).map((tr, index) => {
                const cells = Array.from(tr.querySelectorAll('th,td'));
                const values = {};
                for (let i = 0; i < cells.length; i++) {
                    const header = headers[i] || `COL_${i}`;
                    values[header] = cells[i].innerText.trim();
                }
                const link = tr.querySelector('a[href*="playerId="]');
                let playerId = null;
                let playerName = null;
                let uniformNo = null;

                // Try to find uniform number in the first cell or a cell with specific header
                // Usually '타순' or 'NO' column contains the number for starters,
                // but we can also check for a cell that is purely numeric and not batting order.
                // In KBO box score, the first column is often the number/order.
                if (cells.length > 0) {
                    const firstVal = cells[0].innerText.trim();
                    if (/^\d+$/.test(firstVal)) {
                        uniformNo = firstVal;
                    }
                }

                if (link) {
                    playerName = link.innerText.trim();
                    const href = link.getAttribute('href');
                    try {
                        const url = new URL(href, window.location.origin);
                        playerId = url.searchParams.get('playerId');
                    } catch (e) {
                        playerId = null;
                    }
                }
                
                // Fallback: Use name column if link not found
                if (!playerName && nameIndex !== -1 && cells.length > nameIndex) {
                    playerName = cells[nameIndex].innerText.trim();
                }
                
                return { index, cells: values, playerId, playerName, uniformNo };
            });
        }
        """

        return await page.evaluate(script, selector)

    async def _extract_game_summary(self, page: Page) -> List[Dict[str, str]]:
        """Extracts game summary details from #tblEtc (Winning hit, HR, Errors, Umpires, etc.)"""
        selector = "#tblEtc"
        if not await page.query_selector(selector):
            return []

        script = """
        (sel) => {
            const table = document.querySelector(sel);
            if (!table) return [];
            
            const results = [];
            const rows = table.querySelectorAll('tbody tr');
            
            rows.forEach(tr => {
                const th = tr.querySelector('th');
                const td = tr.querySelector('td');
                if (th && td) {
                    const category = th.innerText.trim();
                    const content = td.innerText.trim();
                    if (content && content !== '없음') {
                         results.push({
                             'summary_type': category, 
                             'detail_text': content
                         });
                    }
                }
            });
            return results;
        }
        """
        return await page.evaluate(script, selector)

    def _populate_hitter_stats(self, stats: Dict[str, Any], extras: Dict[str, Any], cells: Dict[str, str]) -> None:
        for header, value in cells.items():
            key = HITTER_HEADER_MAP.get(header)
            if not key:
                extras.setdefault(header, value)
                continue
            if value in ('', '-', None):
                continue
            if key in HITTER_FLOAT_KEYS:
                try:
                    stats[key] = float(value)
                except ValueError:
                    continue
            else:
                stats[key] = self._safe_int(value)

    def _populate_pitcher_stats(self, stats: Dict[str, Any], extras: Dict[str, Any], cells: Dict[str, str]) -> None:
        for header, value in cells.items():
            key = PITCHER_HEADER_MAP.get(header)
            if not key:
                extras.setdefault(header, value)
                continue
            if value in ('', '-', None):
                continue
            if key in PITCHER_FLOAT_KEYS:
                try:
                    stats[key] = float(value)
                except ValueError:
                    continue
            elif key == 'innings':
                stats['innings_outs'] = self._parse_innings_to_outs(value)
            else:
                stats[key] = self._safe_int(value)

    def _parse_scoreboard_row(self, headers: List[str], row: List[str], season_year: Optional[int] = None) -> Dict[str, Any]:
        if not row:
            return {
                'name': None,
                'code': None,
                'line_score': [],
                'score': None,
                'hits': None,
                'errors': None,
            }

        name = row[0]
        line = row[1:-3] if len(row) > 4 else []
        totals = row[-3:] if len(row) >= 3 else []

        score = self._safe_int(totals[0]) if totals else None
        hits = self._safe_int(totals[1]) if len(totals) > 1 else None
        errors = self._safe_int(totals[2]) if len(totals) > 2 else None

        line_numeric = [self._safe_int(item) for item in line]

        return {
            'name': name,
            'code': resolve_team_code(name, season_year),
            'line_score': line_numeric,
            'score': score,
            'hits': hits,
            'errors': errors,
        }

    def _parse_batting_order(self, cells: Dict[str, str]) -> Optional[int]:
        for key in ('타순', 'NO', 'No', '순', '타순(교체)', 'COL_0'):
            if key in cells:
                value = re.search(r"\d+", cells[key])
                if value:
                    return int(value.group())
        return None
        
    def _parse_position(self, cells: Dict[str, str]) -> Optional[str]:
        for key in ('POS', '포지션', '수비위치', 'COL_1'):
            if key in cells:
                return cells[key] or None
        return None

    def _parse_decision(self, text: Optional[str]) -> Optional[str]:
        if not text:
            return None
        text = text.strip()
        if '승' in text:
            return 'W'
        if '패' in text:
            return 'L'
        if '세' in text:
            return 'S'
        if '홀드' in text or 'H' in text:
            return 'H'
        return None

    def _parse_innings_to_outs(self, text: Optional[str]) -> Optional[int]:
        if not text:
            return None
        cleaned = text.strip()
        if cleaned in ('', '-', '0'):
            return self._safe_int(cleaned) or 0

        cleaned = cleaned.replace('⅓', '.1').replace('⅔', '.2').replace('⅔', '.2')
        match = re.match(r'^(\d+)(?:\s*(\d)/3)?$', cleaned)
        if match:
            whole = int(match.group(1))
            frac = int(match.group(2)) if match.group(2) else 0
            return whole * 3 + frac

        if '.' in cleaned:
            try:
                whole, frac = cleaned.split('.', 1)
                outs = int(whole) * 3 + int(frac[:1])
                return outs
            except ValueError:
                pass

        try:
            value = float(cleaned)
            return int(round(value * 3))
        except ValueError:
            return None

    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        if value in (None, '', '-', 'null'):
            return None
        try:
            return int(str(value).replace(',', ''))
        except ValueError:
            return None

    @staticmethod
    def _parse_duration_minutes(duration: Optional[str]) -> Optional[int]:
        if not duration:
            return None
        parts = duration.strip().split(':')
        if len(parts) != 2:
            return None
        try:
            hours = int(parts[0])
            minutes = int(parts[1])
            return hours * 60 + minutes
        except ValueError:
            return None

    @staticmethod
    def _parse_season_year(game_date: str) -> Optional[int]:
        digits = ''.join(ch for ch in str(game_date) if ch.isdigit())
        if len(digits) >= 4:
            try:
                return int(digits[:4])
            except ValueError:
                return None
        return None

    async def _extract_roster_from_lineup(self, page: Page) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extracts a map of {PlayerName: [{id, uniform_no}, ...]} from the LINEUP page.
        Used to resolve player IDs when the Review page boxscore lacks links (legacy games).
        """
        script = """
        () => {
            const map = {};
            const addToMap = (name, id, uniform) => {
                const cleanName = name.trim();
                if (!cleanName) return;
                
                if (!map[cleanName]) {
                    map[cleanName] = [];
                }
                
                // Avoid duplicates
                const exists = map[cleanName].some(p => p.id === id);
                if (!exists) {
                    map[cleanName].push({id, uniform});
                }
            };

            // Find all anchor tags that look like player links
            // Pattern: Player/PlayerDetail.aspx?playerId=... or p_id=...
            const links = document.querySelectorAll('a[href*="Player/PlayerDetail"]');
            
            links.forEach(a => {
                const name = a.innerText.trim();
                const href = a.getAttribute('href');
                if (!href) return;
                
                const idMatch = href.match(/playerId=(\\d+)/) || href.match(/p_id=(\\d+)/);
                if (name && idMatch) {
                    let uniform = null;
                    
                    // strategy 1: Check nearby lists or text for "No.XX"
                    const parentLi = a.closest('li');
                    if (parentLi) {
                        const text = parentLi.innerText;
                        const uniMatch = text.match(/No\\.(\\d+)/);
                        if (uniMatch) uniform = uniMatch[1];
                    }
                    
                    // strategy 2: Check previous sibling or parent structure (table columns)
                    // (Simplification: just take what we found)
                    
                    addToMap(name, idMatch[1], uniform);
                }
            });
            return map;
        }
        """
        try:
            return await page.evaluate(script)
        except Exception as e:
            print(f"⚠️ Error executing roster extraction script: {e}")
            return {}


async def main():  # pragma: no cover
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--game_id", help="KBO Game ID (e.g., 20251013SKSS0)")
    parser.add_argument("--date", help="Game Date (YYYYMMDD)")
    parser.add_argument("--save", action="store_true", help="Save to local database")
    args = parser.parse_args()

    if not args.game_id:
        print("Usage: python3 -m src.crawlers.game_detail_crawler --game_id <ID> [--date <YYYYMMDD>] [--save]")
        return

    game_id = args.game_id
    game_date = args.date or game_id[:8]
    
    print(f"🚀 Starting crawl for game {game_id} ({game_date})...")
    crawler = GameDetailCrawler()
    game_data = await crawler.crawl_game(game_id, game_date)
    if game_data and args.save:
        from src.repositories.game_repository import save_game_detail
        success = save_game_detail(game_data)
        if success:
            print(f"✅ Successfully saved and triggered sync for {game_id}")
        else:
            print(f"❌ Failed to save {game_id}")
    elif not game_data:
        print(f"❌ Failed to crawl {game_id}")
    else:
        print(game_data)

if __name__ == "__main__":  # pragma: no cover
    import asyncio
    asyncio.run(main())
