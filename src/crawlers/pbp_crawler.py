"""
PBP Crawler - Historical Play-by-play data collection (`LiveTextView2.aspx`).
Navigaes directly to the Live Text View page to collect events.
Computes WPA transitions based on the events.
"""
from __future__ import annotations
import asyncio
import os
from typing import List, Dict, Any, Optional
from playwright.async_api import Page
from src.utils.safe_print import safe_print as print
from src.utils.request_policy import RequestPolicy
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.services.wpa_calculator import WPACalculator
from src.utils.text_parser import KBOTextParser
from src.utils.compliance import compliance

class PBPCrawler:
    def __init__(
        self,
        request_delay: float = 1.0,
        policy: RequestPolicy | None = None,
        pool: AsyncPlaywrightPool | None = None,
    ):
        # Using the older but more robust LiveText.aspx which behaves better with Referer checks
        self.base_url = "https://www.koreabaseball.com/Game/LiveText.aspx"
        self.policy = policy or RequestPolicy(min_delay=request_delay, max_delay=request_delay + 0.5)
        self.pool = pool
        self._context_kwargs = self.policy.build_context_kwargs(locale='ko-KR')
        self.wpa_calc = WPACalculator()
        self.last_failure_reason: str | None = None

    async def crawl_game_events(self, game_id: str) -> Optional[Dict[str, Any]]:
        """
        Loads the LiveText page for a specific game and extracts PBP data.
        """
        self.last_failure_reason = None
        game_date = game_id[:8]
        # Common ids: leagueId=1 (KBO), seriesId=0 (Regular)
        url = f"{self.base_url}?leagueId=1&seriesId=0&gameId={game_id}&gyear={game_date[:4]}"

        pool = self.pool or AsyncPlaywrightPool(max_pages=1, context_kwargs=self._context_kwargs, requires_auth=True)
        owns_pool = self.pool is None

        if owns_pool:
            await pool.start()
            
        try:
            async def do_crawl(retry_count=0):
                try:
                    page = await pool.acquire()
                    try:
                        print(f"[FETCH] PBP Data: {url}")
                        if not await compliance.is_allowed(url):
                            print(f"[COMPLIANCE] Navigation to {url} aborted.")
                            return None
                            
                        await self.policy.delay_async(host="www.koreabaseball.com")
                        
                        # Step 1: Warm up the session by visiting the Scoreboard page
                        parent_url = f"https://www.koreabaseball.com/Schedule/ScoreBoard.aspx?gameDate={game_date}"
                        print(f"[AUTH] Warming up session on Scoreboard: {parent_url}")
                        await page.goto(parent_url, wait_until="networkidle", timeout=20000)
                        await asyncio.sleep(2)

                        # Step 2: Navigate to the actual relay page with explicit Referer
                        print(f"[FETCH] Navigating to Relay page with Referer: {url}")
                        # Use 'domcontentloaded' as KBO pages often have persistent tracking scripts/images that block 'load' or 'networkidle'
                        await page.goto(url, wait_until="domcontentloaded", timeout=60000, referer=parent_url)

                        # Check for redirects
                        if "Error.html" in page.url or "Login.aspx" in page.url:
                            print(f"[ERROR] Redirected to {page.url}.")
                            self.last_failure_reason = "auth_required"
                            if retry_count == 0:
                                await pool.close()
                                await pool.start()
                                return await do_crawl(retry_count=1)
                            return None

                        # Wait for any PBP container on LiveText.aspx
                        try:
                            # Try to wait for any of the containers (1-12)
                            await page.wait_for_selector('div[id^="numCont"]', timeout=20000)
                        except Exception:
                            print(f"[WARN] No PBP containers found for {game_id}.")
                            body = await page.content()
                            if "데이터가 없습니다" in body or "취소" in body:
                                self.last_failure_reason = "empty"
                                return None
                        
                        print("[INFO] Extracting Relay Data...")
                        events = await self._extract_flat_events_legacy(page)
                        
                        if not events:
                            self.last_failure_reason = "empty"
                            return None
                            
                        return {
                            'game_id': game_id,
                            'game_date': game_date,
                            'events': events
                        }

                    except Exception as e:
                        print(f"[ERROR] PBP crawl failed for {game_id}: {e}")
                        self.last_failure_reason = "error"
                        return None
                    finally:
                        await pool.release(page)
                except Exception as e:
                    print(f"[ERROR] Pool error for {game_id}: {e}")
                    self.last_failure_reason = "error"
                    return None

            result = await do_crawl()
            return result
        finally:
            if owns_pool:
                await pool.close()

    async def _extract_flat_events_legacy(self, page: Page) -> List[Dict[str, Any]]:
        """Extract events from LiveText.aspx which are in reverse chronological order."""
        extraction_script = """
        () => {
            const getSpans = (container) => {
                if (!container) return [];
                return Array.from(container.querySelectorAll('span')).map(span => ({
                    text: span.innerText.trim(),
                    class: span.className
                })).filter(item => item.text !== "");
            };

            const mainContainer = document.querySelector('#numCont11');
            let results = getSpans(mainContainer);
            
            if (results.length === 0) {
                // If #numCont11 is empty, try individual innings 1-12
                for (let i = 1; i <= 12; i++) {
                    if (i === 11) continue;
                    const container = document.querySelector('#numCont' + i);
                    const inningSpans = getSpans(container);
                    results = results.concat(inningSpans);
                }
            }
            return results;
        }
        """

        try:
            raw_spans = await page.evaluate(extraction_script)
            if not raw_spans: return []
            
            # Since the page is reverse chronological, we REVERSE the list to process it forward.
            raw_spans.reverse()
            
            # State for parsing
            current_inning = 0
            current_half = 'unknown'
            home_score = 0
            away_score = 0
            current_outs = 0
            current_runners = 0
            sequence = 1
            events = []
            
            for item in raw_spans:
                text = item['text']
                cls = item['class']
                
                # Inning Header Detection (span.blue)
                # Example: "9회초 한화 공격"
                if 'blue' in cls and '회' in text:
                    import re
                    match = re.search(r'(\d+)회(초|말)', text)
                    if match:
                        current_inning = int(match.group(1))
                        current_half = 'top' if match.group(2) == '초' else 'bottom'
                        current_outs = 0
                        current_runners = 0
                    continue
                
                # Metadata line (often just "------")
                if "---" in text and len(text) > 10:
                    continue
                    
                # Actual Event (span.normaiflTxt or span.red)
                if 'normaiflTxt' in cls or 'red' in cls:
                    # Skip administrative messages
                    if "경기 준비중" in text or "경기 시작" in text: continue
                    
                    is_bottom = (current_half == 'bottom')
                    outs_before = current_outs
                    runners_before = current_runners
                    score_diff_before = home_score - away_score
                    
                    # Basic parser logic
                    parsed_outs = KBOTextParser.parse_outs(text)
                    parsed_runners = KBOTextParser.parse_runners(text)
                    if "사" in text and ("루" in text or "무사" in text):
                        if parsed_outs >= 0:
                            outs_before = parsed_outs
                            current_outs = outs_before
                        if parsed_runners >= 0:
                            runners_before = parsed_runners
                            current_runners = runners_before
                    
                    runs_scored = KBOTextParser.parse_score_change(text)
                    if is_bottom: home_score += runs_scored
                    else: away_score += runs_scored
                    
                    # Out tracking
                    if any(kw in text for kw in ["삼진", "아웃", "플라이", "땅볼", "범타"]):
                         if "병살" in text: current_outs += 2
                         elif "삼중살" in text: current_outs += 3
                         else: current_outs += 1
                    
                    current_outs = min(current_outs, 3)
                    outs_after = current_outs
                    runners_after = 0 if current_outs >= 3 else 0 # Placeholder
                    
                    # WPA
                    wp_before = self.wpa_calc.get_win_probability(current_inning, is_bottom, outs_before, runners_before, score_diff_before)
                    wp_after = self.wpa_calc.get_win_probability(current_inning, is_bottom, outs_after, runners_after, home_score - away_score)
                    wpa = round(wp_after - wp_before if is_bottom else wp_before - wp_after, 4)

                    event = {
                        'event_seq': sequence,
                        'inning': current_inning,
                        'inning_half': current_half,
                        'description': text,
                        'event_type': 'batting' if '타자' in text or '전환' in text else 'unknown',
                        'batter': text.split(':')[0].replace('타자', '').strip() if ':' in text else None,
                        'result': text.split(':')[1].strip() if ':' in text else None,
                        'wpa': wpa,
                        'win_expectancy_before': wp_before,
                        'win_expectancy_after': wp_after,
                        'home_score': home_score,
                        'away_score': away_score,
                        'score_diff': score_diff_before,
                        'base_state': runners_before,
                        'outs': outs_before,
                        'bases_before': self._format_base_string(runners_before),
                        'bases_after': self._format_base_string(runners_after)
                    }
                    events.append(event)
                    sequence += 1
                    
            return events
        except Exception as e:
            print(f"[WARN] Error extracting PBP legacy (JS): {e}")
            return []

    def _format_base_string(self, runners: int) -> str:
        s = ""
        s += "1" if (runners & 1) else "-"
        s += "2" if (runners & 2) else "-"
        s += "3" if (runners & 4) else "-"
        return s
            
    def _parse_inning_header(self, text: str, idx: int) -> Dict[str, Any]:
        import re
        match = re.search(r'(\d+)회(초|말)', text)
        if match:
            return {'inning': int(match.group(1)), 'half': 'top' if match.group(2) == '초' else 'bottom'}
        return {'inning': idx + 1, 'half': 'unknown'}

    def _format_base_string(self, runners: int) -> str:
        s = ""
        s += "1" if (runners & 1) else "-"
        s += "2" if (runners & 2) else "-"
        s += "3" if (runners & 4) else "-"
        return s
