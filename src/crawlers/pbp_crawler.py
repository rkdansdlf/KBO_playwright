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
        self.base_url = "https://www.koreabaseball.com/Game/LiveTextView2.aspx"
        self.policy = policy or RequestPolicy(min_delay=request_delay, max_delay=request_delay + 0.5)
        self.pool = pool
        self._context_kwargs = self.policy.build_context_kwargs(locale='ko-KR')
        self.wpa_calc = WPACalculator()
        self.last_failure_reason: str | None = None

    async def crawl_game_events(self, game_id: str) -> Optional[Dict[str, Any]]:
        """
        Loads the LiveTextView2 page for a specific game and extracts PBP data.
        Returns a dictionary with 'game_id', 'game_date', and a list of 'events' (GameEvent structs).
        """
        self.last_failure_reason = None
        game_date = game_id[:8]
        url = f"{self.base_url}?gameDate={game_date}&gameId={game_id}"

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
                        
                        # Step 1: Warm up the session by visiting the main game center page
                        # This allows Akamai scripts to run and set necessary cookies (_abck, etc.)
                        parent_url = f"https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameId={game_id}&gameDate={game_date}&section=REVIEW"
                        print(f"[AUTH] Warming up session on parent page: {parent_url}")
                        await page.goto(parent_url, wait_until="networkidle", timeout=20000)
                        await asyncio.sleep(2) # Allow some time for scripts to stabilize

                        # Step 2: Navigate to the actual relay page with explicit Referer
                        print(f"[FETCH] Navigating to Relay page with Referer: {url}")
                        await page.goto(url, wait_until="networkidle", timeout=30000, referer=parent_url)

                        # Check for login redirect or Error page
                        if "Error.html" in page.url or "Login.aspx" in page.url:
                            print(f"[ERROR] Redirected to {page.url}. Session might be expired or login failed.")
                            self.last_failure_reason = "auth_required"
                            
                            from src.utils.kbo_auth import KboAuthenticator
                            if os.path.exists(KboAuthenticator.AUTH_STATE_PATH):
                                os.remove(KboAuthenticator.AUTH_STATE_PATH)
                                print("[AUTH] Deleted invalid session state file.")

                            if retry_count == 0:
                                print("[AUTH] Attempting one-time re-login and retry...")
                                # Close current pool and re-start it to force new auth state
                                await pool.close()
                                await pool.start()
                                return await do_crawl(retry_count=1)
                            return None

                        # Wait for relay content to confirm it's loaded
                        try:
                            await page.wait_for_selector('.relay-bx, .relay-txt', timeout=10000)
                        except Exception:
                            # It might be empty if the game is cancelled or no PBP available
                            print(f"[WARN] No relay elements found for {game_id}. Trying content check...")
                            body = await page.content()
                            if "경기 준비중" in body or "취소" in body:
                                print(f"[INFO] Game {game_id} seems to have no relay data.")
                                self.last_failure_reason = "empty"
                                return None
                        
                        print("[INFO] Extracting Relay Data...")
                        events = await self._extract_flat_events(page)
                        
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

    async def _extract_flat_events(self, page: Page) -> List[Dict[str, Any]]:
        """Extract all PBP events using single JS execution and Python calculation (Fast Path)."""
        extraction_script = """
        () => {
            const results = [];
            const containers = document.querySelectorAll('.relay-bx');
            
            containers.forEach((container, idx) => {
                const fullText = container.innerText;
                const playEls = container.querySelectorAll('.txt-box, .play-txt, p');
                const plays = Array.from(playEls).map(el => el.innerText.trim()).filter(t => t.length > 0);
                
                results.push({
                    full_text: fullText,
                    plays: plays
                });
            });
            return results;
        }
        """

        try:
            raw_data = await page.evaluate(extraction_script)
            
            # State Tracking
            current_outs = 0
            current_runners = 0
            home_score = 0
            away_score = 0
            sequence = 1
            
            events = []
            
            for idx, item in enumerate(raw_data):
                info = self._parse_inning_header(item['full_text'], idx)
                inning = info['inning']
                is_bottom = (info['half'] == 'bottom')
                
                # Reset outs/runners on new inning half (heuristic check)
                if idx > 0:
                    prev_info = self._parse_inning_header(raw_data[idx-1]['full_text'], idx-1)
                    if prev_info != info:
                        current_outs = 0
                        current_runners = 0
                
                for p_text in item['plays']:
                    # 1. Determine State BEFORE event
                    outs_before = current_outs
                    runners_before = current_runners
                    score_diff_before = home_score - away_score
                    
                    # 2. Extract explicit runners/outs stated in the text
                    parsed_outs = KBOTextParser.parse_outs(p_text)
                    parsed_runners = KBOTextParser.parse_runners(p_text)
                    # If the text explicitly starts with "1사 2루", it represents BEFORE state.
                    if "사" in p_text and ("루" in p_text or "무사" in p_text):
                        if parsed_outs >= 0:
                            outs_before = parsed_outs
                            current_outs = outs_before
                        if parsed_runners >= 0:
                            runners_before = parsed_runners
                            current_runners = runners_before
                            
                    # 3. Determine Result / Update State
                    runs_scored = KBOTextParser.parse_score_change(p_text)
                    
                    if is_bottom:
                        home_score += runs_scored
                    else:
                        away_score += runs_scored
                        
                    # Update Outs (Naive simulation)
                    if "삼진" in p_text or "아웃" in p_text or "플라이" in p_text or "땅볼" in p_text or "범타" in p_text:
                         if "병살" in p_text:
                             current_outs += 2
                         elif "삼중살" in p_text:
                             current_outs += 3
                         else:
                             current_outs += 1
                    
                    current_outs = min(current_outs, 3)
                    
                    outs_after = current_outs
                    # It's hard to track runners accurately; assume they clear on 3 outs
                    if current_outs >= 3:
                        runners_after = 0
                    else:
                        runners_after = 0 # Default placeholder unless we have advanced parser
                        
                    score_diff_after = home_score - away_score
                    
                    # 4. Calculate WPA
                    wp_before = self.wpa_calc.get_win_probability(
                        inning, is_bottom, outs_before, runners_before, score_diff_before
                    )
                    wp_after = self.wpa_calc.get_win_probability(
                        inning, is_bottom, outs_after, runners_after, score_diff_after
                    )
                    
                    wpa = round(wp_after - wp_before if is_bottom else wp_before - wp_after, 4)

                    event = {
                        'event_seq': sequence,
                        'inning': inning,
                        'inning_half': info['half'],
                        'description': p_text,
                        'event_type': 'unknown',
                        'batter': None,
                        'pitcher': None,
                        'result': None,
                        'wpa': wpa,
                        'win_expectancy_before': wp_before,
                        'win_expectancy_after': wp_after,
                        'score_diff': score_diff_before,
                        'home_score': home_score,
                        'away_score': away_score,
                        'base_state': runners_before,
                        'outs': outs_before,
                        'bases_before': self._format_base_string(runners_before),
                        'bases_after': self._format_base_string(runners_after),
                        'result_code': None,
                        'rbi': runs_scored
                    }
                    
                    # Basic parsing logic
                    if '타자' in p_text and ':' in p_text:
                        event['event_type'] = 'batting'
                        parts = p_text.split(':', 1)
                        if len(parts) > 1:
                            event['batter'] = parts[0].replace('타자', '').strip()
                            event['result'] = parts[1].strip()
                            event['result_code'] = parts[1].strip()
                    elif '투수' in p_text and '교체' in p_text:
                         event['event_type'] = 'pitching_change'
                    elif '도루' in p_text:
                         event['event_type'] = 'steal'

                    events.append(event)
                    sequence += 1
            
            return events

        except Exception as e:
            print(f"[WARN] Error extracting PBP events (JS): {e}")
            return []
            
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
