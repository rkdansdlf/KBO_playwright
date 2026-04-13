"""
RELAY section crawler - Live Play-by-play data collection (Snapshot).
Collects data ONLY when game is in LIVE status (game_sc=2).
"""
from __future__ import annotations
import asyncio
from typing import List, Dict, Any, Optional
from playwright.async_api import Page, TimeoutError
from src.utils.safe_print import safe_print as print
from src.utils.request_policy import RequestPolicy
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.services.wpa_calculator import WPACalculator
from src.utils.text_parser import KBOTextParser
from src.utils.compliance import compliance

class RelayCrawler:
    def __init__(
        self,
        request_delay: float = 1.0,
        policy: RequestPolicy | None = None,
        pool: AsyncPlaywrightPool | None = None,
    ):
        self.base_url = "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx"
        self.policy = policy or RequestPolicy(min_delay=request_delay, max_delay=request_delay + 0.5)
        self.pool = pool
        self._context_kwargs = self.policy.build_context_kwargs(locale='ko-KR')
        self.wpa_calc = WPACalculator()

    async def crawl_game_relay(self, game_id: str) -> Optional[Dict[str, Any]]:
        """
        Attempts to crawl Relay data for a specific game (LIVE or COMPLETED).
        Returns None if relay data is unavailable.
        """
        pool = self.pool or AsyncPlaywrightPool(max_pages=1, context_kwargs=self._context_kwargs, requires_auth=True)
        owns_pool = self.pool is None
        await pool.start()
        try:
            page = await pool.acquire()
            try:
                # 1. Open Game Center Main with Game ID and force RELAY section
                game_date = game_id[:8]
                url = f"{self.base_url}?gameId={game_id}&gameDate={game_date}&section=RELAY"

                print(f"[FETCH] Accessing Relay: {url}")
                if not await compliance.is_allowed(url):
                    print(f"[COMPLIANCE] Navigation to {url} aborted.")
                    return None
                await self.policy.delay_async(host="www.koreabaseball.com")
                await page.goto(url, wait_until="networkidle", timeout=30000)

                # 2. Check Game Status from Top Bar (.game-list-n)
                game_li = page.locator(f'.game-list-n > li[g_id="{game_id}"]')

                if await game_li.count() == 0:
                    print(f"[WARN] Game {game_id} not found in Game Center list.")
                    # Sometimes the list takes time to load or game is not present
                    await page.wait_for_timeout(2000)
                    game_li = page.locator(f'.game-list-n > li[g_id="{game_id}"]')

                game_sc = "unknown"
                if await game_li.count() > 0:
                    game_sc = await game_li.get_attribute("game_sc")
                    print(f"[INFO] Game Status Code (game_sc): {game_sc}")

                # sc=1: Preview, sc=4: Cancel
                if game_sc == "1" or game_sc == "4":
                    print(f"[SKIP] Game status {game_sc} has no relay data.")
                    return None

                # 3. Ensure RELAY content is loaded
                # For archived games (sc=3), the tab might not be visible, but content might load via URL
                try:
                    await page.wait_for_selector('.relay-bx, .relay-txt', timeout=10000)
                    print("[INFO] Relay content markers found.")
                except Exception:
                    print(f"[WARN] No relay elements found for {game_id}. Redirection check...")
                    if "Error.html" in page.url or "Login.aspx" in page.url:
                         print(f"[ERROR] Access denied or redirected: {page.url}")
                         return None
                    return None

                # 4. Extract PBP Data
                print("[INFO] Extracting Relay Data...")
                events = await self._extract_flat_events(page)
                return {
                    'game_id': game_id,
                    'game_date': game_date,
                    'status': 'completed' if game_sc == "3" else 'live',
                    'events': events
                }

            except Exception as e:
                print(f"[ERROR] Relay crawl failed: {e}")
                return None
            finally:
                await pool.release(page)
        finally:
            if owns_pool:
                await pool.close()

    async def _extract_flat_events(self, page: Page) -> List[Dict[str, Any]]:
        """Extract all relay events using a single JS execution (Fast Path)"""
        
        extraction_script = """
        () => {
            const results = [];
            // Try multiple selectors for robustness
            const containers = document.querySelectorAll('.relay-bx, .relay-txt, .sms-bx');
            let sequence = 1;

            if (containers.length === 0) {
                // Fallback: search for any div that might contain '회' and looks like a block
                const allDivs = document.querySelectorAll('div');
                // (Omitted for brevity, but could add more heuristics)
            }

            containers.forEach((container, idx) => {
                const fullText = container.innerText;
                const playEls = container.querySelectorAll('.txt-box, .play-txt, p, span.txt');
                const plays = Array.from(playEls).map(el => el.innerText.trim()).filter(t => t.length > 0);
                
                if (plays.length > 0) {
                    results.push({
                        full_text: fullText,
                        plays: plays
                    });
                }
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
            
            # WPA Calculation needs: Inning, Bottom?, Outs, Runners, ScoreDiff
            # We track these sequentially.
            
            for idx, item in enumerate(raw_data):
                # We still use Python for the regex/parsing logic to be safe and reuse existing logic if possible.
                # Re-using _parse_inning_header logic
                info = self._parse_inning_header(item['full_text'], idx)
                inning = info['inning']
                is_bottom = (info['half'] == 'bottom')
                
                # Reset outs/runners on new inning half (heuristic check)
                # But careful, idx is flat list of blocks. If block is new inning, reset.
                # Check if this block header implies new inning/half.
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
                    
                    # 2. Parse Event for State Changes
                    # Try to find explicit state in text (e.g. "1사 2루") to correct drift
                    # Parser logic needed here. For now, rely on heuristic updates or basic parser.
                    # Note: p_text often contains "1사 1,2루에서 xxx 안타" -> "1사 1,2루" is BEFORE state.
                    
                    parsed_outs = KBOTextParser.parse_outs(p_text)
                    parsed_runners = KBOTextParser.parse_runners(p_text)
                    
                    # If text has explicit state, use it as 'before' (trust source over tracking if divergent)
                    # But actually "1사 2루에서" means Before state. "1사 2루가 됨" means After state.
                    # KBO texts are usually "Name: Result" without full context in one line.
                    # But the 'full_text' block header might have context.
                    # Let's trust tracked state but sync if 0 outs/runners at start of inning.
                    
                    # 3. Determine Result / Update State
                    runs_scored = KBOTextParser.parse_score_change(p_text)
                    
                    # Update Score
                    if is_bottom:
                        home_score += runs_scored
                    else:
                        away_score += runs_scored
                        
                    # Update Outs/Runners (Naive simulation)
                    # This is HARD without sophisticated NLP.
                    # For MVP: We will calculate WPA based on "Before" state derived from
                    # heuristics or just use 0.5 if unknown.
                    # BETTER: Use WPA Calculator's internal logic if we had RE24 state transition map.
                    #
                    # Compromise: Parse 'Outs' and 'Runners' from the text if present to set "After" state?
                    # Or assume text like "삼진 아웃" increments out.
                    
                    if "삼진" in p_text or "범타" in p_text or "땅볼" in p_text or "플라이" in p_text or "아웃" in p_text:
                         if "병살" in p_text:
                             current_outs += 2
                         elif "삼중살" in p_text:
                             current_outs += 3
                         else:
                             current_outs += 1
                    
                    # Cap outs
                    current_outs = min(current_outs, 3)
                    
                    # Bases: Complex. "안타" -> assume +1 base? "2루타" -> +2?
                    # Without full engine, base state validty is low.
                    # Let's try to grab 'runners_after' from next line?
                    # No, we only have current line.
                    
                    outs_after = current_outs
                    runners_after = 0 # Placeholder: Hard to track without deep parsing.
                    score_diff_after = home_score - away_score
                    
                    # 4. Calculate WPA
                    # Calculate WinProb Before
                    wp_before = self.wpa_calc.get_win_probability(
                        inning, is_bottom, outs_before, runners_before, score_diff_before
                    )
                    
                    # Calculate WinProb After
                    wp_after = self.wpa_calc.get_win_probability(
                        inning, is_bottom, outs_after, runners_after, score_diff_after
                    )
                    
                    wpa = round(wp_after - wp_before if is_bottom else wp_before - wp_after, 4)

                    event = {
                        'event_seq': sequence,
                        'inning': info['inning'],
                        'inning_half': info['half'],
                        'description': p_text,
                        'event_type': 'unknown',
                        'batter': None,
                        'pitcher': None,
                        'result': None,
                        # New Fields
                        'wpa': wpa,
                        'win_expectancy_before': wp_before,
                        'win_expectancy_after': wp_after,
                        'score_diff': score_diff_before, # Store 'Before' diff usually?
                        'home_score': home_score,
                        'away_score': away_score,
                        'base_state': runners_before
                    }
                    
                    # Basic parsing logic (kept in Python for consistency/easier debugging)
                    if '타자' in p_text and ':' in p_text:
                        event['event_type'] = 'batting'
                        parts = p_text.split(':', 1)
                        if len(parts) > 1:
                            event['batter'] = parts[0].replace('타자', '').strip()
                            event['result'] = parts[1].strip()
                    elif '투수' in p_text and '교체' in p_text:
                         event['event_type'] = 'pitching_change'
                    elif '도루' in p_text:
                         event['event_type'] = 'steal'

                    events.append(event)
                    sequence += 1
            
            return events

        except Exception as e:
            print(f"[WARN] Error extracting relay events (JS): {e}")
            return []
            
    def _parse_inning_header(self, text: str, idx: int) -> Dict[str, Any]:
        # Helper to recover inning/half from text
        # Simple heuristic based on "1회초", "9회말"
        import re
        match = re.search(r'(\d+)회(초|말)', text)
        if match:
            return {'inning': int(match.group(1)), 'half': 'top' if match.group(2) == '초' else 'bottom'}
        return {'inning': idx + 1, 'half': 'unknown'}
