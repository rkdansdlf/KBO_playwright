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

    async def crawl_live_game(self, game_id: str) -> Optional[Dict[str, Any]]:
        """
        Attempts to crawl Relay data for a specific game if it is LIVE.
        Returns None if game is not live or relay tab is missing.
        """
        pool = self.pool or AsyncPlaywrightPool(max_pages=1, context_kwargs=self._context_kwargs)
        owns_pool = self.pool is None
        await pool.start()
        try:
            page = await pool.acquire()
            try:
                # 1. Open Game Center Main with Game ID
                game_date = game_id[:8]
                url = f"{self.base_url}?gameId={game_id}&gameDate={game_date}"

                print(f"[FETCH] Checking Live Status: {url}")
                await page.goto(url, wait_until="networkidle", timeout=30000)

                # 2. Check Game Status from Top Bar (.game-list-n)
                # We need to find the LI element for our game_id
                game_li = page.locator(f'.game-list-n > li[g_id="{game_id}"]')

                if await game_li.count() == 0:
                    print(f"[WARN] Game {game_id} not found in Game Center list.")
                    return None

                game_sc = await game_li.get_attribute("game_sc")
                print(f"[INFO] Game Status Code (game_sc): {game_sc}")

                # sc=1: Preview, sc=2: Live, sc=3: End, sc=4: Cancel, sc=5: Suspended
                if game_sc not in ["2", "5"]:
                    print(f"[SKIP] Game is not LIVE (Status: {game_sc}). Skipping Relay.")
                    return None

                # 3. If Live, Relay Tab should be present.
                # Ensure the game is 'active' (clicked). It should be by default due to URL param,
                # but let's double check class 'on'.
                is_on = "on" in (await game_li.get_attribute("class") or "")
                if not is_on:
                    await game_li.click()
                    await asyncio.sleep(1)

                # 4. Click Relay Tab
                # Try finding "중계" tab.
                # The selectors might be: li[section="RELAY"] or a:has-text("중계")
                relay_tab = page.locator('ul.tab > li > a:text-is("중계")')

                if await relay_tab.count() > 0 and await relay_tab.is_visible():
                    print("[INFO] Found Relay Tab. Clicking...")
                    await relay_tab.click()
                    await page.wait_for_selector('.relay-bx, .relay-txt', timeout=10000)
                    await asyncio.sleep(2) # Stabilize
                else:
                    print("[WARN] Relay Tab NOT found even though status is Live.")
                    # fallback: try direct URL parameter just in case
                    return None

                # 5. Extract PBP Data
                print("[INFO] Extracting Relay Data...")
                events = await self._extract_flat_events(page)
                return {
                    'game_id': game_id,
                    'game_date': game_date,
                    'status': 'live',
                    'events': events
                }

            except Exception as e:
                print(f"[ERROR] Live Relay crawl failed: {e}")
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
            const containers = document.querySelectorAll('.relay-bx');
            let sequence = 1;

            containers.forEach((container, idx) => {
                // Parse Header (Inning info)
                // Typically just the text of the container before the inner text-boxes
                // But structure is complex. Let's assume the first text node or header class.
                // Looking at typical structure: 
                // <div class="relay-bx">
                //    <div class="inn-tit">...</div> or similar text?
                //    Actual implementation used self._parse_inning_header(text). 
                // Let's grab the full text to emulate, OR better:
                // Extract inning from ".tit-b" or assume structure. 
                // For safety, let's grab all text content to parse in JS or return raw.
                
                // Ideally we find .tit-b or similar for "1회초"
                // Let's try to find text directly. 
                const fullText = container.innerText;
                
                // Find play text elements
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
            events = []
            sequence = 1
            
            for idx, item in enumerate(raw_data):
                # We still use Python for the regex/parsing logic to be safe and reuse existing logic if possible.
                # Re-using _parse_inning_header logic
                info = self._parse_inning_header(item['full_text'], idx)
                
                for p_text in item['plays']:
                    event = {
                        'event_seq': sequence,
                        'inning': info['inning'],
                        'inning_half': info['half'],
                        'description': p_text,
                        'event_type': 'unknown',
                        'batter': None,
                        'pitcher': None,
                        'result': None
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
