"""
RELAY section crawler - Live Play-by-play data collection (Snapshot).
Collects data ONLY when game is in LIVE status (game_sc=2).
"""
from __future__ import annotations
import asyncio
from typing import List, Dict, Any, Optional
from playwright.async_api import async_playwright, Page, TimeoutError
from src.utils.safe_print import safe_print as print
from src.utils.request_policy import RequestPolicy

class RelayCrawler:
    def __init__(self, request_delay: float = 1.0, policy: RequestPolicy | None = None):
        self.base_url = "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx"
        self.policy = policy or RequestPolicy(min_delay=request_delay, max_delay=request_delay + 0.5)

    async def crawl_live_game(self, game_id: str) -> Optional[Dict[str, Any]]:
        """
        Attempts to crawl Relay data for a specific game if it is LIVE.
        Returns None if game is not live or relay tab is missing.
        """
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(**self.policy.build_context_kwargs(locale='ko-KR'))
            page = await context.new_page()
            
            # 1. Open Game Center Main with Game ID
            game_date = game_id[:8]
            url = f"{self.base_url}?gameId={game_id}&gameDate={game_date}"
            
            try:
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
                await browser.close()

    async def _extract_flat_events(self, page: Page) -> List[Dict[str, Any]]:
        events = []
        containers = await page.query_selector_all('.relay-bx')
        
        sequence = 1
        for idx, container in enumerate(containers):
            try:
                text = await container.inner_text()
                info = self._parse_inning_header(text, idx)
                
                play_elements = await container.query_selector_all('.txt-box, .play-txt, p')
                for el in play_elements:
                    p_text = (await el.inner_text()).strip()
                    if not p_text: continue
                    
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
                    
                    # Basic parsing
                    if '타자' in p_text and ':' in p_text:
                        event['event_type'] = 'batting'
                        parts = p_text.split(':', 1)
                        event['batter'] = parts[0].replace('타자', '').strip()
                        event['result'] = parts[1].strip()
                    elif '투수' in p_text and '교체' in p_text:
                         event['event_type'] = 'pitching_change'
                         # Parse pitcher? 
                    elif '도루' in p_text:
                         event['event_type'] = 'steal'

                    events.append(event)
                    sequence += 1
            except Exception as e:
                print(f"[WARN] Error parsing inning container {idx}: {e}")
                continue
        return events

