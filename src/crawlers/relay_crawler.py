"""
RELAY section crawler - Play-by-play data collection.
"""
from __future__ import annotations
import asyncio
from typing import List, Dict, Any, Optional
from playwright.async_api import async_playwright, Page
from src.utils.safe_print import safe_print as print
from src.utils.request_policy import RequestPolicy

class RelayCrawler:
    def __init__(self, request_delay: float = 1.5, policy: RequestPolicy | None = None):
        self.base_url = "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx"
        self.policy = policy or RequestPolicy(min_delay=request_delay, max_delay=request_delay + 0.5)

    async def crawl_game_relay(self, game_id: str, game_date: str) -> Optional[Dict[str, Any]]:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(**self.policy.build_context_kwargs(locale='ko-KR'))
            page = await context.new_page()
            try:
                url = f"{self.base_url}?gameId={game_id}&gameDate={game_date}"
                print(f"[FETCH] Loading: {url}")
                await self.policy.delay_async()
                await self.policy.run_with_retry_async(page.goto, url, wait_until="networkidle", timeout=30000)
                await asyncio.sleep(2)

                clicked = False
                for sel in ['li.tab-tit[section="RELAY"] a', 'a:has-text("중계")', '#tabDepth1 li[section="RELAY"]']:
                    try:
                        tab = await page.query_selector(sel)
                        if tab:
                            await tab.click(); clicked = True
                            print(f"[INFO] Clicked RELAY tab: {sel}")
                            break
                    except: continue

                if not clicked:
                    print("[WARN] Tab click failed, using direct URL...")
                    await self.policy.delay_async()
                    await self.policy.run_with_retry_async(
                        page.goto,
                        f"{url}&section=RELAY",
                        wait_until="networkidle",
                    )

                await asyncio.sleep(3)
                try: await page.wait_for_selector('.relay-bx, #gameCenterContents', timeout=15000)
                except: print("[WARN] Wait timeout, parsing available content...")

                innings = await self._extract_innings(page, game_id)
                return {'game_id': game_id, 'game_date': game_date, 'innings': innings}
            except Exception as e:
                print(f"[ERROR] Relay crawl failed: {e}")
                return None
            finally: await browser.close()

    async def _extract_innings(self, page: Page, game_id: str) -> List[Dict[str, Any]]:
        innings = []
        containers = await page.query_selector_all('.relay-bx')
        for idx, container in enumerate(containers):
            try:
                text = await container.inner_text()
                info = self._parse_inning_header(text, idx)
                plays = await self._extract_plays(container)
                if plays: innings.append({'inning': info['inning'], 'half': info['half'], 'plays': plays})
            except: continue
        return innings

    def _parse_inning_header(self, text: str, idx: int) -> Dict[str, Any]:
        for i in range(1, 16):
            if f'{i}회초' in text[:15]: return {'inning': i, 'half': 'top'}
            if f'{i}회말' in text[:15]: return {'inning': i, 'half': 'bottom'}
        return {'inning': (idx // 2) + 1, 'half': 'top' if idx % 2 == 0 else 'bottom'}

    async def _extract_plays(self, container) -> List[Dict[str, Any]]:
        plays = []
        elements = await container.query_selector_all('.txt-box, .play-txt, p')
        for el in elements:
            try:
                text = (await el.inner_text()).strip()
                if len(text) < 5: continue
                play = {'description': text, 'batter': None, 'pitcher': None, 'result': None, 'event_type': 'unknown'}
                if '타자' in text:
                    play['event_type'] = 'batting'
                    if ':' in text:
                        p = text.split(':', 1)
                        play['batter'], play['result'] = p[0].replace('타자', '').strip(), p[1].strip()
                elif '투수' in text: play['event_type'] = 'pitching_change'
                elif '도루' in text: play['event_type'] = 'steal'
                plays.append(play)
            except: continue
        return plays
