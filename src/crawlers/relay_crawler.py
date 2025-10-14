"""
RELAY section crawler - Play-by-play data collection.

Collects inning-by-inning, pitch-by-pitch game events from GameCenter RELAY section.
"""
from __future__ import annotations

import asyncio
from typing import List, Dict, Any, Optional
from playwright.async_api import async_playwright, Page

from src.utils.safe_print import safe_print as print


class RelayCrawler:
    """Crawler for GameCenter RELAY section (play-by-play)."""

    def __init__(self, request_delay: float = 1.5):
        self.base_url = "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx"
        self.request_delay = request_delay

    async def crawl_game_relay(self, game_id: str, game_date: str) -> Optional[Dict[str, Any]]:
        """
        Crawl RELAY section for a single game.

        Args:
            game_id: Game ID (e.g., "20251013SKSS0")
            game_date: Game date in YYYYMMDD format

        Returns:
            Dictionary containing:
                - game_id: str
                - game_date: str
                - innings: List[Dict] - Inning-by-inning play data
        """
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(locale='ko-KR')
            page = await context.new_page()

            try:
                # Load main GameCenter page first
                url = f"{self.base_url}?gameId={game_id}&gameDate={game_date}"
                print(f"Loading GameCenter: {url}")

                await page.goto(url, wait_until="networkidle", timeout=30000)
                await asyncio.sleep(1)

                # Click RELAY tab (중계)
                print("Clicking RELAY tab...")
                relay_tab_selectors = [
                    'li.tab-tit[section="RELAY"] a',
                    'a:has-text("중계")',
                    'a:has-text("텍스트중계")',
                    '#tabDepth1 li:has-text("중계")',
                ]

                clicked = False
                for selector in relay_tab_selectors:
                    try:
                        tab = await page.query_selector(selector)
                        if tab:
                            await tab.click()
                            clicked = True
                            print(f"Clicked RELAY tab using selector: {selector}")
                            break
                    except Exception:
                        continue

                if not clicked:
                    print("[WARN] Could not find RELAY tab, trying direct URL...")
                    relay_url = f"{self.base_url}?gameId={game_id}&gameDate={game_date}&section=RELAY"
                    await page.goto(relay_url, wait_until="networkidle", timeout=30000)

                # Wait for content to load after clicking
                await asyncio.sleep(3)  # Give more time for AJAX to load

                # Verify gameCenterContents exists (it should already be on page)
                content_div = await page.query_selector('#gameCenterContents')
                if not content_div:
                    print("[WARN] gameCenterContents not found, trying to wait...")
                    await page.wait_for_selector('#gameCenterContents', state='attached', timeout=10000)
                    await asyncio.sleep(1)

                innings_data = await self._extract_innings(page, game_id)

                return {
                    'game_id': game_id,
                    'game_date': game_date,
                    'innings': innings_data
                }

            except Exception as e:
                print(f"[ERROR] Failed to crawl RELAY for {game_id}: {e}")
                import traceback
                traceback.print_exc()
                return None

            finally:
                await browser.close()

    async def _extract_innings(self, page: Page, game_id: str) -> List[Dict[str, Any]]:
        """
        Extract inning-by-inning play-by-play data.

        Returns:
            List of dicts, each representing one inning:
                - inning: int (1, 2, 3...)
                - half: str ("top" or "bottom")
                - plays: List[Dict] - Individual plays/events
        """
        innings = []

        # Find all inning containers
        relay_containers = await page.query_selector_all('.relay-bx')

        for idx, container in enumerate(relay_containers):
            try:
                # Extract inning info from header or structure
                inning_text = await container.inner_text()
                inning_info = self._parse_inning_header(inning_text, idx)

                # Extract individual plays within this inning
                plays = await self._extract_plays(container)

                if plays:
                    innings.append({
                        'inning': inning_info['inning'],
                        'half': inning_info['half'],
                        'plays': plays
                    })

            except Exception as e:
                print(f"[WARN] Failed to parse inning {idx + 1} in {game_id}: {e}")
                continue

        return innings

    def _parse_inning_header(self, text: str, idx: int) -> Dict[str, Any]:
        """
        Parse inning header text to determine inning number and half.

        Args:
            text: Header text (e.g., "1회초", "3회말")
            idx: Index as fallback

        Returns:
            Dict with 'inning' (int) and 'half' (str)
        """
        # Korean inning markers
        if '회초' in text[:10]:
            for i in range(1, 20):
                if f'{i}회초' in text[:10]:
                    return {'inning': i, 'half': 'top'}
        elif '회말' in text[:10]:
            for i in range(1, 20):
                if f'{i}회말' in text[:10]:
                    return {'inning': i, 'half': 'bottom'}

        # Fallback: use index
        inning_num = (idx // 2) + 1
        half = 'top' if idx % 2 == 0 else 'bottom'
        return {'inning': inning_num, 'half': half}

    async def _extract_plays(self, container) -> List[Dict[str, Any]]:
        """
        Extract individual plays from an inning container.

        Returns:
            List of play dicts:
                - batter: str
                - pitcher: str
                - result: str (e.g., "안타", "삼진", "홈런")
                - description: str (full play description)
                - outs: int (outs after play)
        """
        plays = []

        # Find all play descriptions
        play_elements = await container.query_selector_all('.txt-box, .play-txt, p')

        for element in play_elements:
            try:
                text = (await element.inner_text()).strip()

                if not text or len(text) < 5:
                    continue

                play_data = self._parse_play_text(text)

                if play_data:
                    plays.append(play_data)

            except Exception:
                continue

        return plays

    def _parse_play_text(self, text: str) -> Optional[Dict[str, Any]]:
        """
        Parse play description text.

        Example inputs:
            - "타자 김선빈: 우전안타"
            - "투수 교체: 임기영 → 서진용"
            - "도루 성공: 2루 → 3루"

        Returns:
            Dict with parsed play info or None if not a valid play
        """
        # Skip empty or very short text
        if len(text) < 5:
            return None

        # Basic play data structure
        play = {
            'description': text,
            'batter': None,
            'pitcher': None,
            'result': None,
            'event_type': 'unknown'
        }

        # Detect play type
        if '타자' in text or '타격' in text:
            play['event_type'] = 'batting'
            # Try to extract batter name and result
            if ':' in text:
                parts = text.split(':', 1)
                if '타자' in parts[0]:
                    play['batter'] = parts[0].replace('타자', '').strip()
                if len(parts) > 1:
                    play['result'] = parts[1].strip()

        elif '투수' in text:
            play['event_type'] = 'pitching_change'

        elif '도루' in text:
            play['event_type'] = 'steal'

        elif '포볼' in text or '볼넷' in text:
            play['event_type'] = 'walk'
            play['result'] = 'BB'

        elif '삼진' in text:
            play['event_type'] = 'strikeout'
            play['result'] = 'K'

        elif '홈런' in text:
            play['event_type'] = 'home_run'
            play['result'] = 'HR'

        elif '안타' in text:
            play['event_type'] = 'hit'
            play['result'] = 'H'

        return play


async def fetch_and_parse_relay(game_id: str, game_date: str) -> Optional[Dict[str, Any]]:
    """
    Convenience function to fetch and parse RELAY section.

    Args:
        game_id: Game ID
        game_date: Game date (YYYYMMDD)

    Returns:
        Parsed relay data dict or None
    """
    crawler = RelayCrawler()
    return await crawler.crawl_game_relay(game_id, game_date)
