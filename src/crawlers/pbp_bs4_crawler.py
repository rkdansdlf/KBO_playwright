"""
PBP BS4 Crawler - Fast Play-by-play data collection for backfilling.
Navigates directly to the Live Text View page using httpx and BeautifulSoup.
Computes WPA transitions based on the events.
"""
from __future__ import annotations
import httpx
from bs4 import BeautifulSoup
from typing import List, Dict, Any, Optional
import re

from src.services.wpa_calculator import WPACalculator
from src.utils.text_parser import KBOTextParser
from src.utils.safe_print import safe_print as print

class PBPBS4Crawler:
    def __init__(self):
        self.base_url = "https://www.koreabaseball.com/Game/LiveTextView2.aspx"
        self.wpa_calc = WPACalculator()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        }

    def crawl_game_events(self, game_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetches the LiveTextView2 page for a specific game and extracts PBP data.
        Returns a dictionary with 'game_id', 'game_date', and a list of 'events' (GameEvent structs).
        """
        game_date = game_id[:8]
        url = f"{self.base_url}?gameDate={game_date}&gameId={game_id}"

        try:
            print(f"[FETCH] BS4 PBP Data: {url}")
            # Use a longer timeout for KBO server stability, though 15s is usually plenty.
            response = httpx.get(url, headers=self.headers, timeout=15.0, follow_redirects=True)
            response.raise_for_status()
            html = response.text

            # If redirected to KBO global Error page
            if "Error.html" in str(response.url):
                 print(f"[WARN] Redirected to Error page for {game_id} (No PBP data available).")
                 return None

            if "경기 준비중" in html or "취소" in html:
                print(f"[INFO] Game {game_id} seems to have no relay data.")
                return None
            
            # Quick check for relay elements before full BS4 parsing
            if "relay-bx" not in html and "relay-txt" not in html:
                print(f"[WARN] No relay containers found in HTML for {game_id}.")
                return None

            print("[INFO] Extracting Relay Data via BeautifulSoup...")
            events = self._parse_html_to_events(html)
            
            if not events:
                return None
                
            return {
                'game_id': game_id,
                'game_date': game_date,
                'events': events
            }

        except httpx.HTTPError as e:
            print(f"[ERROR] HTTP fetch failed for {game_id}: {e}")
            return None
        except Exception as e:
            print(f"[ERROR] BS4 PBP crawl failed for {game_id}: {e}")
            return None

    def _parse_html_to_events(self, html: str) -> List[Dict[str, Any]]:
        """Extract all PBP events using BeautifulSoup and compute states."""
        soup = BeautifulSoup(html, 'lxml')
        containers = soup.select('.relay-bx')
        
        if not containers:
            return []
            
        raw_data = []
        for container in containers:
            full_text = container.get_text(separator=' ', strip=True)
            play_els = container.select('.txt-box, .play-txt, p')
            plays = [el.get_text(strip=True) for el in play_els if el.get_text(strip=True)]
            
            raw_data.append({
                'full_text': full_text,
                'plays': plays
            })

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

    def _parse_inning_header(self, text: str, idx: int) -> Dict[str, Any]:
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
