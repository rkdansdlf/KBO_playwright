
import re
from typing import List, Dict, Optional, Any
from playwright.sync_api import Page
from src.utils.team_codes import resolve_team_code
from src.crawlers.game_detail_crawler import HITTER_HEADER_MAP, PITCHER_HEADER_MAP, HITTER_FLOAT_KEYS, PITCHER_FLOAT_KEYS

class LegacyGameDetailCrawler:
    """
    Crawler for pre-2010 game details (e.g., 2009).
    Handles navigation from Schedule page and legacy table structures.
    Returns data structured identically to GameDetailCrawler.
    """
    
    def __init__(self, resolver: Optional[Any] = None):
        self.resolver = resolver
        
    def extract_game_details(self, page: Page, game_id: str, game_date: str) -> Dict[str, Any]:
        """
        Extracts all details (metadata, lineups, stats) from the current Game Center page.
        """
        # 1. Metadata & Team Info
        metadata = self._extract_metadata(page)
        team_info = self._extract_team_info(page, game_id)
        game_summary = self._extract_game_summary(page)
        
        # 2. Lineups & Stats
        hitters = {
            'away': self._extract_hitters(page, 'Away', team_info['away']['code'], team_info),
            'home': self._extract_hitters(page, 'Home', team_info['home']['code'], team_info),
        }
        
        pitchers = {
            'away': self._extract_pitchers(page, 'Away', team_info['away']['code']),
            'home': self._extract_pitchers(page, 'Home', team_info['home']['code']),
        }
        
        # Log unresolved IDs
        self._log_unresolved_player_ids(game_id, hitters, pitchers)
        
        return {
            'game_id': game_id,
            'game_date': game_date,
            'metadata': metadata,
            'summary': game_summary,
            'teams': team_info,
            'home_team_code': team_info['home']['code'],
            'away_team_code': team_info['away']['code'],
            'hitters': hitters,
            'pitchers': pitchers,
        }

    def _extract_metadata(self, page: Page) -> Dict[str, Any]:
        metadata = {
            'stadium': None,
            'attendance': None,
            'start_time': None,
            'end_time': None,
            'game_time': None,
            'duration_minutes': None,
        }
        try:
            # 2009 Schedule/GameCenter usually has a game info header
            # .sub-tit-area ? or .game-info ?
            # Based on 2009 check, maybe just 'stadium' from schedule table?
            # But here we are in Game Center.
            # Often generic info area exists.
            text = page.inner_text('.box-score-area') if page.query_selector('.box-score-area') else ""
            if not text:
                 text = page.inner_text('body') # Fallback search in body? Too risky.
            
            # Use regex similar to modern crawler
            stadium_match = re.search(r'구장\s*[:：]\s*([^\s]+)', text)
            if stadium_match:
                metadata['stadium'] = stadium_match.group(1).strip()
                
            attendance_match = re.search(r'관중\s*[:：]\s*([\d,]+)', text)
            if attendance_match:
                metadata['attendance'] = int(attendance_match.group(1).replace(',', '').strip())

            start_match = re.search(r'개시\s*[:：]\s*([\d:]+)', text)
            if start_match:
                metadata['start_time'] = start_match.group(1).strip()
                
            duration_match = re.search(r'경기시간\s*[:：]\s*([\d:]+)', text)
            if duration_match:
                metadata['game_time'] = duration_match.group(1).strip()
            
        except Exception:
            pass
        return metadata

    def _extract_team_info(self, page: Page, game_id: str) -> Dict[str, Dict[str, Any]]:
        # In 2009, team names might be in header/scoreboard
        # Fallback to game_id parsing if not found in scoreboard
        season_year = 2009 # Hardcoded or passed?
        try:
            season_year = int(game_id[:4])
        except: pass
        
        # Try to find scoreboard
        # .score-board or table?
        # TODO: Implement actual scoreboard parsing if needed.
        # For now, derive from Game ID segments (HH, SK)
        # Note: 2009 codes might differ from 2024.
        # 'HH' -> Hanwha. 'SK' -> SK.
        
        away_code = game_id[8:10] if len(game_id) >= 10 else "AWAY"
        home_code = game_id[10:12] if len(game_id) >= 12 else "HOME"
        
        # Need to map 'HH' to 'Hanwha'? Or keep 'HH'?
        # src.utils.team_codes has `team_code_from_game_id_segment`.
        # But this crawler needs to return 'code', 'name', 'score', etc.
        
        return {
            'away': {'code': away_code, 'name': away_code, 'score': 0, 'line_score': []},
            'home': {'code': home_code, 'name': home_code, 'score': 0, 'line_score': []}
        }

    def _extract_game_summary(self, page: Page) -> List[Dict[str, str]]:
        # TODO: Implement if exists
        return []

    def _extract_hitters(self, page: Page, side: str, team_code: str, team_info: Dict) -> List[Dict[str, Any]]:
        prefix = f"tbl{side}"
        name_table = page.query_selector(f"#{prefix}Hitter1")
        stat_table = page.query_selector(f"#{prefix}Hitter3")
        
        team_side = side.lower() # 'away' or 'home'
        
        if not name_table or not stat_table:
            return []
            
        names_data = []
        name_rows = name_table.query_selector_all('tbody tr')
        for row in name_rows:
            cells = row.query_selector_all('td, th')
            if len(cells) >= 3:
                # <tr><th>1</th><th>二</th><td>정근우</td></tr>
                # idx 0: Order (1)
                # idx 1: Pos (二)
                # idx 2: Name (정근우)
                order_txt = cells[0].inner_text().strip()
                pos = cells[1].inner_text().strip()
                name = cells[2].inner_text().strip()
                
                batting_order = int(order_txt) if order_txt.isdigit() else None
                
                names_data.append({
                    'name': name,
                    'position': pos,
                    'batting_order': batting_order
                })
            else:
                 names_data.append({'name': 'Unknown', 'position': None, 'batting_order': None})
                
        stats_data = []
        stat_rows = stat_table.query_selector_all('tbody tr')
        # Headers: 타수, 안타, 타점, 득점, 타율
        # Map: AB, H, RBI, RUN, AVG
        # We need to map to modern keys using HITTER_HEADER_MAP if possible, 
        # BUT the headers here are Korean: ['타수', '안타', '타점', '득점', '타율']
        # HITTER_HEADER_MAP has these keys!
        
        # We need headers from the table to be safe
        headers = []
        th_cells = stat_table.query_selector_all('thead th')
        for th in th_cells:
            headers.append(th.inner_text().strip())
            
        for row in stat_rows:
            cells = row.query_selector_all('td')
            row_stats = {}
            for i, cell in enumerate(cells):
                if i < len(headers):
                    key_kr = headers[i]
                    val = cell.inner_text().strip()
                    row_stats[key_kr] = val
            stats_data.append(row_stats)
            
        results = []
        count = min(len(names_data), len(stats_data))
        
        for i in range(count):
            nd = names_data[i]
            sd = stats_data[i]
            
            player_name = nd['name']
            if not player_name or player_name == 'Unknown': continue
            
            # Map stats
            stats = {}
            extras = {}
            for k, v in sd.items():
                key = HITTER_HEADER_MAP.get(k)
                if not key:
                    extras[k] = v
                    continue
                if v in ('', '-'): continue
                
                if key in HITTER_FLOAT_KEYS:
                    try: stats[key] = float(v)
                    except: pass
                else:
                    try: stats[key] = int(v)
                    except: pass
            
            # Resolve ID
            player_id = None
            if self.resolver:
                # Pass approximate year
                try: year = int(team_info['away']['code'][:4]) if '20' in team_info['away']['code'] else 2009 # Hack
                except: year = 2009
                # Actually usage: resolve_id(name, team_code, year)
                # We need real team code (e.g. SK, HH)
                player_id = self.resolver.resolve_id(player_name, team_code, 2009) # Force 2009 for now
                
            payload = {
                'player_id': player_id,
                'player_name': player_name,
                'uniform_no': None, # Not in table?
                'team_code': team_code,
                'team_side': team_side,
                'batting_order': nd['batting_order'],
                'position': nd['position'],
                'is_starter': nd['batting_order'] and nd['batting_order'] <= 9, # Approximation
                'appearance_seq': i + 1,
                'stats': stats,
                'extras': extras or None
            }
            results.append(payload)
            
        return results

    def _extract_pitchers(self, page: Page, side: str, team_code: str) -> List[Dict[str, Any]]:
        suffix = "Pitcher" 
        table_id = f"#tbl{side}{suffix}"
        table = page.query_selector(table_id)
        if not table: return []
             
        team_side = side.lower()
        results = []
        
        # Headers
        headers = []
        for th in table.query_selector_all('thead th'):
            headers.append(th.inner_text().strip())
            
        rows = table.query_selector_all('tbody tr')
        for idx, row in enumerate(rows, start=1):
            cells = row.query_selector_all('td')
            if not cells: continue
            
            row_data = {}
            # Verify col count?
            # Just map by index
            for i, cell in enumerate(cells):
                if i < len(headers):
                    row_data[headers[i]] = cell.inner_text().strip()
            
            player_name = row_data.get('선수명')
            if not player_name: continue
            
            stats = {}
            extras = {}
            
            for k, v in row_data.items():
                if k == '선수명': continue
                key = PITCHER_HEADER_MAP.get(k)
                if not key:
                    extras[k] = v
                    continue
                if v in ('', '-'): continue
                
                if key in PITCHER_FLOAT_KEYS:
                    try: stats[key] = float(v)
                    except: pass
                elif key == 'innings': # '이닝'
                     # Parse innings "5 1/3"
                     stats['innings_outs'] = self._parse_innings_to_outs(v)
                else:
                    try: stats[key] = int(v)
                    except: pass
            
            # Decision
            # '결과' -> W, L, S
            if '결과' in row_data:
                decision = self._parse_decision(row_data['결과'])
                if decision:
                    stats['decision'] = decision
                    
            # Resolve ID
            player_id = None
            if self.resolver:
                player_id = self.resolver.resolve_id(player_name, team_code, 2009)

            payload = {
                'player_id': player_id,
                'player_name': player_name,
                'uniform_no': None,
                'team_code': team_code,
                'team_side': team_side,
                'is_starting': idx == 1,
                'appearance_seq': idx,
                'stats': stats,
                'extras': extras or None
            }
            results.append(payload)
            
        return results

    def _parse_decision(self, text: str) -> Optional[str]:
        if '승' in text: return 'W'
        if '패' in text: return 'L'
        if '세' in text: return 'S'
        if '홀' in text: return 'H'
        return None

    def _parse_innings_to_outs(self, text: str) -> int:
        # "5 1/3" -> 16
        if not text: return 0
        try:
            parts = text.split()
            whole = int(parts[0]) if parts[0].isdigit() else 0
            frac = 0
            if len(parts) > 1:
                if '1/3' in parts[1]: frac = 1
                elif '2/3' in parts[1]: frac = 2
            return whole * 3 + frac
        except:
            return 0
            
    def _log_unresolved_player_ids(self, game_id, hitters, pitchers):
        # Implementation omitted for brevity, similar to GameDetailCrawler
        pass
