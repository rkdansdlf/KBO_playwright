from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy import select, and_

from src.models.player import PlayerBasic, PlayerSeasonBatting, PlayerSeasonPitching

class PlayerIdResolver:
    """
    Resolver ensuring player IDs are found even if missing in game crawl data.
    Strategy:
    1. Query 'player_basic' (metadata from search crawler).
    2. Query 'player_season_batting' (season stats).
    3. Query 'player_season_pitching' (season stats).
    """

    def __init__(self, session: Session):
        self.session = session
        self._cache = {}

    def resolve_id(self, player_name: str, team_code: str, season: int) -> Optional[int]:
        """
        Resolve player_id by Name, Team, and Season.
        
        Args:
            player_name: Korean name (e.g. "박찬호")
            team_code: Team code (e.g. "HT")
            season: Season year (e.g. 2025)
        """
        if not player_name:
            return None
            
        cache_key = f"{player_name}_{team_code}_{season}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        # 1. Try PlayerBasic (most reliable for active roster)
        # Note: PlayerBasic.team is a Korean name (e.g. "KIA"), we might need mapping if team_code is "HT"
        # Since mapping is complex, let's try direct matches first or use mapped team name if available.
        # But for now, let's look at season stats first which use codes.

        # 2. Try PlayerSeasonBatting
        stmt = select(PlayerSeasonBatting.player_id).join(PlayerBasic, PlayerSeasonBatting.player_id == PlayerBasic.player_id).where(
            PlayerBasic.name == player_name,
            PlayerSeasonBatting.season == season,
            PlayerSeasonBatting.team_code == team_code
        )
        result = self.session.execute(stmt).first()
        if result:
            self._cache[cache_key] = result[0]
            return result[0]

        # 3. Try PlayerSeasonPitching
        stmt = select(PlayerSeasonPitching.player_id).join(PlayerBasic, PlayerSeasonPitching.player_id == PlayerBasic.player_id).where(
            PlayerBasic.name == player_name,
            PlayerSeasonPitching.season == season,
            PlayerSeasonPitching.team_code == team_code
        )
        result = self.session.execute(stmt).first()
        if result:
            self._cache[cache_key] = result[0]
            return result[0]

        # 4. Fallback to PlayerBasic independent of season/team code strict match
        # We try to match by name and team (fuzzy or mapped)
        
        team_name_map = {
            'HT': 'KIA', 'KIA': 'KIA',
            'LG': 'LG',
            'SS': '삼성', 'SAMSUNG': '삼성',
            'KT': 'KT',
            'NC': 'NC',
            'OB': '두산', 'DOOSAN': '두산', 'BEARS': '두산',
            'LT': '롯데', 'LOT': '롯데', 'LOTTE': '롯데',
            'HH': '한화', 'HANWHA': '한화',
            'WO': '키움', 'KIWOOM': '키움', 'HEROES': '키움',
            'SK': 'SSG', 'SSG': 'SSG',
            'NX': '키움', # Nexen -> Kiwoom lineage (simple map)
        }
        
        kor_team_name = team_name_map.get(team_code, '')
        
        if kor_team_name:
            stmt = select(PlayerBasic.player_id).where(
                PlayerBasic.name == player_name,
                PlayerBasic.team.contains(kor_team_name)
            )
            result = self.session.execute(stmt).first()
            if result:
                self._cache[cache_key] = result[0]
                return result[0]
        
        # 5. Fallback: Unique Name Check (Handle traded players where team mismatch exists)
        # If there is only ONE player with this name in the entire DB, assume it's them.
        stmt = select(PlayerBasic.player_id).where(PlayerBasic.name == player_name)
        results = self.session.execute(stmt).fetchall()
        
        if len(results) == 1:
            pid = results[0][0]
            self._cache[cache_key] = pid
            return pid
            
        return None
