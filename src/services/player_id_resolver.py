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
        
        # Known KBO Player Name Changes (Historical -> Current)
        self.NAME_ALIASES = {
            '한동민': '한유섬',
            '신용수': '신윤후',
            '유장혁': '유로결',
            '노성호': '노유상',
            '윤지웅': '윤지완',
            '강윤구': '강리호',
            '백민기': '백동훈',
            '박계현': '박승욱',
            '이병휘': '이유찬',
            '김지수': '김지수', # Handle duplicates later
        }
        
        # All-Star team mappings to Korean lookup names
        self.ALL_STAR_TEAMS = {
            'EA': 'East', 
            'WE': 'West', 
            'DRE': '드림', 
            'NAN': '나눔',
            '드림': '드림',
            '나눔': '나눔'
        }

    def preload_season_index(self, season: int) -> None:
        """
        Preload all player mappings for a given season into cache to avoid N+1 queries.
        """
        print(f"🔄 Preloading player index for season {season}...")
        
        # 1. Batters
        stmt = select(PlayerBasic.name, PlayerSeasonBatting.team_code, PlayerSeasonBatting.player_id)\
            .join(PlayerBasic, PlayerSeasonBatting.player_id == PlayerBasic.player_id)\
            .where(PlayerSeasonBatting.season == season)
        
        batters = self.session.execute(stmt).fetchall()
        for name, team, pid in batters:
            key = f"{name}_{team}_{season}"
            self._cache[key] = pid
            
        # 2. Pitchers 
        stmt = select(PlayerBasic.name, PlayerSeasonPitching.team_code, PlayerSeasonPitching.player_id)\
            .join(PlayerBasic, PlayerSeasonPitching.player_id == PlayerBasic.player_id)\
            .where(PlayerSeasonPitching.season == season)
        
        pitchers = self.session.execute(stmt).fetchall()
        for name, team, pid in pitchers:
            key = f"{name}_{team}_{season}"
            self._cache[key] = pid
            
        print(f"✅ Preloaded {len(batters) + len(pitchers)} mappings.")

    def resolve_id(self, player_name: str, team_code: str, season: int) -> Optional[int]:
        """
        Resolve player_id by Name, Team, and Season.
        """
        if not player_name:
            return None

        # 0. Handle Name Aliases (Name Changes)
        orig_name = player_name
        if player_name in self.NAME_ALIASES:
            player_name = self.NAME_ALIASES[player_name]
            
        cache_key = f"{player_name}_{team_code}_{season}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        # 1. Try PlayerSeasonBatting (with team context)
        # Note: If All-Star team code, we might not have season stats with that code.
        # But usually All-Star players have regular season stats.
        
        stmt = select(PlayerSeasonBatting.player_id).join(PlayerBasic, PlayerSeasonBatting.player_id == PlayerBasic.player_id).where(
            PlayerBasic.name == player_name,
            PlayerSeasonBatting.season == season
        )
        if team_code and team_code not in self.ALL_STAR_TEAMS:
            stmt = stmt.where(PlayerSeasonBatting.team_code == team_code)
            
        result = self.session.execute(stmt).first()
        if result:
            self._cache[cache_key] = result[0]
            return result[0]

        # 2. Try PlayerSeasonPitching
        stmt = select(PlayerSeasonPitching.player_id).join(PlayerBasic, PlayerSeasonPitching.player_id == PlayerBasic.player_id).where(
            PlayerBasic.name == player_name,
            PlayerSeasonPitching.season == season
        )
        if team_code and team_code not in self.ALL_STAR_TEAMS:
            stmt = stmt.where(PlayerSeasonPitching.team_code == team_code)
            
        result = self.session.execute(stmt).first()
        if result:
            self._cache[cache_key] = result[0]
            return result[0]

        # 3. Fallback to PlayerBasic
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
            'NX': '키움',
        }
        
        # Add All-Star mappings to team_name_map
        team_name_map.update(self.ALL_STAR_TEAMS)
        
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
        
        # 4. Fallback: Unique Name Check (Global)
        stmt = select(PlayerBasic.player_id).where(PlayerBasic.name == player_name)
        results = self.session.execute(stmt).fetchall()
        
        if len(results) == 1:
            pid = results[0][0]
            self._cache[cache_key] = pid
            return pid
        elif len(results) > 1:
            # If multiple matches, and we have a team name, try harder
            if kor_team_name:
                # This was already tried in step 3, but maybe the team field is complex
                # Let's try to find if any of them have this team in their career
                for r in results:
                    pid = r[0]
                    stmt_career = select(PlayerBasic.career).where(PlayerBasic.player_id == pid)
                    career = self.session.execute(stmt_career).scalar()
                    if career and kor_team_name in career:
                        self._cache[cache_key] = pid
                        return pid
            
        return None
