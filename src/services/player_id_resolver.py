
import csv
import os
from typing import Optional, Dict
from sqlalchemy.orm import Session
from sqlalchemy import select, and_, or_

from src.models.player import PlayerBasic, PlayerSeasonBatting, PlayerSeasonPitching

ALIAS_CSV_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'player_name_aliases.csv')

class PlayerIdResolver:
    """
    Resolver ensuring player IDs are found even if missing in game crawl data.
    """

    def __init__(self, session: Session):
        self.session = session
        self._cache = {}
        
        # Load name aliases from CSV
        self.NAME_ALIASES: Dict[str, str] = self._load_aliases_from_csv()
        
        # All-Star and International team mappings
        self.ALL_STAR_TEAMS = {
            'EA': 'East', 
            'WE': 'West', 
            'DRE': '드림', 
            'NAN': '나눔',
            '드림': '드림',
            '나눔': '나눔',
            'KR': 'Korea',
            'JP': 'Japan',
            'TW': 'Taiwan',
            'NL': 'Nanum',
            'DL': 'Dream'
        }
        
        # Comprehensive historical team mapping for disambiguation
        self.TEAM_NAME_MAP = {
            # Active
            'LG': 'LG',
            'SS': '삼성', 'SAMSUNG': '삼성',
            'KT': 'KT',
            'NC': 'NC',
            'LT': '롯데', 'LOT': '롯데', 'LOTTE': '롯데',
            'HH': '한화', 'HANWHA': '한화',
            'KIA': 'KIA', 'HT': 'KIA', '해태': 'KIA',
            'DB': '두산', 'OB': '두산', 'DOOSAN': '두산', 'BEARS': '두산',
            'SSG': 'SSG', 'SK': 'SSG',
            'KH': '키움', 'WO': '키움', 'NX': '키움', 'KIWOOM': '키움', 'HEROES': '키움',
            
            # Historical / Defunct
            'HD': '현대', 'HYUNDAI': '현대', '현대': '현대',
            'SL': '쌍방울', '쌍방울': '쌍방울',
            'TP': '태평양', '태평양': '태평양',
            'CB': '청보', '청보': '청보',
            'SM': '삼미', '삼미': '삼미',
            'BE': '빙그레', '빙그레': '빙그레',
            'MBC': 'MBC', '청룡': 'MBC',
        }
        self.TEAM_NAME_MAP.update(self.ALL_STAR_TEAMS)

    @staticmethod
    def _load_aliases_from_csv() -> Dict[str, str]:
        aliases: Dict[str, str] = {}
        csv_path = os.path.normpath(ALIAS_CSV_PATH)
        if not os.path.exists(csv_path):
            return aliases
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    old = row.get('old_name', '').strip()
                    new = row.get('new_name', '').strip()
                    if old and new and old != new:
                        aliases[old] = new
        except Exception:
            pass
        return aliases

    def preload_season_index(self, season: int) -> None:
        print(f"🔄 Preloading player index for season {season}...")
        
        # Batters
        stmt = select(PlayerBasic.name, PlayerSeasonBatting.team_code, PlayerSeasonBatting.player_id)\
            .join(PlayerBasic, PlayerSeasonBatting.player_id == PlayerBasic.player_id)\
            .where(PlayerSeasonBatting.season == season)
        for name, team, pid in self.session.execute(stmt).fetchall():
            self._cache[f"{name}_{team}_{season}"] = pid
            
        # Pitchers 
        stmt = select(PlayerBasic.name, PlayerSeasonPitching.team_code, PlayerSeasonPitching.player_id)\
            .join(PlayerBasic, PlayerSeasonPitching.player_id == PlayerBasic.player_id)\
            .where(PlayerSeasonPitching.season == season)
        for name, team, pid in self.session.execute(stmt).fetchall():
            self._cache[f"{name}_{team}_{season}"] = pid

    def resolve_id(self, player_name: str, team_code: str, season: int, uniform_no: Optional[str] = None) -> Optional[int]:
        if not player_name:
            return None

        if player_name in self.NAME_ALIASES:
            player_name = self.NAME_ALIASES[player_name]
            
        cache_key = f"{player_name}_{team_code}_{season}_{uniform_no or ''}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        # 1. Try Seasonal Data (Most accurate)
        for model in [PlayerSeasonBatting, PlayerSeasonPitching]:
            stmt = select(PlayerBasic.player_id).select_from(model).join(
                PlayerBasic, model.player_id == PlayerBasic.player_id
            ).where(
                PlayerBasic.name == player_name,
                model.season == season
            )
            if team_code and team_code not in self.ALL_STAR_TEAMS:
                stmt = stmt.where(model.team_code == team_code)
            if uniform_no:
                stmt = stmt.where(PlayerBasic.uniform_no == str(uniform_no))
            
            result = self.session.execute(stmt).first()
            if result:
                self._cache[cache_key] = result[0]
                return result[0]

        # 2. Try PlayerBasic with Team/Career context
        kor_team_name = self.TEAM_NAME_MAP.get(team_code, '')
        if kor_team_name:
            stmt = select(PlayerBasic.player_id).where(
                PlayerBasic.name == player_name,
                or_(
                    PlayerBasic.team.contains(kor_team_name),
                    PlayerBasic.career.contains(kor_team_name)
                )
            )
            if uniform_no:
                stmt = stmt.where(PlayerBasic.uniform_no == str(uniform_no))
            
            results = self.session.execute(stmt).fetchall()
            if len(results) == 1:
                pid = results[0][0]
                self._cache[cache_key] = pid
                return pid

        # 3. Fallback: Relaxed Uniqueness Check
        # If we have uniform_no, try unique by (name, uniform_no) global
        if uniform_no:
            stmt = select(PlayerBasic.player_id).where(
                PlayerBasic.name == player_name,
                PlayerBasic.uniform_no == str(uniform_no)
            )
            results = self.session.execute(stmt).fetchall()
            if len(results) == 1:
                pid = results[0][0]
                self._cache[cache_key] = pid
                return pid

        # 4. Ultimate Fallback: Is the name unique in the entire KBO history?
        stmt = select(PlayerBasic.player_id).where(PlayerBasic.name == player_name)
        results = self.session.execute(stmt).fetchall()
        if len(results) == 1:
            pid = results[0][0]
            self._cache[cache_key] = pid
            return pid
            
        # 5. Last resort: Try relaxed季節(season) resolution without uniform_no or strict team
        relaxed_id = self._resolve_relaxed(player_name, team_code, season)
        if relaxed_id:
            self._cache[cache_key] = relaxed_id
            return relaxed_id

        return None

    def _resolve_relaxed(self, player_name: str, team_code: str, season: int) -> Optional[int]:
        """Relaxed matching: Name + Season match, ensuring exactly one candidate."""
        candidates = set()
        for model in [PlayerSeasonBatting, PlayerSeasonPitching]:
            stmt = select(PlayerBasic.player_id).select_from(model).join(
                PlayerBasic, model.player_id == PlayerBasic.player_id
            ).where(
                PlayerBasic.name == player_name,
                model.season == season
            )
            if team_code and team_code not in self.ALL_STAR_TEAMS:
                # Still try to filter by team if possible, but don't fail if team_code is weird
                pass
            
            for row in self.session.execute(stmt).fetchall():
                candidates.add(row[0])
        
        if len(candidates) == 1:
            return list(candidates)[0]
        
        # Try PlayerBasic with team/career again but even more relaxed
        kor_team_name = self.TEAM_NAME_MAP.get(team_code, '')
        if kor_team_name:
            stmt = select(PlayerBasic.player_id).where(
                PlayerBasic.name == player_name,
                or_(
                    PlayerBasic.team.contains(kor_team_name),
                    PlayerBasic.career.contains(kor_team_name)
                )
            )
            results = self.session.execute(stmt).fetchall()
            if len(results) == 1:
                return results[0][0]

        return None
