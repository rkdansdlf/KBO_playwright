"""
Repository for Team related data (Roster, Info, etc.)
"""
from typing import List, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import text

from src.models.team import TeamDailyRoster

class TeamRepository:
    def __init__(self, session: Session):
        self.session = session

    def save_daily_rosters(self, rosters: List[Dict[str, Any]]) -> int:
        """
        Save daily roster records.
        Uses deletion of same (date, team, player) conflict or ignore?
        Better: UPSERT or Ignore.
        Since it's a snapshot, if we run it again for same date/team, we might have same data.
        If data changed? (Unlikely for same date).
        
        We will use Merge or Upsert logic.
        Since we have UniqueConstraint(roster_date, team_code, player_id), we can check existence or just merge.
        Bulk insert with ignore might be faster for large sets.
        Merging individual objects:
        """
        count = 0
        for r in rosters:
            # Check existing? 
            # Or use merge.
            # Roster object
            roster = TeamDailyRoster(
                roster_date=r['roster_date'],
                team_code=r['team_code'],
                player_id=r['player_id'],
                player_name=r['player_name'],
                position=r['position'],
                back_number=r['back_number']
            )
            
            # Merge is safest
            self.session.merge(roster)
            count += 1
            
        self.session.commit()
        return count
