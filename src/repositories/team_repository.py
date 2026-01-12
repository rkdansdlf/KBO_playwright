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
        Save daily roster records with UPSERT logic.
        """
        from sqlalchemy import select
        
        # Deduplicate input list by unique key (date, team, player)
        # to prevent IntegrityError if the list contains duplicates
        unique_rosters = {}
        for r in rosters:
            key = (r['roster_date'], r['team_code'], r['player_id'])
            # If duplicate, keep the last one (arbitrary decision, or first?)
            unique_rosters[key] = r
            
        count = 0
        for r in unique_rosters.values():
            # Check existing by Unique Constraint keys
            stmt = select(TeamDailyRoster).where(
                TeamDailyRoster.roster_date == r['roster_date'],
                TeamDailyRoster.team_code == r['team_code'],
                TeamDailyRoster.player_id == r['player_id']
            )
            existing = self.session.execute(stmt).scalar_one_or_none()
            
            if existing:
                # Update fields
                existing.player_name = r['player_name']
                existing.position = r['position']
                existing.back_number = r['back_number']
                existing.updated_at = text('CURRENT_TIMESTAMP')
            else:
                # Create new
                new_roster = TeamDailyRoster(
                    roster_date=r['roster_date'],
                    team_code=r['team_code'],
                    player_id=r['player_id'],
                    player_name=r['player_name'],
                    position=r['position'],
                    back_number=r['back_number']
                )
                self.session.add(new_roster)
            
            count += 1
            
        self.session.commit()
        return count
