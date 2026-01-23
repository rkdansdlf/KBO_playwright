from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import select, delete

from ..models.award import Award

class AwardRepository:
    def __init__(self, session: Session):
        self.session = session

    def save_award(self, award_data: dict) -> Award:
        """
        Insert or update an award record.
        Uses unique constraints to detect duplicates. 
        For now, we'll strive for upsert-like behavior or just ignore if exists.
        But given the unique constraint on (year, award_type, category, player_name, team_name),
        if we try to insert a duplicate, it will fail.
        
        Since awards are static historical data, if it exists, we usually don't need to update it unless corrected.
        We will use a 'get or create' approach.
        """
        year = award_data["year"]
        award_type = award_data["award_type"]
        category = award_data.get("category")
        player_name = award_data["player_name"]
        team_name = award_data["team_name"]

        stmt = select(Award).where(
            Award.year == year,
            Award.award_type == award_type,
            Award.category == category,
            Award.player_name == player_name,
            Award.team_name == team_name
        )
        existing = self.session.execute(stmt).scalar_one_or_none()

        if existing:
            return existing
        
        new_award = Award(
            year=year,
            award_type=award_type,
            category=category,
            player_name=player_name,
            team_name=team_name
        )
        self.session.add(new_award)
        # We don't commit here to allow batch processing by the caller
        return new_award

    def get_awards_by_year(self, year: int) -> List[Award]:
        stmt = select(Award).where(Award.year == year)
        return list(self.session.execute(stmt).scalars().all())

    def clear_awards_by_year(self, year: int):
        stmt = delete(Award).where(Award.year == year)
        self.session.execute(stmt)
