"""Repository for Player Draft History entries."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from src.models.player_draft import PlayerDraftHistory

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class PlayerDraftRepository:
    """Repository for managing player rookie draft history records."""

    def __init__(self, session: Session) -> None:
        """Initialize repository.

        Args:
            session: DB Session.

        """
        self.session = session

    def save_draft_entry(self, data: dict[str, Any]) -> PlayerDraftHistory:
        """Save or update a draft history record.

        Args:
            data: Draft dictionary data.

        Returns:
            Saved ORM instance.

        """
        season = data["season"]
        draft_type = data.get("draft_type", "2차")
        round_num = data.get("round_num", 1)
        pick_seq = data.get("pick_seq", 1)

        stmt = select(PlayerDraftHistory).where(
            PlayerDraftHistory.season == season,
            PlayerDraftHistory.draft_type == draft_type,
            PlayerDraftHistory.round_num == round_num,
            PlayerDraftHistory.pick_seq == pick_seq,
        )
        existing = self.session.execute(stmt).scalar_one_or_none()

        if existing:
            existing.team_code = data.get("team_code", existing.team_code)
            existing.player_name = data.get("player_name", existing.player_name)
            existing.player_id = data.get("player_id", existing.player_id)
            existing.position = data.get("position", existing.position)
            existing.school = data.get("school", existing.school)
            existing.sign_fee = data.get("sign_fee", existing.sign_fee)
            return existing

        record = PlayerDraftHistory(
            season=season,
            draft_type=draft_type,
            round_num=round_num,
            pick_seq=pick_seq,
            team_code=data["team_code"],
            player_name=data["player_name"],
            player_id=data.get("player_id"),
            position=data.get("position"),
            school=data.get("school"),
            sign_fee=data.get("sign_fee"),
        )
        self.session.add(record)
        self.session.flush()
        return record

    def get_draft_by_season(self, season: int) -> list[PlayerDraftHistory]:
        """Get draft history records for a season.

        Args:
            season: Season year.

        Returns:
            List of draft records sorted by pick sequence.

        """
        stmt = (
            select(PlayerDraftHistory)
            .where(PlayerDraftHistory.season == season)
            .order_by(PlayerDraftHistory.pick_seq.asc())
        )
        return list(self.session.execute(stmt).scalars().all())
