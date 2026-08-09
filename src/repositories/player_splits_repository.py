"""Repository for Player Splits Statistics entries."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from src.models.player_splits_stat import PlayerSplitsStat

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class PlayerSplitsRepository:
    """Repository for managing player situational/split statistics."""

    def __init__(self, session: Session) -> None:
        """Initialize repository.

        Args:
            session: DB Session.

        """
        self.session = session

    def save_splits_entry(self, data: dict[str, Any]) -> PlayerSplitsStat:
        """Save or update a player splits stat record.

        Args:
            data: Splits dictionary data.

        Returns:
            Saved ORM instance.

        """
        season = data["season"]
        player_id = data["player_id"]
        split_type = data["split_type"]
        split_key = data["split_key"]

        stmt = select(PlayerSplitsStat).where(
            PlayerSplitsStat.season == season,
            PlayerSplitsStat.player_id == player_id,
            PlayerSplitsStat.split_type == split_type,
            PlayerSplitsStat.split_key == split_key,
        )
        existing = self.session.execute(stmt).scalar_one_or_none()

        if existing:
            existing.ab = data.get("ab", existing.ab)
            existing.hits = data.get("hits", existing.hits)
            existing.hr = data.get("hr", existing.hr)
            existing.rbi = data.get("rbi", existing.rbi)
            existing.bb = data.get("bb", existing.bb)
            existing.so = data.get("so", existing.so)
            existing.avg = data.get("avg", existing.avg)
            existing.obp = data.get("obp", existing.obp)
            existing.slg = data.get("slg", existing.slg)
            existing.ops = data.get("ops", existing.ops)
            return existing

        record = PlayerSplitsStat(
            season=season,
            player_id=player_id,
            player_name=data["player_name"],
            team_code=data.get("team_code"),
            split_type=split_type,
            split_key=split_key,
            ab=data.get("ab", 0),
            hits=data.get("hits", 0),
            hr=data.get("hr", 0),
            rbi=data.get("rbi", 0),
            bb=data.get("bb", 0),
            so=data.get("so", 0),
            avg=data.get("avg", 0.0),
            obp=data.get("obp", 0.0),
            slg=data.get("slg", 0.0),
            ops=data.get("ops", 0.0),
        )
        self.session.add(record)
        self.session.flush()
        return record

    def get_splits_by_player(
        self,
        player_id: str,
        season: int = 2026,
    ) -> list[PlayerSplitsStat]:
        """Get situational splits for a player in a season.

        Args:
            player_id: Player ID.
            season: Season year.

        Returns:
            List of player splits records.

        """
        stmt = (
            select(PlayerSplitsStat)
            .where(PlayerSplitsStat.player_id == player_id, PlayerSplitsStat.season == season)
            .order_by(PlayerSplitsStat.split_type.asc(), PlayerSplitsStat.split_key.asc())
        )
        return list(self.session.execute(stmt).scalars().all())
