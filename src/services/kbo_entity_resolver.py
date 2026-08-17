"""Resolve extracted KBO names to canonical database entities."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import or_, select
from sqlalchemy.exc import SQLAlchemyError

from src.models.player import PlayerBasic, PlayerSeasonBatting, PlayerSeasonPitching
from src.utils.kbo_entity_extractor import ExtractedKboEntities, extract_kbo_entities

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@dataclass(frozen=True)
class ResolvedKboEntities:
    """Hold extracted entities plus canonical IDs resolved from the database."""

    extracted: ExtractedKboEntities
    player_id: str | None = None
    ambiguous_player: bool = False

    def to_filters(self) -> dict[str, Any]:
        """Return filters suitable for structured, sparse, and vector retrieval."""
        filters = self.extracted.to_filters()
        if self.player_id:
            filters["player_id"] = self.player_id
        else:
            # An unresolved token is not reliable enough for a strict text filter.
            filters.pop("player_name", None)
        return filters


def resolve_kbo_entities(
    session: Session,
    query: str,
    filters: dict[str, Any] | None = None,
    *,
    extract_player: bool = True,
) -> ResolvedKboEntities:
    """Resolve a natural-language query against canonical KBO player records."""
    explicit_filters = filters or {}
    has_null_player = "player_name" in explicit_filters and explicit_filters["player_name"] is None
    extract_player = extract_player and not has_null_player
    extracted = extract_kbo_entities(query, extract_player=extract_player)
    explicit_player_id = explicit_filters.get("player_id")
    if explicit_player_id is not None:
        return ResolvedKboEntities(extracted, player_id=str(explicit_player_id))

    if not extracted.player_name:
        return ResolvedKboEntities(extracted)

    stmt = select(PlayerBasic).where(PlayerBasic.name == extracted.player_name)
    if extracted.team_id and extracted.season_year is None:
        stmt = stmt.where(
            or_(
                PlayerBasic.team == extracted.team_id,
                PlayerBasic.team.icontains(extracted.team_id),
            )
        )

    try:
        players = list(session.execute(stmt).scalars().all())
    except (SQLAlchemyError, RuntimeError, TypeError, AttributeError):
        session.rollback()
        return ResolvedKboEntities(extracted)

    if extracted.season_year and extracted.team_id and players:
        try:
            player_ids = [player.player_id for player in players]
            seasonal_ids: set[int] = set()
            for model in (PlayerSeasonBatting, PlayerSeasonPitching):
                seasonal_stmt = select(model.player_id).where(
                    model.player_id.in_(player_ids),
                    model.season == extracted.season_year,
                    or_(model.team_code == extracted.team_id, model.canonical_team_code == extracted.team_id),
                )
                seasonal_ids.update(row[0] for row in session.execute(seasonal_stmt).all())
            players = [player for player in players if player.player_id in seasonal_ids]
        except (SQLAlchemyError, RuntimeError, TypeError, AttributeError):
            session.rollback()
            return ResolvedKboEntities(extracted)

    if len(players) != 1:
        return ResolvedKboEntities(extracted, ambiguous_player=len(players) > 1)
    return ResolvedKboEntities(extracted, player_id=str(players[0].player_id))
