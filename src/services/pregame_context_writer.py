"""Persistence for pregame context, shared by the preview batch and replay.

The pregame write lives here rather than in the batch that calls it, because
two callers own it: the daily preview batch and the replay dispatcher. The
dispatcher has no batch of its own -- a replay exists to store what it
refreshed -- so leaving the writer inside the CLI meant a service importing a
command module, which inverts the layering this repository is built on.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import SessionLocal
from src.repositories.game_repository import save_pregame_lineups
from src.services.context_aggregator import ContextAggregator
from src.utils.date_helpers import parse_date_str
from src.utils.team_codes import resolve_team_code

if TYPE_CHECKING:
    from datetime import date

logger = logging.getLogger(__name__)

#: Failures that must not stop the rest of the date from being stored. Moved
#: verbatim from the batch module so the write keeps swallowing exactly what it
#: swallowed before: a context lookup that fails still leaves the lineups saved.
PREVIEW_CONTEXT_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, KeyError)


def _add_team_context(
    preview: dict[str, object],
    agg: ContextAggregator,
    season_year: int,
    target_dt_obj: date,
) -> None:
    """Attach recent form, roster and series context to one preview."""
    game_id = preview.get("game_id")
    away_code = resolve_team_code(preview.get("away_team_name"), season_year)  # type: ignore[arg-type]
    home_code = resolve_team_code(preview.get("home_team_name"), season_year)  # type: ignore[arg-type]
    if not away_code or not home_code:
        return

    try:
        logger.info("📊 Aggregating pregame context for %s...", game_id)
        preview["matchup_h2h"] = agg.get_head_to_head_summary(
            away_code,
            home_code,
            season_year,
            target_dt_obj,
        )
        preview["away_recent_l10"] = agg.get_team_l10_summary(away_code, target_dt_obj)
        preview["home_recent_l10"] = agg.get_team_l10_summary(home_code, target_dt_obj)
        preview["away_metrics"] = agg.get_team_recent_metrics(away_code, target_dt_obj)
        preview["home_metrics"] = agg.get_team_recent_metrics(home_code, target_dt_obj)
        preview["away_movements"] = agg.get_recent_player_movements(away_code, target_dt_obj)
        preview["home_movements"] = agg.get_recent_player_movements(home_code, target_dt_obj)
        preview["away_roster_changes"] = agg.get_daily_roster_changes(away_code, target_dt_obj)
        preview["home_roster_changes"] = agg.get_daily_roster_changes(home_code, target_dt_obj)

        series_context = agg.get_postseason_series_summary(away_code, home_code, season_year, target_dt_obj)
        if series_context:
            preview["series_context"] = series_context
    except PREVIEW_CONTEXT_EXCEPTIONS:
        logger.exception("⚠️ Context aggregation failed for %s", game_id)


def _add_pitcher_context(preview: dict[str, object], agg: ContextAggregator, season_year: int) -> None:
    """Attach the starting pitchers' season statistics to one preview."""
    game_id = preview.get("game_id")
    try:
        away_starter_id = preview.get("away_starter_id")
        home_starter_id = preview.get("home_starter_id")
        if away_starter_id:
            preview["away_starter_stats"] = agg.get_pitcher_season_stats(away_starter_id, season_year)  # type: ignore[arg-type]
        if home_starter_id:
            preview["home_starter_stats"] = agg.get_pitcher_season_stats(home_starter_id, season_year)  # type: ignore[arg-type]
    except PREVIEW_CONTEXT_EXCEPTIONS:
        logger.exception("⚠️ Pitcher stats aggregation failed for %s", game_id)


def save_preview_contexts(previews: list[dict[str, object]], target_date: str) -> list[str]:
    """Persist pregame lineups and context for one date.

    Args:
        previews: Pregame documents to store.
        target_date: Target date as ``YYYYMMDD``.

    Returns:
        The game ids that were stored.

    """
    saved_ids: list[str] = []
    target_dt_obj = parse_date_str(target_date)
    season_year = target_dt_obj.year

    with SessionLocal() as session:
        agg = ContextAggregator(session)
        for preview in previews:
            game_id = preview.get("game_id")
            if not game_id:
                continue
            _add_team_context(preview, agg, season_year, target_dt_obj)
            _add_pitcher_context(preview, agg, season_year)
            if save_pregame_lineups(preview):
                saved_ids.append(str(game_id))
    return saved_ids


__all__ = ["save_preview_contexts"]
