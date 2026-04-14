"""Legacy relay repository compatibility layer."""
from __future__ import annotations

from typing import Any, Dict, List

from src.db.engine import SessionLocal
from src.models.game import GamePlayByPlay
from src.repositories.game_repository import save_relay_data as save_normalized_relay_data


def save_relay_data(game_id: str, innings_data: List[Dict[str, Any]]) -> int:
    """
    Backward-compatible wrapper that flattens inning-grouped relay payloads and
    forwards them to the canonical writer in game_repository.
    """
    flat_rows: list[dict[str, Any]] = []
    for inning_data in innings_data or []:
        inning = inning_data.get("inning")
        half = inning_data.get("half")
        for play in inning_data.get("plays", []) or []:
            flat_rows.append(
                {
                    "inning": inning,
                    "inning_half": half,
                    "pitcher_name": play.get("pitcher"),
                    "batter_name": play.get("batter"),
                    "play_description": play.get("description"),
                    "event_type": play.get("event_type"),
                    "result": play.get("result"),
                }
            )
    return save_normalized_relay_data(game_id, events=None, raw_pbp_rows=flat_rows, allow_derived_pbp=False)


def get_game_relay_summary(game_id: str) -> Dict[str, Any]:
    with SessionLocal() as session:
        plays = (
            session.query(GamePlayByPlay)
            .filter(GamePlayByPlay.game_id == game_id)
            .all()
        )

        if not plays:
            return {
                "game_id": game_id,
                "total_plays": 0,
                "innings": 0,
                "event_types": {},
            }

        innings_set = {(play.inning, play.inning_half) for play in plays}

        return {
            "game_id": game_id,
            "total_plays": len(plays),
            "innings": len(innings_set),
            "event_types": {
                "batting": sum(1 for play in plays if play.event_type == "batting"),
                "strikeout": sum(1 for play in plays if play.event_type == "strikeout"),
                "walk": sum(1 for play in plays if play.event_type == "walk"),
                "hit": sum(1 for play in plays if play.event_type == "hit"),
                "home_run": sum(1 for play in plays if play.event_type == "home_run"),
            },
        }
