"""
Relay data persistence and backfill functions.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Iterable

from src.db.engine import SessionLocal
from src.models.game import (
    Game,
    GameBattingStat,
    GameEvent,
    GameInningScore,
    GameLineup,
    GamePitchingStat,
    GamePlayByPlay,
)
from src.repositories.game_helpers import (
    GAME_STATUS_UNRESOLVED,
    _apply_game_team_identity,
    _auto_sync_to_oci,
    _canonicalize_game_id,
    _enrich_existing_child_team_identity,
    _ensure_game_stub,
    _has_game_child_rows,
    _infer_pitcher_from_children,
    _infer_score_from_children,
    _infer_team_code_from_children,
    _record_game_id_alias,
    _replace_orm_records,
    _resolve_game_season_id,
    _resolve_terminal_status,
    _resolve_winner,
)
from src.services.game_write_contract import GameWriteContract, GameWriteSource
from src.sources.relay.base import event_has_minimum_state, event_to_pbp_row, normalize_pbp_row
from src.utils.safe_print import safe_print as print

logger = logging.getLogger(__name__)


def derive_play_by_play_rows_from_events(events: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Deterministically project normalized game_events into lightweight play_by_play rows."""
    return [event_to_pbp_row(event) for event in events]


def backfill_game_play_by_play_from_existing_events(game_id: str) -> int:
    """Regenerate game_play_by_play rows from stored game_events for one game."""
    game_id, _ = _canonicalize_game_id(game_id)
    if not game_id:
        return 0
    with SessionLocal() as session:
        try:
            _ensure_game_stub(session, game_id)
            stored_events = (
                session.query(GameEvent).filter(GameEvent.game_id == game_id).order_by(GameEvent.event_seq.asc()).all()
            )
            if not stored_events:
                return 0

            pbp_mappings = derive_play_by_play_rows_from_events(
                [
                    {
                        "inning": event.inning,
                        "inning_half": event.inning_half,
                        "pitcher_name": event.pitcher_name,
                        "batter_name": event.batter_name,
                        "description": event.description,
                        "event_type": event.event_type,
                        "result_code": event.result_code,
                    }
                    for event in stored_events
                ]
            )
            session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == game_id).delete()
            session.add_all(
                [
                    GamePlayByPlay(
                        game_id=game_id,
                        inning=row.get("inning"),
                        inning_half=row.get("inning_half"),
                        batter_name=row.get("batter_name"),
                        pitcher_name=row.get("pitcher_name"),
                        play_description=row.get("play_description"),
                        event_type=row.get("event_type"),
                        result=row.get("result"),
                    )
                    for row in pbp_mappings
                ]
            )
            session.commit()
            _auto_sync_to_oci(game_id)
            return len(pbp_mappings)
        except Exception:
            session.rollback()
            logger.exception("[ERROR] DB Error (Derived Relay Backfill)")
            return 0


def backfill_missing_game_stubs_for_relays(
    *,
    seasons: Iterable[int] | None = None,
    sync_to_oci: bool = False,
) -> int:
    """
    Ensure a parent `game` row exists for any relay-bearing game_id.

    This repairs local integrity when historical backfills inserted `game_events`
    or `game_play_by_play` before the corresponding `game` row existed.
    """
    season_prefixes = {str(season) for season in (seasons or []) if season}

    with SessionLocal() as session:
        try:
            event_ids = {row[0] for row in session.query(GameEvent.game_id).distinct().all()}
            pbp_ids = {row[0] for row in session.query(GamePlayByPlay.game_id).distinct().all()}
            candidate_ids = sorted(event_ids | pbp_ids)
            if season_prefixes:
                candidate_ids = [
                    game_id
                    for game_id in candidate_ids
                    if any(game_id.startswith(prefix) for prefix in season_prefixes)
                ]

            existing_ids = (
                {row[0] for row in session.query(Game.game_id).filter(Game.game_id.in_(candidate_ids)).all()}
                if candidate_ids
                else set()
            )

            missing_ids = [game_id for game_id in candidate_ids if game_id not in existing_ids]
            for game_id in missing_ids:
                _ensure_game_stub(session, game_id)

            session.commit()
        except Exception:
            session.rollback()
            logger.exception("[ERROR] DB Error (Game Stub Backfill)")
            return 0

    if sync_to_oci:
        for game_id in missing_ids:
            _auto_sync_to_oci(game_id)
    return len(missing_ids)


def repair_game_parent_from_existing_children(
    game_id: str,
    *,
    sync_to_oci: bool = False,
) -> bool:
    """
    Rebuild/repair one parent `game` row from existing child tables.

    Historical backfills sometimes inserted box-score children before the parent
    `game` row. If those children exist, they are more authoritative than a
    later lightweight crawler miss/cancel signal.
    """
    game_id, original_game_id = _canonicalize_game_id(game_id)
    if not game_id:
        return False

    with SessionLocal() as session:
        try:
            _record_game_id_alias(
                session,
                original_game_id,
                game_id,
                source="parent_repair",
                reason="normalized_to_kbo_legacy_game_id",
            )
            has_children = any(
                _has_game_child_rows(session, model, game_id)
                for model in (GameInningScore, GameLineup, GameBattingStat, GamePitchingStat)
            )
            if not has_children:
                return False

            try:
                game_date = datetime.strptime(game_id[:8], "%Y%m%d").date()
            except Exception:
                game_date = datetime.now().date()
            season_year = game_date.year

            game = session.query(Game).filter(Game.game_id == game_id).one_or_none()
            if not game:
                game = Game(game_id=game_id, game_date=game_date)
                session.add(game)
                session.flush()

            game.game_date = game_date
            season_id = _resolve_game_season_id(
                session,
                {"season_year": season_year, "season_type": "regular"},
                game_date,
                game.season_id,
            )
            if season_id:
                game.season_id = season_id

            away_team = _infer_team_code_from_children(session, game_id, "away", season_year)
            home_team = _infer_team_code_from_children(session, game_id, "home", season_year)
            if away_team:
                game.away_team = away_team
            if home_team:
                game.home_team = home_team

            away_score = _infer_score_from_children(session, game_id, "away")
            home_score = _infer_score_from_children(session, game_id, "home")
            if away_score is not None:
                game.away_score = away_score
            if home_score is not None:
                game.home_score = home_score

            # Infer starting pitchers
            away_pitcher = _infer_pitcher_from_children(session, game_id, "away")
            home_pitcher = _infer_pitcher_from_children(session, game_id, "home")
            if away_pitcher:
                game.away_pitcher = away_pitcher
            if home_pitcher:
                game.home_pitcher = home_pitcher

            if game.home_score is not None and game.away_score is not None:
                game.winning_team, game.winning_score = _resolve_winner(
                    {"code": game.home_team, "score": game.home_score},
                    {"code": game.away_team, "score": game.away_score},
                )
                game.game_status = _resolve_terminal_status(game.home_score, game.away_score)
            elif not game.game_status:
                game.game_status = GAME_STATUS_UNRESOLVED

            _apply_game_team_identity(game, season_year)
            _enrich_existing_child_team_identity(session, game_id, season_year)
            session.commit()
        except Exception:
            session.rollback()
            logger.exception("[ERROR] DB Error (Game Parent Repair)")
            return False

    if sync_to_oci:
        _auto_sync_to_oci(game_id)
    return True


def save_relay_data(
    game_id: str,
    events: list[dict[str, Any]] | None = None,
    raw_pbp_rows: list[dict[str, Any]] | None = None,
    *,
    source_name: str | None = None,
    notes: str | None = None,
    allow_derived_pbp: bool = True,
    write_contract: GameWriteContract | None = None,
    source_stage: str = "relay",
    source_crawler: str = "RelayCrawler",
    source_reason: str = "relay_recovery",
) -> int:
    """
    Persist normalized relay data.

    Rules:
    - When normalized events have enough state, persist both game_events and game_play_by_play.
    - When only lightweight play-by-play rows exist, persist game_play_by_play only.
    - Never synthesize game_events if WPA/state coverage is insufficient.
    """
    game_id, original_game_id = _canonicalize_game_id(game_id)
    if not game_id:
        return 0
    source = GameWriteSource(source_stage, source_crawler, source_reason)
    if write_contract:
        write_contract.claim_game(game_id, source)

    events = list(events or [])
    raw_pbp_rows = [normalize_pbp_row(row) for row in (raw_pbp_rows or [])]
    if events and not raw_pbp_rows:
        raw_pbp_rows = derive_play_by_play_rows_from_events(events)

    valid_event_rows = events if events and all(event_has_minimum_state(event) for event in events) else []
    if not valid_event_rows and not raw_pbp_rows:
        return 0

    with SessionLocal() as session:
        try:
            _ensure_game_stub(session, game_id)
            _record_game_id_alias(
                session,
                original_game_id,
                game_id,
                source="relay",
                reason="normalized_to_kbo_legacy_game_id",
            )

            pbp_rows = []
            event_rows = []
            for row in raw_pbp_rows:
                pbp_rows.append(
                    GamePlayByPlay(
                        game_id=game_id,
                        inning=row.get("inning"),
                        inning_half=row.get("inning_half"),
                        batter_name=row.get("batter_name"),
                        pitcher_name=row.get("pitcher_name"),
                        play_description=row.get("play_description"),
                        event_type=row.get("event_type"),
                        result=row.get("result"),
                    )
                )
            for idx, event in enumerate(valid_event_rows, start=1):
                inning = event.get("inning")
                half = event.get("inning_half")
                batter_name = event.get("batter_name") or event.get("batter")
                pitcher_name = event.get("pitcher_name") or event.get("pitcher")
                extra_json = dict(event.get("extra_json") or {})
                if source_name:
                    extra_json.setdefault("relay_source", source_name)
                if notes:
                    extra_json.setdefault("relay_notes", notes)
                event_rows.append(
                    GameEvent(
                        game_id=game_id,
                        event_seq=event.get("event_seq") or idx,
                        inning=inning,
                        inning_half=half,
                        outs=event.get("outs"),
                        batter_name=batter_name,
                        pitcher_name=pitcher_name,
                        description=event.get("description"),
                        event_type=event.get("event_type"),
                        result_code=event.get("result_code") or event.get("result"),
                        rbi=event.get("rbi"),
                        bases_before=event.get("bases_before"),
                        bases_after=event.get("bases_after"),
                        extra_json=extra_json or None,
                        wpa=event.get("wpa"),
                        win_expectancy_before=event.get("win_expectancy_before"),
                        win_expectancy_after=event.get("win_expectancy_after"),
                        score_diff=event.get("score_diff"),
                        base_state=event.get("base_state"),
                        home_score=event.get("home_score"),
                        away_score=event.get("away_score"),
                    )
                )

            changed = False
            if pbp_rows:
                changed |= _replace_orm_records(
                    session,
                    GamePlayByPlay,
                    game_id,
                    pbp_rows,
                    source=source,
                    write_contract=write_contract,
                )
            if event_rows:
                changed |= _replace_orm_records(
                    session,
                    GameEvent,
                    game_id,
                    event_rows,
                    source=source,
                    write_contract=write_contract,
                )
            session.commit()
            if changed:
                _auto_sync_to_oci(game_id)
            if events and not valid_event_rows:
                print(f"[WARN] Skipped game_events save for {game_id}: insufficient relay state")
            return len(event_rows) if event_rows else len(pbp_rows)
        except Exception:
            session.rollback()
            logger.exception("[ERROR] DB Error (Relay)")
            return 0
