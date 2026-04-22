"""
Repository for saving game details, box scores, and normalized relay data.
"""
from __future__ import annotations

import os
from datetime import date, datetime
from decimal import Decimal
from typing import Dict, Any, List, Iterable, Optional

from sqlalchemy import text

from src.db.engine import SessionLocal
from src.models.game import (
    Game, GameMetadata, GameInningScore, GameLineup,
    GameBattingStat, GamePitchingStat, GamePlayByPlay, GameEvent,
    GameSummary, GameIdAlias
)
import re
from src.services.player_id_resolver import PlayerIdResolver
from src.sources.relay.base import event_has_minimum_state, event_to_pbp_row, normalize_pbp_row
from src.utils.game_status import (
    GAME_STATUS_CANCELLED,
    GAME_STATUS_COMPLETED,
    GAME_STATUS_DELAYED,
    GAME_STATUS_DRAW,
    GAME_STATUS_LIVE,
    GAME_STATUS_POSTPONED,
    GAME_STATUS_SCHEDULED,
    GAME_STATUS_SUSPENDED,
    GAME_STATUS_UNRESOLVED,
    LIVE_GAME_STATUSES,
    TERMINAL_GAME_STATUSES,
    is_live_status,
    is_terminal_status,
    normalize_game_status,
)
from src.utils.player_positions import get_primary_position
from src.utils.safe_print import safe_print as print
from src.utils.team_codes import normalize_kbo_game_id, resolve_team_code, team_code_from_game_id_segment
from src.utils.team_history import FRANCHISE_CANONICAL_CODE, iter_team_history, resolve_team_code_for_season

SEASON_TYPE_TO_LEAGUE_CODE = {
    "regular": 0,
    "exhibition": 1,
    "wildcard": 2,
    "semi_playoff": 3,
    "semi-playoff": 3,
    "playoff": 4,
    "korean_series": 5,
}


def _coerce_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _resolve_league_type_code(season_type: Any) -> int:
    as_int = _coerce_int(season_type)
    if as_int is not None:
        return as_int
    key = str(season_type or "regular").strip().lower()
    return SEASON_TYPE_TO_LEAGUE_CODE.get(key, 0)


def _resolve_schedule_season_id(session, game_data: Dict[str, Any], existing_season_id: Optional[int]) -> Optional[int]:
    explicit = _coerce_int(game_data.get("season_id"))
    if explicit is not None:
        return explicit

    season_year = _coerce_int(game_data.get("season_year"))
    if season_year is None:
        return existing_season_id

    league_type_code = _resolve_league_type_code(game_data.get("season_type"))
    mapped: Optional[int] = None
    try:
        mapped = _coerce_int(
            session.execute(
                text(
                    """
                    SELECT MIN(season_id)
                    FROM kbo_seasons
                    WHERE season_year = :season_year
                      AND league_type_code = :league_type_code
                    """
                ),
                {"season_year": season_year, "league_type_code": league_type_code},
            ).scalar()
        )
    except Exception:
        mapped = None

    if mapped is not None:
        return mapped
    if existing_season_id is not None:
        return existing_season_id
    return season_year


def _resolve_game_season_id(
    session,
    game_data: Dict[str, Any],
    game_date: date,
    existing_season_id: Optional[int],
) -> Optional[int]:
    """Resolve season_id for non-schedule write paths that only know game_date."""
    season_data = {
        "season_id": game_data.get("season_id"),
        "season_year": game_data.get("season_year") or game_date.year,
        "season_type": game_data.get("season_type") or "regular",
    }
    return _resolve_schedule_season_id(session, season_data, existing_season_id)


def _canonicalize_game_id(game_id: Any) -> tuple[Optional[str], Optional[str]]:
    """Return (canonical legacy game_id, original game_id)."""
    if not game_id:
        return None, None
    original = str(game_id).strip().upper()
    canonical = normalize_kbo_game_id(original)
    return canonical, original


def _record_game_id_alias(
    session,
    alias_game_id: Optional[str],
    canonical_game_id: Optional[str],
    *,
    source: str,
    reason: str,
) -> None:
    if not alias_game_id or not canonical_game_id or alias_game_id == canonical_game_id:
        return

    existing = session.query(GameIdAlias).filter(GameIdAlias.alias_game_id == alias_game_id).one_or_none()
    if existing:
        existing.canonical_game_id = canonical_game_id
        existing.source = source
        existing.reason = reason
        return

    session.add(
        GameIdAlias(
            alias_game_id=alias_game_id,
            canonical_game_id=canonical_game_id,
            source=source,
            reason=reason,
        )
    )


def get_games_by_date(target_date: str) -> List[Game]:
    """Retrieve Game objects for a specific date (YYYYMMDD)."""
    try:
        dt = datetime.strptime(target_date, "%Y%m%d").date()
    except ValueError:
        return []
    
    with SessionLocal() as session:
        return session.query(Game).filter(Game.game_date == dt).all()


def resolve_canonical_game_id(game_id: str) -> Optional[str]:
    """Resolve an external/alias game_id to the canonical legacy KBO game_id."""
    canonical, original = _canonicalize_game_id(game_id)
    if not canonical:
        return None
    with SessionLocal() as session:
        alias = session.query(GameIdAlias).filter(GameIdAlias.alias_game_id == original).one_or_none()
        return alias.canonical_game_id if alias else canonical

def save_schedule_game(game_data: Dict[str, Any]) -> bool:
    """Persist basic game info from schedule crawler."""
    game_id, original_game_id = _canonicalize_game_id(game_data.get("game_id"))
    if not game_id:
        return False

    game_date_str = str(game_data.get("game_date", "")).replace("-", "")
    try:
        game_date = datetime.strptime(game_date_str, "%Y%m%d").date()
    except Exception:
        return False

    with SessionLocal() as session:
        try:
            game = session.query(Game).filter(Game.game_id == game_id).one_or_none()
            if not game:
                game = Game(game_id=game_id)
                session.add(game)
            
            game.game_date = game_date
            game.home_team = game_data.get("home_team_code")
            game.away_team = game_data.get("away_team_code")
            resolved_season_id = _resolve_schedule_season_id(session, game_data, game.season_id)
            if resolved_season_id is not None:
                game.season_id = resolved_season_id
            _apply_game_team_identity(game, game_date.year)
            _record_game_id_alias(
                session,
                original_game_id,
                game_id,
                source="schedule",
                reason="normalized_to_kbo_legacy_game_id",
            )

            # Schedule crawl should keep already finalized statuses intact.
            if game.home_score is not None and game.away_score is not None:
                game.game_status = _resolve_terminal_status(game.home_score, game.away_score)
            elif game.game_status not in {
                *TERMINAL_GAME_STATUSES,
                *LIVE_GAME_STATUSES,
            }:
                game.game_status = GAME_STATUS_SCHEDULED
            
            # Note: Scores and other details are not available in basic schedule crawl
            
            # Save Metadata (Time/Stadium)
            meta_payload = {
                "start_time": game_data.get("game_time"),
                "stadium": game_data.get("stadium")
            }
            if meta_payload["start_time"] or meta_payload["stadium"]:
                _upsert_metadata(session, game_id, meta_payload)

            session.commit()
            return True
        except Exception as exc:
            session.rollback()
            print(f"[ERROR] DB Error (Schedule): {exc}")
            return False


def save_game_detail(game_data: Dict[str, Any]) -> bool:
    """Persist full game snapshot including box score + player stats."""
    if not game_data:
        return False

    game_id, original_game_id = _canonicalize_game_id(game_data["game_id"])
    if not game_id:
        return False
    game_date_str = str(game_data.get("game_date", "")).replace("-", "") or game_id[:8]
    try:
        game_date = datetime.strptime(game_date_str, "%Y%m%d").date()
    except Exception:
        game_date = datetime.now().date()

    metadata = game_data.get("metadata", {}) or {}
    teams = game_data.get("teams", {}) or {}
    hitters = game_data.get("hitters", {}) or {}
    pitchers = game_data.get("pitchers", {}) or {}
    explicit_status = normalize_game_status(game_data.get("game_status"))

    with SessionLocal() as session:
        try:
            game = session.query(Game).filter(Game.game_id == game_id).one_or_none()
            if not game:
                game = Game(game_id=game_id, game_date=game_date)
                session.add(game)
                session.flush()  # Ensure game_id exists before child inserts
            _record_game_id_alias(
                session,
                original_game_id,
                game_id,
                source="detail",
                reason="normalized_to_kbo_legacy_game_id",
            )

            away_info = teams.get("away", {})
            home_info = teams.get("home", {})

            game.game_date = game_date
            game.stadium = metadata.get("stadium")
            game.home_team = home_info.get("code")
            game.away_team = away_info.get("code")
            game.home_score = home_info.get("score")
            game.away_score = away_info.get("score")
            game.winning_team, game.winning_score = _resolve_winner(home_info, away_info)
            if game.home_score is not None and game.away_score is not None:
                game.game_status = _resolve_terminal_status(game.home_score, game.away_score)
            elif explicit_status:
                game.game_status = explicit_status
            
            # Update Starting Pitchers
            home_pitcher_data = next((p for p in pitchers.get("home", []) if p.get("is_starting")), None)
            away_pitcher_data = next((p for p in pitchers.get("away", []) if p.get("is_starting")), None)
            if home_pitcher_data:
                game.home_pitcher = home_pitcher_data.get("player_name")
            if away_pitcher_data:
                game.away_pitcher = away_pitcher_data.get("player_name")

            season_id = _resolve_game_season_id(session, game_data, game_date, game.season_id)
            if season_id:
                game.season_id = season_id
            _apply_game_team_identity(game, game_date.year)

            _upsert_metadata(session, game_id, metadata)
            inning_rows = _build_inning_scores(game_id, teams, season_year=game_date.year)
            if inning_rows:
                _replace_records(session, GameInningScore, game_id, inning_rows)

            lineup_rows = _build_lineups(game_id, hitters, season_year=game_date.year)
            if lineup_rows:
                _replace_records(session, GameLineup, game_id, lineup_rows)

            batting_rows = _build_batting_stats(game_id, hitters, season_year=game_date.year)
            if batting_rows:
                _replace_records(session, GameBattingStat, game_id, batting_rows)

            pitching_rows = _build_pitching_stats(game_id, pitchers, season_year=game_date.year)
            if pitching_rows:
                _replace_records(session, GamePitchingStat, game_id, pitching_rows)

            # Game Summary Handling
            resolver = PlayerIdResolver(session)
            summary_rows = []
            
            # Map for quick name lookup for this game
            participant_map = {} # name -> id
            for side in ("away", "home"):
                for p in hitters.get(side, []) + pitchers.get(side, []):
                    if p.get("player_name") and p.get("player_id"):
                        participant_map[p["player_name"]] = _normalize_player_id(p["player_id"])

            for item in (game_data.get("summary") or []):
                summary_type = item.get("summary_type")
                detail_text = item.get("detail_text")
                
                # Extract individual player entries if possible
                entries = _extract_players_from_text(summary_type, detail_text)
                
                if not entries:
                    # Fallback or category without specific players (like weather potentially)
                    summary_rows.append({
                        "game_id": game_id,
                        "summary_type": summary_type,
                        "player_name": None,
                        "player_id": None,
                        "detail_text": detail_text,
                    })
                else:
                    for p_name, p_detail in entries:
                        p_id = participant_map.get(p_name)
                        if not p_id and summary_type != "심판":
                             p_id = resolver.resolve_id(p_name, None, game_date.year) # Try global resolve if not in participant list
                        
                        summary_rows.append({
                            "game_id": game_id,
                            "summary_type": summary_type,
                            "player_name": p_name,
                            "player_id": p_id,
                            "detail_text": p_detail or detail_text,
                        })
            if summary_rows:
                _replace_records(session, GameSummary, game_id, summary_rows)

            session.commit()
            _auto_sync_to_oci(game_id)
            return True
        except Exception as exc:
            session.rollback()
            print(f"[ERROR] DB Error (Detail): {exc}")
            return False


def save_game_snapshot(game_data: Dict[str, Any], *, status: Optional[str] = None) -> bool:
    """Persist live/lightweight scoreboard data without touching full detail sections."""
    if not game_data:
        return False

    game_id, original_game_id = _canonicalize_game_id(game_data.get("game_id"))
    if not game_id:
        return False

    game_date_str = str(game_data.get("game_date", "")).replace("-", "") or game_id[:8]
    try:
        game_date = datetime.strptime(game_date_str, "%Y%m%d").date()
    except Exception:
        game_date = datetime.now().date()

    metadata = game_data.get("metadata", {}) or {}
    teams = game_data.get("teams", {}) or {}
    pitchers = game_data.get("pitchers", {}) or {}

    with SessionLocal() as session:
        try:
            game = session.query(Game).filter(Game.game_id == game_id).one_or_none()
            if not game:
                game = Game(game_id=game_id, game_date=game_date)
                session.add(game)
                session.flush()
            _record_game_id_alias(
                session,
                original_game_id,
                game_id,
                source="snapshot",
                reason="normalized_to_kbo_legacy_game_id",
            )

            away_info = teams.get("away", {}) or {}
            home_info = teams.get("home", {}) or {}

            game.game_date = game_date
            game.stadium = metadata.get("stadium") or game.stadium
            game.home_team = home_info.get("code") or game.home_team
            game.away_team = away_info.get("code") or game.away_team
            if home_info.get("score") is not None:
                game.home_score = home_info.get("score")
            if away_info.get("score") is not None:
                game.away_score = away_info.get("score")

            home_pitcher_data = next((p for p in pitchers.get("home", []) if p.get("is_starting")), None)
            away_pitcher_data = next((p for p in pitchers.get("away", []) if p.get("is_starting")), None)
            if home_pitcher_data and home_pitcher_data.get("player_name"):
                game.home_pitcher = home_pitcher_data.get("player_name")
            if away_pitcher_data and away_pitcher_data.get("player_name"):
                game.away_pitcher = away_pitcher_data.get("player_name")

            season_id = _resolve_game_season_id(session, game_data, game_date, game.season_id)
            if season_id:
                game.season_id = season_id

            explicit_status = normalize_game_status(status)
            if explicit_status:
                if not is_terminal_status(game.game_status) or is_terminal_status(explicit_status):
                    game.game_status = explicit_status

            _apply_game_team_identity(game, game_date.year)
            _upsert_metadata(session, game_id, metadata)

            inning_rows = _build_inning_scores(game_id, teams, season_year=game_date.year)
            if inning_rows:
                _replace_records(session, GameInningScore, game_id, inning_rows)

            score_complete = game.home_score is not None and game.away_score is not None
            should_resolve_score_terminal = score_complete and (
                explicit_status in {GAME_STATUS_COMPLETED, GAME_STATUS_DRAW}
                or (
                    explicit_status is None
                    and game.game_date < date.today()
                    and inning_rows
                )
            )
            if should_resolve_score_terminal:
                game.winning_team, game.winning_score = _resolve_winner(
                    {"code": game.home_team, "score": game.home_score},
                    {"code": game.away_team, "score": game.away_score},
                )
                game.game_status = _resolve_terminal_status(game.home_score, game.away_score)

            session.commit()
            _auto_sync_to_oci(game_id)
            return True
        except Exception as exc:
            session.rollback()
            print(f"[ERROR] DB Error (Snapshot): {exc}")
            return False


def save_pregame_lineups(preview_data: Dict[str, Any]) -> bool:
    """Persist pregame start time, announced starters, and published starting lineups."""
    if not preview_data:
        return False

    game_id, original_game_id = _canonicalize_game_id(preview_data.get("game_id"))
    game_date_str = str(preview_data.get("game_date", "")).replace("-", "") or str(game_id or "")[:8]
    if not game_id or not game_date_str:
        return False

    try:
        game_date = datetime.strptime(game_date_str, "%Y%m%d").date()
    except Exception:
        return False

    season_year = game_date.year
    away_code = resolve_team_code(preview_data.get("away_team_name"), season_year) or team_code_from_game_id_segment(
        game_id[8:10] if len(game_id) >= 10 else None,
        season_year,
    )
    home_code = resolve_team_code(preview_data.get("home_team_name"), season_year) or team_code_from_game_id_segment(
        game_id[10:12] if len(game_id) >= 12 else None,
        season_year,
    )
    preview_payload = {
        "game_id": game_id,
        "game_date": game_date_str,
        "stadium": preview_data.get("stadium"),
        "start_time": preview_data.get("start_time"),
        "away_team_name": preview_data.get("away_team_name"),
        "home_team_name": preview_data.get("home_team_name"),
        "away_starter": preview_data.get("away_starter"),
        "away_starter_id": preview_data.get("away_starter_id"),
        "home_starter": preview_data.get("home_starter"),
        "home_starter_id": preview_data.get("home_starter_id"),
        "away_lineup": preview_data.get("away_lineup") or [],
        "home_lineup": preview_data.get("home_lineup") or [],
    }

    with SessionLocal() as session:
        try:
            game = session.query(Game).filter(Game.game_id == game_id).one_or_none()
            if not game:
                game = Game(game_id=game_id, game_date=game_date)
                session.add(game)
                session.flush()
            _record_game_id_alias(
                session,
                original_game_id,
                game_id,
                source="preview",
                reason="normalized_to_kbo_legacy_game_id",
            )

            game.game_date = game_date
            game.away_team = away_code or game.away_team
            game.home_team = home_code or game.home_team
            game.stadium = preview_data.get("stadium") or game.stadium
            if preview_data.get("away_starter"):
                game.away_pitcher = preview_data.get("away_starter")
            if preview_data.get("home_starter"):
                game.home_pitcher = preview_data.get("home_starter")
            if not is_terminal_status(game.game_status) and not is_live_status(game.game_status):
                game.game_status = GAME_STATUS_SCHEDULED
            _apply_game_team_identity(game, season_year)

            _upsert_metadata(
                session,
                game_id,
                {
                    "stadium": preview_data.get("stadium"),
                    "start_time": preview_data.get("start_time"),
                },
            )

            resolver = PlayerIdResolver(session)
            away_rows = _build_pregame_lineup_rows(
                game_id,
                team_side="away",
                team_code=away_code,
                season_year=season_year,
                lineup=preview_data.get("away_lineup") or [],
                resolver=resolver,
            )
            home_rows = _build_pregame_lineup_rows(
                game_id,
                team_side="home",
                team_code=home_code,
                season_year=season_year,
                lineup=preview_data.get("home_lineup") or [],
                resolver=resolver,
            )

            if away_rows:
                _replace_records_for_side(session, GameLineup, game_id, "away", away_rows)
            if home_rows:
                _replace_records_for_side(session, GameLineup, game_id, "home", home_rows)

            _upsert_game_summary_entry(
                session,
                game_id=game_id,
                summary_type="프리뷰",
                detail_text=_json_dumps(preview_payload),
            )

            session.commit()
            _auto_sync_to_oci(game_id)
            return True
        except Exception as exc:
            session.rollback()
            print(f"[ERROR] DB Error (Pregame): {exc}")
            return False


def update_game_status(game_id: str, status: str) -> bool:
    """Update one game's status."""
    game_id, _ = _canonicalize_game_id(game_id)
    if not game_id or not status:
        return False
    with SessionLocal() as session:
        try:
            game = session.query(Game).filter(Game.game_id == game_id).one_or_none()
            if not game:
                return False
            game.game_status = status
            session.commit()
            return True
        except Exception as exc:
            session.rollback()
            print(f"[ERROR] DB Error (Status): {exc}")
            return False


def refresh_game_status_for_date(target_date: str, today: Optional[date] = None) -> Dict[str, Any]:
    """
    Recompute game_status only for one target date (YYYYMMDD).
    """
    try:
        dt = datetime.strptime(target_date, "%Y%m%d").date()
    except ValueError:
        return {"target_date": target_date, "total": 0, "updated": 0, "status_counts": {}}

    today = today or date.today()
    with SessionLocal() as session:
        try:
            games = session.query(Game).filter(Game.game_date == dt).all()
            status_counts: Dict[str, int] = {}
            updated = 0
            for game in games:
                has_metadata = _has_game_child_rows(session, GameMetadata, game.game_id)
                has_inning = _has_game_child_rows(session, GameInningScore, game.game_id)
                has_lineup = _has_game_child_rows(session, GameLineup, game.game_id)
                has_batting = _has_game_child_rows(session, GameBattingStat, game.game_id)
                has_pitching = _has_game_child_rows(session, GamePitchingStat, game.game_id)
                next_status = _derive_game_status(
                    game_date=game.game_date,
                    home_score=game.home_score,
                    away_score=game.away_score,
                    current_status=game.game_status,
                    has_metadata=has_metadata,
                    has_inning_scores=has_inning,
                    has_lineups=has_lineup,
                    has_batting=has_batting,
                    has_pitching=has_pitching,
                    today=today,
                )
                status_counts[next_status] = status_counts.get(next_status, 0) + 1
                if game.game_status != next_status:
                    game.game_status = next_status
                    updated += 1
            session.commit()
            return {
                "target_date": target_date,
                "total": len(games),
                "updated": updated,
                "status_counts": status_counts,
            }
        except Exception as exc:
            session.rollback()
            print(f"[ERROR] DB Error (Status Refresh): {exc}")
            return {"target_date": target_date, "total": 0, "updated": 0, "status_counts": {}}


def derive_play_by_play_rows_from_events(events: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
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
                session.query(GameEvent)
                .filter(GameEvent.game_id == game_id)
                .order_by(GameEvent.event_seq.asc())
                .all()
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
        except Exception as exc:
            session.rollback()
            print(f"[ERROR] DB Error (Derived Relay Backfill): {exc}")
            return 0


def backfill_missing_game_stubs_for_relays(
    *,
    seasons: Optional[Iterable[int]] = None,
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
                    game_id for game_id in candidate_ids if any(game_id.startswith(prefix) for prefix in season_prefixes)
                ]

            existing_ids = {
                row[0]
                for row in session.query(Game.game_id).filter(Game.game_id.in_(candidate_ids)).all()
            } if candidate_ids else set()

            missing_ids = [game_id for game_id in candidate_ids if game_id not in existing_ids]
            for game_id in missing_ids:
                _ensure_game_stub(session, game_id)

            session.commit()
        except Exception as exc:
            session.rollback()
            print(f"[ERROR] DB Error (Game Stub Backfill): {exc}")
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
        except Exception as exc:
            session.rollback()
            print(f"[ERROR] DB Error (Game Parent Repair): {exc}")
            return False

    if sync_to_oci:
        _auto_sync_to_oci(game_id)
    return True


def save_relay_data(
    game_id: str,
    events: Optional[List[Dict[str, Any]]] = None,
    raw_pbp_rows: Optional[List[Dict[str, Any]]] = None,
    *,
    source_name: Optional[str] = None,
    notes: Optional[str] = None,
    allow_derived_pbp: bool = True,
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
            if raw_pbp_rows:
                session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == game_id).delete()
            if valid_event_rows:
                session.query(GameEvent).filter(GameEvent.game_id == game_id).delete()

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

            if pbp_rows:
                session.add_all(pbp_rows)
            if event_rows:
                session.add_all(event_rows)
            session.commit()
            _auto_sync_to_oci(game_id)
            if events and not valid_event_rows:
                print(f"[WARN] Skipped game_events save for {game_id}: insufficient relay state")
            return len(event_rows) if event_rows else len(pbp_rows)
        except Exception as exc:
            session.rollback()
            print(f"[ERROR] DB Error (Relay): {exc}")
            return 0


def _has_game_child_rows(session, model, game_id: str) -> bool:
    return session.query(model).filter(model.game_id == game_id).first() is not None


def _infer_team_code_from_children(
    session,
    game_id: str,
    team_side: str,
    season_year: Optional[int],
) -> Optional[str]:
    for model in (GameInningScore, GameLineup, GameBattingStat, GamePitchingStat):
        row = (
            session.query(model.team_code)
            .filter(model.game_id == game_id, model.team_side == team_side, model.team_code.isnot(None))
            .first()
        )
        if row and row[0]:
            return row[0]

    segment = game_id[8:10] if team_side == "away" and len(game_id) >= 10 else None
    if team_side == "home" and len(game_id) >= 12:
        segment = game_id[10:12]
    return team_code_from_game_id_segment(segment, season_year)


def _infer_score_from_children(session, game_id: str, team_side: str) -> Optional[int]:
    inning_rows = (
        session.query(GameInningScore.runs)
        .filter(GameInningScore.game_id == game_id, GameInningScore.team_side == team_side)
        .all()
    )
    if inning_rows:
        return sum(int(row[0] or 0) for row in inning_rows)

    batting_rows = (
        session.query(GameBattingStat.runs)
        .filter(GameBattingStat.game_id == game_id, GameBattingStat.team_side == team_side)
        .all()
    )
    if batting_rows:
        return sum(int(row[0] or 0) for row in batting_rows)
    return None


def _infer_pitcher_from_children(session, game_id: str, team_side: str) -> Optional[str]:
    """Find starting pitcher name from game_pitching_stats."""
    row = (
        session.query(GamePitchingStat.player_name)
        .filter(
            GamePitchingStat.game_id == game_id,
            GamePitchingStat.team_side == team_side,
            GamePitchingStat.is_starting == True
        )
        .first()
    )
    return row[0] if row else None


def _enrich_existing_child_team_identity(session, game_id: str, season_year: Optional[int]) -> None:
    for model in (GameInningScore, GameLineup, GameBattingStat, GamePitchingStat):
        for row in session.query(model).filter(model.game_id == game_id).all():
            franchise_id, canonical_team_code, season_code = _resolve_team_identity(row.team_code, season_year)
            if season_code:
                row.team_code = season_code
            row.franchise_id = franchise_id
            row.canonical_team_code = canonical_team_code


def _ensure_game_stub(session, game_id: str) -> None:
    game_id, original_game_id = _canonicalize_game_id(game_id)
    if not game_id:
        return

    existing = session.query(Game).filter(Game.game_id == game_id).one_or_none()
    if existing:
        _record_game_id_alias(
            session,
            original_game_id,
            game_id,
            source="game_stub",
            reason="normalized_to_kbo_legacy_game_id",
        )
        return

    try:
        game_date = datetime.strptime(game_id[:8], "%Y%m%d").date()
    except Exception:
        game_date = datetime.now().date()

    away_team = None
    home_team = None
    if len(game_id) >= 12:
        away_team = game_id[8:10] or None
        home_team = game_id[10:12] or None

    season_id = None
    try:
        season_year = int(game_id[:4])
    except Exception:
        season_year = None
    if season_year is not None:
        try:
            season_id = _coerce_int(
                session.execute(
                    text(
                        """
                        SELECT MIN(season_id)
                        FROM kbo_seasons
                        WHERE season_year = :season_year
                          AND league_type_code = 0
                        """
                    ),
                    {"season_year": season_year},
                ).scalar()
            )
        except Exception:
            season_id = None
        if season_id is None:
            season_id = season_year

    session.add(
        Game(
            game_id=game_id,
            game_date=game_date,
            away_team=away_team,
            home_team=home_team,
            season_id=season_id,
            game_status=GAME_STATUS_COMPLETED,
        )
    )
    session.flush()
    _record_game_id_alias(
        session,
        original_game_id,
        game_id,
        source="game_stub",
        reason="normalized_to_kbo_legacy_game_id",
    )


def _derive_game_status(
    *,
    game_date: Optional[date],
    home_score: Any,
    away_score: Any,
    current_status: Optional[str],
    has_metadata: bool,
    has_inning_scores: bool,
    has_lineups: bool,
    has_batting: bool,
    has_pitching: bool,
    today: date,
) -> str:
    if home_score is not None and away_score is not None and (has_batting or has_pitching or (game_date and game_date < today and has_inning_scores)):
        return _resolve_terminal_status(home_score, away_score)
    if game_date and game_date > today:
        return GAME_STATUS_SCHEDULED
    has_any_detail = has_inning_scores or has_lineups or has_batting or has_pitching
    if current_status in {GAME_STATUS_CANCELLED, GAME_STATUS_POSTPONED} and not has_any_detail:
        return current_status
    if game_date == today and has_any_detail and current_status in LIVE_GAME_STATUSES:
        return current_status
    if game_date == today and has_any_detail:
        return GAME_STATUS_LIVE
    if has_metadata and not has_any_detail:
        return GAME_STATUS_CANCELLED
    return GAME_STATUS_UNRESOLVED


def _upsert_metadata(session, game_id: str, metadata: Dict[str, Any]) -> None:
    meta = session.query(GameMetadata).filter(GameMetadata.game_id == game_id).one_or_none()
    if not meta:
        meta = GameMetadata(game_id=game_id)
        session.add(meta)

    if metadata.get("stadium_code") not in (None, ""):
        meta.stadium_code = metadata.get("stadium_code")
    if metadata.get("stadium") not in (None, ""):
        meta.stadium_name = metadata.get("stadium")
    if metadata.get("attendance") not in (None, ""):
        meta.attendance = metadata.get("attendance")

    start_time = _safe_time(metadata.get("start_time"))
    if start_time is not None:
        meta.start_time = start_time

    end_time = _safe_time(metadata.get("end_time"))
    if end_time is not None:
        meta.end_time = end_time

    if metadata.get("duration_minutes") not in (None, ""):
        meta.game_time_minutes = metadata.get("duration_minutes")
    if metadata.get("weather") not in (None, ""):
        meta.weather = metadata.get("weather")

    if metadata:
        existing_payload = meta.source_payload if isinstance(meta.source_payload, dict) else {}
        merged_payload = dict(existing_payload)
        for key, value in metadata.items():
            if value not in (None, ""):
                merged_payload[key] = value
        meta.source_payload = merged_payload or None


def _replace_records(session, model, game_id: str, mappings: List[Dict[str, Any]]) -> None:
    session.query(model).filter(model.game_id == game_id).delete()
    if mappings:
        now = datetime.utcnow()
        has_created_at = "created_at" in model.__table__.columns
        has_updated_at = "updated_at" in model.__table__.columns
        if has_created_at or has_updated_at:
            for mapping in mappings:
                if has_created_at and not mapping.get("created_at"):
                    mapping["created_at"] = now
                if has_updated_at and not mapping.get("updated_at"):
                    mapping["updated_at"] = now
        session.execute(model.__table__.insert(), mappings)


def _replace_records_for_side(session, model, game_id: str, team_side: str, mappings: List[Dict[str, Any]]) -> None:
    session.query(model).filter(model.game_id == game_id, model.team_side == team_side).delete()
    if mappings:
        now = datetime.utcnow()
        has_created_at = "created_at" in model.__table__.columns
        has_updated_at = "updated_at" in model.__table__.columns
        if has_created_at or has_updated_at:
            for mapping in mappings:
                if has_created_at and not mapping.get("created_at"):
                    mapping["created_at"] = now
                if has_updated_at and not mapping.get("updated_at"):
                    mapping["updated_at"] = now
        session.execute(model.__table__.insert(), mappings)


def _build_inning_scores(game_id: str, teams: Dict[str, Any], *, season_year: Optional[int] = None) -> List[Dict[str, Any]]:
    records = []
    for side in ("away", "home"):
        team_info = teams.get(side, {}) or {}
        line_score = team_info.get("line_score") or []
        team_code = team_info.get("code")
        for idx, runs in enumerate(line_score, start=1):
            records.append({
                "game_id": game_id,
                "team_side": side,
                "team_code": team_code,
                "inning": idx,
                "runs": runs if runs is not None else 0,
                "is_extra": idx > 9,
            })
    _apply_team_identity_to_mappings(records, season_year)
    return records


def _build_lineups(game_id: str, hitters: Dict[str, List[Dict[str, Any]]], *, season_year: Optional[int] = None) -> List[Dict[str, Any]]:
    records = []
    for side, entries in hitters.items():
        for entry in entries:
            player_name = entry.get("player_name")
            if not player_name:
                continue
            records.append({
                "game_id": game_id,
                "team_side": side,
                "team_code": entry.get("team_code"),
                "player_id": _normalize_player_id(entry.get("player_id")),
                "player_name": player_name,
                "uniform_no": entry.get("uniform_no"),
                "batting_order": entry.get("batting_order"),
                "position": entry.get("position"),
                "standard_position": get_primary_position(entry.get("position")).value,
                "is_starter": bool(entry.get("is_starter")),
                "appearance_seq": entry.get("appearance_seq") or len(records) + 1,
                "notes": _format_notes(entry.get("extras")),
            })
    _apply_team_identity_to_mappings(records, season_year)
    return records


def _format_notes(extras: Optional[Dict[str, Any]]) -> Optional[str]:
    if not extras:
        return None
    ignore_keys = {"COL_0", "COL_1", "선수명", "PlayerName", "playerName"}
    cleaned = {k: v for k, v in extras.items() if k not in ignore_keys}
    if not cleaned:
        return None
    if len(cleaned) == 1:
        return str(next(iter(cleaned.values())))
    return str(cleaned)


def _build_batting_stats(game_id: str, hitters: Dict[str, List[Dict[str, Any]]], *, season_year: Optional[int] = None) -> List[Dict[str, Any]]:
    records = []
    for side, entries in hitters.items():
        for entry in entries:
            player_name = entry.get("player_name")
            stats = entry.get("stats") or {}
            if not player_name:
                continue
            records.append({
                "game_id": game_id,
                "team_side": side,
                "team_code": entry.get("team_code"),
                "player_id": _normalize_player_id(entry.get("player_id")),
                "player_name": player_name,
                "uniform_no": entry.get("uniform_no"),
                "batting_order": entry.get("batting_order"),
                "is_starter": bool(entry.get("is_starter")),
                "appearance_seq": entry.get("appearance_seq") or len(records) + 1,
                "position": entry.get("position"),
                "standard_position": get_primary_position(entry.get("position")).value,
                "plate_appearances": _stat_int(stats, "plate_appearances"),
                "at_bats": _stat_int(stats, "at_bats"),
                "runs": _stat_int(stats, "runs"),
                "hits": _stat_int(stats, "hits"),
                "doubles": _stat_int(stats, "doubles"),
                "triples": _stat_int(stats, "triples"),
                "home_runs": _stat_int(stats, "home_runs"),
                "rbi": _stat_int(stats, "rbi"),
                "walks": _stat_int(stats, "walks"),
                "intentional_walks": _stat_int(stats, "intentional_walks"),
                "hbp": _stat_int(stats, "hbp"),
                "strikeouts": _stat_int(stats, "strikeouts"),
                "stolen_bases": _stat_int(stats, "stolen_bases"),
                "caught_stealing": _stat_int(stats, "caught_stealing"),
                "sacrifice_hits": _stat_int(stats, "sacrifice_hits"),
                "sacrifice_flies": _stat_int(stats, "sacrifice_flies"),
                "gdp": _stat_int(stats, "gdp"),
                "avg": _stat_float(stats, "avg"),
                "obp": _stat_float(stats, "obp"),
                "slg": _stat_float(stats, "slg"),
                "ops": _stat_float(stats, "ops"),
                "iso": _stat_float(stats, "iso"),
                "babip": _stat_float(stats, "babip"),
                "extra_stats": _clean_extras(entry.get("extras")),
            })
    _apply_team_identity_to_mappings(records, season_year)
    return records


def _build_pitching_stats(game_id: str, pitchers: Dict[str, List[Dict[str, Any]]], *, season_year: Optional[int] = None) -> List[Dict[str, Any]]:
    records = []
    for side, entries in pitchers.items():
        for entry in entries:
            player_name = entry.get("player_name")
            stats = entry.get("stats") or {}
            if not player_name:
                continue
            innings_outs = stats.get("innings_outs")
            records.append({
                "game_id": game_id,
                "team_side": side,
                "team_code": entry.get("team_code"),
                "player_id": _normalize_player_id(entry.get("player_id")),
                "player_name": player_name,
                "uniform_no": entry.get("uniform_no"),
                "is_starting": bool(entry.get("is_starting")),
                "appearance_seq": entry.get("appearance_seq") or len(records) + 1,
                "standard_position": "P",
                "innings_outs": innings_outs,
                "innings_pitched": _outs_to_decimal(innings_outs),
                "batters_faced": _stat_int(stats, "batters_faced"),
                "pitches": _stat_int(stats, "pitches"),
                "hits_allowed": _stat_int(stats, "hits_allowed"),
                "runs_allowed": _stat_int(stats, "runs_allowed"),
                "earned_runs": _stat_int(stats, "earned_runs"),
                "home_runs_allowed": _stat_int(stats, "home_runs_allowed"),
                "walks_allowed": _stat_int(stats, "walks_allowed"),
                "strikeouts": _stat_int(stats, "strikeouts"),
                "hit_batters": _stat_int(stats, "hit_batters"),
                "wild_pitches": _stat_int(stats, "wild_pitches"),
                "balks": _stat_int(stats, "balks"),
                "wins": _stat_int(stats, "wins"),
                "losses": _stat_int(stats, "losses"),
                "saves": _stat_int(stats, "saves"),
                "holds": _stat_int(stats, "holds"),
                "decision": stats.get("decision"),
                "era": _stat_float(stats, "era"),
                "whip": _stat_float(stats, "whip"),
                "k_per_nine": _stat_float(stats, "k_per_nine"),
                "bb_per_nine": _stat_float(stats, "bb_per_nine"),
                "kbb": _stat_float(stats, "kbb"),
                "extra_stats": _clean_extras(entry.get("extras")),
            })
    _apply_team_identity_to_mappings(records, season_year)
    return records


def _stat_int(stats: Dict[str, Any], key: str) -> int:
    value = stats.get(key)
    if value in (None, ""):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _stat_float(stats: Dict[str, Any], key: str) -> Any:
    value = stats.get(key)
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_player_id(value: Any) -> Any:
    if value in (None, "", "null"):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _outs_to_decimal(outs: Any) -> Any:
    if outs in (None, "", 0):
        return Decimal("0") if outs in (0,) else None
    try:
        whole, remainder = divmod(int(outs), 3)
        return Decimal(whole) + (Decimal(remainder) / Decimal(3))
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def _safe_time(value: Any):
    if not value:
        return None
    if isinstance(value, datetime):
        return value.time()
    try:
        parts = str(value).split(":")
        if len(parts) >= 2:
            return datetime.strptime(":".join(parts[:2]), "%H:%M").time()
    except Exception:
        return None
    return None


def _resolve_winner(home: Dict[str, Any], away: Dict[str, Any]) -> tuple[str | None, int | None]:
    """Determine winning team code and score based on box score."""
    home_score = home.get("score")
    away_score = away.get("score")
    if home_score is None or away_score is None:
        return None, None
    if home_score > away_score:
        return home.get("code"), home_score
    if away_score > home_score:
        return away.get("code"), away_score
    return None, home_score


def _resolve_terminal_status(home_score: Any, away_score: Any) -> str:
    if home_score is not None and away_score is not None and home_score == away_score:
        return GAME_STATUS_DRAW
    return GAME_STATUS_COMPLETED


def _apply_game_team_identity(game: Game, season_year: Optional[int]) -> None:
    home_franchise_id, _, _ = _resolve_team_identity(game.home_team, season_year)
    away_franchise_id, _, _ = _resolve_team_identity(game.away_team, season_year)
    winning_franchise_id, _, _ = _resolve_team_identity(game.winning_team, season_year)
    game.home_franchise_id = home_franchise_id
    game.away_franchise_id = away_franchise_id
    game.winning_franchise_id = winning_franchise_id


def _apply_team_identity_to_mappings(mappings: List[Dict[str, Any]], season_year: Optional[int]) -> None:
    for mapping in mappings:
        team_code = mapping.get("team_code")
        franchise_id, canonical_team_code, season_code = _resolve_team_identity(team_code, season_year)
        mapping["team_code"] = season_code or team_code
        mapping["franchise_id"] = franchise_id
        mapping["canonical_team_code"] = canonical_team_code


def _resolve_team_identity(team_code: Any, season_year: Optional[int]) -> tuple[Optional[int], Optional[str], Optional[str]]:
    if not team_code:
        return None, None, None
    raw_code = str(team_code).strip().upper()
    normalized_code = team_code_from_game_id_segment(raw_code, season_year) or raw_code
    season_code = resolve_team_code_for_season(normalized_code, season_year) if season_year else normalized_code
    season_code = season_code or normalized_code

    franchise_id = None
    for entry in iter_team_history():
        if entry.team_code.upper() in {season_code, normalized_code, raw_code}:
            franchise_id = entry.franchise_id
            break
    canonical_code = FRANCHISE_CANONICAL_CODE.get(franchise_id)
    return franchise_id, canonical_code, season_code


def _build_pregame_lineup_rows(
    game_id: str,
    *,
    team_side: str,
    team_code: Optional[str],
    season_year: int,
    lineup: List[Dict[str, Any]],
    resolver: PlayerIdResolver,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for idx, entry in enumerate(lineup, start=1):
        player_name = str(entry.get("player_name") or "").strip()
        if not player_name:
            continue
        batting_order = _coerce_int(entry.get("batting_order")) or idx
        position = entry.get("position")
        player_id = resolver.resolve_id(player_name, team_code, season_year) if team_code else None
        rows.append(
            {
                "game_id": game_id,
                "team_side": team_side,
                "team_code": team_code,
                "player_id": _normalize_player_id(player_id),
                "player_name": player_name,
                "uniform_no": entry.get("uniform_no"),
                "batting_order": batting_order,
                "position": position,
                "standard_position": get_primary_position(position).value,
                "is_starter": True,
                "appearance_seq": _coerce_int(entry.get("appearance_seq")) or batting_order,
                "notes": None,
            }
        )
    _apply_team_identity_to_mappings(rows, season_year)
    return rows


def _upsert_game_summary_entry(
    session,
    *,
    game_id: str,
    summary_type: str,
    detail_text: str,
    player_name: Optional[str] = None,
    player_id: Optional[int] = None,
) -> None:
    existing = session.query(GameSummary).filter(
        GameSummary.game_id == game_id,
        GameSummary.summary_type == summary_type,
        GameSummary.player_name == player_name,
    ).one_or_none()
    if existing:
        existing.player_id = player_id
        existing.detail_text = detail_text
        return

    session.add(
        GameSummary(
            game_id=game_id,
            summary_type=summary_type,
            player_name=player_name,
            player_id=player_id,
            detail_text=detail_text,
        )
    )


def _json_dumps(payload: Dict[str, Any]) -> str:
    import json

    return json.dumps(payload, ensure_ascii=False)

def _extract_players_from_text(category: str, text: str) -> List[tuple[str, str | None]]:
    """
    Extract (player_name, detail) from summary text blocks.
    Example: '강민호1호(2회1점 쿠에바스) 로하스1호(4회1점 코너)' 
             -> [('강민호', '강민호1호(...)'), ('로하스', '로하스1호(...)')]
    """
    if not text or text == "없음":
        return []

    if category == "심판":
        return [(name.strip(), None) for name in text.split() if name.strip()]

    # Pattern: Name[Count]호?(Inning/Detail)
    # Matches '강민호(3회)', '김지찬2(5회)', '강민호1호(2회)'
    # Parentheses content included in detail
    entries = []
    
    # regex matches: Name(OptionalCount)(Optional '호')(ParenthesesContent)
    # Group 1: Name, Group 2: Parentheses content (including parentheses)
    pattern = r"([가-힣]{2,5})(?:\d*(?:호)?)(\([^\)]+\))"
    matches = re.finditer(pattern, text)
    
    last_end = 0
    found_any = False
    for m in matches:
        found_any = True
        name = m.group(1)
        detail = m.group(0) # Include the whole match as detail
        entries.append((name, detail))
    
    if not found_any:
        # Fallback for simple name lists without parentheses (like possibly '폭투: 반즈') 
        # or if it's just 'Name'
        if ":" in text:
            parts = text.split(":", 1)
            name = parts[0].strip()
            if 2 <= len(name) <= 5:
                return [(name, text)]
        
        # Check if it's a single name
        if 2 <= len(text.strip()) <= 5 and " " not in text.strip():
             return [(text.strip(), None)]

    return entries


def _clean_extras(extras: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not extras:
        return None
    ignore_keys = {'COL_0', 'COL_1', '선수명', 'PlayerName', 'playerName'}
    cleaned = {k: v for k, v in extras.items() if k not in ignore_keys}
    return cleaned if cleaned else None

def _auto_sync_to_oci(game_id: str):
    """Helper to trigger OCI synchronization if enabled."""
    if os.getenv("AUTO_SYNC_OCI") == "true":
        try:
            from src.sync.oci_sync import OCISync
            oci_url = os.getenv("OCI_DB_URL")
            if oci_url:
                # Use a fresh session to read the committed data
                with SessionLocal() as sync_session:
                    syncer = OCISync(oci_url, sync_session)
                    syncer.sync_specific_game(game_id)
                    syncer.close()
                print(f" ✨ Auto-synced {game_id} to OCI")
        except Exception as e:
            print(f" ⚠️ Auto-sync OCI failed: {e}")
