"""
Repository for saving game details, box scores, and normalized relay data.
"""
from __future__ import annotations

import os
from datetime import datetime
from decimal import Decimal
from typing import Dict, Any, List, Iterable, Optional

from src.db.engine import SessionLocal
from src.models.game import (
    Game, GameMetadata, GameInningScore, GameLineup,
    GameBattingStat, GamePitchingStat, GamePlayByPlay, GameEvent,
    GameSummary
)
import re
from src.services.player_id_resolver import PlayerIdResolver
from src.utils.player_positions import get_primary_position
from src.utils.safe_print import safe_print as print


def get_games_by_date(target_date: str) -> List[Game]:
    """Retrieve Game objects for a specific date (YYYYMMDD)."""
    try:
        dt = datetime.strptime(target_date, "%Y%m%d").date()
    except ValueError:
        return []
    
    with SessionLocal() as session:
        return session.query(Game).filter(Game.game_date == dt).all()

def save_schedule_game(game_data: Dict[str, Any]) -> bool:
    """Persist basic game info from schedule crawler."""
    game_id = game_data.get("game_id")
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
            game.season_id = game_data.get("season_year")
            
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

    game_id = game_data["game_id"]
    game_date_str = str(game_data.get("game_date", "")).replace("-", "") or game_id[:8]
    try:
        game_date = datetime.strptime(game_date_str, "%Y%m%d").date()
    except Exception:
        game_date = datetime.now().date()

    metadata = game_data.get("metadata", {}) or {}
    teams = game_data.get("teams", {}) or {}
    hitters = game_data.get("hitters", {}) or {}
    pitchers = game_data.get("pitchers", {}) or {}

    with SessionLocal() as session:
        try:
            game = session.query(Game).filter(Game.game_id == game_id).one_or_none()
            if not game:
                game = Game(game_id=game_id, game_date=game_date)
                session.add(game)
                session.flush()  # Ensure game_id exists before child inserts

            away_info = teams.get("away", {})
            home_info = teams.get("home", {})

            game.game_date = game_date
            game.stadium = metadata.get("stadium")
            game.home_team = home_info.get("code")
            game.away_team = away_info.get("code")
            game.home_score = home_info.get("score")
            game.away_score = away_info.get("score")
            game.winning_team, game.winning_score = _resolve_winner(home_info, away_info)
            
            # Update Starting Pitchers
            home_pitcher_data = next((p for p in pitchers.get("home", []) if p.get("is_starting")), None)
            away_pitcher_data = next((p for p in pitchers.get("away", []) if p.get("is_starting")), None)
            if home_pitcher_data:
                game.home_pitcher = home_pitcher_data.get("player_name")
            if away_pitcher_data:
                game.away_pitcher = away_pitcher_data.get("player_name")

            season_id = game_data.get("season_id")
            if season_id:
                game.season_id = season_id

            _upsert_metadata(session, game_id, metadata)
            _replace_records(session, GameInningScore, game_id, _build_inning_scores(game_id, teams))
            _replace_records(session, GameLineup, game_id, _build_lineups(game_id, hitters))
            _replace_records(session, GameBattingStat, game_id, _build_batting_stats(game_id, hitters))
            _replace_records(session, GamePitchingStat, game_id, _build_pitching_stats(game_id, pitchers))

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
            _replace_records(session, GameSummary, game_id, summary_rows)

            session.commit()
            _auto_sync_to_oci(game_id)
            return True
        except Exception as exc:
            session.rollback()
            print(f"[ERROR] DB Error (Detail): {exc}")
            return False


def save_relay_data(game_id: str, events: List[Dict[str, Any]]) -> int:
    """Persist both legacy play_by_play and normalized game_events."""
    if not events:
        return 0

    with SessionLocal() as session:
        try:
            session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == game_id).delete()
            session.query(GameEvent).filter(GameEvent.game_id == game_id).delete()

            pbp_rows = []
            event_rows = []
            for idx, event in enumerate(events, start=1):
                inning = event.get("inning")
                half = event.get("inning_half")
                pbp_rows.append(
                    GamePlayByPlay(
                        game_id=game_id,
                        inning=inning,
                        inning_half=half,
                        batter_name=event.get("batter"),
                        pitcher_name=event.get("pitcher"),
                        play_description=event.get("description"),
                        event_type=event.get("event_type"),
                        result=event.get("result"),
                    )
                )
                event_rows.append(
                    GameEvent(
                        game_id=game_id,
                        event_seq=event.get("event_seq") or idx,
                        inning=inning,
                        inning_half=half,
                        outs=event.get("outs"),
                        batter_name=event.get("batter"),
                        pitcher_name=event.get("pitcher"),
                        description=event.get("description"),
                        event_type=event.get("event_type"),
                        result_code=event.get("result_code") or event.get("result"),
                        rbi=event.get("rbi"),
                        bases_before=event.get("bases_before"),
                        bases_after=event.get("bases_after"),
                        extra_json=event.get("extra_json"),
                        # New WPA columns
                        wpa=event.get("wpa"),
                        win_expectancy_before=event.get("win_expectancy_before"),
                        win_expectancy_after=event.get("win_expectancy_after"),
                        score_diff=event.get("score_diff"),
                        base_state=event.get("base_state"),
                        home_score=event.get("home_score"),
                        away_score=event.get("away_score"),
                    )
                )

            session.add_all(pbp_rows + event_rows)
            session.commit()
            _auto_sync_to_oci(game_id)
            return len(events)
        except Exception as exc:
            session.rollback()
            print(f"[ERROR] DB Error (Relay): {exc}")
            return 0


def _upsert_metadata(session, game_id: str, metadata: Dict[str, Any]) -> None:
    meta = session.query(GameMetadata).filter(GameMetadata.game_id == game_id).one_or_none()
    if not meta:
        meta = GameMetadata(game_id=game_id)
        session.add(meta)

    meta.stadium_code = metadata.get("stadium_code")
    meta.stadium_name = metadata.get("stadium")
    meta.attendance = metadata.get("attendance")
    meta.start_time = _safe_time(metadata.get("start_time"))
    meta.end_time = _safe_time(metadata.get("end_time"))
    meta.game_time_minutes = metadata.get("duration_minutes")
    meta.weather = metadata.get("weather")
    meta.source_payload = metadata or None


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


def _build_inning_scores(game_id: str, teams: Dict[str, Any]) -> List[Dict[str, Any]]:
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
    return records


def _build_lineups(game_id: str, hitters: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
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


def _build_batting_stats(game_id: str, hitters: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
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
    return records


def _build_pitching_stats(game_id: str, pitchers: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
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
