"""
Public save functions for game details, snapshots, and schedules.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import SessionLocal
from src.models.game import (
    Game,
    GameBattingStat,
    GameIdAlias,
    GameInningScore,
    GameLineup,
    GamePitchingStat,
    GameSummary,
)
from src.repositories.game_helpers import (
    _apply_game_team_identity,
    _apply_game_team_identity_with_contract,
    _assign_field_if_changed,
    _auto_sync_to_oci,
    _build_batting_stats,
    _build_inning_scores,
    _build_lineups,
    _build_pitching_stats,
    _build_pregame_lineup_rows,
    _canonicalize_game_id,
    _canonicalize_game_id_for_payload,
    _coerce_int,
    _ensure_player_basic_stubs,
    _extract_players_from_text,
    _json_dumps,
    _new_strict_player_resolver,
    _normalize_player_id,
    _prepare_player_rows,
    _record_game_id_alias,
    _replace_records,
    _replace_records_for_side,
    _resolve_game_season_id,
    _resolve_schedule_season_id,
    _resolve_winner,
    _upsert_game_summary_entry,
    _upsert_metadata,
)
from src.services.game_write_contract import GameWriteContract, GameWriteSource
from src.utils.game_status import (
    GAME_STATUS_SCHEDULED,
    derive_stable_game_status,
    is_live_status,
    is_terminal_status,
    normalize_game_status,
)
from src.utils.team_codes import resolve_team_code, team_code_from_game_id_segment

logger = logging.getLogger(__name__)


def get_games_by_date(target_date: str) -> list[Game]:
    """Retrieve Game objects for a specific date (YYYYMMDD)."""
    try:
        dt = datetime.strptime(target_date, "%Y%m%d").date()
    except ValueError:
        return []

    with SessionLocal() as session:
        return session.query(Game).filter(Game.game_date == dt).all()


def _clean_pregame_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _extract_existing_preview_payload(summary: GameSummary | None) -> dict[str, Any]:
    if not summary or not summary.detail_text:
        return {}
    try:
        payload = json.loads(summary.detail_text)
    except (TypeError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _resolve_pregame_starter(
    preview_data: dict[str, Any],
    game: Game,
    existing_payload: dict[str, Any],
    side: str,
) -> tuple[str, Any | None]:
    starter_key = f"{side}_starter"
    starter_id_key = f"{side}_starter_id"
    pitcher_attr = f"{side}_pitcher"

    raw_starter = _clean_pregame_text(preview_data.get(starter_key))
    existing_game_starter = _clean_pregame_text(getattr(game, pitcher_attr, None))
    existing_payload_starter = _clean_pregame_text(existing_payload.get(starter_key))
    resolved_starter = raw_starter or existing_game_starter or existing_payload_starter

    resolved_starter_id = preview_data.get(starter_id_key)
    if not resolved_starter_id and existing_payload_starter and existing_payload_starter == resolved_starter:
        resolved_starter_id = existing_payload.get(starter_id_key)

    return resolved_starter, resolved_starter_id


def resolve_canonical_game_id(game_id: str) -> str | None:
    """Resolve an external/alias game_id to the canonical legacy KBO game_id."""
    canonical, original = _canonicalize_game_id(game_id)
    if not canonical:
        return None
    with SessionLocal() as session:
        alias = session.query(GameIdAlias).filter(GameIdAlias.alias_game_id == original).one_or_none()
        return alias.canonical_game_id if alias else canonical


def save_schedule_game(
    game_data: dict[str, Any],
    *,
    write_contract: GameWriteContract | None = None,
    source_stage: str = "schedule",
    source_crawler: str = "ScheduleCrawler",
    source_reason: str = "schedule_refresh",
) -> bool:
    """Persist basic game info from schedule crawler."""
    game_date_str = str(game_data.get("game_date", "")).replace("-", "")
    try:
        game_date = datetime.strptime(game_date_str, "%Y%m%d").date()
    except ValueError:
        return False

    season_year = _coerce_int(game_data.get("season_year")) or game_date.year
    game_id, original_game_id = _canonicalize_game_id_for_payload(
        game_data.get("game_id"),
        game_date=game_date_str,
        away_team_code=game_data.get("away_team_code"),
        home_team_code=game_data.get("home_team_code"),
        season_year=season_year,
        doubleheader_no=game_data.get("doubleheader_no"),
    )
    if not game_id:
        return False

    source = GameWriteSource(source_stage, source_crawler, source_reason)
    if write_contract:
        write_contract.claim_game(game_id, source)

    with SessionLocal() as session:
        try:
            game = session.query(Game).filter(Game.game_id == game_id).one_or_none()
            changed = False
            if not game:
                game = Game(game_id=game_id)
                session.add(game)
                changed = True
                if write_contract:
                    write_contract.field_updated(game_id, source, "game.created", None, True)

            changed |= _assign_field_if_changed(
                game,
                "game_date",
                game_date,
                game_id=game_id,
                source=source,
                write_contract=write_contract,
            )
            changed |= _assign_field_if_changed(
                game,
                "home_team",
                game_data.get("home_team_code"),
                game_id=game_id,
                source=source,
                write_contract=write_contract,
            )
            changed |= _assign_field_if_changed(
                game,
                "away_team",
                game_data.get("away_team_code"),
                game_id=game_id,
                source=source,
                write_contract=write_contract,
            )
            resolved_season_id = _resolve_schedule_season_id(session, game_data, game.season_id)
            if resolved_season_id is not None:
                changed |= _assign_field_if_changed(
                    game,
                    "season_id",
                    resolved_season_id,
                    game_id=game_id,
                    source=source,
                    write_contract=write_contract,
                )
            changed |= _apply_game_team_identity_with_contract(
                game,
                game_date.year,
                source=source,
                write_contract=write_contract,
            )
            _record_game_id_alias(
                session,
                original_game_id,
                game_id,
                source="schedule",
                reason="normalized_to_kbo_legacy_game_id",
            )

            # Schedule crawl should keep already finalized statuses intact.
            new_status = derive_stable_game_status(
                game_date=game_date,
                current_status=game.game_status,
                new_status=game_data.get("game_status"),
                home_score=game.home_score,
                away_score=game.away_score,
            )
            changed |= _assign_field_if_changed(
                game,
                "game_status",
                new_status,
                game_id=game_id,
                source=source,
                write_contract=write_contract,
            )

            # Note: Scores and other details are not available in basic schedule crawl

            # Save Metadata (Time/Stadium)
            meta_payload = {"start_time": game_data.get("game_time"), "stadium": game_data.get("stadium")}
            if meta_payload["start_time"] or meta_payload["stadium"]:
                changed |= _upsert_metadata(
                    session,
                    game_id,
                    meta_payload,
                    source=source,
                    write_contract=write_contract,
                )

            session.commit()
            return True
        except SQLAlchemyError:
            session.rollback()
            logger.exception("[ERROR] DB Error (Schedule)")
            return False


def save_game_detail(
    game_data: dict[str, Any],
    *,
    write_contract: GameWriteContract | None = None,
    source_stage: str = "detail",
    source_crawler: str = "GameDetailCrawler",
    source_reason: str = "detail_recovery",
) -> bool:
    """Persist full game snapshot including box score + player stats."""
    if not game_data:
        return False

    teams = game_data.get("teams", {}) or {}
    away_info = teams.get("away", {}) or {}
    home_info = teams.get("home", {}) or {}
    provisional_game_id, _ = _canonicalize_game_id(game_data["game_id"])
    game_date_str = str(game_data.get("game_date", "")).replace("-", "") or str(provisional_game_id or "")[:8]
    try:
        game_date = datetime.strptime(game_date_str, "%Y%m%d").date()
    except ValueError:
        game_date = datetime.now().date()

    game_id, original_game_id = _canonicalize_game_id_for_payload(
        game_data["game_id"],
        game_date=game_date_str,
        away_team_code=away_info.get("code"),
        home_team_code=home_info.get("code"),
        season_year=game_date.year,
    )
    if not game_id:
        return False

    source = GameWriteSource(source_stage, source_crawler, source_reason)
    if write_contract:
        write_contract.claim_game(game_id, source)

    metadata = game_data.get("metadata", {}) or {}
    hitters = game_data.get("hitters", {}) or {}
    pitchers = game_data.get("pitchers", {}) or {}
    explicit_status = normalize_game_status(game_data.get("game_status"))

    with SessionLocal() as session:
        try:
            game = session.query(Game).filter(Game.game_id == game_id).one_or_none()
            changed = False
            if not game:
                game = Game(game_id=game_id, game_date=game_date)
                session.add(game)
                session.flush()  # Ensure game_id exists before child inserts
                changed = True
                if write_contract:
                    write_contract.field_updated(game_id, source, "game.created", None, True)
            _record_game_id_alias(
                session,
                original_game_id,
                game_id,
                source="detail",
                reason="normalized_to_kbo_legacy_game_id",
            )

            changed |= _assign_field_if_changed(
                game,
                "game_date",
                game_date,
                game_id=game_id,
                source=source,
                write_contract=write_contract,
            )
            changed |= _assign_field_if_changed(
                game,
                "stadium",
                metadata.get("stadium"),
                game_id=game_id,
                source=source,
                write_contract=write_contract,
            )
            changed |= _assign_field_if_changed(
                game,
                "home_team",
                home_info.get("code"),
                game_id=game_id,
                source=source,
                write_contract=write_contract,
            )
            changed |= _assign_field_if_changed(
                game,
                "away_team",
                away_info.get("code"),
                game_id=game_id,
                source=source,
                write_contract=write_contract,
            )
            changed |= _assign_field_if_changed(
                game,
                "home_score",
                home_info.get("score"),
                game_id=game_id,
                source=source,
                write_contract=write_contract,
                allow_empty=True,
            )
            changed |= _assign_field_if_changed(
                game,
                "away_score",
                away_info.get("score"),
                game_id=game_id,
                source=source,
                write_contract=write_contract,
                allow_empty=True,
            )
            # Resolve stable status using evidence from detail payload
            inning_rows = _build_inning_scores(game_id, teams, season_year=game_date.year)
            has_progress = bool(inning_rows) or game.home_score is not None or game.away_score is not None

            new_status = derive_stable_game_status(
                game_date=game_date,
                current_status=game.game_status,
                new_status=explicit_status,
                home_score=game.home_score,
                away_score=game.away_score,
                has_progress_evidence=has_progress,
            )
            changed |= _assign_field_if_changed(
                game,
                "game_status",
                new_status,
                game_id=game_id,
                source=source,
                write_contract=write_contract,
            )

            # Winner resolution logic
            score_complete = game.home_score is not None and game.away_score is not None
            if score_complete and is_terminal_status(new_status):
                winning_team, winning_score = _resolve_winner(home_info, away_info)
                changed |= _assign_field_if_changed(
                    game,
                    "winning_team",
                    winning_team,
                    game_id=game_id,
                    source=source,
                    write_contract=write_contract,
                    allow_empty=True,
                )
                changed |= _assign_field_if_changed(
                    game,
                    "winning_score",
                    winning_score,
                    game_id=game_id,
                    source=source,
                    write_contract=write_contract,
                    allow_empty=True,
                )

            # Update Starting Pitchers
            home_pitcher_data = next((p for p in pitchers.get("home", []) if p.get("is_starting")), None)
            away_pitcher_data = next((p for p in pitchers.get("away", []) if p.get("is_starting")), None)
            if home_pitcher_data:
                changed |= _assign_field_if_changed(
                    game,
                    "home_pitcher",
                    home_pitcher_data.get("player_name"),
                    game_id=game_id,
                    source=source,
                    write_contract=write_contract,
                )
            if away_pitcher_data:
                changed |= _assign_field_if_changed(
                    game,
                    "away_pitcher",
                    away_pitcher_data.get("player_name"),
                    game_id=game_id,
                    source=source,
                    write_contract=write_contract,
                )

            season_id = _resolve_game_season_id(session, game_data, game_date, game.season_id)
            if season_id:
                changed |= _assign_field_if_changed(
                    game,
                    "season_id",
                    season_id,
                    game_id=game_id,
                    source=source,
                    write_contract=write_contract,
                )
            changed |= _apply_game_team_identity_with_contract(
                game,
                game_date.year,
                source=source,
                write_contract=write_contract,
            )

            changed |= _upsert_metadata(
                session,
                game_id,
                metadata,
                source=source,
                write_contract=write_contract,
            )
            inning_rows = _build_inning_scores(game_id, teams, season_year=game_date.year)
            if inning_rows:
                changed |= _replace_records(
                    session,
                    GameInningScore,
                    game_id,
                    inning_rows,
                    source=source,
                    write_contract=write_contract,
                )

            lineup_rows = _prepare_player_rows(
                game_id,
                "game_lineups",
                _build_lineups(game_id, hitters, season_year=game_date.year),
            )
            batting_rows = _prepare_player_rows(
                game_id,
                "game_batting_stats",
                _build_batting_stats(game_id, hitters, season_year=game_date.year),
            )
            pitching_rows = _prepare_player_rows(
                game_id,
                "game_pitching_stats",
                _build_pitching_stats(game_id, pitchers, season_year=game_date.year),
            )

            changed |= _ensure_player_basic_stubs(
                session,
                [*lineup_rows, *batting_rows, *pitching_rows],
            )
            if lineup_rows:
                changed |= _replace_records(
                    session,
                    GameLineup,
                    game_id,
                    lineup_rows,
                    source=source,
                    write_contract=write_contract,
                )
            if batting_rows:
                changed |= _replace_records(
                    session,
                    GameBattingStat,
                    game_id,
                    batting_rows,
                    source=source,
                    write_contract=write_contract,
                )
            if pitching_rows:
                changed |= _replace_records(
                    session,
                    GamePitchingStat,
                    game_id,
                    pitching_rows,
                    source=source,
                    write_contract=write_contract,
                )

            # Game Summary Handling
            resolver = _new_strict_player_resolver(session)
            summary_rows = []

            # Map for quick name lookup for this game
            participant_map = {}  # name -> id
            for side in ("away", "home"):
                for p in hitters.get(side, []) + pitchers.get(side, []):
                    if p.get("player_name") and p.get("player_id"):
                        participant_map[p["player_name"]] = _normalize_player_id(p["player_id"])

            for item in game_data.get("summary") or []:
                summary_type = item.get("summary_type")
                detail_text = item.get("detail_text")

                # Extract individual player entries if possible
                entries = _extract_players_from_text(summary_type, detail_text)

                if not entries:
                    # Fallback or category without specific players (like weather potentially)
                    summary_rows.append(
                        {
                            "game_id": game_id,
                            "summary_type": summary_type,
                            "player_name": None,
                            "player_id": None,
                            "detail_text": detail_text,
                        },
                    )
                else:
                    for p_name, p_detail in entries:
                        p_id = participant_map.get(p_name)
                        if not p_id and summary_type != "심판":
                            p_id = resolver.resolve_id(
                                p_name,
                                None,
                                game_date.year,
                            )  # Try global resolve if not in participant list

                        summary_rows.append(
                            {
                                "game_id": game_id,
                                "summary_type": summary_type,
                                "player_name": p_name,
                                "player_id": p_id,
                                "detail_text": p_detail or detail_text,
                            },
                        )
            if summary_rows:
                changed |= _replace_records(
                    session,
                    GameSummary,
                    game_id,
                    summary_rows,
                    source=source,
                    write_contract=write_contract,
                )

            session.commit()
            if changed:
                _auto_sync_to_oci(game_id)
            return True
        except Exception:
            session.rollback()
            logger.exception("[ERROR] DB Error (Detail)")
            return False


def save_game_snapshot(game_data: dict[str, Any], *, status: str | None = None) -> bool:
    """Persist live/lightweight scoreboard data without touching full detail sections."""
    if not game_data:
        return False

    teams = game_data.get("teams", {}) or {}
    away_info = teams.get("away", {}) or {}
    home_info = teams.get("home", {}) or {}
    provisional_game_id, _ = _canonicalize_game_id(game_data.get("game_id"))
    game_date_str = str(game_data.get("game_date", "")).replace("-", "") or str(provisional_game_id or "")[:8]
    try:
        game_date = datetime.strptime(game_date_str, "%Y%m%d").date()
    except ValueError:
        game_date = datetime.now().date()

    game_id, original_game_id = _canonicalize_game_id_for_payload(
        game_data.get("game_id"),
        game_date=game_date_str,
        away_team_code=away_info.get("code"),
        home_team_code=home_info.get("code"),
        season_year=game_date.year,
    )
    if not game_id:
        return False

    metadata = game_data.get("metadata", {}) or {}
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

            # Resolve stable status using evidence
            # snapshots often have lineups but not necessarily progress
            has_progress = bool(inning_rows) or game.home_score is not None or game.away_score is not None

            stable_status = derive_stable_game_status(
                game_date=game_date,
                current_status=game.game_status,
                new_status=status,
                home_score=game.home_score,
                away_score=game.away_score,
                has_progress_evidence=has_progress,
            )
            game.game_status = stable_status

            score_complete = game.home_score is not None and game.away_score is not None
            should_resolve_winner = score_complete and is_terminal_status(stable_status)

            if should_resolve_winner:
                game.winning_team, game.winning_score = _resolve_winner(
                    {"code": game.home_team, "score": game.home_score},
                    {"code": game.away_team, "score": game.away_score},
                )

            session.commit()
            _auto_sync_to_oci(game_id)
            return True
        except Exception:
            session.rollback()
            logger.exception("[ERROR] DB Error (Snapshot)")
            return False


def save_pregame_lineups(preview_data: dict[str, Any]) -> bool:
    """Persist pregame start time, announced starters, and published starting lineups."""
    if not preview_data:
        return False

    provisional_game_id, _ = _canonicalize_game_id(preview_data.get("game_id"))
    game_date_str = str(preview_data.get("game_date", "")).replace("-", "") or str(provisional_game_id or "")[:8]
    if not provisional_game_id or not game_date_str:
        return False

    try:
        game_date = datetime.strptime(game_date_str, "%Y%m%d").date()
    except ValueError:
        return False

    season_year = game_date.year
    away_code = resolve_team_code(preview_data.get("away_team_name"), season_year) or team_code_from_game_id_segment(
        provisional_game_id[8:10] if len(provisional_game_id) >= 10 else None,
        season_year,
    )
    home_code = resolve_team_code(preview_data.get("home_team_name"), season_year) or team_code_from_game_id_segment(
        provisional_game_id[10:12] if len(provisional_game_id) >= 12 else None,
        season_year,
    )
    game_id, original_game_id = _canonicalize_game_id_for_payload(
        preview_data.get("game_id"),
        game_date=game_date_str,
        away_team_code=away_code,
        home_team_code=home_code,
        season_year=season_year,
    )
    if not game_id:
        return False

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

            existing_preview_summary = (
                session.query(GameSummary)
                .filter(
                    GameSummary.game_id == game_id,
                    GameSummary.summary_type == "프리뷰",
                    GameSummary.player_name.is_(None),
                )
                .one_or_none()
            )
            existing_preview_payload = _extract_existing_preview_payload(existing_preview_summary)
            away_starter, away_starter_id = _resolve_pregame_starter(
                preview_data,
                game,
                existing_preview_payload,
                "away",
            )
            home_starter, home_starter_id = _resolve_pregame_starter(
                preview_data,
                game,
                existing_preview_payload,
                "home",
            )
            start_pitcher_announced = preview_data.get("start_pitcher_announced")
            if not start_pitcher_announced and away_starter and home_starter:
                start_pitcher_announced = True

            game.game_date = game_date
            game.away_team = away_code or game.away_team
            game.home_team = home_code or game.home_team
            game.stadium = preview_data.get("stadium") or game.stadium
            if away_starter:
                game.away_pitcher = away_starter
            if home_starter:
                game.home_pitcher = home_starter
            if not is_terminal_status(game.game_status) and not is_live_status(game.game_status):
                game.game_status = GAME_STATUS_SCHEDULED
            _apply_game_team_identity(game, season_year)

            _upsert_metadata(
                session,
                game_id,
                {
                    "stadium": preview_data.get("stadium"),
                    "start_time": preview_data.get("start_time"),
                    "start_pitcher_announced": start_pitcher_announced,
                    "lineup_announced": preview_data.get("lineup_announced"),
                },
            )

            resolver = _new_strict_player_resolver(session)
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
            prepared_lineups = _prepare_player_rows(game_id, "game_lineups", away_rows + home_rows)
            away_rows = [row for row in prepared_lineups if row.get("team_side") == "away"]
            home_rows = [row for row in prepared_lineups if row.get("team_side") == "home"]

            if away_rows:
                _replace_records_for_side(session, GameLineup, game_id, "away", away_rows)
            if home_rows:
                _replace_records_for_side(session, GameLineup, game_id, "home", home_rows)

            preview_payload = {
                "game_id": game_id,
                "game_date": game_date_str,
                "stadium": preview_data.get("stadium"),
                "start_time": preview_data.get("start_time"),
                "away_team_name": preview_data.get("away_team_name"),
                "home_team_name": preview_data.get("home_team_name"),
                "away_starter": away_starter,
                "away_starter_id": away_starter_id,
                "home_starter": home_starter,
                "home_starter_id": home_starter_id,
                "start_pitcher_announced": start_pitcher_announced,
                "lineup_announced": preview_data.get("lineup_announced"),
                "away_lineup": preview_data.get("away_lineup") or [],
                "home_lineup": preview_data.get("home_lineup") or [],
            }
            _upsert_game_summary_entry(
                session,
                game_id=game_id,
                summary_type="프리뷰",
                detail_text=_json_dumps(preview_payload),
            )

            session.commit()
            _auto_sync_to_oci(game_id)
            return True
        except Exception:
            session.rollback()
            logger.exception("[ERROR] DB Error (Pregame)")
            return False
