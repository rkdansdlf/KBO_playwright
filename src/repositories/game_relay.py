"""Relay data persistence and backfill functions."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy.exc import SQLAlchemyError

from src.constants import GAME_ID_FULL_LEN, GAME_ID_MIN_LEN, GAME_ID_YEAR_LEN, KST
from src.db.engine import SessionLocal
from src.models.game import (
    Game,
    GameBattingStat,
    GameEvent,
    GameInningScore,
    GameLineup,
    GamePitchingStat,
    GamePlayByPlay,
    GameValidationMetrics,
)
from src.repositories.game_helpers import (
    GAME_STATUS_UNRESOLVED,
    RecordReplaceContext,
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
from src.utils.date_helpers import parse_date_str
from src.utils.team_codes import team_code_from_game_id_segment

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

_OFFENSIVE_RELAY_ROLE_RE = re.compile(r"^(?P<role>\d+번타자|[123]루주자|대타|대주자|지명타자)\s+(?P<name>.+)$")
_DEFENSIVE_RELAY_ROLE_RE = re.compile(
    r"^(?P<role>투수|포수|1루수|2루수|3루수|유격수|좌익수|중견수|우익수)\s+(?P<name>.+)$",
)
_DEFENSIVE_RELAY_TARGET_RE = re.compile(r"^(?:투수|포수|1루수|2루수|3루수|유격수|좌익수|중견수|우익수)\b")
_RELAY_TURN_NOISE_RE = re.compile(r"^\d+회(?:초|말)\s+\d+번타순\b")
_RELAY_DECISION_LABELS = ("승리투수", "패전투수", "세이브", "홀드")


def _coerce_player_id(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _relay_text_indicates_defense_side(text_value: str, play_description: object = None) -> bool:
    description = " ".join(str(play_description or "").strip().split())
    if not description:
        return False
    if "수비위치 변경" in description:
        return True
    if "(으)로 교체" not in description:
        return False

    source_is_defensive = _DEFENSIVE_RELAY_ROLE_RE.match(text_value) is not None
    if source_is_defensive:
        return True

    _, _, target = description.partition(":")
    return bool(_DEFENSIVE_RELAY_TARGET_RE.match(target.strip()))


def _relay_player_resolution_context(
    name: object,
    play_description: object = None,
) -> tuple[str, str, bool | None] | None:
    text_value = " ".join(str(name or "").strip().split())
    if not text_value:
        return None
    if _RELAY_TURN_NOISE_RE.match(text_value):
        return None
    if any(text_value.startswith(label) for label in _RELAY_DECISION_LABELS):
        return None

    offensive_match = _OFFENSIVE_RELAY_ROLE_RE.match(text_value)
    if offensive_match:
        side = "defense" if _relay_text_indicates_defense_side(text_value, play_description) else "offense"
        return offensive_match.group("name").strip(), side, False

    defensive_match = _DEFENSIVE_RELAY_ROLE_RE.match(text_value)
    if defensive_match:
        role = defensive_match.group("role")
        return defensive_match.group("name").strip(), "defense", role == "투수"

    return text_value, "offense", False


def _upsert_validation_metrics(
    session: Session,
    game_id: str,
    data: ValidationMetricsData,
) -> GameValidationMetrics:
    from src.utils.relay_validation import VALIDATION_RECOVERED, VALIDATION_VERIFIED

    events_list = list(data.events or [])
    pbp_list = list(data.raw_pbp_rows or [])
    metrics = session.query(GameValidationMetrics).filter(GameValidationMetrics.game_id == game_id).one_or_none()
    if metrics is None:
        metrics = GameValidationMetrics(game_id=game_id, validation_status=data.validation_status)
        session.add(metrics)
    elif metrics.validation_status != data.validation_status:
        metrics.previous_status = metrics.validation_status
        metrics.validation_status = data.validation_status

    metrics.source_used = (data.source_name or metrics.source_used or "unknown")[:16]
    metrics.parser_version = data.parser_version or metrics.parser_version
    metrics.source_schema_version = data.source_schema_version or metrics.source_schema_version
    metrics.payload_hash = data.payload_hash or metrics.payload_hash
    metrics.duplicate_event_count = _duplicate_provider_count(events_list, pbp_list)
    metrics.unclassified_event_count = sum(
        1
        for row in [*events_list, *pbp_list]
        if str(row.get("event_type") or "").strip().lower() in {"unknown", "unclassified", "other"}
    )
    if data.error_reason and ("score_mismatch" in data.error_reason or "inning_score_mismatch" in data.error_reason):
        metrics.finish_mismatch_count = (metrics.finish_mismatch_count or 0) + 1
    if data.validation_status in {VALIDATION_VERIFIED, VALIDATION_RECOVERED} and (events_list or pbp_list):
        metrics.last_successful_event_at = datetime.now(KST)
    if data.error_reason:
        metrics.fallback_trigger_reason = str(data.error_reason)[:64]
    if data.evidence:
        existing = metrics.evidence_json if isinstance(metrics.evidence_json, dict) else {}
        merged = dict(existing)
        merged.update(data.evidence)
        metrics.evidence_json = merged
    return metrics


def _duplicate_provider_count(events: list[dict[str, Any]], raw_pbp_rows: list[dict[str, Any]]) -> int:
    seen: set[str] = set()
    duplicates = 0
    for row in [*events, *raw_pbp_rows]:
        provider_log_id = str(row.get("provider_log_id") or "").strip()
        if not provider_log_id:
            continue
        if provider_log_id in seen:
            duplicates += 1
        seen.add(provider_log_id)
    return duplicates


def derive_play_by_play_rows_from_events(events: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Deterministically project normalized game_events into lightweight play_by_play rows."""
    return [event_to_pbp_row(event) for event in events]


def backfill_game_play_by_play_from_existing_events(game_id: str) -> int:
    """Regenerate game_play_by_play rows from stored game_events for one game.

    Note: this is a legacy backfill that projects a minimal subset of fields
    (inning, inning_half, batter_name, pitcher_name, play_description,
    event_type, result). Newer fields like at_bat_seq, balls, strikes,
    player_id, resolver_*, and provider_log_id are NOT populated.
    """
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
                ],
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
                ],
            )
            session.commit()
            _auto_sync_to_oci(game_id)
            return len(pbp_mappings)
        except SQLAlchemyError:
            session.rollback()
            logger.exception("[ERROR] DB Error (Derived Relay Backfill)")
            return 0


def backfill_missing_game_stubs_for_relays(
    *,
    seasons: Iterable[int] | None = None,
    sync_to_oci: bool = False,
) -> int:
    """Ensure a parent `game` row exists for any relay-bearing game_id.

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
        except SQLAlchemyError:
            session.rollback()
            logger.exception("[ERROR] DB Error (Game Stub Backfill)")
            return 0

    if sync_to_oci:
        for game_id in missing_ids:
            _auto_sync_to_oci(game_id)
    return len(missing_ids)


def mark_relay_source_unavailable(
    game_id: str,
    *,
    reason: str,
    source_name: str = "none",
    evidence: dict[str, Any] | None = None,
    sync_to_oci: bool = False,
) -> bool:
    """Mark a completed game as explainably unrecoverable from public relay sources."""
    game_id, original_game_id = _canonicalize_game_id(game_id)
    if not game_id:
        return False
    with SessionLocal() as session:
        try:
            _ensure_game_stub(session, game_id)
            _record_game_id_alias(
                session,
                original_game_id,
                game_id,
                source="relay_source_unavailable",
                reason="normalized_to_kbo_legacy_game_id",
            )
            from src.repositories.game_helpers import _upsert_metadata
            from src.utils.relay_validation import VALIDATION_SOURCE_UNAVAILABLE

            payload = {
                "pbp_validation_status": VALIDATION_SOURCE_UNAVAILABLE,
                "pbp_validation_error": reason,
                "relay_source_used": source_name,
            }
            _upsert_metadata(session, game_id, payload)
            _upsert_validation_metrics(
                session,
                game_id,
                ValidationMetricsData(
                    validation_status=VALIDATION_SOURCE_UNAVAILABLE,
                    source_name=source_name,
                    error_reason=reason,
                    evidence=evidence or {"reason": reason},
                ),
            )
            session.commit()
        except SQLAlchemyError:
            session.rollback()
            logger.exception("[ERROR] DB Error (Relay Source Unavailable)")
            return False

    if sync_to_oci:
        _auto_sync_to_oci(game_id)
    return True


def _has_repairable_game_children(session: Session, game_id: str) -> bool:
    return any(
        _has_game_child_rows(session, model, game_id)
        for model in (GameInningScore, GameLineup, GameBattingStat, GamePitchingStat)
    )


def _game_date_from_game_id(game_id: str) -> date:
    try:
        return parse_date_str(game_id[:8])
    except ValueError:
        return datetime.now(KST).date()


def _get_or_create_game_parent(session: Session, game_id: str, game_date: date) -> Game:
    game = session.query(Game).filter(Game.game_id == game_id).one_or_none()
    if game:
        return game
    game = Game(game_id=game_id, game_date=game_date)
    session.add(game)
    session.flush()
    return game


def _apply_repaired_game_season(session: Session, game: Game, game_date: date, season_year: int) -> None:
    game.game_date = game_date
    season_id = _resolve_game_season_id(
        session,
        {"season_year": season_year, "season_type": "regular"},
        game_date,
        game.season_id,
    )
    if season_id:
        game.season_id = season_id


def _apply_repaired_game_teams(session: Session, game: Game, game_id: str, season_year: int) -> None:
    away_team = _infer_team_code_from_children(session, game_id, "away", season_year)
    home_team = _infer_team_code_from_children(session, game_id, "home", season_year)
    if away_team:
        game.away_team = away_team
    if home_team:
        game.home_team = home_team


def _apply_repaired_game_scores(session: Session, game: Game, game_id: str) -> None:
    away_score = _infer_score_from_children(session, game_id, "away")
    home_score = _infer_score_from_children(session, game_id, "home")
    if away_score is not None:
        game.away_score = away_score
    if home_score is not None:
        game.home_score = home_score


def _apply_repaired_game_pitchers(session: Session, game: Game, game_id: str) -> None:
    away_pitcher = _infer_pitcher_from_children(session, game_id, "away")
    home_pitcher = _infer_pitcher_from_children(session, game_id, "home")
    if away_pitcher:
        game.away_pitcher = away_pitcher
    if home_pitcher:
        game.home_pitcher = home_pitcher


def _apply_repaired_game_status(game: Game) -> None:
    if game.home_score is not None and game.away_score is not None:
        game.winning_team, game.winning_score = _resolve_winner(
            {"code": game.home_team, "score": game.home_score},
            {"code": game.away_team, "score": game.away_score},
        )
        game.game_status = _resolve_terminal_status(game.home_score, game.away_score)
    elif not game.game_status:
        game.game_status = GAME_STATUS_UNRESOLVED


def _apply_repaired_game_fields(session: Session, game_id: str) -> None:
    game_date = _game_date_from_game_id(game_id)
    season_year = game_date.year
    game = _get_or_create_game_parent(session, game_id, game_date)
    _apply_repaired_game_season(session, game, game_date, season_year)
    _apply_repaired_game_teams(session, game, game_id, season_year)
    _apply_repaired_game_scores(session, game, game_id)
    _apply_repaired_game_pitchers(session, game, game_id)
    _apply_repaired_game_status(game)
    _apply_game_team_identity(game, season_year)
    _enrich_existing_child_team_identity(session, game_id, season_year)


def repair_game_parent_from_existing_children(
    game_id: str,
    *,
    sync_to_oci: bool = False,
) -> bool:
    """Rebuild/repair one parent `game` row from existing child tables.

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
            if not _has_repairable_game_children(session, game_id):
                return False

            _apply_repaired_game_fields(session, game_id)
            session.commit()
        except SQLAlchemyError:
            session.rollback()
            logger.exception("[ERROR] DB Error (Game Parent Repair)")
            return False

    if sync_to_oci:
        _auto_sync_to_oci(game_id)
    return True


@dataclass
class _RelayValidationResult:
    status: str
    error_reason: str | None
    live_warnings: list[Any]


@dataclass
class _RelayResolutionContext:
    resolver: Any
    season_year: int | None
    away_team_code: str | None
    home_team_code: str | None

    def offense_team(self, half: str | None) -> str | None:
        """Handles the offense team operation.

        Args:
            half: Half.

        Returns:
            The result of the operation.

        """
        if half == "top":
            return self.away_team_code
        if half == "bottom":
            return self.home_team_code
        return None

    def defense_team(self, half: str | None) -> str | None:
        """Handles the defense team operation.

        Args:
            half: Half.

        Returns:
            The result of the operation.

        """
        if half == "top":
            return self.home_team_code
        if half == "bottom":
            return self.away_team_code
        return None

    def resolve_participant(
        self,
        name: str | None,
        team_code: str | None,
        *,
        is_pitcher: bool | None = None,
    ) -> tuple[int | None, str | None, str | None]:
        """Resolves participant.

        Args:
            name: Name.
            team_code: Team Code.

        Returns:
            Tuple result.

        """
        if self.resolver is None or not name or not team_code or not self.season_year:
            return None, None, None
        try:
            pid = self.resolver.resolve_id(name, team_code, self.season_year, is_pitcher=is_pitcher)
        except (SQLAlchemyError, RuntimeError, ValueError, TypeError) as exc:
            logger.warning("Player ID resolution encountered exception: %s", exc)
            return None, "error", "resolve_exception"
        if pid is None:
            return None, "unresolved", f"no_match_{team_code}_{self.season_year}"
        return _coerce_player_id(pid), "resolved", f"name_match_{team_code}_{self.season_year}"


@dataclass
class ValidationMetricsData:
    """ValidationMetricsData class."""

    validation_status: str
    source_name: str | None = None
    error_reason: str | None = None
    events: list[dict[str, Any]] | None = None
    raw_pbp_rows: list[dict[str, Any]] | None = None
    parser_version: str | None = None
    source_schema_version: str | None = None
    payload_hash: str | None = None
    evidence: dict[str, Any] | None = None
    notes: str | None = None
    valid_event_rows: list[dict[str, Any]] | None = None
    live_warnings: list[Any] | None = None


@dataclass
class RelayValidationInput:
    """RelayValidationInput class."""

    events: list[dict[str, Any]] | None = None
    raw_pbp_rows: list[dict[str, Any]] | None = None
    valid_event_rows: list[dict[str, Any]] | None = None


@dataclass
class PlayerResolutionContext:
    """PlayerResolutionContext class."""

    batter_name: str | None = None
    resolved_batter_name: str | None = None
    batter_team: str | None = None
    batter_confidence: str | None = None
    batter_reason: str | None = None
    pitcher_name: str | None = None
    pitcher_team: str | None = None
    pitcher_confidence: str | None = None
    pitcher_reason: str | None = None


@dataclass
class RelaySaveOptions:
    """RelaySaveOptions class."""

    source_name: str | None = None
    notes: str | None = None
    allow_derived_pbp: bool = True
    write_contract: GameWriteContract | None = None
    source_stage: str = "relay"
    source_crawler: str = "RelayCrawler"
    source_reason: str = "relay_recovery"
    parser_version: str | None = None
    source_schema_version: str | None = None
    payload_hash: str | None = None
    game_lifecycle_state: str | None = None


def _prepare_relay_payloads(
    events: list[dict[str, Any]] | None,
    raw_pbp_rows: list[dict[str, Any]] | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    normalized_events = list(events or [])
    normalized_pbp_rows = [normalize_pbp_row(row) for row in (raw_pbp_rows or [])]
    if normalized_events and not normalized_pbp_rows:
        normalized_pbp_rows = derive_play_by_play_rows_from_events(normalized_events)
    valid_event_rows = (
        normalized_events
        if normalized_events and all(event_has_minimum_state(event) for event in normalized_events)
        else []
    )
    return normalized_events, normalized_pbp_rows, valid_event_rows


def _resolve_relay_validation(
    session: Session,
    game_id: str,
    input_data: RelayValidationInput,
    game_row: Game | None,
    game_lifecycle_state: str | None,
) -> _RelayValidationResult:
    from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES
    from src.utils.relay_validation import (
        VALIDATION_PENDING_LIVE,
        VALIDATION_PROVISIONALLY_VALID,
        VALIDATION_SOURCE_INCOMPLETE,
        VALIDATION_UNVERIFIED,
        VALIDATION_VERIFIED,
        cross_validate_with_box_score,
        validate_live_events,
        validate_pbp_payload,
    )

    live_warnings = validate_live_events(input_data.events)
    game_status = str(getattr(game_row, "game_status", "") or "").upper()
    is_terminal_game = bool(
        game_lifecycle_state in ("final", "result_pending_stabilization", "cancelled")
        or game_status in COMPLETED_LIKE_GAME_STATUSES,
    )
    if input_data.raw_pbp_rows and not input_data.valid_event_rows and is_terminal_game:
        return _RelayValidationResult(VALIDATION_SOURCE_INCOMPLETE, "raw_pbp_without_valid_event_state", live_warnings)
    if is_terminal_game:
        is_valid, error_reason = validate_pbp_payload(session, game_id, input_data.events, input_data.raw_pbp_rows)
        if is_valid:
            score_match, score_err = cross_validate_with_box_score(session, game_id, input_data.events)
            is_valid = score_match
            if not score_match:
                error_reason = score_err
        status = VALIDATION_VERIFIED if is_valid else VALIDATION_UNVERIFIED
        return _RelayValidationResult(status, error_reason, live_warnings)

    if live_warnings:
        status = VALIDATION_PENDING_LIVE
    elif input_data.events:
        status = VALIDATION_PROVISIONALLY_VALID
    else:
        status = VALIDATION_PENDING_LIVE
    return _RelayValidationResult(status, None, live_warnings)


def _upsert_relay_validation_metadata(
    session: Session,
    game_id: str,
    validation: _RelayValidationResult,
    data: ValidationMetricsData,
) -> None:
    from src.repositories.game_helpers import _upsert_metadata
    from src.utils.relay_validation import VALIDATION_PENDING_LIVE, VALIDATION_UNVERIFIED

    metadata_payload: dict[str, Any] = {
        "pbp_validation_status": validation.status,
        "pbp_validation_error": validation.error_reason or "none",
    }
    if data.parser_version:
        metadata_payload["parser_version"] = data.parser_version
    if data.source_schema_version:
        metadata_payload["source_schema_version"] = data.source_schema_version
    if data.payload_hash:
        metadata_payload["payload_hash"] = data.payload_hash
    if validation.status in (VALIDATION_UNVERIFIED, VALIDATION_PENDING_LIVE) and validation.error_reason:
        logger.info("[PBP VALIDATION] Game %s status=%s reason=%s", game_id, validation.status, validation.error_reason)

    _upsert_metadata(session, game_id, metadata_payload)

    evidence: dict[str, Any] = {
        "notes": data.notes,
        "live_warnings": validation.live_warnings,
        "valid_event_rows": len(data.valid_event_rows or []),
        "raw_pbp_rows": len(data.raw_pbp_rows or []),
    }
    update_data = ValidationMetricsData(
        validation_status=validation.status,
        source_name=data.source_name,
        error_reason=validation.error_reason,
        events=data.events,
        raw_pbp_rows=data.raw_pbp_rows,
        parser_version=data.parser_version,
        source_schema_version=data.source_schema_version,
        payload_hash=data.payload_hash,
        evidence=evidence,
    )
    _upsert_validation_metrics(session, game_id, update_data)


def _apply_relay_lifecycle_state(game_row: Game | None, game_id: str, game_lifecycle_state: str | None) -> None:
    if not game_lifecycle_state or not game_row:
        return
    from src.utils.game_state import validate_transition

    is_valid_state, _ = validate_transition(game_row.game_lifecycle_state, game_lifecycle_state)
    if is_valid_state:
        game_row.game_lifecycle_state = game_lifecycle_state
    else:
        logger.warning(
            "Invalid lifecycle transition: %s -> %s for %s",
            game_row.game_lifecycle_state,
            game_lifecycle_state,
            game_id,
        )


def _relay_resolution_context(session: Session, game_id: str) -> _RelayResolutionContext:
    season_year = None
    away_team_code = None
    home_team_code = None
    try:
        season_year = int(game_id[:4]) if len(game_id) >= GAME_ID_YEAR_LEN else None
        away_team_code = (
            team_code_from_game_id_segment(game_id[8:10], season_year) if len(game_id) >= GAME_ID_MIN_LEN else None
        )
        home_team_code = (
            team_code_from_game_id_segment(game_id[10:12], season_year) if len(game_id) >= GAME_ID_FULL_LEN else None
        )
    except (ValueError, IndexError):
        logger.debug("Failed to parse team codes from game_id: %s", game_id)

    game_row = session.query(Game).filter(Game.game_id == game_id).one_or_none()
    if game_row is not None:
        away_team_code = game_row.away_team or away_team_code
        home_team_code = game_row.home_team or home_team_code

    try:
        from src.services.player_id_resolver import PlayerIdResolver

        resolver = PlayerIdResolver(session, allow_unknown_registration=False)
    except (SQLAlchemyError, RuntimeError, ValueError, TypeError):
        logger.warning("Failed to initialize PlayerIdResolver — player_id resolution will be skipped")
        resolver = None
    return _RelayResolutionContext(resolver, season_year, away_team_code, home_team_code)


def _resolve_pbp_player(
    row: dict[str, Any],
    resolution: _RelayResolutionContext,
) -> tuple[int | None, str | None, str | None, str | None]:
    batter_name = row.get("batter_name")
    pitcher_name = row.get("pitcher_name")
    inning_half = row.get("inning_half")
    play_description = row.get("play_description")
    if (
        resolution.resolver is not None
        and batter_name
        and resolution.season_year
        and resolution.away_team_code
        and resolution.home_team_code
    ):
        batter_context = _relay_player_resolution_context(batter_name, play_description)
        if batter_context is not None:
            resolved_name, side, is_pitcher = batter_context
            batter_team = (
                resolution.offense_team(inning_half) if side == "offense" else resolution.defense_team(inning_half)
            )
            player_id, confidence, reason = resolution.resolve_participant(
                resolved_name,
                batter_team,
                is_pitcher=is_pitcher,
            )
            return player_id, confidence, reason, resolved_name if player_id is None else None
    if resolution.resolver is not None and pitcher_name and resolution.season_year:
        pitcher_team = resolution.defense_team(inning_half)
        player_id, confidence, reason = resolution.resolve_participant(pitcher_name, pitcher_team, is_pitcher=True)
        return player_id, confidence, reason, pitcher_name if player_id is None else None
    return None, None, None, None


def _build_relay_pbp_rows(
    game_id: str,
    raw_pbp_rows: list[dict[str, Any]],
    source_name: str | None,
    resolution: _RelayResolutionContext,
) -> list[GamePlayByPlay]:
    pbp_rows = []
    for row in raw_pbp_rows:
        player_id, resolver_confidence, resolver_reason, unresolved_player_name = _resolve_pbp_player(row, resolution)
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
                player_id=player_id,
                resolver_confidence=resolver_confidence,
                resolver_reason=resolver_reason,
                unresolved_player_name=unresolved_player_name,
                provider_log_id=row.get("provider_log_id"),
                source_row_index=row.get("source_row_index"),
                source_name=row.get("source_name") or source_name,
            ),
        )
    return pbp_rows


def _resolve_event_batter(
    batter_name: str | None,
    half: str | None,
    description: str | None,
    resolution: _RelayResolutionContext,
) -> tuple[str | None, str | None, int | None, str | None, str | None]:
    batter_context = _relay_player_resolution_context(batter_name, description)
    if batter_context is None:
        return None, None, None, None, None
    resolved_name, side, is_pitcher = batter_context
    batter_team = resolution.offense_team(half) if side == "offense" else resolution.defense_team(half)
    batter_id, confidence, reason = resolution.resolve_participant(resolved_name, batter_team, is_pitcher=is_pitcher)
    return resolved_name, batter_team, batter_id, confidence, reason


def _build_player_resolution_payload(
    ctx: PlayerResolutionContext,
) -> dict[str, Any]:
    resolver_payload: dict[str, Any] = {}
    if ctx.batter_name and (ctx.resolved_batter_name or ctx.batter_team or ctx.batter_confidence or ctx.batter_reason):
        resolver_payload["batter"] = {
            "name": ctx.resolved_batter_name,
            "team_code": ctx.batter_team,
            "confidence": ctx.batter_confidence,
            "reason": ctx.batter_reason,
        }
    if ctx.pitcher_name and (ctx.pitcher_team or ctx.pitcher_confidence or ctx.pitcher_reason):
        resolver_payload["pitcher"] = {
            "name": ctx.pitcher_name,
            "team_code": ctx.pitcher_team,
            "confidence": ctx.pitcher_confidence,
            "reason": ctx.pitcher_reason,
        }
    return resolver_payload


def _build_relay_event_rows(
    game_id: str,
    valid_event_rows: list[dict[str, Any]],
    source_name: str | None,
    notes: str | None,
    resolution: _RelayResolutionContext,
) -> list[GameEvent]:
    event_rows = []
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
        resolved_batter_name, batter_team, batter_id, batter_confidence, batter_reason = _resolve_event_batter(
            batter_name,
            half,
            event.get("description"),
            resolution,
        )
        pitcher_team = resolution.defense_team(half)
        pitcher_id, pitcher_confidence, pitcher_reason = resolution.resolve_participant(
            pitcher_name,
            pitcher_team,
            is_pitcher=True,
        )
        resolver_payload = _build_player_resolution_payload(
            PlayerResolutionContext(
                batter_name=batter_name,
                resolved_batter_name=resolved_batter_name,
                batter_team=batter_team,
                batter_confidence=batter_confidence,
                batter_reason=batter_reason,
                pitcher_name=pitcher_name,
                pitcher_team=pitcher_team,
                pitcher_confidence=pitcher_confidence,
                pitcher_reason=pitcher_reason,
            ),
        )
        if resolver_payload:
            extra_json.setdefault("player_resolution", resolver_payload)
        event_rows.append(
            GameEvent(
                game_id=game_id,
                event_seq=event.get("event_seq") or idx,
                inning=inning,
                inning_half=half,
                outs=event.get("outs"),
                batter_id=batter_id or _coerce_player_id(event.get("batter_id")),
                batter_name=batter_name,
                pitcher_id=pitcher_id or _coerce_player_id(event.get("pitcher_id")),
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
                provider_log_id=event.get("provider_log_id"),
                source_row_index=event.get("source_row_index"),
                at_bat_seq=event.get("at_bat_seq"),
                at_bat_event_role=event.get("at_bat_event_role"),
                at_bat_confidence=event.get("at_bat_confidence"),
                balls=event.get("balls"),
                strikes=event.get("strikes"),
            ),
        )
    return event_rows


@dataclass
class RelayRowReplaceContext:
    """RelayRowReplaceContext class."""

    pbp_rows: list[GamePlayByPlay]
    event_rows: list[GameEvent]
    source: GameWriteSource
    write_contract: GameWriteContract | None


def _replace_relay_rows(
    session: Session,
    game_id: str,
    *,
    ctx: RelayRowReplaceContext,
) -> bool:
    changed = False
    if ctx.pbp_rows:
        changed |= _replace_orm_records(
            session,
            GamePlayByPlay,
            game_id,
            ctx.pbp_rows,
            RecordReplaceContext(source=ctx.source, write_contract=ctx.write_contract),
        )
    if ctx.event_rows:
        changed |= _replace_orm_records(
            session,
            GameEvent,
            game_id,
            ctx.event_rows,
            RecordReplaceContext(source=ctx.source, write_contract=ctx.write_contract),
        )
    return changed


def _log_relay_save_result(
    game_id: str,
    events: list[dict[str, Any]],
    valid_event_rows: list[dict[str, Any]],
    event_count: int,
    pbp_count: int,
) -> None:
    if events and not valid_event_rows:
        logger.warning(
            "Relay save for %s: saved_event_rows=0 saved_pbp_rows=%d skipped_event_rows_reason=insufficient_relay_state",
            game_id,
            pbp_count,
        )
    else:
        logger.info("Relay save for %s: saved_event_rows=%d saved_pbp_rows=%d", game_id, event_count, pbp_count)


def save_relay_data(
    game_id: str,
    events: list[dict[str, Any]] | None = None,
    raw_pbp_rows: list[dict[str, Any]] | None = None,
    *,
    options: RelaySaveOptions | None = None,
    **overrides: object,
) -> int:
    """Persist normalized relay data.

    Rules:
    - When normalized events have enough state, persist both game_events and game_play_by_play.
    - When only lightweight play-by-play rows exist, persist game_play_by_play only.
    - Never synthesize game_events if WPA/state coverage is insufficient.
    """
    opts: RelaySaveOptions = options if options is not None else RelaySaveOptions(**overrides)
    game_id, original_game_id = _canonicalize_game_id(game_id)
    if not game_id:
        return 0
    source = GameWriteSource(opts.source_stage, opts.source_crawler, opts.source_reason)
    if opts.write_contract:
        opts.write_contract.claim_game(game_id, source)

    events, raw_pbp_rows, valid_event_rows = _prepare_relay_payloads(events, raw_pbp_rows)
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

            game_row = session.query(Game).filter(Game.game_id == game_id).first()
            validation = _resolve_relay_validation(
                session,
                game_id,
                RelayValidationInput(
                    events=events,
                    raw_pbp_rows=raw_pbp_rows,
                    valid_event_rows=valid_event_rows,
                ),
                game_row=game_row,
                game_lifecycle_state=opts.game_lifecycle_state,
            )
            _upsert_relay_validation_metadata(
                session,
                game_id,
                validation,
                ValidationMetricsData(
                    validation_status=validation.status,
                    source_name=opts.source_name,
                    error_reason=validation.error_reason,
                    events=events,
                    raw_pbp_rows=raw_pbp_rows,
                    parser_version=opts.parser_version,
                    source_schema_version=opts.source_schema_version,
                    payload_hash=opts.payload_hash,
                    notes=opts.notes,
                    valid_event_rows=valid_event_rows,
                    live_warnings=validation.live_warnings,
                ),
            )
            _apply_relay_lifecycle_state(game_row, game_id, opts.game_lifecycle_state)

            resolution = _relay_resolution_context(session, game_id)
            pbp_rows = _build_relay_pbp_rows(game_id, raw_pbp_rows, opts.source_name, resolution)
            event_rows = _build_relay_event_rows(game_id, valid_event_rows, opts.source_name, opts.notes, resolution)
            changed = _replace_relay_rows(
                session,
                game_id,
                ctx=RelayRowReplaceContext(
                    pbp_rows=pbp_rows,
                    event_rows=event_rows,
                    source=source,
                    write_contract=opts.write_contract,
                ),
            )
            session.commit()
            if changed:
                _auto_sync_to_oci(game_id)
            _log_relay_save_result(game_id, events, valid_event_rows, len(event_rows), len(pbp_rows))
            return len(event_rows) if event_rows else len(pbp_rows)
        except (SQLAlchemyError, RuntimeError, ValueError, TypeError, KeyError):
            session.rollback()
            logger.exception("[ERROR] DB Error (Relay)")
            return 0
