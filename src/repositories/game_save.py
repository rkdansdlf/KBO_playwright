"""Public save functions for game details, snapshots, and schedules."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy.exc import SQLAlchemyError

from src.constants import GAME_ID_FULL_LEN, GAME_ID_MIN_LEN, KST
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
    GameSummaryEntry,
    RecordKey,
    RecordReplaceContext,
    TeamSideContext,
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
from src.utils.date_helpers import parse_date_str
from src.utils.game_status import (
    GAME_STATUS_SCHEDULED,
    GameStatusEvidence,
    derive_stable_game_status,
    is_live_status,
    is_terminal_status,
    normalize_game_status,
)
from src.utils.team_codes import resolve_team_code, team_code_from_game_id_segment

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

GAME_SAVE_EXCEPTIONS = (SQLAlchemyError, ValueError, TypeError, OSError)


@dataclass(frozen=True)
class DetailSaveContext:
    """DetailSaveContext class."""

    game_id: str
    game_date: date
    source: GameWriteSource
    write_contract: GameWriteContract | None


@dataclass(frozen=True)
class SnapshotContext:
    """SnapshotContext class."""

    game_data: dict[str, Any]
    game_date: date
    metadata: dict[str, Any]
    away_info: dict[str, Any]
    home_info: dict[str, Any]
    pitchers: dict[str, Any]
    status: str | None


@dataclass(frozen=True)
class PregameContext:
    """PregameContext class."""

    game_id: str
    game_date: date
    away_code: str | None
    home_code: str | None


@dataclass(frozen=True)
class PregameGameFieldInput:
    """PregameGameFieldInput class."""

    game_date: date
    away_code: str | None
    home_code: str | None
    away_starter: str | None
    home_starter: str | None


@dataclass(frozen=True)
class PregameLineupContext:
    """PregameLineupContext class."""

    game_id: str
    season_year: int
    away_code: str | None
    home_code: str | None


@dataclass(frozen=True)
class StartersInfo:
    """StartersInfo class."""

    away_starter: str | None
    away_starter_id: int | None
    home_starter: str | None
    home_starter_id: int | None
    start_pitcher_announced: object


def get_games_by_date(target_date: str) -> list[Game]:
    """Retrieve Game objects for a specific date (YYYYMMDD)."""
    try:
        dt = parse_date_str(target_date)
    except ValueError:
        return []

    with SessionLocal() as session:
        return session.query(Game).filter(Game.game_date == dt).all()


def _clean_pregame_text(value: object) -> str:
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
) -> tuple[str, object | None]:
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
        game_date = parse_date_str(game_date_str)
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
                GameStatusEvidence(
                    game_date=game_date,
                    current_status=game.game_status,
                    new_status=game_data.get("game_status"),
                    home_score=game.home_score,
                    away_score=game.away_score,
                ),
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
        except SQLAlchemyError:
            session.rollback()
            logger.exception("[ERROR] DB Error (Schedule)")
            return False
        else:
            return True


def _parse_detail_game_date(game_data: dict[str, Any], provisional_game_id: str | None) -> tuple[str, date]:
    game_date_str = str(game_data.get("game_date", "")).replace("-", "") or str(provisional_game_id or "")[:8]
    try:
        return game_date_str, parse_date_str(game_date_str)
    except ValueError:
        return game_date_str, datetime.now(KST).date()


def _get_or_create_game(
    session: Session,
    game_id: str,
    game_date: date,
    source: GameWriteSource,
    write_contract: GameWriteContract | None,
) -> tuple[Game, bool]:
    game = session.query(Game).filter(Game.game_id == game_id).one_or_none()
    if game:
        return game, False
    game = Game(game_id=game_id, game_date=game_date)
    session.add(game)
    session.flush()
    if write_contract:
        write_contract.field_updated(game_id, source, "game.created", None, True)
    return game, True


def _update_detail_core_fields(
    game: Game,
    ctx: DetailSaveContext,
    metadata: dict[str, Any],
    home_info: dict[str, Any],
    away_info: dict[str, Any],
) -> bool:
    changed = False
    for field_name, value, allow_empty in (
        ("game_date", ctx.game_date, False),
        ("stadium", metadata.get("stadium"), False),
        ("home_team", home_info.get("code"), False),
        ("away_team", away_info.get("code"), False),
        ("home_score", home_info.get("score"), True),
        ("away_score", away_info.get("score"), True),
    ):
        changed |= _assign_field_if_changed(
            game,
            field_name,
            value,
            game_id=ctx.game_id,
            source=ctx.source,
            write_contract=ctx.write_contract,
            allow_empty=allow_empty,
        )
    return changed


def _update_detail_status(
    game: Game,
    ctx: DetailSaveContext,
    teams: dict[str, Any],
    explicit_status: str | None,
) -> tuple[bool, list[dict[str, Any]], str | None]:
    inning_rows = _build_inning_scores(ctx.game_id, teams, season_year=ctx.game_date.year)
    has_progress = bool(inning_rows) or game.home_score is not None or game.away_score is not None
    new_status = derive_stable_game_status(
        GameStatusEvidence(
            game_date=ctx.game_date,
            current_status=game.game_status,
            new_status=explicit_status,
            home_score=game.home_score,
            away_score=game.away_score,
            has_progress_evidence=has_progress,
        ),
    )
    changed = _assign_field_if_changed(
        game,
        "game_status",
        new_status,
        game_id=ctx.game_id,
        source=ctx.source,
        write_contract=ctx.write_contract,
    )
    return changed, inning_rows, new_status


def _update_detail_winner(
    game: Game,
    ctx: DetailSaveContext,
    home_info: dict[str, Any],
    away_info: dict[str, Any],
    new_status: str | None,
) -> bool:
    if game.home_score is None or game.away_score is None or not is_terminal_status(new_status):
        return False
    winning_team, winning_score = _resolve_winner(home_info, away_info)
    changed = _assign_field_if_changed(
        game,
        "winning_team",
        winning_team,
        game_id=ctx.game_id,
        source=ctx.source,
        write_contract=ctx.write_contract,
        allow_empty=True,
    )
    changed |= _assign_field_if_changed(
        game,
        "winning_score",
        winning_score,
        game_id=ctx.game_id,
        source=ctx.source,
        write_contract=ctx.write_contract,
        allow_empty=True,
    )
    return changed


def _update_starting_pitchers(
    game: Game,
    game_id: str,
    pitchers: dict[str, list[dict[str, Any]]],
    source: GameWriteSource,
    write_contract: GameWriteContract | None,
) -> bool:
    changed = False
    for side, field_name in (("home", "home_pitcher"), ("away", "away_pitcher")):
        pitcher_data = next((pitcher for pitcher in pitchers.get(side, []) if pitcher.get("is_starting")), None)
        if pitcher_data:
            changed |= _assign_field_if_changed(
                game,
                field_name,
                pitcher_data.get("player_name"),
                game_id=game_id,
                source=source,
                write_contract=write_contract,
            )
    return changed


def _update_detail_children(
    session: Session,
    ctx: DetailSaveContext,
    hitters: dict[str, list[dict[str, Any]]],
    pitchers: dict[str, list[dict[str, Any]]],
    inning_rows: list[dict[str, Any]],
) -> bool:
    changed = False
    if inning_rows:
        changed |= _replace_records(
            session,
            GameInningScore,
            ctx.game_id,
            inning_rows,
            RecordReplaceContext(ctx.source, ctx.write_contract),
        )
    lineup_rows = _prepare_player_rows(
        ctx.game_id,
        "game_lineups",
        _build_lineups(ctx.game_id, hitters, season_year=ctx.game_date.year),
    )
    batting_rows = _prepare_player_rows(
        ctx.game_id,
        "game_batting_stats",
        _build_batting_stats(ctx.game_id, hitters, season_year=ctx.game_date.year),
    )
    pitching_rows = _prepare_player_rows(
        ctx.game_id,
        "game_pitching_stats",
        _build_pitching_stats(ctx.game_id, pitchers, season_year=ctx.game_date.year),
    )
    changed |= _ensure_player_basic_stubs(session, [*lineup_rows, *batting_rows, *pitching_rows])
    for model, rows in ((GameLineup, lineup_rows), (GameBattingStat, batting_rows), (GamePitchingStat, pitching_rows)):
        if rows:
            changed |= _replace_records(
                session,
                model,
                ctx.game_id,
                rows,
                RecordReplaceContext(ctx.source, ctx.write_contract),
            )
    return changed


def _summary_item_rows(
    item: dict[str, Any],
    game_id: str,
    game_date: date,
    participant_map: dict[str, str],
    resolver: object,
) -> list[dict[str, Any]]:
    summary_type = item.get("summary_type")
    detail_text = item.get("detail_text")
    entries = _extract_players_from_text(summary_type, detail_text)
    if not entries:
        return [
            {
                "game_id": game_id,
                "summary_type": summary_type,
                "player_name": None,
                "player_id": None,
                "detail_text": detail_text,
            },
        ]

    rows = []
    for player_name, player_detail in entries:
        player_id = participant_map.get(player_name)
        if not player_id and summary_type != "심판":
            player_id = resolver.resolve_id(player_name, None, game_date.year)
        rows.append(
            {
                "game_id": game_id,
                "summary_type": summary_type,
                "player_name": player_name,
                "player_id": player_id,
                "detail_text": player_detail or detail_text,
            },
        )
    return rows


def _build_summary_rows(
    session: Session,
    game_id: str,
    game_date: date,
    roster: dict[str, dict[str, list[dict[str, Any]]]],
    summary_items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    resolver = _new_strict_player_resolver(session)
    hitters = roster.get("hitters", {})
    pitchers = roster.get("pitchers", {})
    participant_map = {
        player["player_name"]: _normalize_player_id(player["player_id"])
        for side in ("away", "home")
        for player in hitters.get(side, []) + pitchers.get(side, [])
        if player.get("player_name") and player.get("player_id")
    }
    summary_rows = []
    for item in summary_items:
        summary_rows.extend(_summary_item_rows(item, game_id, game_date, participant_map, resolver))
    return summary_rows


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
    game_date_str, game_date = _parse_detail_game_date(game_data, provisional_game_id)

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
            game, changed = _get_or_create_game(session, game_id, game_date, source, write_contract)
            detail_ctx = DetailSaveContext(game_id, game_date, source, write_contract)
            _record_game_id_alias(
                session,
                original_game_id,
                game_id,
                source="detail",
                reason="normalized_to_kbo_legacy_game_id",
            )

            changed |= _update_detail_core_fields(
                game,
                detail_ctx,
                metadata=metadata,
                home_info=home_info,
                away_info=away_info,
            )
            status_changed, inning_rows, new_status = _update_detail_status(
                game,
                detail_ctx,
                teams,
                explicit_status,
            )
            changed |= status_changed
            changed |= _update_detail_winner(
                game,
                detail_ctx,
                home_info=home_info,
                away_info=away_info,
                new_status=new_status,
            )
            changed |= _update_starting_pitchers(game, game_id, pitchers, source, write_contract)

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
            changed |= _update_detail_children(
                session,
                detail_ctx,
                hitters,
                pitchers,
                inning_rows,
            )

            summary_rows = _build_summary_rows(
                session,
                game_id,
                game_date,
                {"hitters": hitters, "pitchers": pitchers},
                game_data.get("summary") or [],
            )
            if summary_rows:
                changed |= _replace_records(
                    session,
                    GameSummary,
                    game_id,
                    summary_rows,
                    RecordReplaceContext(source, write_contract),
                )

            session.commit()
        except GAME_SAVE_EXCEPTIONS:
            session.rollback()
            logger.exception("[ERROR] DB Error (Detail)")
            return False
        else:
            if changed:
                _auto_sync_to_oci(game_id)
            return True


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
        game_date = parse_date_str(game_date_str)
    except ValueError:
        game_date = datetime.now(KST).date()

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
            game = _get_or_create_snapshot_game(session, game_id, game_date)
            _record_game_id_alias(
                session,
                original_game_id,
                game_id,
                source="snapshot",
                reason="normalized_to_kbo_legacy_game_id",
            )

            _apply_snapshot_game_fields(
                session,
                game,
                SnapshotContext(
                    game_data=game_data,
                    game_date=game_date,
                    metadata=metadata,
                    away_info=away_info,
                    home_info=home_info,
                    pitchers=pitchers,
                    status=status,
                ),
            )
            _apply_game_team_identity(game, game_date.year)
            _upsert_metadata(session, game_id, metadata)

            inning_rows = _build_inning_scores(game_id, teams, season_year=game_date.year)
            if inning_rows:
                _replace_records(session, GameInningScore, game_id, inning_rows)

            _apply_snapshot_status_and_winner(game, game_date, status, has_inning_rows=bool(inning_rows))

            lifecycle_state = game_data.get("lifecycle_state")
            if lifecycle_state:
                _assign_field_if_changed(game, "game_lifecycle_state", lifecycle_state)

            session.commit()
        except GAME_SAVE_EXCEPTIONS:
            session.rollback()
            logger.exception("[ERROR] DB Error (Snapshot)")
            return False
        else:
            _auto_sync_to_oci(game_id)
            return True


def _get_or_create_snapshot_game(session: Session, game_id: str, game_date: date) -> Game:
    game = session.query(Game).filter(Game.game_id == game_id).one_or_none()
    if not game:
        game = Game(game_id=game_id, game_date=game_date)
        session.add(game)
        session.flush()
    return game


def _apply_snapshot_game_fields(
    session: Session,
    game: Game,
    ctx: SnapshotContext,
) -> None:
    game.game_date = ctx.game_date
    game.stadium = ctx.metadata.get("stadium") or game.stadium
    game.home_team = ctx.home_info.get("code") or game.home_team
    game.away_team = ctx.away_info.get("code") or game.away_team
    _apply_snapshot_scores(game, ctx.away_info, ctx.home_info)
    _apply_snapshot_starting_pitchers(game, ctx.pitchers)

    season_id = _resolve_game_season_id(session, ctx.game_data, ctx.game_date, game.season_id)
    if season_id:
        game.season_id = season_id

    explicit_status = normalize_game_status(ctx.status)
    if explicit_status and (not is_terminal_status(game.game_status) or is_terminal_status(explicit_status)):
        game.game_status = explicit_status


def _apply_snapshot_scores(game: Game, away_info: dict[str, Any], home_info: dict[str, Any]) -> None:
    if home_info.get("score") is not None:
        game.home_score = home_info.get("score")
    if away_info.get("score") is not None:
        game.away_score = away_info.get("score")


def _apply_snapshot_starting_pitchers(game: Game, pitchers: dict[str, Any]) -> None:
    home_pitcher_data = next((p for p in pitchers.get("home", []) if p.get("is_starting")), None)
    away_pitcher_data = next((p for p in pitchers.get("away", []) if p.get("is_starting")), None)
    if home_pitcher_data and home_pitcher_data.get("player_name"):
        game.home_pitcher = home_pitcher_data.get("player_name")
    if away_pitcher_data and away_pitcher_data.get("player_name"):
        game.away_pitcher = away_pitcher_data.get("player_name")


def _apply_snapshot_status_and_winner(
    game: Game,
    game_date: date,
    status: str | None,
    *,
    has_inning_rows: bool,
) -> None:
    has_progress = has_inning_rows or game.home_score is not None or game.away_score is not None
    stable_status = derive_stable_game_status(
        GameStatusEvidence(
            game_date=game_date,
            current_status=game.game_status,
            new_status=status,
            home_score=game.home_score,
            away_score=game.away_score,
            has_progress_evidence=has_progress,
        ),
    )
    game.game_status = stable_status

    if game.home_score is not None and game.away_score is not None and is_terminal_status(stable_status):
        game.winning_team, game.winning_score = _resolve_winner(
            {"code": game.home_team, "score": game.home_score},
            {"code": game.away_team, "score": game.away_score},
        )


def save_pregame_lineups(preview_data: dict[str, Any]) -> bool:
    """Persist pregame start time, announced starters, and published starting lineups."""
    if not preview_data:
        return False

    provisional_game_id, _ = _canonicalize_game_id(preview_data.get("game_id"))
    game_date_str = str(preview_data.get("game_date", "")).replace("-", "") or str(provisional_game_id or "")[:8]
    if not provisional_game_id or not game_date_str:
        return False

    try:
        game_date = parse_date_str(game_date_str)
    except ValueError:
        return False

    season_year = game_date.year
    away_code = resolve_team_code(preview_data.get("away_team_name"), season_year) or team_code_from_game_id_segment(
        provisional_game_id[8:10] if len(provisional_game_id) >= GAME_ID_MIN_LEN else None,
        season_year,
    )
    home_code = resolve_team_code(preview_data.get("home_team_name"), season_year) or team_code_from_game_id_segment(
        provisional_game_id[10:12] if len(provisional_game_id) >= GAME_ID_FULL_LEN else None,
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
            game = _get_or_create_snapshot_game(session, game_id, game_date)
            _record_game_id_alias(
                session,
                original_game_id,
                game_id,
                source="preview",
                reason="normalized_to_kbo_legacy_game_id",
            )

            _apply_pregame_payload(
                session,
                game,
                preview_data,
                PregameContext(
                    game_id=game_id,
                    game_date=game_date,
                    away_code=away_code,
                    home_code=home_code,
                ),
            )

            session.commit()
        except GAME_SAVE_EXCEPTIONS:
            session.rollback()
            logger.exception("[ERROR] DB Error (Pregame)")
            return False
        else:
            _auto_sync_to_oci(game_id)
            return True


def _apply_pregame_payload(
    session: Session,
    game: Game,
    preview_data: dict[str, Any],
    ctx: PregameContext,
) -> None:
    existing_preview_payload = _load_existing_preview_payload(session, ctx.game_id)
    away_starter, away_starter_id = _resolve_pregame_starter(preview_data, game, existing_preview_payload, "away")
    home_starter, home_starter_id = _resolve_pregame_starter(preview_data, game, existing_preview_payload, "home")
    start_pitcher_announced = preview_data.get("start_pitcher_announced")
    if not start_pitcher_announced and away_starter and home_starter:
        start_pitcher_announced = True

    _apply_pregame_game_fields(
        game,
        preview_data,
        PregameGameFieldInput(
            game_date=ctx.game_date,
            away_code=ctx.away_code,
            home_code=ctx.home_code,
            away_starter=away_starter,
            home_starter=home_starter,
        ),
    )
    _upsert_pregame_metadata(session, ctx.game_id, preview_data, start_pitcher_announced)
    _replace_pregame_lineups(
        session,
        preview_data,
        PregameLineupContext(
            game_id=ctx.game_id,
            season_year=ctx.game_date.year,
            away_code=ctx.away_code,
            home_code=ctx.home_code,
        ),
    )
    game_date_str = ctx.game_date.strftime("%Y%m%d")
    _upsert_pregame_summary(
        session,
        preview_data,
        ctx.game_id,
        game_date_str,
        StartersInfo(
            away_starter=away_starter,
            away_starter_id=away_starter_id,
            home_starter=home_starter,
            home_starter_id=home_starter_id,
            start_pitcher_announced=start_pitcher_announced,
        ),
    )


def _load_existing_preview_payload(session: Session, game_id: str) -> dict[str, Any]:
    existing_preview_summary = (
        session.query(GameSummary)
        .filter(
            GameSummary.game_id == game_id,
            GameSummary.summary_type == "프리뷰",
            GameSummary.player_name.is_(None),
        )
        .one_or_none()
    )
    return _extract_existing_preview_payload(existing_preview_summary)


def _apply_pregame_game_fields(
    game: Game,
    preview_data: dict[str, Any],
    input_data: PregameGameFieldInput,
) -> None:
    game.game_date = input_data.game_date
    game.away_team = input_data.away_code or game.away_team
    game.home_team = input_data.home_code or game.home_team
    game.stadium = preview_data.get("stadium") or game.stadium
    if input_data.away_starter:
        game.away_pitcher = input_data.away_starter
    if input_data.home_starter:
        game.home_pitcher = input_data.home_starter
    if not is_terminal_status(game.game_status) and not is_live_status(game.game_status):
        game.game_status = GAME_STATUS_SCHEDULED
    _apply_game_team_identity(game, input_data.game_date.year)


def _upsert_pregame_metadata(
    session: Session,
    game_id: str,
    preview_data: dict[str, Any],
    start_pitcher_announced: object,
) -> None:
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


def _replace_pregame_lineups(
    session: Session,
    preview_data: dict[str, Any],
    ctx: PregameLineupContext,
) -> None:
    resolver = _new_strict_player_resolver(session)
    away_rows = _build_pregame_lineup_rows(
        ctx.game_id,
        TeamSideContext(team_side="away", team_code=ctx.away_code, season_year=ctx.season_year),
        lineup=preview_data.get("away_lineup") or [],
        resolver=resolver,
    )
    home_rows = _build_pregame_lineup_rows(
        ctx.game_id,
        TeamSideContext(team_side="home", team_code=ctx.home_code, season_year=ctx.season_year),
        lineup=preview_data.get("home_lineup") or [],
        resolver=resolver,
    )
    prepared_lineups = _prepare_player_rows(ctx.game_id, "game_lineups", away_rows + home_rows)
    _replace_prepared_lineup_side(session, ctx.game_id, "away", prepared_lineups)
    _replace_prepared_lineup_side(session, ctx.game_id, "home", prepared_lineups)


def _replace_prepared_lineup_side(
    session: Session,
    game_id: str,
    team_side: str,
    prepared_lineups: list[dict[str, Any]],
) -> None:
    side_rows = [row for row in prepared_lineups if row.get("team_side") == team_side]
    if side_rows:
        _replace_records_for_side(session, RecordKey(GameLineup, game_id, team_side), side_rows)


def _upsert_pregame_summary(
    session: Session,
    preview_data: dict[str, Any],
    game_id: str,
    game_date_str: str,
    starters: StartersInfo,
) -> None:
    preview_payload = {
        "game_id": game_id,
        "game_date": game_date_str,
        "stadium": preview_data.get("stadium"),
        "start_time": preview_data.get("start_time"),
        "away_team_name": preview_data.get("away_team_name"),
        "home_team_name": preview_data.get("home_team_name"),
        "away_starter": starters.away_starter,
        "away_starter_id": starters.away_starter_id,
        "home_starter": starters.home_starter,
        "home_starter_id": starters.home_starter_id,
        "start_pitcher_announced": starters.start_pitcher_announced,
        "lineup_announced": preview_data.get("lineup_announced"),
        "away_lineup": preview_data.get("away_lineup") or [],
        "home_lineup": preview_data.get("home_lineup") or [],
    }
    _upsert_game_summary_entry(
        session,
        GameSummaryEntry(
            game_id=game_id,
            summary_type="프리뷰",
            detail_text=_json_dumps(preview_payload),
        ),
    )
