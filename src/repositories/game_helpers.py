"""
Repository for saving game details, box scores, and normalized relay data.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterable
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

logger = logging.getLogger(__name__)

import contextlib  # noqa: E402
import re  # noqa: E402

from sqlalchemy import text  # noqa: E402
from sqlalchemy.exc import SQLAlchemyError  # noqa: E402

from src.db.engine import SessionLocal  # noqa: E402
from src.models.game import (  # noqa: E402
    Game,
    GameBattingStat,
    GameIdAlias,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
    GameSummary,
)
from src.models.player import PlayerBasic  # noqa: E402
from src.services.game_write_contract import GameWriteContract, GameWriteSource  # noqa: E402
from src.services.player_id_resolver import PlayerIdResolver  # noqa: E402
from src.utils.game_status import (  # noqa: E402
    GAME_STATUS_CANCELLED,
    GAME_STATUS_COMPLETED,
    GAME_STATUS_DRAW,
    GAME_STATUS_LIVE,
    GAME_STATUS_POSTPONED,
    GAME_STATUS_SCHEDULED,
    GAME_STATUS_UNRESOLVED,
    LIVE_GAME_STATUSES,
)
from src.utils.player_positions import get_primary_position  # noqa: E402
from src.utils.team_codes import (  # noqa: E402
    build_kbo_game_id,
    normalize_kbo_game_id,
    team_code_from_game_id_segment,
)
from src.utils.team_history import FRANCHISE_CANONICAL_CODE, find_team_history_entry  # noqa: E402

SEASON_TYPE_TO_LEAGUE_CODE = {
    "regular": 0,
    "exhibition": 1,
    "wildcard": 2,
    "semi_playoff": 3,
    "semi-playoff": 3,
    "playoff": 4,
    "korean_series": 5,
}


def _coerce_int(value: Any) -> int | None:
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


def _resolve_schedule_season_id(session, game_data: dict[str, Any], existing_season_id: int | None) -> int | None:
    from datetime import date, datetime

    explicit = _coerce_int(game_data.get("season_id"))
    if explicit is not None:
        return explicit

    season_year = _coerce_int(game_data.get("season_year"))

    # Resolve game date object if available
    raw_date = game_data.get("game_date")
    game_date_obj = None
    if isinstance(raw_date, date):
        game_date_obj = raw_date
    elif isinstance(raw_date, datetime):
        game_date_obj = raw_date.date()
    elif isinstance(raw_date, str):
        val_clean = raw_date.replace("-", "").replace("/", "").strip()
        if len(val_clean) == 8 and val_clean.isdigit():
            with contextlib.suppress(ValueError):
                game_date_obj = date(int(val_clean[:4]), int(val_clean[4:6]), int(val_clean[6:]))

    if game_date_obj is not None:
        season_year = game_date_obj.year

        # 1. Try to query database using start_date/end_date ranges
        try:
            mapped = _coerce_int(
                session.execute(
                    text(
                        """
                        SELECT MIN(season_id)
                        FROM kbo_seasons
                        WHERE season_year = :season_year
                          AND start_date IS NOT NULL
                          AND end_date IS NOT NULL
                          AND :game_date BETWEEN start_date AND end_date
                        """
                    ),
                    {"season_year": season_year, "game_date": game_date_obj},
                ).scalar()
            )
            if mapped is not None:
                return mapped
        except SQLAlchemyError:
            logger.warning("Failed to query season_id from database")

        # 2. Fallback to hardcoded historical rules for 2023, 2024, and 2025
        SEASON_DATE_RULES = {
            2023: [
                ("2023-03-13", "2023-03-28", "시범경기"),
                ("2023-10-19", "2023-10-19", "와일드카드"),
                ("2023-10-22", "2023-10-25", "준플레이오프"),
                ("2023-10-30", "2023-11-05", "플레이오프"),
                ("2023-11-07", "2023-11-13", "한국시리즈"),
            ],
            2024: [
                ("2024-03-09", "2024-03-19", "시범경기"),
                ("2024-07-06", "2024-07-06", "올스타전"),
                ("2024-10-02", "2024-10-03", "와일드카드"),
                ("2024-10-05", "2024-10-11", "준플레이오프"),
                ("2024-10-13", "2024-10-19", "플레이오프"),
                ("2024-10-21", "2024-10-30", "한국시리즈"),
            ],
            2025: [
                ("2025-03-08", "2025-03-18", "시범경기"),
                ("2025-10-06", "2025-10-07", "와일드카드"),
                ("2025-10-09", "2025-10-14", "준플레이오프"),
                ("2025-10-18", "2025-10-24", "플레이오프"),
                ("2025-10-26", "2025-11-01", "한국시리즈"),
            ],
        }

        rules = SEASON_DATE_RULES.get(season_year, [])
        for start_str, end_str, type_name in rules:
            try:
                start_dt = date.fromisoformat(start_str)
                end_dt = date.fromisoformat(end_str)
                if start_dt <= game_date_obj <= end_dt:
                    mapped = _coerce_int(
                        session.execute(
                            text(
                                """
                                SELECT MIN(season_id)
                                FROM kbo_seasons
                                WHERE season_year = :season_year
                                  AND league_type_name = :league_type_name
                                """
                            ),
                            {"season_year": season_year, "league_type_name": type_name},
                        ).scalar()
                    )
                    if mapped is not None:
                        return mapped
            except SQLAlchemyError:
                logger.warning("Failed to apply season date rule")

    if season_year is None:
        return existing_season_id

    league_type_code = _resolve_league_type_code(game_data.get("season_type"))
    mapped: int | None = None
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
    except SQLAlchemyError:
        logger.warning("Failed to query season_id from kbo_seasons")
        mapped = None

    if mapped is not None:
        return mapped
    if existing_season_id is not None:
        return existing_season_id
    return season_year


def _resolve_game_season_id(
    session,
    game_data: dict[str, Any],
    game_date: date,
    existing_season_id: int | None,
) -> int | None:
    """Resolve season_id for non-schedule write paths that only know game_date."""
    if existing_season_id is not None:
        return existing_season_id
    season_data = {
        "season_id": game_data.get("season_id"),
        "season_year": game_data.get("season_year") or game_date.year,
        "season_type": game_data.get("season_type") or "regular",
    }
    return _resolve_schedule_season_id(session, season_data, existing_season_id)


def _canonicalize_game_id(game_id: Any) -> tuple[str | None, str | None]:
    """Return (canonical legacy game_id, original game_id)."""
    if not game_id:
        return None, None
    original = str(game_id).strip().upper()
    canonical = normalize_kbo_game_id(original)
    return canonical, original


def _canonicalize_game_id_for_payload(
    game_id: Any,
    *,
    game_date: Any = None,
    away_team_code: Any = None,
    home_team_code: Any = None,
    season_year: int | None = None,
    doubleheader_no: Any = None,
) -> tuple[str | None, str | None]:
    """Return a canonical game_id, preferring explicit payload teams when available."""
    canonical, original = _canonicalize_game_id(game_id)
    if not original:
        return canonical, original

    fallback_date = canonical[:8] if canonical else ""
    date_part = str(game_date or fallback_date).replace("-", "").strip()

    dh = doubleheader_no
    if dh is None and original[-1:].isdigit():
        dh = original[-1]
    expected = build_kbo_game_id(
        date_part,
        str(away_team_code).strip().upper() if away_team_code not in (None, "") else None,
        str(home_team_code).strip().upper() if home_team_code not in (None, "") else None,
        doubleheader_no=dh,
        season_year=season_year,
    )
    if expected and expected != canonical:
        return expected, original
    return canonical, original


def _record_game_id_alias(
    session,
    alias_game_id: str | None,
    canonical_game_id: str | None,
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


def _new_strict_player_resolver(session) -> PlayerIdResolver:
    try:
        return PlayerIdResolver(
            session,
            strict_game_resolution=True,
            allow_auto_register=False,
        )
    except TypeError:
        return PlayerIdResolver(session)


def _assign_field_if_changed(
    target: Any,
    attr: str,
    value: Any,
    *,
    game_id: str,
    source: GameWriteSource,
    write_contract: GameWriteContract | None,
    field: str | None = None,
    allow_empty: bool = False,
) -> bool:
    if not allow_empty and value in (None, ""):
        return False

    current = getattr(target, attr)
    if _values_equal(current, value):
        if write_contract:
            write_contract.field_duplicate(game_id, source, field or attr, current)
        return False

    if write_contract:
        write_contract.field_updated(game_id, source, field or attr, current, value)
    setattr(target, attr, value)
    return True


def _values_equal(left: Any, right: Any) -> bool:
    if isinstance(left, Decimal) or isinstance(right, Decimal):
        try:
            return Decimal(str(left)) == Decimal(str(right))
        except (TypeError, ValueError, InvalidOperation):
            logger.info("Decimal comparison fallback to equality")
            return left == right
    return left == right


def _apply_game_team_identity_with_contract(
    game: Game,
    season_year: int | None,
    *,
    source: GameWriteSource,
    write_contract: GameWriteContract | None,
) -> bool:
    before = {
        "home_franchise_id": game.home_franchise_id,
        "away_franchise_id": game.away_franchise_id,
        "winning_franchise_id": game.winning_franchise_id,
    }
    _apply_game_team_identity(game, season_year)
    changed = False
    for attr, old_value in before.items():
        new_value = getattr(game, attr)
        if not _values_equal(old_value, new_value):
            changed = True
            if write_contract:
                write_contract.field_updated(game.game_id, source, attr, old_value, new_value)
        elif write_contract:
            write_contract.field_duplicate(game.game_id, source, attr, new_value)
    return changed


def _has_game_child_rows(session, model, game_id: str) -> bool:
    return session.query(model).filter(model.game_id == game_id).first() is not None


def _infer_team_code_from_children(
    session,
    game_id: str,
    team_side: str,
    season_year: int | None,
) -> str | None:
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


def _infer_score_from_children(session, game_id: str, team_side: str) -> int | None:
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


def _infer_pitcher_from_children(session, game_id: str, team_side: str) -> str | None:
    """Find starting pitcher name from game_pitching_stats."""
    row = (
        session.query(GamePitchingStat.player_name)
        .filter(
            GamePitchingStat.game_id == game_id, GamePitchingStat.team_side == team_side, GamePitchingStat.is_starting
        )
        .first()
    )
    return row[0] if row else None


def _enrich_existing_child_team_identity(session, game_id: str, season_year: int | None) -> None:
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
    except ValueError:
        logger.warning("Failed to parse game date from game_id")
        game_date = datetime.now().date()

    away_team = None
    home_team = None
    if len(game_id) >= 12:
        away_team = game_id[8:10] or None
        home_team = game_id[10:12] or None

    season_id = None
    try:
        season_year = int(game_id[:4])
    except ValueError:
        logger.warning("Failed to parse season year from game_id")
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
        except SQLAlchemyError:
            logger.warning("Failed to query min season_id for year")
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
    game_date: date | None,
    home_score: Any,
    away_score: Any,
    current_status: str | None,
    has_metadata: bool,
    has_inning_scores: bool,
    has_lineups: bool,
    has_batting: bool,
    has_pitching: bool,
    today: date,
) -> str:
    if (
        home_score is not None
        and away_score is not None
        and (has_batting or has_pitching or (game_date and game_date < today and has_inning_scores))
    ):
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


def _upsert_metadata(
    session,
    game_id: str,
    metadata: dict[str, Any],
    *,
    source: GameWriteSource | None = None,
    write_contract: GameWriteContract | None = None,
) -> bool:
    meta = session.query(GameMetadata).filter(GameMetadata.game_id == game_id).one_or_none()
    changed = False
    if source and write_contract:
        write_contract.claim_game(game_id, source)
    if not meta:
        meta = GameMetadata(game_id=game_id)
        session.add(meta)
        changed = True
        if source and write_contract:
            write_contract.field_updated(game_id, source, "metadata.created", None, True)

    if metadata.get("stadium_code") not in (None, ""):
        changed |= _assign_field_if_changed(
            meta,
            "stadium_code",
            metadata.get("stadium_code"),
            game_id=game_id,
            source=source or GameWriteSource("metadata", "unknown"),
            write_contract=write_contract,
            field="metadata.stadium_code",
        )
    if metadata.get("stadium") not in (None, ""):
        changed |= _assign_field_if_changed(
            meta,
            "stadium_name",
            metadata.get("stadium"),
            game_id=game_id,
            source=source or GameWriteSource("metadata", "unknown"),
            write_contract=write_contract,
            field="metadata.stadium_name",
        )
    if metadata.get("attendance") not in (None, ""):
        changed |= _assign_field_if_changed(
            meta,
            "attendance",
            metadata.get("attendance"),
            game_id=game_id,
            source=source or GameWriteSource("metadata", "unknown"),
            write_contract=write_contract,
            field="metadata.attendance",
        )

    start_time = _safe_time(metadata.get("start_time"))
    if start_time is not None:
        changed |= _assign_field_if_changed(
            meta,
            "start_time",
            start_time,
            game_id=game_id,
            source=source or GameWriteSource("metadata", "unknown"),
            write_contract=write_contract,
            field="metadata.start_time",
        )

    end_time = _safe_time(metadata.get("end_time"))
    if end_time is not None:
        changed |= _assign_field_if_changed(
            meta,
            "end_time",
            end_time,
            game_id=game_id,
            source=source or GameWriteSource("metadata", "unknown"),
            write_contract=write_contract,
            field="metadata.end_time",
        )

    if metadata.get("duration_minutes") not in (None, ""):
        changed |= _assign_field_if_changed(
            meta,
            "game_time_minutes",
            metadata.get("duration_minutes"),
            game_id=game_id,
            source=source or GameWriteSource("metadata", "unknown"),
            write_contract=write_contract,
            field="metadata.game_time_minutes",
        )
    if metadata.get("weather") not in (None, ""):
        changed |= _assign_field_if_changed(
            meta,
            "weather",
            metadata.get("weather"),
            game_id=game_id,
            source=source or GameWriteSource("metadata", "unknown"),
            write_contract=write_contract,
            field="metadata.weather",
        )

    if metadata:
        existing_payload = meta.source_payload if isinstance(meta.source_payload, dict) else {}
        merged_payload = dict(existing_payload)
        for key, value in metadata.items():
            if value not in (None, ""):
                merged_payload[key] = value
        changed |= _assign_field_if_changed(
            meta,
            "source_payload",
            merged_payload or None,
            game_id=game_id,
            source=source or GameWriteSource("metadata", "unknown"),
            write_contract=write_contract,
            field="metadata.source_payload",
            allow_empty=True,
        )

    return changed


def _prepare_player_rows(game_id: str, dataset: str, mappings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped = _dedupe_exact_player_rows(game_id, dataset, mappings)
    _assert_no_player_team_collisions(game_id, dataset, deduped)
    return deduped


def _dedupe_exact_player_rows(game_id: str, dataset: str, mappings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    removed = 0
    for mapping in mappings:
        key = repr(sorted(_normalize_record_for_compare(mapping).items()))
        if key in seen:
            removed += 1
            continue
        seen.add(key)
        deduped.append(mapping)
    if removed:
        logger.info(f"[WARN] Removed {removed} exact duplicate rows from {dataset} for {game_id}")
    return deduped


def _assert_no_player_team_collisions(game_id: str, dataset: str, mappings: list[dict[str, Any]]) -> None:
    by_player: dict[int, set[tuple[str, str]]] = {}
    for mapping in mappings:
        player_id = _normalize_player_id(mapping.get("player_id"))
        if player_id is None:
            continue
        team_key = (
            str(mapping.get("team_side") or "").strip(),
            str(mapping.get("team_code") or "").strip(),
        )
        by_player.setdefault(player_id, set()).add(team_key)

    collisions = {player_id: sorted(team_keys) for player_id, team_keys in by_player.items() if len(team_keys) > 1}
    if not collisions:
        return

    preview = ", ".join(f"{player_id}:{team_keys}" for player_id, team_keys in list(collisions.items())[:5])
    raise ValueError(
        f"{dataset} has player_id team collisions for {game_id}: {preview}. "
        "Refusing to save ambiguous game detail rows."
    )


def _ensure_player_basic_stubs(session, mappings: Iterable[dict[str, Any]]) -> bool:
    candidates: dict[int, dict[str, Any]] = {}
    for mapping in mappings:
        player_id = _normalize_player_id(mapping.get("player_id"))
        player_name = str(mapping.get("player_name") or "").strip()
        if player_id is None or not player_name:
            continue
        candidates.setdefault(player_id, mapping)

    if not candidates:
        return False

    existing_ids = {
        int(row[0])
        for row in session.query(PlayerBasic.player_id).filter(PlayerBasic.player_id.in_(list(candidates))).all()
        if row[0] is not None
    }
    missing_ids = sorted(set(candidates) - existing_ids)
    if not missing_ids:
        return False

    for player_id in missing_ids:
        mapping = candidates[player_id]
        standard_position = str(mapping.get("standard_position") or "").strip()
        session.add(
            PlayerBasic(
                player_id=player_id,
                name=str(mapping.get("player_name") or f"Unknown {player_id}"),
                uniform_no=str(mapping.get("uniform_no")) if mapping.get("uniform_no") not in (None, "") else None,
                team=str(mapping.get("team_code")) if mapping.get("team_code") not in (None, "") else None,
                position=str(mapping.get("position") or ("투수" if standard_position == "P" else "")) or None,
                status="STUB",
                status_source="game_detail",
            )
        )
    session.flush()
    logger.info(f"[WARN] Created {len(missing_ids)} player_basic stub(s) for game detail save")
    return True


def _replace_records(
    session,
    model,
    game_id: str,
    mappings: list[dict[str, Any]],
    *,
    source: GameWriteSource | None = None,
    write_contract: GameWriteContract | None = None,
) -> bool:
    dataset = model.__tablename__
    query = session.query(model).filter(model.game_id == game_id)
    if _records_match_existing(query.all(), model, mappings):
        if source and write_contract:
            write_contract.dataset_duplicate(game_id, source, dataset, len(mappings))
        return False

    query.delete()
    if mappings:
        now = datetime.now(UTC).replace(tzinfo=None)
        has_created_at = "created_at" in model.__table__.columns
        has_updated_at = "updated_at" in model.__table__.columns
        if has_created_at or has_updated_at:
            for mapping in mappings:
                if has_created_at and not mapping.get("created_at"):
                    mapping["created_at"] = now
                if has_updated_at and not mapping.get("updated_at"):
                    mapping["updated_at"] = now
        session.execute(model.__table__.insert(), mappings)
    if source and write_contract:
        write_contract.dataset_replaced(game_id, source, dataset, len(mappings))
    return True


def _replace_records_for_side(
    session,
    model,
    game_id: str,
    team_side: str,
    mappings: list[dict[str, Any]],
    *,
    source: GameWriteSource | None = None,
    write_contract: GameWriteContract | None = None,
) -> bool:
    dataset = f"{model.__tablename__}.{team_side}"
    query = session.query(model).filter(model.game_id == game_id, model.team_side == team_side)
    if _records_match_existing(query.all(), model, mappings):
        if source and write_contract:
            write_contract.dataset_duplicate(game_id, source, dataset, len(mappings))
        return False

    query.delete()
    if mappings:
        now = datetime.now(UTC).replace(tzinfo=None)
        has_created_at = "created_at" in model.__table__.columns
        has_updated_at = "updated_at" in model.__table__.columns
        if has_created_at or has_updated_at:
            for mapping in mappings:
                if has_created_at and not mapping.get("created_at"):
                    mapping["created_at"] = now
                if has_updated_at and not mapping.get("updated_at"):
                    mapping["updated_at"] = now
        session.execute(model.__table__.insert(), mappings)
    if source and write_contract:
        write_contract.dataset_replaced(game_id, source, dataset, len(mappings))
    return True


def _replace_orm_records(
    session,
    model,
    game_id: str,
    records: list[Any],
    *,
    source: GameWriteSource | None = None,
    write_contract: GameWriteContract | None = None,
) -> bool:
    dataset = model.__tablename__
    query = session.query(model).filter(model.game_id == game_id)
    if _records_match_existing_objects(query.all(), model, records):
        if source and write_contract:
            write_contract.dataset_duplicate(game_id, source, dataset, len(records))
        return False

    query.delete()
    if records:
        session.add_all(records)
    if source and write_contract:
        write_contract.dataset_replaced(game_id, source, dataset, len(records))
    return True


def _records_match_existing(existing_rows: list[Any], model, mappings: list[dict[str, Any]]) -> bool:
    if len(existing_rows) != len(mappings):
        return False

    comparable_columns = [
        column.name for column in model.__table__.columns if column.name not in {"id", "created_at", "updated_at"}
    ]
    existing = [
        _normalize_record_for_compare({name: getattr(row, name) for name in comparable_columns})
        for row in existing_rows
    ]
    incoming = [
        _normalize_record_for_compare({name: mapping.get(name) for name in comparable_columns}) for mapping in mappings
    ]
    return sorted(existing, key=repr) == sorted(incoming, key=repr)


def _records_match_existing_objects(existing_rows: list[Any], model, records: list[Any]) -> bool:
    if len(existing_rows) != len(records):
        return False

    comparable_columns = [
        column.name for column in model.__table__.columns if column.name not in {"id", "created_at", "updated_at"}
    ]
    existing = [
        _normalize_record_for_compare({name: getattr(row, name) for name in comparable_columns})
        for row in existing_rows
    ]
    incoming = [
        _normalize_record_for_compare({name: getattr(row, name) for name in comparable_columns}) for row in records
    ]
    return sorted(existing, key=repr) == sorted(incoming, key=repr)


def _normalize_record_for_compare(record: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in record.items():
        if isinstance(value, Decimal):
            normalized[key] = str(value.normalize())
        elif isinstance(value, (datetime, date)):
            normalized[key] = value.isoformat()
        else:
            normalized[key] = value
    return normalized


def _stat_int(stats: dict[str, Any], key: str) -> int:
    value = stats.get(key)
    if value in (None, ""):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _stat_float(stats: dict[str, Any], key: str) -> Any:
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
    except ValueError:
        logger.info("Failed to parse start_time value")
        return None
    return None


def _resolve_winner(home: dict[str, Any], away: dict[str, Any]) -> tuple[str | None, int | None]:
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


def _apply_game_team_identity(game: Game, season_year: int | None) -> None:
    home_franchise_id, _, _ = _resolve_team_identity(game.home_team, season_year)
    away_franchise_id, _, _ = _resolve_team_identity(game.away_team, season_year)
    winning_franchise_id, _, _ = _resolve_team_identity(game.winning_team, season_year)
    game.home_franchise_id = home_franchise_id
    game.away_franchise_id = away_franchise_id
    game.winning_franchise_id = winning_franchise_id


def _apply_team_identity_to_mappings(mappings: list[dict[str, Any]], season_year: int | None) -> None:
    for mapping in mappings:
        team_code = mapping.get("team_code")
        franchise_id, canonical_team_code, season_code = _resolve_team_identity(team_code, season_year)
        mapping["team_code"] = season_code or team_code
        mapping["franchise_id"] = franchise_id
        mapping["canonical_team_code"] = canonical_team_code


def _resolve_team_identity(team_code: Any, season_year: int | None) -> tuple[int | None, str | None, str | None]:
    if not team_code:
        return None, None, None
    raw_code = str(team_code).strip().upper()
    normalized_code = team_code_from_game_id_segment(raw_code, season_year) or raw_code
    entry = find_team_history_entry(normalized_code, season_year)
    if entry is None and season_year is None:
        entry = find_team_history_entry(raw_code)
    if entry is None:
        return None, None, normalized_code

    canonical_code = FRANCHISE_CANONICAL_CODE.get(entry.franchise_id)
    return entry.franchise_id, canonical_code, entry.team_code.upper()


def _build_inning_scores(
    game_id: str, teams: dict[str, Any], *, season_year: int | None = None
) -> list[dict[str, Any]]:
    records = []
    for side in ("away", "home"):
        team_info = teams.get(side, {}) or {}
        line_score = team_info.get("line_score") or []
        team_code = team_info.get("code")
        for idx, runs in enumerate(line_score, start=1):
            if runs is None:
                continue
            records.append(
                {
                    "game_id": game_id,
                    "team_side": side,
                    "team_code": team_code,
                    "inning": idx,
                    "runs": runs,
                    "is_extra": idx > 9,
                }
            )
    _apply_team_identity_to_mappings(records, season_year)
    return records


def _build_lineups(
    game_id: str, hitters: dict[str, list[dict[str, Any]]], *, season_year: int | None = None
) -> list[dict[str, Any]]:
    records = []
    for side, entries in hitters.items():
        for entry in entries:
            player_name = entry.get("player_name")
            if not player_name:
                continue
            records.append(
                {
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
                }
            )
    _apply_team_identity_to_mappings(records, season_year)
    return records


def _format_notes(extras: dict[str, Any] | None) -> str | None:
    if not extras:
        return None
    ignore_keys = {"COL_0", "COL_1", "선수명", "PlayerName", "playerName"}
    cleaned = {k: v for k, v in extras.items() if k not in ignore_keys}
    if not cleaned:
        return None
    if len(cleaned) == 1:
        return str(next(iter(cleaned.values())))
    return str(cleaned)


def _build_batting_stats(
    game_id: str, hitters: dict[str, list[dict[str, Any]]], *, season_year: int | None = None
) -> list[dict[str, Any]]:
    records = []
    for side, entries in hitters.items():
        for entry in entries:
            player_name = entry.get("player_name")
            stats = entry.get("stats") or {}
            if not player_name:
                continue
            records.append(
                {
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
                }
            )
    _apply_team_identity_to_mappings(records, season_year)
    return records


def _build_pitching_stats(
    game_id: str, pitchers: dict[str, list[dict[str, Any]]], *, season_year: int | None = None
) -> list[dict[str, Any]]:
    records = []
    for side, entries in pitchers.items():
        for entry in entries:
            player_name = entry.get("player_name")
            stats = entry.get("stats") or {}
            if not player_name:
                continue
            innings_outs = stats.get("innings_outs")
            records.append(
                {
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
                }
            )
    _apply_team_identity_to_mappings(records, season_year)
    return records


def _build_pregame_lineup_rows(
    game_id: str,
    *,
    team_side: str,
    team_code: str | None,
    season_year: int,
    lineup: list[dict[str, Any]],
    resolver: PlayerIdResolver,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for idx, entry in enumerate(lineup, start=1):
        player_name = str(entry.get("player_name") or "").strip()
        if not player_name:
            continue
        batting_order = _coerce_int(entry.get("batting_order")) or idx
        position = entry.get("position")
        is_pitcher = position == "투수" or position == "P"
        player_id = (
            resolver.resolve_id(player_name, team_code, season_year, is_pitcher=is_pitcher) if team_code else None
        )
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
    player_name: str | None = None,
    player_id: int | None = None,
) -> None:
    existing = (
        session.query(GameSummary)
        .filter(
            GameSummary.game_id == game_id,
            GameSummary.summary_type == summary_type,
            GameSummary.player_name == player_name,
        )
        .one_or_none()
    )
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


def _json_dumps(payload: dict[str, Any]) -> str:
    import json

    return json.dumps(payload, ensure_ascii=False)


def _extract_players_from_text(category: str, text: str) -> list[tuple[str, str | None]]:
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

    found_any = False
    for m in matches:
        found_any = True
        name = m.group(1)
        detail = m.group(0)  # Include the whole match as detail
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


def _clean_extras(extras: dict[str, Any] | None) -> dict[str, Any] | None:
    if not extras:
        return None
    ignore_keys = {"COL_0", "COL_1", "선수명", "PlayerName", "playerName"}
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
                logger.info(f" ✨ Auto-synced {game_id} to OCI")
        except Exception:
            logger.exception(" ⚠️ Auto-sync OCI failed")
