"""Sync validated data from SQLite to OCI (Oracle Cloud Infrastructure) PostgreSQL.

Dual-repository pattern: SQLite (dev/validation) → OCI (production).
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from itertools import count
from typing import TYPE_CHECKING, Any, Protocol, cast

if TYPE_CHECKING:
    from collections.abc import Callable

    from src.models.base import Base

from psycopg2 import Error as PsycopgError
from sqlalchemy import bindparam, create_engine, inspect, text
from sqlalchemy.exc import DBAPIError, OperationalError, SQLAlchemyError
from sqlalchemy.orm import Query, Session, sessionmaker

try:
    import oracledb

    OracleError = oracledb.Error
except ImportError:
    OracleError = None

if OracleError is not None:
    DBAPI_EXCEPTIONS = (PsycopgError, OracleError, SQLAlchemyError, OSError, RuntimeError)
    DBAPI_EXCEPTIONS_VAL = (PsycopgError, OracleError, SQLAlchemyError, OSError, RuntimeError, ValueError)
    DBAPI_EXCEPTIONS_SQLITE = (PsycopgError, OracleError, sqlite3.Error, SQLAlchemyError, OSError, RuntimeError)
else:
    DBAPI_EXCEPTIONS = (PsycopgError, SQLAlchemyError, OSError, RuntimeError)
    DBAPI_EXCEPTIONS_VAL = (PsycopgError, SQLAlchemyError, OSError, RuntimeError, ValueError)
    DBAPI_EXCEPTIONS_SQLITE = (PsycopgError, sqlite3.Error, SQLAlchemyError, OSError, RuntimeError)

from src.constants import KST
from src.db.engine import normalize_oracle_url
from src.models.game import (
    Game,
    GameBattingStat,
    GameEvent,
    GameInningScore,
    GameLineup,
    GamePitchingStat,
    GamePlayByPlay,
    GameSummary,
)

# 현재 사용 가능한 모델들만 import
from src.utils.game_status import (
    COMPLETED_LIKE_GAME_STATUSES,
    GAME_STATUS_CANCELLED,
    GAME_STATUS_POSTPONED,
    GAME_STATUS_SCHEDULED,
)

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection, Result

RawConnection = Any  # psycopg2 connection, typed loosely to avoid heavy deps
SYNC_LOG_SAMPLE_LIMIT = 10
LEGACY_OCI_VARCHAR_MAX_LENGTH = 255
SQL_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class SyncBaseProtocol(Protocol):
    """Protocol interface for sync mixin classes."""

    sqlite_session: Any
    target_session: Any
    oci_engine: Any

    def sync_simple_table(
        self,
        model: type[Base],
        options: SimpleTableSyncOptions,
    ) -> int:
        """Sync a simple table with upsert semantics."""
        ...

    def _bulk_copy_upsert(
        self,
        table_name: str,
        options: BulkCopyUpsertOptions,
    ) -> None: ...
    def _ensure_table(self, model: type[Base]) -> None: ...
    def _target_table_exists(self, model: type[Base]) -> bool: ...
    def _get_franchise_id_mapping(self) -> dict[int, int]: ...
    def _get_season_map(self) -> dict[tuple[Any, ...], int]: ...
    def _reset_target_sequence_for_table(self, table_name: str, column_name: str = ...) -> bool: ...
    def _run_target_session_with_retries(self, *args: Any, **kwargs: Any) -> Any: ...  # noqa: ANN401
    def test_connection(self) -> bool:
        """Test source and target database connectivity."""
        ...

    def close(self) -> None:
        """Close sync resources."""
        ...

    def sync_batting_data(self, *args: Any, **kwargs: Any) -> int:  # noqa: ANN401
        """Sync batting data."""
        ...

    def sync_pitcher_data(self, *args: Any, **kwargs: Any) -> int:  # noqa: ANN401
        """Sync pitcher data."""
        ...

    def sync_players(self) -> int:
        """Sync player records."""
        ...

    def sync_player_identities(self) -> int:
        """Sync player identity records."""
        ...

    def sync_game_schedules(self, limit: int | None = ...) -> int:
        """Sync game schedule records."""
        ...

    def _chunked(self, items: list[str], size: int) -> list[list[str]]: ...
    def _sync_referenced_player_basic_for_games(self, *args: Any, **kwargs: Any) -> Any: ...  # noqa: ANN401


LEAGUE_NAME_TO_CODE = {
    "REGULAR": 0,
    "EXHIBITION": 1,
    "WILDCARD": 2,
    "SEMI_PLAYOFF": 3,
    "PLAYOFF": 4,
    "KOREAN_SERIES": 5,
}

GAME_SIGNATURE_CHILD_TABLES = (
    "game_metadata",
    "game_inning_scores",
    "game_lineups",
    "game_events",
    "game_summary",
    "game_play_by_play",
    "game_validation_metrics",
    "player_game_batting",
    "player_game_pitching",
)

NON_DETAIL_TERMINAL_STATUSES = {GAME_STATUS_CANCELLED, GAME_STATUS_POSTPONED}
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class GameSyncEligibility:
    """GameSyncEligibility class."""

    parent_game_ids: list[str] = field(default_factory=list)
    detail_game_ids: list[str] = field(default_factory=list)
    relay_game_ids: list[str] = field(default_factory=list)
    skipped_schedule_only: list[str] = field(default_factory=list)
    skipped_incomplete_detail: list[str] = field(default_factory=list)
    skipped_empty_relay: list[str] = field(default_factory=list)
    skipped_cancelled: list[str] = field(default_factory=list)

    def counts(self) -> dict[str, int]:
        """Return sync operation counts.

        Returns:
            Dictionary result.

        """
        return {
            "skipped_schedule_only": len(self.skipped_schedule_only),
            "skipped_incomplete_detail": len(self.skipped_incomplete_detail),
            "skipped_empty_relay": len(self.skipped_empty_relay),
            "skipped_cancelled": len(self.skipped_cancelled),
        }


@dataclass
class SyncBatchConfig:
    """SyncBatchConfig class."""

    model: type[Base]
    query: Query[Any]
    total_count: int
    columns: list[str]
    conflict_keys: list[str]
    transform_fn: Callable[[dict[str, Any]], dict[str, Any]] | None
    update_timestamp: bool
    batch_size: int = 5000
    dedupe_keys: list[str] | None = None


@dataclass(frozen=True, slots=True)
class SimpleTableSyncOptions:
    """Column, conflict, and batch settings for one table sync."""

    conflict_keys: list[str]
    exclude_cols: list[str] | None = None
    filters: list[Any] | None = None
    transform_fn: Callable[..., Any] | None = None
    batch_size: int = 5000
    update_timestamp: bool | None = None
    dedupe_keys: list[str] | None = None


@dataclass(frozen=True, slots=True)
class BulkCopyUpsertOptions:
    """COPY upsert payload and connection settings."""

    records: list[dict[str, Any]]
    unique_cols: list[str]
    update_timestamp: bool = True
    connection: RawConnection | None = None
    reconnect_on_fail: bool = True


@dataclass(frozen=True, slots=True)
class CopyFallbackOptions:
    """Connection and diagnostic settings for a batch COPY fallback."""

    records: list[dict[str, Any]]
    connection: RawConnection | None
    copy_label: str
    fallback_label: str
    failure_context: str
    reconnect_on_fail: bool


def _serialize_scalar(value: object) -> Any:  # noqa: ANN401
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _serialize_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert records to the values expected by PostgreSQL COPY."""
    return [
        {
            key: r"\N"
            if value is None
            else json.dumps(value, ensure_ascii=False)
            if isinstance(value, (dict, list))
            else value
            for key, value in record.items()
        }
        for record in records
    ]


def _serialize_records_oracle(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert records to the values expected by Oracle SQL MERGE/INSERT."""
    return [
        {
            key: json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else value
            for key, value in record.items()
        }
        for record in records
    ]


def _dedupe_records_for_conflict_keys(
    records: list[dict[str, Any]],
    conflict_keys: list[str],
) -> list[dict[str, Any]]:
    """Mirror Postgres unique semantics while removing duplicate upsert keys.

    Postgres unique indexes allow multiple rows when any indexed column is NULL.
    Python tuple-based dedupe would otherwise collapse rows such as away/home
    pitching lines that share ``(game_id, NULL, appearance_seq)``.
    """
    if not conflict_keys:
        return records

    seen = set()
    deduped_records = []
    for record in records:
        key = tuple(record.get(column) for column in conflict_keys)
        if any(value is None for value in key):
            deduped_records.append(record)
            continue
        if key in seen:
            continue
        seen.add(key)
        deduped_records.append(record)
    return deduped_records


def _row_to_record(
    row: object,
    columns: list[str],
    transform_fn: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    now = datetime.now(KST)
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        data = {c: mapping[c] for c in columns if c in mapping}
    else:
        data = {c: getattr(row, c) for c in columns if hasattr(row, c)}
    if "created_at" in columns and data.get("created_at") is None:
        data["created_at"] = now
    if "updated_at" in columns and data.get("updated_at") is None:
        data["updated_at"] = now
    if transform_fn:
        data = transform_fn(data)
    for k, v in data.items():
        if isinstance(v, (dict, list)):
            data[k] = json.dumps(v, ensure_ascii=False)
    return data


def _execute_signature_query(
    session_or_conn: Session | Connection,
    sql: str,
    *,
    game_ids: list[str] | None = None,
) -> Result[Any]:
    stmt = text(sql)
    params = {}
    if game_ids is not None:
        stmt = stmt.bindparams(bindparam("game_ids", expanding=True))
        params["game_ids"] = list(game_ids)
    return session_or_conn.execute(stmt, params)


def _build_composite_signature_query(game_ids: list[str] | None) -> str:
    """Build a single composite query with correlated subqueries for all child tables."""
    filter_clause = "WHERE g.game_id IN :game_ids" if game_ids is not None else ""

    child_subqueries: list[str] = []
    for i, table_name in enumerate(GAME_SIGNATURE_CHILD_TABLES):
        alias = f"t{i}"
        if table_name == "game_metadata":
            child_subqueries.append(
                f"(SELECT COUNT(*) FROM game_metadata {alias} WHERE {alias}.game_id = g.game_id) AS meta_count,\n"  # noqa: S608
                f"            (SELECT MAX({alias}.updated_at) FROM game_metadata {alias} "
                f"WHERE {alias}.game_id = g.game_id) AS meta_max_updated,\n"
                f"            (SELECT MAX({alias}.start_time) FROM game_metadata {alias} "
                f"WHERE {alias}.game_id = g.game_id) AS meta_start_time",
            )
        else:
            child_subqueries.append(
                f"(SELECT COUNT(*) FROM {table_name} {alias} WHERE {alias}.game_id = g.game_id) AS {alias}_count,\n"  # noqa: S608
                f"            (SELECT MAX({alias}.updated_at) FROM {table_name} {alias} "
                f"WHERE {alias}.game_id = g.game_id) AS {alias}_max_updated",
            )

    children_sql = ",\n            ".join(child_subqueries)

    return f"""
        SELECT
            g.game_id,
            g.game_status,
            g.home_score,
            g.away_score,
            g.home_pitcher,
            g.away_pitcher,
            g.home_team,
            g.away_team,
            g.updated_at,
            {children_sql}
        FROM game g
        {filter_clause}
    """  # noqa: S608


def load_game_sync_signatures(
    session_or_conn: Session | Connection,
    *,
    game_ids: list[str] | None = None,
) -> dict[str, dict[str, Any]]:
    """Load game signatures.

    Args:
        session_or_conn: Session Or Conn.
        game_ids: Game Ids.

    Returns:
        Dictionary result.

    """
    composite_sql = _build_composite_signature_query(game_ids)
    rows = _execute_signature_query(session_or_conn, composite_sql, game_ids=game_ids).mappings().all()

    signatures: dict[str, dict[str, Any]] = {}
    for row in rows:
        game_id = str(row["game_id"])
        sig: dict[str, Any] = {
            "game": {
                "game_status": row["game_status"],
                "home_score": row["home_score"],
                "away_score": row["away_score"],
                "home_pitcher": row["home_pitcher"],
                "away_pitcher": row["away_pitcher"],
                "home_team": row["home_team"],
                "away_team": row["away_team"],
                "updated_at": _serialize_scalar(row["updated_at"]),
            },
        }

        for i, table_name in enumerate(GAME_SIGNATURE_CHILD_TABLES):
            alias = f"t{i}"
            if table_name == "game_metadata":
                sig[table_name] = {
                    "row_count": int(row["meta_count"] or 0),
                    "max_updated_at": _serialize_scalar(row["meta_max_updated"]),
                    "start_time": _serialize_scalar(row["meta_start_time"]),
                }
            else:
                count_col = f"{alias}_count"
                max_col = f"{alias}_max_updated"
                sig[table_name] = {
                    "row_count": int(row[count_col] or 0),
                    "max_updated_at": _serialize_scalar(row[max_col]),
                }

        signatures[game_id] = sig

    return signatures


def _is_game_dirty_by_game_section(_game_id: str, local_sig: dict[str, Any], remote_sig: dict[str, Any]) -> bool:
    local_game = local_sig.get("game", {})
    remote_game = remote_sig.get("game", {})
    for key in (
        "game_status",
        "home_score",
        "away_score",
        "home_pitcher",
        "away_pitcher",
        "home_team",
        "away_team",
    ):
        if local_game.get(key) != remote_game.get(key):
            return True
    return local_game.get("updated_at") is not None and (
        remote_game.get("updated_at") is None or str(local_game.get("updated_at")) > str(remote_game.get("updated_at"))
    )


def _is_game_dirty_by_metadata_section(_game_id: str, local_sig: dict[str, Any], remote_sig: dict[str, Any]) -> bool:
    metadata_local = local_sig.get("game_metadata", {})
    metadata_remote = remote_sig.get("game_metadata", {})
    if metadata_local.get("row_count") != metadata_remote.get("row_count"):
        return True
    if metadata_local.get("start_time") != metadata_remote.get("start_time"):
        return True
    return metadata_local.get("max_updated_at") is not None and (
        metadata_remote.get("max_updated_at") is None
        or str(metadata_local.get("max_updated_at")) > str(metadata_remote.get("max_updated_at"))
    )


def _is_game_dirty_by_child_tables(_game_id: str, local_sig: dict[str, Any], remote_sig: dict[str, Any]) -> bool:
    for table_name in GAME_SIGNATURE_CHILD_TABLES:
        if table_name == "game_metadata":
            continue
        local_child = local_sig.get(table_name, {})
        remote_child = remote_sig.get(table_name, {})
        if local_child.get("row_count") != remote_child.get("row_count"):
            return True
        if local_child.get("max_updated_at") is not None and (
            remote_child.get("max_updated_at") is None
            or str(local_child.get("max_updated_at")) > str(remote_child.get("max_updated_at"))
        ):
            return True
    return False


def _is_game_dirty(game_id: str, local_sig: dict[str, Any], remote_sig: dict[str, Any]) -> bool:
    return (
        _is_game_dirty_by_game_section(game_id, local_sig, remote_sig)
        or _is_game_dirty_by_metadata_section(game_id, local_sig, remote_sig)
        or _is_game_dirty_by_child_tables(game_id, local_sig, remote_sig)
    )


def detect_dirty_game_ids(
    local_session_or_conn: Session | Connection,
    remote_session_or_conn: Session | Connection,
    *,
    game_ids: list[str] | None = None,
) -> list[str]:
    """Detect dirty game IDs by comparing local and remote signatures.

    Args:
        local_session_or_conn: Local Session Or Conn.
        remote_session_or_conn: Remote Session Or Conn.
        game_ids: Game Ids.

    Returns:
        List of results.

    """
    local_signatures = load_game_sync_signatures(local_session_or_conn, game_ids=game_ids)
    remote_signatures = load_game_sync_signatures(remote_session_or_conn, game_ids=list(local_signatures.keys()))

    dirty: list[str] = []
    for game_id, local_signature in local_signatures.items():
        remote_signature = remote_signatures.get(game_id)
        if remote_signature is None or _is_game_dirty(game_id, local_signature, remote_signature):
            dirty.append(game_id)

    return dirty


def filter_game_ids_by_year(game_ids: list[str], year: int | None) -> list[str]:
    """Filter game IDs by year.

    Args:
        game_ids: Game Ids.
        year: Season year.

    Returns:
        List of results.

    """
    if year is None:
        return list(game_ids)
    prefix = str(int(year))
    return [game_id for game_id in game_ids if str(game_id).startswith(prefix)]


def _load_team_sides(session: Session, model: type[Base], game_ids: list[str]) -> dict[str, set[str]]:
    if not game_ids:
        return {}
    rows = session.query(model.game_id, model.team_side).filter(model.game_id.in_(game_ids)).distinct().all()  # type: ignore[attr-defined]
    result: dict[str, set[str]] = {}
    for game_id, team_side in rows:
        if not game_id or not team_side:
            continue
        result.setdefault(str(game_id), set()).add(str(team_side))
    return result


def _load_game_ids_with_rows(session: Session, model: type[Base], game_ids: list[str]) -> set[str]:
    if not game_ids:
        return set()
    return {str(row[0]) for row in session.query(model.game_id).filter(model.game_id.in_(game_ids)).distinct().all()}  # type: ignore[attr-defined]


def _has_both_team_sides(side_map: dict[str, set[str]], game_id: str) -> bool:
    sides = side_map.get(game_id, set())
    return "away" in sides and "home" in sides


def build_game_sync_eligibility(session: Session, game_ids: list[str]) -> GameSyncEligibility:
    """Classify which game datasets are safe to publish to OCI."""
    target_game_ids = sorted({str(game_id) for game_id in game_ids if game_id})
    if not target_game_ids:
        return GameSyncEligibility()

    rows = (
        session.query(
            Game.game_id,
            Game.game_status,
            Game.home_score,
            Game.away_score,
        )
        .filter(Game.game_id.in_(target_game_ids))
        .all()
    )
    game_rows = {
        str(game_id): (str(game_status or "").upper(), home_score, away_score)
        for game_id, game_status, home_score, away_score in rows
    }

    batting_sides = _load_team_sides(session, GameBattingStat, target_game_ids)
    pitching_sides = _load_team_sides(session, GamePitchingStat, target_game_ids)
    inning_ids = _load_game_ids_with_rows(session, GameInningScore, target_game_ids)
    lineup_ids = _load_game_ids_with_rows(session, GameLineup, target_game_ids)
    summary_ids = _load_game_ids_with_rows(session, GameSummary, target_game_ids)
    event_ids = _load_game_ids_with_rows(session, GameEvent, target_game_ids)
    pbp_ids = _load_game_ids_with_rows(session, GamePlayByPlay, target_game_ids)

    parent_game_ids: list[str] = []
    detail_game_ids: list[str] = []
    relay_game_ids: list[str] = []
    skipped_schedule_only: list[str] = []
    skipped_incomplete_detail: list[str] = []
    skipped_empty_relay: list[str] = []
    skipped_cancelled: list[str] = []

    for game_id in target_game_ids:
        game_status, home_score, away_score = game_rows.get(game_id, ("", None, None))
        is_cancelled = game_status in NON_DETAIL_TERMINAL_STATUSES
        is_scheduled = game_status == GAME_STATUS_SCHEDULED
        is_completed = game_status in COMPLETED_LIKE_GAME_STATUSES
        has_score = home_score is not None or away_score is not None
        has_complete_detail = _has_both_team_sides(batting_sides, game_id) and _has_both_team_sides(
            pitching_sides,
            game_id,
        )
        has_any_detail_or_relay = (
            any(game_id in ids for ids in (inning_ids, lineup_ids, summary_ids, event_ids, pbp_ids))
            or bool(batting_sides.get(game_id))
            or bool(pitching_sides.get(game_id))
        )
        has_relay = game_id in event_ids or game_id in pbp_ids

        if is_cancelled:
            skipped_cancelled.append(game_id)

        if is_scheduled and not has_score and not has_any_detail_or_relay:
            skipped_schedule_only.append(game_id)
        else:
            parent_game_ids.append(game_id)

        if has_complete_detail:
            detail_game_ids.append(game_id)
        elif is_completed:
            skipped_incomplete_detail.append(game_id)

        if has_relay:
            relay_game_ids.append(game_id)
        elif is_completed:
            skipped_empty_relay.append(game_id)

    return GameSyncEligibility(
        parent_game_ids=sorted(set(parent_game_ids)),
        detail_game_ids=sorted(set(detail_game_ids)),
        relay_game_ids=sorted(set(relay_game_ids)),
        skipped_schedule_only=sorted(set(skipped_schedule_only)),
        skipped_incomplete_detail=sorted(set(skipped_incomplete_detail)),
        skipped_empty_relay=sorted(set(skipped_empty_relay)),
        skipped_cancelled=sorted(set(skipped_cancelled)),
    )


def filter_publishable_game_ids(session: Session, game_ids: list[str]) -> list[str]:
    """Restrict parent-game sync to rows that are more than schedule-only stubs."""
    return build_game_sync_eligibility(session, game_ids).parent_game_ids


def _log_sync_eligibility(eligibility: GameSyncEligibility) -> None:
    samples = {
        "skipped_schedule_only": eligibility.skipped_schedule_only,
        "skipped_incomplete_detail": eligibility.skipped_incomplete_detail,
        "skipped_empty_relay": eligibility.skipped_empty_relay,
        "skipped_cancelled": eligibility.skipped_cancelled,
    }
    for reason, game_ids in samples.items():
        if not game_ids:
            continue
        logger.warning(
            "⚠️ %s=%s sample=%s%s",
            reason,
            len(game_ids),
            ", ".join(game_ids[:SYNC_LOG_SAMPLE_LIMIT]),
            " ..." if len(game_ids) > SYNC_LOG_SAMPLE_LIMIT else "",
        )


class OCISyncBase:
    """Sync data from SQLite to OCI."""

    def __init__(self, oci_url: str, sqlite_session: Session) -> None:
        """Initialize OCI sync.

        Args:
            oci_url: PostgreSQL connection string for OCI
            sqlite_session: Active SQLite session to read from

        """
        if not oci_url:
            from sqlalchemy.exc import ArgumentError

            msg = "Could not parse rfc1738 URL from None"
            raise ArgumentError(msg)

        self.sqlite_session = sqlite_session
        self.concurrency = 4

        # Create OCI engine
        # pool_recycle: force connection refresh every 5 min to beat cloud DB idle-timeout
        if oci_url.startswith("oracle"):
            tns_admin = os.getenv("TNS_ADMIN")
            connect_args: dict[str, Any] = {}
            if tns_admin:
                connect_args["config_dir"] = tns_admin
                connect_args["wallet_location"] = tns_admin
                try:
                    auth_part = oci_url.split("oracle+oracledb://")[1].rsplit("@", 1)[0]
                    if ":" in auth_part:
                        _, password = auth_part.split(":", 1)
                        from urllib.parse import unquote

                        connect_args["wallet_password"] = unquote(password)
                except (IndexError, ValueError):
                    logger.debug("Could not parse Oracle wallet credentials from URL")
            normalized_url = normalize_oracle_url(oci_url)
            self.oci_engine = create_engine(
                normalized_url,
                echo=False,
                pool_pre_ping=True,
                pool_recycle=240,
                pool_timeout=30,
                connect_args=connect_args,
            )
            if not hasattr(self.oci_engine.dialect, "_json_deserializer"):
                self.oci_engine.dialect._json_deserializer = None  # noqa: SLF001
        else:
            self.oci_engine = create_engine(
                oci_url,
                echo=False,
                pool_pre_ping=True,
                pool_recycle=240,  # recycle connections every 4 minutes
                pool_timeout=30,
                connect_args={
                    "connect_timeout": 60,
                    "application_name": "KBO_Crawler_Sync",
                    "keepalives": 1,
                    "keepalives_idle": 60,
                    "keepalives_interval": 10,
                    "keepalives_count": 5,
                    "options": "-c statement_timeout=180000",
                    "tcp_user_timeout": 60000,
                },
            )

        # Create OCI session
        target_session_factory = sessionmaker(bind=self.oci_engine)
        self.target_session = target_session_factory()

        # Performance: caches for mapping queries called multiple times per sync
        self._season_map_cache: dict[tuple[Any, ...], int] | None = None
        self._franchise_id_mapping_cache: dict[int, int] | None = None
        self._oracle_columns_cache: dict[str, set[str]] = {}
        self._temp_table_counter = count(1)

    @staticmethod
    def _chunked(items: list[str], size: int) -> list[list[str]]:
        return [items[idx : idx + size] for idx in range(0, len(items), size)]

    def _target_table_exists(self, model: type[Base]) -> bool:
        if not getattr(self, "oci_engine", None):
            return True
        try:
            return inspect(self.oci_engine).has_table(model.__tablename__)
        except SQLAlchemyError:
            return True

    @staticmethod
    def _validate_identifier(identifier: str) -> str:
        if not identifier:
            msg = "identifier must not be empty"
            raise ValueError(msg)
        if not isinstance(identifier, str) or not SQL_IDENTIFIER_PATTERN.fullmatch(identifier):
            msg = f"unsafe SQL identifier: {identifier!r}"
            raise ValueError(msg)
        return identifier

    @staticmethod
    def _validate_oracle_identifiers(table_name: str, identifiers: tuple[str, ...]) -> None:
        OCISyncBase._validate_identifier(table_name)
        for identifier in identifiers:
            OCISyncBase._validate_identifier(identifier)

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        return f'"{OCISyncBase._validate_identifier(identifier)}"'

    def _reset_target_sequence_for_table(self, table_name: str, column_name: str = "id") -> bool:
        """Align a PostgreSQL serial/identity sequence with MAX(id).

        Some sync paths use ORM inserts that rely on the target-side sequence
        after prior COPY/upsert or manual repair jobs have changed row ids. If
        that sequence lags behind the table maximum, the next insert can reuse
        an existing primary key and fail mid-sync.
        """
        bind = self.target_session.get_bind()
        if not bind or bind.dialect.name != "postgresql":
            return False

        quoted_table = self._quote_identifier(table_name)
        quoted_column = self._quote_identifier(column_name)
        sequence_name = self.target_session.execute(
            text("SELECT pg_get_serial_sequence(:table_name, :column_name)"),
            {"table_name": table_name, "column_name": column_name},
        ).scalar()
        if not sequence_name:
            return False

        self.target_session.execute(
            text(
                f"""
                SELECT setval(
                    to_regclass(:sequence_name),
                    GREATEST(COALESCE(MAX({quoted_column}), 0), 1),
                    COALESCE(MAX({quoted_column}), 0) > 0
                )
                FROM {quoted_table}
                """,  # noqa: S608
            ),
            {"sequence_name": sequence_name},
        )
        self.target_session.commit()
        return True

    def test_connection(self) -> bool:
        """Test OCI connection."""
        try:
            self.target_session.execute(text("SELECT 1"))
        except DBAPI_EXCEPTIONS as e:
            logger.exception("❌ OCI connection failed")
            self._rollback_target_session(label="test_connection")
            if self._is_transient_oci_error(e):
                try:
                    self._reconnect_oci()
                except DBAPI_EXCEPTIONS as reconnect_exc:
                    logger.warning("OCI reconnect after connection test failed: %s", reconnect_exc)
            return False
        else:
            logger.info("✅ OCI connection successful")
            return True

    def _get_season_map(self) -> dict[tuple[Any, ...], int]:
        """Fetch and cache OCI season mapping (year, league_type_code) -> season_id."""
        cache = getattr(self, "_season_map_cache", None)
        if cache is not None:
            return cast("dict[tuple[Any, ...], int]", cache)

        queries = [
            "SELECT season_id, season_year, league_type_code FROM kbo_seasons",
            "SELECT season_id, season_year, league_type_code FROM kbo_seasons_meta",
            "SELECT season_id, season_year, league_type_code FROM seasons",
        ]

        for q in queries:
            try:
                rows = self.target_session.execute(text(q)).all()
                self._season_map_cache = {(row.season_year, row.league_type_code): row.season_id for row in rows}
            except SQLAlchemyError:
                logger.warning("Season map query failed, trying next fallback")
                continue
            else:
                return self._season_map_cache
                logger.warning("Season map query failed, trying next fallback")
                continue

        logger.warning("⚠️ Warning: Could not fetch season map from OCI")
        self._season_map_cache = {}
        return self._season_map_cache

    def _get_franchise_id_mapping(self) -> dict[int, int]:
        """Get and cache SQLite franchise_id → OCI franchise_id mapping (single batch query)."""
        cache = getattr(self, "_franchise_id_mapping_cache", None)
        if cache is not None:
            return cast("dict[int, int]", cache)

        from src.models.franchise import Franchise

        sqlite_franchises = self.sqlite_session.query(Franchise).all()
        original_codes = [sf.original_code for sf in sqlite_franchises if sf.original_code]

        if not original_codes:
            self._franchise_id_mapping_cache = {}
            return self._franchise_id_mapping_cache

        oci_rows = self.target_session.query(Franchise).filter(Franchise.original_code.in_(original_codes)).all()
        oci_by_code = {oci.original_code: oci.id for oci in oci_rows}

        self._franchise_id_mapping_cache = {
            sf.id: oci_by_code[sf.original_code] for sf in sqlite_franchises if sf.original_code in oci_by_code
        }
        return self._franchise_id_mapping_cache

    def _bulk_copy_upsert(
        self,
        table_name: str,
        options: BulkCopyUpsertOptions,
    ) -> None:
        if not options.records:
            return

        dialect_name = self.oci_engine.dialect.name if self.oci_engine is not None else "postgresql"
        if dialect_name == "oracle":
            serialized_records = _serialize_records_oracle(options.records)
            self._execute_with_retry(
                lambda active_connection: self._do_bulk_merge_oracle(
                    table_name,
                    serialized_records,
                    options.unique_cols,
                    update_timestamp=options.update_timestamp,
                    connection=active_connection,
                ),
                table_name=table_name,
                connection=options.connection,
                reconnect_on_fail=options.reconnect_on_fail,
            )
            return

        serialized_records = _serialize_records(options.records)
        self._execute_with_retry(
            lambda active_connection: self._do_bulk_copy_upsert(
                table_name,
                serialized_records,
                options.unique_cols,
                update_timestamp=options.update_timestamp,
                connection=active_connection,
            ),
            table_name=table_name,
            connection=options.connection,
            reconnect_on_fail=options.reconnect_on_fail,
        )

    def _close_raw_connection(self, connection: RawConnection | None, *, label: str) -> None:
        if connection is None:
            return
        try:
            connection.close()
        except DBAPI_EXCEPTIONS as exc:
            logger.warning("Failed to close OCI connection label=%s: %s", label, exc)

    def _retry_copy_connection(
        self,
        connection: RawConnection | None,
        *,
        table_name: str,
        reconnect_on_fail: bool,
    ) -> RawConnection | None:
        if reconnect_on_fail:
            self._reconnect_oci()
            return None
        self._close_raw_connection(connection, label=f"{table_name}.retry.thread")
        return self._raw_oci_connection_with_retries(label=f"{table_name}.retry.thread")

    def _execute_with_retry(
        self,
        operation: Callable[[RawConnection | None], None],
        *,
        table_name: str,
        connection: RawConnection | None,
        reconnect_on_fail: bool,
    ) -> None:
        max_attempts = 3
        last_exception: Exception | None = None

        for attempt in range(1, max_attempts + 1):
            try:
                operation(connection)
            except DBAPI_EXCEPTIONS as exc:
                last_exception = exc
                if attempt == max_attempts:
                    break
                wait = 3 ** (attempt - 1)
                logger.warning(
                    "   [RETRY] %s: %s (attempt %d/%d, retry in %ds)",
                    table_name,
                    exc,
                    attempt,
                    max_attempts,
                    wait,
                )
                time.sleep(wait)
                connection = self._retry_copy_connection(
                    connection,
                    table_name=table_name,
                    reconnect_on_fail=reconnect_on_fail,
                )
            else:
                return

        logger.error("❌ Batch COPY Error on %s after %s attempts: %s", table_name, max_attempts, last_exception)
        if last_exception is not None:
            raise last_exception
        msg = "COPY retry loop exited without an exception"
        raise RuntimeError(msg)

    def _reconnect_oci(self) -> None:
        """Close and recreate the OCI connection pool."""
        try:
            self.target_session.close()
            self.oci_engine.dispose()
        except DBAPI_EXCEPTIONS as e:
            logger.warning("Ignore exception during OCI reconnection cleanup: %s", e)
        target_session_factory = sessionmaker(bind=self.oci_engine)
        self.target_session = target_session_factory()

    @staticmethod
    def _is_transient_oci_error(exc: Exception) -> bool:
        """Return True for DB errors that are reasonable to retry after reconnecting."""
        if isinstance(exc, OperationalError):
            return True
        if isinstance(exc, DBAPIError) and getattr(exc, "connection_invalidated", False):
            return True

        message = str(exc).lower()
        transient_markers = (
            "could not receive data from server",
            "server closed the connection",
            "closed the connection",
            "connection closed",
            "connection not open",
            "connection already closed",
            "operation timed out",
            "timeout expired",
            "ssl syscall",
            "terminating connection",
            "connection reset",
            "dpy-4011",
        )
        return any(marker in message for marker in transient_markers)

    def _raw_oci_connection_with_retries(
        self,
        *,
        label: str,
        max_retries: int = 2,
        base_delay_seconds: float = 1.0,
    ) -> Any:  # noqa: ANN401
        """Open a raw OCI connection with bounded retry for transient network loss."""
        max_attempts = max_retries + 1

        for attempt in range(1, max_attempts + 1):
            try:
                conn = self.oci_engine.raw_connection()
                try:
                    is_oracle = False
                    if self.oci_engine is not None:
                        dialect = getattr(self.oci_engine, "dialect", None)
                        if (
                            dialect is not None
                            and hasattr(dialect, "name")
                            and getattr(dialect, "name", None) == "oracle"
                        ):
                            is_oracle = True
                    if not is_oracle:
                        cursor = conn.cursor()
                        cursor.execute("SET statement_timeout = 600000;")
                        cursor.close()
                except DBAPI_EXCEPTIONS_SQLITE as set_timeout_exc:
                    logger.warning("Failed to set statement_timeout on OCI connection: %s", set_timeout_exc)
            except DBAPI_EXCEPTIONS as exc:
                transient = self._is_transient_oci_error(exc)

                if not transient or attempt >= max_attempts:
                    logger.exception(
                        "OCI raw connection failed label=%s attempt=%d/%d transient=%s",
                        label,
                        attempt,
                        max_attempts,
                        transient,
                    )
                    raise

                wait_seconds = base_delay_seconds * (2 ** (attempt - 1))
                logger.warning(
                    "OCI raw connection transient failure label=%s attempt=%d/%d retry_in=%.1fs error=%s",
                    label,
                    attempt,
                    max_attempts,
                    wait_seconds,
                    exc,
                )
                self._reconnect_oci()
                time.sleep(wait_seconds)
            else:
                return conn
        msg = "Unreachable: connection loop exited without return"
        raise RuntimeError(msg)

    def _rollback_target_session(self, *, label: str) -> None:
        try:
            self.target_session.rollback()
        except DBAPI_EXCEPTIONS as rollback_exc:
            logger.warning("OCI rollback failed label=%s error=%s", label, rollback_exc)

    def _run_target_session_with_retries(
        self,
        operation: Callable[[], Any],
        *,
        label: str,
        max_retries: int = 2,
        base_delay_seconds: float = 1.0,
    ) -> Any:  # noqa: ANN401
        """Run an OCI session operation with bounded retry for transient connection loss."""
        max_attempts = max_retries + 1

        for attempt in range(1, max_attempts + 1):
            try:
                return operation()
            except DBAPI_EXCEPTIONS as exc:
                transient = self._is_transient_oci_error(exc)
                self._rollback_target_session(label=label)

                if not transient or attempt >= max_attempts:
                    logger.exception(
                        "OCI session operation failed label=%s attempt=%d/%d transient=%s",
                        label,
                        attempt,
                        max_attempts,
                        transient,
                    )
                    raise

                wait_seconds = base_delay_seconds * (2 ** (attempt - 1))
                logger.warning(
                    "OCI transient failure label=%s attempt=%d/%d retry_in=%.1fs error=%s",
                    label,
                    attempt,
                    max_attempts,
                    wait_seconds,
                    exc,
                )
                self._reconnect_oci()
                time.sleep(wait_seconds)
        msg = "Unreachable: reconnect loop exited"
        raise RuntimeError(msg)

    def _resolve_sync_columns(self, model: type[Base], exclude_cols: list[str]) -> list[str]:
        target_column_defs = {}
        target_columns = {c.key for c in model.__table__.columns}
        if getattr(self, "oci_engine", None) is not None:
            try:
                cols = inspect(self.oci_engine).get_columns(model.__tablename__)
                target_column_defs = {c["name"]: c for c in cols}
                target_columns = set(target_column_defs)
            except SQLAlchemyError as exc:
                logger.warning("Failed to inspect OCI columns for %s: %s", model.__tablename__, exc)

        sqlite_bind = None
        if hasattr(self.sqlite_session, "get_bind"):
            try:
                sqlite_bind = self.sqlite_session.get_bind()
            except SQLAlchemyError:
                logger.warning("Failed to get SQLite bind for %s", model.__tablename__)

        if sqlite_bind is not None:
            try:
                local_columns = {c["name"] for c in inspect(sqlite_bind).get_columns(model.__tablename__)}
            except SQLAlchemyError:
                local_columns = set()
        else:
            local_columns = {c.key for c in model.__table__.columns}

        columns = [
            c.key
            for c in model.__table__.columns
            if c.key not in exclude_cols and c.key in target_columns and c.key in local_columns
        ]
        if (
            model.__tablename__ == "game_metadata"
            and "source_payload" in columns
            and "source_payload" in target_column_defs
        ):
            source_payload_type = target_column_defs["source_payload"].get("type")
            source_payload_length = getattr(source_payload_type, "length", None)
            if source_payload_length and source_payload_length <= LEGACY_OCI_VARCHAR_MAX_LENGTH:
                columns.remove("source_payload")
                logger.info("[info] Skipping game_metadata.source_payload for legacy OCI varchar column")
        return columns

    @staticmethod
    def _normalize_simple_table_exclude_cols(
        exclude_cols: list[str] | None,
        dialect_name: str,
    ) -> list[str]:
        if dialect_name == "oracle":
            if exclude_cols is None:
                return []
            if "id" in exclude_cols:
                exclude_cols.remove("id")
            return exclude_cols
        if exclude_cols is None:
            return ["id"]
        if "id" not in exclude_cols:
            exclude_cols.append("id")
        return exclude_cols

    def sync_simple_table(
        self,
        model: type[Base],
        options: SimpleTableSyncOptions,
    ) -> int:
        """Sync simple table using Batched UPSERT or COPY.

        Generic sync parameter for simple tables.
        """
        exclude_cols = list(options.exclude_cols) if options.exclude_cols is not None else None
        oci_engine = getattr(self, "oci_engine", None)
        dialect_name = oci_engine.dialect.name if oci_engine is not None else "postgresql"
        exclude_cols = self._normalize_simple_table_exclude_cols(exclude_cols, dialect_name)

        self._ensure_table(model)

        if not self._target_table_exists(model):
            logger.info("[info] Skipping missing OCI table: %s", model.__tablename__)
            return 0

        columns = self._resolve_sync_columns(model, exclude_cols)
        if not columns:
            logger.info("[info] No compatible columns for %s", model.__tablename__)
            return 0

        query = self.sqlite_session.query(*[getattr(model, column) for column in columns])
        if options.filters:
            query = query.filter(*options.filters)

        total_count = query.count()
        if total_count == 0:
            logger.info("[info] No records for %s", model.__tablename__)
            return 0

        logger.info("🚚 Syncing %s (%s rows, batch=%s)...", model.__tablename__, total_count, options.batch_size)
        update_timestamp = options.update_timestamp
        if update_timestamp is None:
            update_timestamp = "updated_at" not in exclude_cols

        return self._sync_in_batches(
            SyncBatchConfig(
                model=model,
                query=query,
                total_count=total_count,
                columns=columns,
                conflict_keys=options.conflict_keys,
                transform_fn=options.transform_fn,
                batch_size=options.batch_size,
                update_timestamp=update_timestamp,
                dedupe_keys=options.dedupe_keys,
            ),
        )

    def _sync_concurrency(self) -> int:
        concurrency = getattr(self, "concurrency", 4)
        try:
            bind_url = str(self.sqlite_session.get_bind().url)  # type: ignore[union-attr]
        except (ValueError, TypeError, AttributeError, SQLAlchemyError):
            return concurrency
        return 1 if ":memory:" in bind_url else concurrency

    def _load_batch_records(
        self,
        config: SyncBatchConfig,
        offset: int,
        *,
        query: Query[Any] | None = None,
    ) -> list[dict[str, Any]]:
        source_query = config.query if query is None else query
        rows = source_query.offset(offset).limit(config.batch_size).all()
        records = [_row_to_record(row, config.columns, config.transform_fn) for row in rows]
        filtered_records = [record for record in records if record is not None]
        return _dedupe_records_for_conflict_keys(filtered_records, config.dedupe_keys or config.conflict_keys)

    def _sync_records_row_by_row(
        self,
        config: SyncBatchConfig,
        records: list[dict[str, Any]],
        connection: RawConnection,
    ) -> int:
        synced = 0
        for record in records:
            try:
                self._direct_insert_upsert(
                    config.model.__tablename__,
                    record,
                    config.conflict_keys,
                    update_timestamp=config.update_timestamp,
                    connection=connection,
                )
                synced += 1
            except DBAPI_EXCEPTIONS_VAL as exc:
                if self._is_transient_oci_error(exc):
                    raise
                logger.warning("Skipping bad row in %s: %s", config.model.__tablename__, exc)
        return synced

    def _sync_records_with_fallback(
        self,
        config: SyncBatchConfig,
        options: CopyFallbackOptions,
    ) -> tuple[int, str]:
        try:
            self._bulk_copy_upsert(
                config.model.__tablename__,
                BulkCopyUpsertOptions(
                    records=options.records,
                    unique_cols=config.conflict_keys,
                    update_timestamp=config.update_timestamp,
                    connection=options.connection,
                    reconnect_on_fail=options.reconnect_on_fail,
                ),
            )
        except DBAPI_EXCEPTIONS_VAL as exc:
            logger.warning(
                "Batch COPY failed for %s%s, falling back to row-by-row: %s",
                config.model.__tablename__,
                options.failure_context,
                exc,
            )
            self._close_raw_connection(options.connection, label=options.copy_label)
            fallback_connection = self._raw_oci_connection_with_retries(label=options.fallback_label)
            try:
                return self._sync_records_row_by_row(config, options.records, fallback_connection), "row-by-row"
            finally:
                self._close_raw_connection(fallback_connection, label=options.fallback_label)
        else:
            return len(options.records), "COPY"
        finally:
            self._close_raw_connection(options.connection, label=options.copy_label)

    def _sync_sequential(self, config: SyncBatchConfig) -> int:
        synced = 0
        for offset in range(0, config.total_count, config.batch_size):
            records = self._load_batch_records(config, offset)
            table_name = config.model.__tablename__
            connection = (
                self._raw_oci_connection_with_retries(label=f"{table_name}.sync")
                if self.oci_engine is not None
                else None
            )
            synced_batch, method = self._sync_records_with_fallback(
                config,
                CopyFallbackOptions(
                    records=records,
                    connection=connection,
                    copy_label=f"{table_name}.sync",
                    fallback_label=f"{table_name}.sync.fallback",
                    failure_context="",
                    reconnect_on_fail=True,
                ),
            )
            synced += synced_batch
            logger.info("   Synced %s/%s rows via %s...", synced, config.total_count, method)
        return synced

    def _sync_concurrent_worker(self, config: SyncBatchConfig, offset: int) -> int:
        session_factory = sessionmaker(bind=self.sqlite_session.get_bind())
        thread_session = session_factory()
        try:
            thread_query = config.query.with_session(thread_session)
            records = self._load_batch_records(config, offset, query=thread_query)
            if not records:
                return 0
            table_name = config.model.__tablename__
            connection = self._raw_oci_connection_with_retries(label=f"{table_name}.sync.thread")
            synced, _ = self._sync_records_with_fallback(
                config,
                CopyFallbackOptions(
                    records=records,
                    connection=connection,
                    copy_label=f"{table_name}.sync.thread",
                    fallback_label=f"{table_name}.sync.fallback.thread",
                    failure_context=f" at offset {offset}",
                    reconnect_on_fail=False,
                ),
            )
            return synced
        finally:
            thread_session.close()

    def _sync_concurrent(self, config: SyncBatchConfig, *, max_workers: int) -> int:
        offsets = list(range(0, config.total_count, config.batch_size))
        logger.info("   Running concurrent sync with %d workers across %d chunks...", max_workers, len(offsets))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(self._sync_concurrent_worker, [config] * len(offsets), offsets))
        synced = sum(results)
        logger.info("   Synced %s/%s rows via concurrent COPY...", synced, config.total_count)
        return synced

    def _sync_in_batches(self, config: SyncBatchConfig) -> int:
        concurrency = self._sync_concurrency()
        if config.total_count <= config.batch_size or concurrency <= 1:
            return self._sync_sequential(config)
        return self._sync_concurrent(config, max_workers=concurrency)

    def _do_bulk_copy_upsert(
        self,
        table_name: str,
        records: list[dict[str, Any]],
        unique_cols: list[str],
        *,
        update_timestamp: bool,
        connection: Any | None = None,  # noqa: ANN401
    ) -> None:
        close_connection = connection is None
        if connection is None:
            connection = self.oci_engine.raw_connection()
        cursor = connection.cursor()

        try:
            keys = list(records[0].keys())
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=keys, delimiter="\t", quoting=csv.QUOTE_MINIMAL)
            writer.writerows(records)
            output.seek(0)

            counter = getattr(self, "_temp_table_counter", None) or count(1)
            seq = next(counter)
            temp_table = f"temp_{table_name}_{seq}"
            cursor.execute(f"CREATE TEMP TABLE {temp_table} (LIKE {table_name} INCLUDING DEFAULTS)")

            columns_str = ", ".join([f'"{k}"' for k in keys])
            cursor.copy_expert(
                f"COPY {temp_table} ({columns_str}) FROM STDIN WITH (FORMAT CSV, DELIMITER '\t', NULL '\\N')",
                output,
            )

            update_cols = [k for k in keys if k not in unique_cols and k not in ("created_at", "id")]

            if not unique_cols:
                conflict_action = ""
            elif not update_cols:
                conflict_action = "DO NOTHING"
            else:
                set_clause = ", ".join([f'"{k}" = EXCLUDED."{k}"' for k in update_cols])
                if update_timestamp and "updated_at" not in keys:
                    set_clause += ', "updated_at" = CURRENT_TIMESTAMP'
                conflict_target = ", ".join([f'"{k}"' for k in unique_cols])
                conflict_action = f"ON CONFLICT ({conflict_target}) DO UPDATE SET {set_clause}"
            cols_list = ", ".join([f'"{k}"' for k in keys])
            insert_sql = f"""
                INSERT INTO {table_name} ({cols_list})
                SELECT {cols_list} FROM {temp_table}
                {conflict_action}
            """  # noqa: S608
            cursor.execute(insert_sql)

            cursor.execute(f"DROP TABLE {temp_table}")
            connection.commit()

        except DBAPI_EXCEPTIONS:
            logger.exception("Bulk COPY-INSERT failed for %s", table_name)
            connection.rollback()
            raise
        finally:
            cursor.close()
            if close_connection:
                connection.close()

    def _direct_insert_upsert(
        self,
        table_name: str,
        record: dict[str, Any],
        conflict_keys: list[str],
        *,
        update_timestamp: bool,
        connection: Any,  # noqa: ANN401
    ) -> None:
        """Perform a single direct SQL INSERT with ON CONFLICT UPDATE/DO NOTHING."""
        bind = self.target_session.get_bind()
        if bind.dialect.name == "oracle":
            self._direct_insert_upsert_oracle(
                table_name,
                record,
                conflict_keys,
                update_timestamp=update_timestamp,
                connection=connection,
            )
            return

        cursor = connection.cursor()
        try:
            keys = list(record.keys())
            placeholders = ", ".join([f"%({k})s" for k in keys])
            cols_str = ", ".join([f'"{k}"' for k in keys])

            update_cols = [k for k in keys if k not in conflict_keys and k not in ("created_at", "id")]

            if not conflict_keys:
                conflict_action = ""
            elif not update_cols:
                conflict_action = "ON CONFLICT (" + ", ".join([f'"{k}"' for k in conflict_keys]) + ") DO NOTHING"
            else:
                set_clause = ", ".join([f'"{k}" = EXCLUDED."{k}"' for k in update_cols])
                if update_timestamp and "updated_at" not in keys:
                    set_clause += ', "updated_at" = CURRENT_TIMESTAMP'
                conflict_target = ", ".join([f'"{k}"' for k in conflict_keys])
                conflict_action = f"ON CONFLICT ({conflict_target}) DO UPDATE SET {set_clause}"

            sql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders}) {conflict_action}"  # noqa: S608
            cursor.execute(sql, record)
            connection.commit()
        except DBAPI_EXCEPTIONS:
            connection.rollback()
            raise
        finally:
            cursor.close()

    def _direct_insert_upsert_oracle(
        self,
        table_name: str,
        record: dict[str, Any],
        conflict_keys: list[str],
        *,
        update_timestamp: bool,
        connection: Any,  # noqa: ANN401
    ) -> None:
        """Perform a single direct SQL MERGE for Oracle Database."""
        self._validate_oracle_identifiers(table_name, (*record.keys(), *conflict_keys))

        cursor = connection.cursor()
        try:
            # Inspect target columns to detect created_at / updated_at existence
            target_cols = self._oracle_columns(table_name)

            # Serialize json values in the record
            serialized_record = {
                key: json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else value
                for key, value in record.items()
            }

            keys = list(serialized_record.keys())

            insert_cols = [self._quote_identifier(k.upper()) for k in keys]
            insert_vals = [f":{k}" for k in keys]

            if "created_at" in target_cols and "created_at" not in keys:
                insert_cols.append('"CREATED_AT"')
                insert_vals.append("CURRENT_TIMESTAMP")
            if "updated_at" in target_cols and "updated_at" not in keys:
                insert_cols.append('"UPDATED_AT"')
                insert_vals.append("CURRENT_TIMESTAMP")

            cols_str = ", ".join(insert_cols)
            vals_str = ", ".join(insert_vals)

            if not conflict_keys:
                sql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({vals_str})"  # noqa: S608
            else:
                on_clause = " AND ".join([f"t.{self._quote_identifier(k.upper())} = :{k}" for k in conflict_keys])
                update_cols = [k for k in keys if k not in conflict_keys and k not in ("created_at", "id")]

                update_clause = ""
                if update_cols:
                    set_parts = [f"t.{self._quote_identifier(k.upper())} = :{k}" for k in update_cols]
                    if update_timestamp and "updated_at" in target_cols and "updated_at" not in keys:
                        set_parts.append('t."UPDATED_AT" = CURRENT_TIMESTAMP')
                    update_clause = "WHEN MATCHED THEN UPDATE SET " + ", ".join(set_parts)

                insert_clause = f"WHEN NOT MATCHED THEN INSERT ({cols_str}) VALUES ({vals_str})"  # noqa: S608

                sql = f"""
                    MERGE INTO {table_name} t
                    USING DUAL s
                    ON ({on_clause})
                    {update_clause}
                    {insert_clause}
                """

            cursor.execute(sql, serialized_record)
            connection.commit()
        except DBAPI_EXCEPTIONS:
            connection.rollback()
            raise
        finally:
            cursor.close()

    def _do_bulk_merge_oracle(
        self,
        table_name: str,
        records: list[dict[str, Any]],
        unique_cols: list[str],
        *,
        update_timestamp: bool,
        connection: Any | None = None,  # noqa: ANN401
    ) -> None:
        """Perform bulk merge using Oracle SQL MERGE INTO DUAL statement."""
        serialized_records = _serialize_records_oracle(records)
        keys = list(serialized_records[0].keys())
        self._validate_oracle_identifiers(table_name, (*keys, *unique_cols))

        close_connection = connection is None
        if connection is None:
            connection = self.oci_engine.raw_connection()
        cursor = connection.cursor()

        try:
            target_cols = self._oracle_columns(table_name)

            insert_cols = [self._quote_identifier(k.upper()) for k in keys]
            insert_vals = [f":{k}" for k in keys]

            if "created_at" in target_cols and "created_at" not in keys:
                insert_cols.append('"CREATED_AT"')
                insert_vals.append("CURRENT_TIMESTAMP")
            if "updated_at" in target_cols and "updated_at" not in keys:
                insert_cols.append('"UPDATED_AT"')
                insert_vals.append("CURRENT_TIMESTAMP")

            cols_str = ", ".join(insert_cols)
            vals_str = ", ".join(insert_vals)

            if not unique_cols:
                sql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({vals_str})"  # noqa: S608
            else:
                on_clause = " AND ".join([f"t.{self._quote_identifier(k.upper())} = :{k}" for k in unique_cols])
                update_cols = [k for k in keys if k not in unique_cols and k not in ("created_at", "id")]

                update_clause = ""
                if update_cols:
                    set_parts = [f"t.{self._quote_identifier(k.upper())} = :{k}" for k in update_cols]
                    if update_timestamp and "updated_at" in target_cols and "updated_at" not in keys:
                        set_parts.append('t."UPDATED_AT" = CURRENT_TIMESTAMP')
                    update_clause = "WHEN MATCHED THEN UPDATE SET " + ", ".join(set_parts)

                insert_clause = f"WHEN NOT MATCHED THEN INSERT ({cols_str}) VALUES ({vals_str})"  # noqa: S608

                sql = f"""
                    MERGE INTO {table_name} t
                    USING DUAL s
                    ON ({on_clause})
                    {update_clause}
                    {insert_clause}
                """

            cursor.executemany(sql, serialized_records)
            connection.commit()
        except DBAPI_EXCEPTIONS:
            connection.rollback()
            raise
        finally:
            cursor.close()
            if close_connection:
                connection.close()

    def _oracle_columns(self, table_name: str) -> set[str]:
        """Return cached lowercase Oracle column names for a table."""
        columns = self._oracle_columns_cache.get(table_name)
        if columns is None:
            inspector = inspect(self.oci_engine)
            columns = {column["name"].lower() for column in inspector.get_columns(table_name)}
            self._oracle_columns_cache[table_name] = columns
        return columns

    def _ensure_table(self, model: type[Base]) -> None:
        """Create table on OCI if it doesn't exist."""
        oci_engine = getattr(self, "oci_engine", None)
        if oci_engine is None:
            return
        if not hasattr(model, "__table__"):
            return
        from src.models.base import Base

        max_attempts = 5
        backoff = 2
        for attempt in range(1, max_attempts + 1):
            try:
                Base.metadata.create_all(oci_engine, tables=[model.__table__])  # type: ignore[list-item]
                break
            except Exception as e:
                if self._is_transient_oci_error(e) and attempt < max_attempts:
                    logger.warning(
                        "⚠️ Transient error during _ensure_table for %s: %s. Retrying in %ss (attempt %s/%s)...",
                        model.__tablename__,
                        e,
                        backoff,
                        attempt,
                        max_attempts,
                    )
                    time.sleep(backoff)
                    backoff *= 2
                    oci_engine.dispose()
                else:
                    raise

    def close(self) -> None:
        """Close OCI session."""
        self.target_session.close()
        self.oci_engine.dispose()
