"""
Sync validated data from SQLite to OCI (Oracle Cloud Infrastructure) PostgreSQL
Dual-repository pattern: SQLite (dev/validation) → OCI (production)
"""

import csv
import io
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from itertools import count
from typing import Any, Callable

from sqlalchemy import bindparam, create_engine, inspect, text
from sqlalchemy.exc import DBAPIError, OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

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
    parent_game_ids: list[str] = field(default_factory=list)
    detail_game_ids: list[str] = field(default_factory=list)
    relay_game_ids: list[str] = field(default_factory=list)
    skipped_schedule_only: list[str] = field(default_factory=list)
    skipped_incomplete_detail: list[str] = field(default_factory=list)
    skipped_empty_relay: list[str] = field(default_factory=list)
    skipped_cancelled: list[str] = field(default_factory=list)

    def counts(self) -> dict[str, int]:
        return {
            "skipped_schedule_only": len(self.skipped_schedule_only),
            "skipped_incomplete_detail": len(self.skipped_incomplete_detail),
            "skipped_empty_relay": len(self.skipped_empty_relay),
            "skipped_cancelled": len(self.skipped_cancelled),
        }


def _serialize_scalar(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


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


def _execute_signature_query(session_or_conn, sql: str, *, game_ids: list[str] | None = None):
    stmt = text(sql)
    params = {}
    if game_ids is not None:
        stmt = stmt.bindparams(bindparam("game_ids", expanding=True))
        params["game_ids"] = list(game_ids)
    return session_or_conn.execute(stmt, params)


def load_game_sync_signatures(session_or_conn, *, game_ids: list[str] | None = None) -> dict[str, dict[str, Any]]:
    filter_sql = "WHERE g.game_id IN :game_ids" if game_ids is not None else ""
    game_rows = (
        _execute_signature_query(
            session_or_conn,
            f"""
        SELECT
            g.game_id,
            g.game_status,
            g.home_score,
            g.away_score,
            g.home_pitcher,
            g.away_pitcher,
            g.home_team,
            g.away_team,
            g.updated_at
        FROM game g
        {filter_sql}
        """,
            game_ids=game_ids,
        )
        .mappings()
        .all()
    )

    signatures: dict[str, dict[str, Any]] = {}
    for row in game_rows:
        game_id = str(row["game_id"])
        signatures[game_id] = {
            "game": {
                "game_status": row["game_status"],
                "home_score": row["home_score"],
                "away_score": row["away_score"],
                "home_pitcher": row["home_pitcher"],
                "away_pitcher": row["away_pitcher"],
                "home_team": row["home_team"],
                "away_team": row["away_team"],
                "updated_at": _serialize_scalar(row["updated_at"]),
            }
        }
        for table_name in GAME_SIGNATURE_CHILD_TABLES:
            signatures[game_id][table_name] = {
                "row_count": 0,
                "max_updated_at": None,
            }

    if not signatures:
        return signatures

    child_game_ids = sorted(signatures.keys())
    metadata_rows = (
        _execute_signature_query(
            session_or_conn,
            """
        SELECT
            g.game_id,
            COUNT(g.game_id) AS row_count,
            MAX(g.updated_at) AS max_updated_at,
            MAX(g.start_time) AS start_time
        FROM game_metadata g
        WHERE g.game_id IN :game_ids
        GROUP BY g.game_id
        """,
            game_ids=child_game_ids,
        )
        .mappings()
        .all()
    )
    for row in metadata_rows:
        signatures[str(row["game_id"])]["game_metadata"] = {
            "row_count": int(row["row_count"] or 0),
            "max_updated_at": _serialize_scalar(row["max_updated_at"]),
            "start_time": _serialize_scalar(row["start_time"]),
        }

    for table_name in GAME_SIGNATURE_CHILD_TABLES:
        if table_name == "game_metadata":
            continue
        rows = (
            _execute_signature_query(
                session_or_conn,
                f"""
            SELECT
                t.game_id,
                COUNT(*) AS row_count,
                MAX(t.updated_at) AS max_updated_at
            FROM {table_name} t
            WHERE t.game_id IN :game_ids
            GROUP BY t.game_id
            """,
                game_ids=child_game_ids,
            )
            .mappings()
            .all()
        )
        for row in rows:
            signatures[str(row["game_id"])][table_name] = {
                "row_count": int(row["row_count"] or 0),
                "max_updated_at": _serialize_scalar(row["max_updated_at"]),
            }

    return signatures


def detect_dirty_game_ids(
    local_session_or_conn, remote_session_or_conn, *, game_ids: list[str] | None = None
) -> list[str]:
    local_signatures = load_game_sync_signatures(local_session_or_conn, game_ids=game_ids)
    remote_signatures = load_game_sync_signatures(remote_session_or_conn, game_ids=list(local_signatures.keys()))

    dirty: list[str] = []
    for game_id, local_signature in local_signatures.items():
        remote_signature = remote_signatures.get(game_id)
        if remote_signature is None:
            dirty.append(game_id)
            continue

        local_game = local_signature["game"]
        remote_game = remote_signature.get("game", {})
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
                dirty.append(game_id)
                break
        else:
            if local_game.get("updated_at") is not None and (
                remote_game.get("updated_at") is None
                or str(local_game.get("updated_at")) > str(remote_game.get("updated_at"))
            ):
                dirty.append(game_id)
                continue

        if dirty and dirty[-1] == game_id:
            continue

        metadata_local = local_signature.get("game_metadata", {})
        metadata_remote = remote_signature.get("game_metadata", {})
        if metadata_local.get("row_count") != metadata_remote.get("row_count"):
            dirty.append(game_id)
            continue
        if metadata_local.get("start_time") != metadata_remote.get("start_time"):
            dirty.append(game_id)
            continue
        if metadata_local.get("max_updated_at") is not None and (
            metadata_remote.get("max_updated_at") is None
            or str(metadata_local.get("max_updated_at")) > str(metadata_remote.get("max_updated_at"))
        ):
            dirty.append(game_id)
            continue

        for table_name in GAME_SIGNATURE_CHILD_TABLES:
            if table_name == "game_metadata":
                continue
            local_child = local_signature.get(table_name, {})
            remote_child = remote_signature.get(table_name, {})
            if local_child.get("row_count") != remote_child.get("row_count"):
                dirty.append(game_id)
                break
            if local_child.get("max_updated_at") is not None and (
                remote_child.get("max_updated_at") is None
                or str(local_child.get("max_updated_at")) > str(remote_child.get("max_updated_at"))
            ):
                dirty.append(game_id)
                break

    return dirty


def filter_game_ids_by_year(game_ids: list[str], year: int | None) -> list[str]:
    if year is None:
        return list(game_ids)
    prefix = str(int(year))
    return [game_id for game_id in game_ids if str(game_id).startswith(prefix)]


def _load_team_sides(session, model: type, game_ids: list[str]) -> dict[str, set[str]]:
    if not game_ids:
        return {}
    rows = session.query(model.game_id, model.team_side).filter(model.game_id.in_(game_ids)).distinct().all()
    result: dict[str, set[str]] = {}
    for game_id, team_side in rows:
        if not game_id or not team_side:
            continue
        result.setdefault(str(game_id), set()).add(str(team_side))
    return result


def _load_game_ids_with_rows(session, model: type, game_ids: list[str]) -> set[str]:
    if not game_ids:
        return set()
    return {str(row[0]) for row in session.query(model.game_id).filter(model.game_id.in_(game_ids)).distinct().all()}


def _has_both_team_sides(side_map: dict[str, set[str]], game_id: str) -> bool:
    sides = side_map.get(game_id, set())
    return "away" in sides and "home" in sides


def build_game_sync_eligibility(session, game_ids: list[str]) -> GameSyncEligibility:
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
            pitching_sides, game_id
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


def filter_publishable_game_ids(session, game_ids: list[str]) -> list[str]:
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
            f"⚠️ {reason}={len(game_ids)} sample={', '.join(game_ids[:10])}" + (" ..." if len(game_ids) > 10 else "")
        )


class OCISyncBase:
    """Sync data from SQLite to OCI"""

    def __init__(self, oci_url: str, sqlite_session: Session):
        """
        Initialize OCI sync

        Args:
            oci_url: PostgreSQL connection string for OCI
            sqlite_session: Active SQLite session to read from
        """
        self.sqlite_session = sqlite_session

        # Create OCI engine
        # pool_recycle: force connection refresh every 5 min to beat cloud DB idle-timeout (OCI typically kills
        # idle TCP sockets after ~5-10 min). pool_pre_ping alone is not enough when the engine is long-lived.
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
                "options": "-c statement_timeout=120000",
                "tcp_user_timeout": 60000,
            },
        )

        # Create OCI session
        target_session_factory = sessionmaker(bind=self.oci_engine)
        self.target_session = target_session_factory()

        # Performance: caches for mapping queries called multiple times per sync
        self._season_map_cache: dict[tuple, int] | None = None
        self._franchise_id_mapping_cache: dict[int, int] | None = None
        self._temp_table_counter = count(1)

    @staticmethod
    def _chunked(items: list[str], size: int) -> list[list[str]]:
        return [items[idx : idx + size] for idx in range(0, len(items), size)]

    def _target_table_exists(self, model: type) -> bool:
        if not getattr(self, "oci_engine", None):
            return True
        try:
            return inspect(self.oci_engine).has_table(model.__tablename__)
        except SQLAlchemyError:
            return True

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        if not identifier:
            raise ValueError("identifier must not be empty")
        if not (identifier[0].isalpha() or identifier[0] == "_"):
            raise ValueError(f"unsafe SQL identifier: {identifier!r}")
        if not all(char.isalnum() or char == "_" for char in identifier):
            raise ValueError(f"unsafe SQL identifier: {identifier!r}")
        return f'"{identifier}"'

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
                """
            ),
            {"sequence_name": sequence_name},
        )
        self.target_session.commit()
        return True

    def test_connection(self) -> bool:
        """Test OCI connection"""
        try:
            self.target_session.execute(text("SELECT 1"))
            logger.info("✅ OCI connection successful")
            return True
        except Exception as e:
            logger.exception("❌ OCI connection failed: %s", e)
            return False

    def _get_season_map(self) -> dict[tuple, int]:
        """Fetch and cache OCI season mapping (year, league_type_code) -> season_id."""
        if self._season_map_cache is not None:
            return self._season_map_cache

        queries = [
            "SELECT season_id, season_year, league_type_code FROM kbo_seasons",
            "SELECT season_id, season_year, league_type_code FROM kbo_seasons_meta",
            "SELECT season_id, season_year, league_type_code FROM seasons",
        ]

        for q in queries:
            try:
                rows = self.target_session.execute(text(q)).all()
                self._season_map_cache = {(row.season_year, row.league_type_code): row.season_id for row in rows}
                return self._season_map_cache
            except Exception:
                continue

        logger.warning("⚠️ Warning: Could not fetch season map from OCI")
        self._season_map_cache = {}
        return self._season_map_cache

    def _get_franchise_id_mapping(self) -> dict[int, int]:
        """Get and cache SQLite franchise_id → OCI franchise_id mapping (single batch query)."""
        if self._franchise_id_mapping_cache is not None:
            return self._franchise_id_mapping_cache

        from src.models.franchise import Franchise

        sqlite_franchises = self.sqlite_session.query(Franchise).all()
        original_codes = [sf.original_code for sf in sqlite_franchises if sf.original_code]

        if not original_codes:
            self._franchise_id_mapping_cache = {}
            return self._franchise_id_mapping_cache

        oci_rows = self.target_session.query(Franchise).filter(Franchise.original_code.in_(original_codes)).all()
        oci_by_code = {oci.original_code: oci.id for oci in oci_rows}

        self._franchise_id_mapping_cache = {sf.id: oci_by_code[sf.original_code] for sf in sqlite_franchises if sf.original_code in oci_by_code}
        return self._franchise_id_mapping_cache

    def _bulk_copy_upsert(
        self, table_name: str, records: list[dict[str, Any]], unique_cols: list[str], update_timestamp: bool = True
    ):
        if not records:
            return

        serialized_records = []
        for r in records:
            serialized_r = {}
            for k, v in r.items():
                if v is None:
                    serialized_r[k] = r"\N"
                elif isinstance(v, (dict, list)):
                    serialized_r[k] = json.dumps(v, ensure_ascii=False)
                else:
                    serialized_r[k] = v
            serialized_records.append(serialized_r)
        records = serialized_records

        max_attempts = 3
        last_exception: Exception | None = None

        for attempt in range(1, max_attempts + 1):
            try:
                return self._do_bulk_copy_upsert(table_name, records, unique_cols, update_timestamp)
            except Exception as e:
                last_exception = e
                if attempt < max_attempts:
                    wait = 1 * (3 ** (attempt - 1))
                    logger.warning(
                        "   [RETRY] %s: %s (attempt %d/%d, retry in %ds)",
                        table_name,
                        e,
                        attempt,
                        max_attempts,
                        wait,
                    )
                    time.sleep(wait)
                    self._reconnect_oci()

        logger.error(f"❌ Batch COPY Error on {table_name} after {max_attempts} attempts: {last_exception}")
        raise last_exception  # type: ignore[misc]

    def _reconnect_oci(self) -> None:
        """Close and recreate the OCI connection pool."""
        try:
            self.target_session.close()
            self.oci_engine.dispose()
        except Exception as e:
            logger.info("Ignore exception during OCI reconnection cleanup: %s", e)
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
            "connection not open",
            "connection already closed",
            "operation timed out",
            "ssl syscall",
            "terminating connection",
            "connection reset",
        )
        return any(marker in message for marker in transient_markers)

    def _rollback_target_session(self, *, label: str) -> None:
        try:
            self.target_session.rollback()
        except Exception as rollback_exc:
            logger.warning("OCI rollback failed label=%s error=%s", label, rollback_exc)

    def _run_target_session_with_retries(
        self,
        operation: Callable[[], Any],
        *,
        label: str,
        max_retries: int = 2,
        base_delay_seconds: float = 1.0,
    ) -> Any:
        """Run an OCI session operation with bounded retry for transient connection loss."""
        max_attempts = max_retries + 1

        for attempt in range(1, max_attempts + 1):
            try:
                return operation()
            except Exception as exc:
                transient = self._is_transient_oci_error(exc)
                self._rollback_target_session(label=label)

                if not transient or attempt >= max_attempts:
                    logger.error(
                        "OCI session operation failed label=%s attempt=%d/%d transient=%s error=%s",
                        label,
                        attempt,
                        max_attempts,
                        transient,
                        exc,
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

    def sync_simple_table(
        self,
        model: type,
        conflict_keys: list[str],
        exclude_cols: list[str] = None,
        filters: list = None,
        transform_fn: Callable | None = None,
        batch_size: int = 10000,
        update_timestamp: bool | None = None,
    ) -> int:
        """Generic sync parameter for simple tables using Batched UPSERT or COPY."""
        if exclude_cols is None:
            exclude_cols = ["id"]  # Default to exclude ID for auto-inc compatibility
        elif "id" not in exclude_cols:
            exclude_cols.append("id")

        if not self._target_table_exists(model):
            logger.info(f"ℹ️ Skipping missing OCI table: {model.__tablename__}")
            return 0

        # Use all columns except those explicitly excluded and not present in target DB.
        if getattr(self, "oci_engine", None) is not None:
            target_column_defs = {c["name"]: c for c in inspect(self.oci_engine).get_columns(model.__tablename__)}
            target_columns = set(target_column_defs)
        else:
            target_column_defs = {}
            target_columns = {c.key for c in model.__table__.columns}

        sqlite_bind = None
        if hasattr(self.sqlite_session, "get_bind"):
            try:
                sqlite_bind = self.sqlite_session.get_bind()
            except SQLAlchemyError:
                pass

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
            if source_payload_length and source_payload_length <= 255:
                columns.remove("source_payload")
                logger.info("ℹ️ Skipping game_metadata.source_payload for legacy OCI varchar column")
        if not columns:
            logger.info(f"ℹ️ No compatible columns for {model.__tablename__}")
            return 0

        query = self.sqlite_session.query(*[getattr(model, column) for column in columns])
        if filters:
            query = query.filter(*filters)

        total_count = query.count()
        if total_count == 0:
            logger.info(f"ℹ️  No records for {model.__tablename__}")
            return 0

        logger.info(f"🚚 Syncing {model.__tablename__} ({total_count} rows, batch={batch_size})...")

        # Always use Bulk COPY Upsert to be safe from schema mismatches (e.g. created_at/updated_at missing on OCI).
        synced = 0
        for offset in range(0, total_count, batch_size):
            rows = query.offset(offset).limit(batch_size).all()
            records = []
            for row in rows:
                mapping = getattr(row, "_mapping", None)
                if mapping is not None:
                    data = {c: mapping[c] for c in columns if c in mapping}
                else:
                    data = {c: getattr(row, c) for c in columns if hasattr(row, c)}

                # Ensure created_at/updated_at are never null if the table requires them.
                now = datetime.now()
                if "created_at" in columns and data.get("created_at") is None:
                    data["created_at"] = now
                if "updated_at" in columns and data.get("updated_at") is None:
                    data["updated_at"] = now

                if transform_fn:
                    data = transform_fn(data)

                for k, v in data.items():
                    if isinstance(v, (dict, list)):
                        data[k] = json.dumps(v, ensure_ascii=False)
                records.append(data)

            # Deduplicate records to avoid Postgres "affect row a second time"
            # errors, while preserving SQL's distinct-NULL unique semantics.
            records = _dedupe_records_for_conflict_keys(records, conflict_keys)

            if update_timestamp is None:
                effective_update_timestamp = "updated_at" not in exclude_cols
            else:
                effective_update_timestamp = update_timestamp
            self._bulk_copy_upsert(
                model.__tablename__, records, conflict_keys, update_timestamp=effective_update_timestamp
            )
            synced += len(records)
            logger.info(f"   Synced {synced}/{total_count} rows via COPY...")

        return synced

    def _do_bulk_copy_upsert(
        self,
        table_name: str,
        records: list[dict[str, Any]],
        unique_cols: list[str],
        update_timestamp: bool,
    ):
        connection = self.oci_engine.raw_connection()
        cursor = connection.cursor()

        try:
            keys = list(records[0].keys())
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=keys, delimiter="\t", quoting=csv.QUOTE_MINIMAL)
            writer.writerows(records)
            output.seek(0)

            seq = next(self._temp_table_counter)
            temp_table = f"temp_{table_name}_{seq}"
            cursor.execute(f"CREATE TEMP TABLE {temp_table} (LIKE {table_name} INCLUDING DEFAULTS)")

            columns_str = ", ".join([f'"{k}"' for k in keys])
            cursor.copy_expert(
                f"COPY {temp_table} ({columns_str}) FROM STDIN WITH (FORMAT CSV, DELIMITER '\t', NULL '\\N')", output
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
            """
            cursor.execute(insert_sql)

            cursor.execute(f"DROP TABLE {temp_table}")
            connection.commit()

        except Exception as e:
            connection.rollback()
            raise e
        finally:
            cursor.close()
            connection.close()

    def _ensure_table(self, model: type) -> None:
        """Create table on OCI if it doesn't exist."""
        from src.models.base import Base

        Base.metadata.create_all(self.oci_engine, tables=[model.__table__])

    def close(self):
        """Close OCI session"""
        self.target_session.close()
        self.oci_engine.dispose()
