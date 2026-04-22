from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shutil
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

from dotenv import load_dotenv
from sqlalchemy import MetaData, Table, and_, create_engine, inspect, or_, select, text

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.utils.team_codes import build_kbo_game_id, normalize_kbo_game_id, resolve_team_code, team_code_from_game_id_segment
from src.utils.team_history import iter_team_history


DEFAULT_DB_URL = "sqlite:///./data/kbo_dev.db"
DEFAULT_YEARS = (2024, 2025, 2026)
TIMESTAMP_COLUMNS = {"created_at", "updated_at"}
MERGE_KEEP_TARGET_COLUMNS = {"franchise_id", "canonical_team_code"}
MERGE_SOURCE_COLUMNS = {
    "avg",
    "obp",
    "slg",
    "ops",
    "iso",
    "babip",
    "era",
    "whip",
    "k_per_nine",
    "bb_per_nine",
    "kbb",
    "innings_pitched",
    "innings_outs",
    "uniform_no",
    "extra_stats",
}
TEAM_CODE_TO_FRANCHISE_ID = {entry.team_code.upper(): entry.franchise_id for entry in iter_team_history()}
ACTIONABLE_BACKFILL_CLASSIFICATIONS = {"pending_recrawl", "past_scheduled_missing_detail"}
MERGEABLE_MASTER_STATUSES = {"", "SCHEDULED", "UNRESOLVED_MISSING"}


@dataclass(frozen=True)
class ChildSpec:
    table_name: str
    unique_columns: tuple[str, ...] | None


CHILD_SPECS = (
    ChildSpec("game_metadata", ("game_id",)),
    ChildSpec("game_inning_scores", ("game_id", "team_side", "inning")),
    ChildSpec("game_lineups", ("game_id", "team_side", "appearance_seq")),
    ChildSpec("game_batting_stats", ("game_id", "player_id", "appearance_seq")),
    ChildSpec("game_pitching_stats", ("game_id", "player_id", "appearance_seq")),
    ChildSpec("game_events", ("game_id", "event_seq")),
    ChildSpec("game_summary", ("game_id", "summary_type", "player_name", "detail_text")),
    ChildSpec("game_play_by_play", None),
)


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple)):
        return repr(value)
    return str(value)


def _decode_json_repeated(value: str) -> Any:
    decoded: Any = value
    for _ in range(2):
        if not isinstance(decoded, str):
            break
        stripped = decoded.strip()
        if stripped == "null":
            return None
        if not stripped or stripped[0] not in "[{\"":
            break
        try:
            decoded = json.loads(stripped)
        except json.JSONDecodeError:
            break
    return decoded


def _is_zeroish_raw(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, (dict, list, tuple, set)):
        return len(value) == 0
    if isinstance(value, (int, float)):
        return float(value) == 0.0
    return str(value).strip() in {"", "0", "0.0", "0.00", "0.000", "0.000000", "False", "false", "None", "null"}


def _prune_zeroish_json(value: Any) -> Any:
    if isinstance(value, dict):
        pruned = {
            key: _prune_zeroish_json(item)
            for key, item in value.items()
            if not _is_zeroish_raw(item)
        }
        return {key: item for key, item in pruned.items() if not _is_zeroish_raw(item)}
    if isinstance(value, list):
        pruned = [_prune_zeroish_json(item) for item in value if not _is_zeroish_raw(item)]
        return [item for item in pruned if not _is_zeroish_raw(item)]
    return value


def _normalized_compare_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        decoded = _decode_json_repeated(value.strip())
        if isinstance(decoded, str):
            return decoded.strip()
        value = decoded
        if value is None:
            return ""
    if isinstance(value, (dict, list, tuple)):
        value = _prune_zeroish_json(value)
        if _is_zeroish_raw(value):
            return ""
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    return str(value)


def _write_csv(path: Path, rows: Iterable[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name) for name in fieldnames})


def _sqlite_path_from_url(db_url: str) -> Path | None:
    if not db_url.startswith("sqlite:///"):
        return None
    raw_path = db_url.removeprefix("sqlite:///")
    return Path(raw_path)


def _backup_sqlite_database(db_url: str, output_dir: Path) -> Path | None:
    db_path = _sqlite_path_from_url(db_url)
    if db_path is None or not db_path.exists():
        return None
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = output_dir / f"{db_path.name}.backup_{stamp}"
    output_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(db_path, backup_path)
    return backup_path


def _backup_affected_rows(
    conn,
    tables: dict[str, Table],
    years: Iterable[int],
    groups: list[dict[str, Any]],
    output_dir: Path,
    stamp: str,
) -> Path | None:
    year_game_ids = {str(row["game_id"]) for row in _game_rows(conn, tables, years)}
    duplicate_game_ids = {
        str(game_id)
        for group in groups
        for game_id in group["game_ids"]
    }
    if not year_game_ids and not duplicate_game_ids:
        return None

    backup_dir = output_dir / f"affected_rows_backup_{stamp}"
    backup_dir.mkdir(parents=True, exist_ok=True)
    wrote_file = False

    game = tables.get("game")
    if game is not None and year_game_ids:
        rows = [
            dict(row)
            for row in conn.execute(select(game).where(game.c.game_id.in_(sorted(year_game_ids)))).mappings()
        ]
        if rows:
            _write_csv(backup_dir / "game.csv", rows, [col.name for col in game.columns])
            wrote_file = True

    alias_table = tables.get("game_id_aliases")
    if alias_table is not None and year_game_ids:
        clauses = []
        if "alias_game_id" in alias_table.c:
            clauses.append(alias_table.c.alias_game_id.in_(sorted(year_game_ids)))
        if "canonical_game_id" in alias_table.c:
            clauses.append(alias_table.c.canonical_game_id.in_(sorted(year_game_ids)))
        if clauses:
            rows = [
                dict(row)
                for row in conn.execute(select(alias_table).where(or_(*clauses))).mappings()
            ]
            if rows:
                _write_csv(backup_dir / "game_id_aliases.csv", rows, [col.name for col in alias_table.columns])
                wrote_file = True

    for spec in CHILD_SPECS:
        table = tables.get(spec.table_name)
        if table is None or "game_id" not in table.c or not duplicate_game_ids:
            continue
        rows = [
            dict(row)
            for row in conn.execute(select(table).where(table.c.game_id.in_(sorted(duplicate_game_ids)))).mappings()
        ]
        if rows:
            _write_csv(backup_dir / f"{spec.table_name}.csv", rows, [col.name for col in table.columns])
            wrote_file = True

    return backup_dir if wrote_file else None


def _table_exists(inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _load_tables(conn) -> dict[str, Table]:
    inspector = inspect(conn)
    metadata = MetaData()
    tables: dict[str, Table] = {}
    for table_name in ["game", "kbo_seasons", "game_id_aliases", *[spec.table_name for spec in CHILD_SPECS]]:
        if _table_exists(inspector, table_name):
            tables[table_name] = Table(table_name, metadata, autoload_with=conn)
    return tables


def _ensure_repair_schema(conn) -> None:
    inspector = inspect(conn)
    dialect = conn.dialect.name
    if "is_primary" not in {col["name"] for col in inspector.get_columns("game")}:
        if dialect == "postgresql":
            conn.execute(text("ALTER TABLE game ADD COLUMN is_primary BOOLEAN NOT NULL DEFAULT TRUE"))
        else:
            conn.execute(text("ALTER TABLE game ADD COLUMN is_primary BOOLEAN DEFAULT 1"))
    if not _table_exists(inspector, "game_id_aliases"):
        if dialect == "postgresql":
            conn.execute(
                text(
                    """
                    CREATE TABLE game_id_aliases (
                        alias_game_id VARCHAR(20) PRIMARY KEY,
                        canonical_game_id VARCHAR(20) NOT NULL REFERENCES game(game_id),
                        source VARCHAR(50),
                        reason VARCHAR(120),
                        created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
                        updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now()
                    )
                    """
                )
            )
        else:
            conn.execute(
                text(
                    """
                    CREATE TABLE game_id_aliases (
                        alias_game_id VARCHAR(20) PRIMARY KEY,
                        canonical_game_id VARCHAR(20) NOT NULL,
                        source VARCHAR(50),
                        reason VARCHAR(120),
                        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY(canonical_game_id) REFERENCES game(game_id)
                    )
                    """
                )
            )
        conn.execute(text("CREATE INDEX idx_game_id_aliases_canonical_game_id ON game_id_aliases (canonical_game_id)"))


def _season_map(conn, tables: dict[str, Table]) -> dict[int, tuple[int, int]]:
    seasons = tables.get("kbo_seasons")
    if seasons is None:
        return {}
    mapping = {}
    for row in conn.execute(select(seasons)).mappings():
        season_id = row.get("season_id")
        if season_id is None:
            continue
        mapping[int(season_id)] = (
            int(row.get("season_year") or 0),
            int(row.get("league_type_code") or 0),
        )
    return mapping


def _preferred_season_ids(conn, tables: dict[str, Table]) -> dict[tuple[int, int], int]:
    seasons = tables.get("kbo_seasons")
    if seasons is None:
        return {}
    preferred: dict[tuple[int, int], int] = {}
    for row in conn.execute(select(seasons)).mappings():
        year = row.get("season_year")
        league_type = row.get("league_type_code")
        season_id = row.get("season_id")
        if year is None or league_type is None or season_id is None:
            continue
        key = (int(year), int(league_type))
        preferred[key] = min(int(season_id), preferred.get(key, int(season_id)))
    return preferred


def _game_rows(conn, tables: dict[str, Table], years: Iterable[int]) -> list[dict[str, Any]]:
    game = tables["game"]
    filters = [game.c.game_id.like(f"{year}%") for year in years]
    stmt = select(game).where(or_(*filters))
    return [dict(row) for row in conn.execute(stmt).mappings()]


def _child_counts(conn, tables: dict[str, Table], game_ids: Iterable[str]) -> dict[str, int]:
    ids = list(game_ids)
    counts = {game_id: 0 for game_id in ids}
    if not ids:
        return counts
    for spec in CHILD_SPECS:
        table = tables.get(spec.table_name)
        if table is None or "game_id" not in table.c:
            continue
        rows = conn.execute(
            select(table.c.game_id).where(table.c.game_id.in_(ids))
        ).all()
        for row in rows:
            counts[row[0]] = counts.get(row[0], 0) + 1
    return counts


def _logical_key(row: dict[str, Any], season_by_id: dict[int, tuple[int, int]]) -> tuple[Any, ...] | None:
    game_id = str(row.get("game_id") or "")
    if len(game_id) < 9:
        return None
    derived = _derive_game_franchise_ids(row)
    away_fid = row.get("away_franchise_id") or derived.get("away_franchise_id")
    home_fid = row.get("home_franchise_id") or derived.get("home_franchise_id")
    if away_fid is None or home_fid is None:
        return None
    season_year = int(game_id[:4])
    league_type_code = 0
    season_id = row.get("season_id")
    if season_id is not None and int(season_id) in season_by_id:
        _, league_type_code = season_by_id[int(season_id)]
    dh = game_id[-1] if game_id[-1].isdigit() else "0"
    return (
        season_year,
        league_type_code,
        row.get("game_date"),
        int(away_fid),
        int(home_fid),
        dh,
    )


def _franchise_id_for_code(code: Any, season_year: int | None = None) -> int | None:
    if not code:
        return None
    raw = str(code).strip().upper()
    if not raw:
        return None
    season_code = team_code_from_game_id_segment(raw, season_year) if season_year else raw
    return TEAM_CODE_TO_FRANCHISE_ID.get(str(season_code or raw).upper()) or TEAM_CODE_TO_FRANCHISE_ID.get(raw)


def _team_code_for_franchise(franchise_id: int, season_year: int) -> str | None:
    for entry in iter_team_history():
        if entry.franchise_id != franchise_id:
            continue
        end_season = entry.end_season or season_year
        if entry.start_season <= season_year <= end_season:
            return entry.team_code.upper()
    return None


def _canonical_game_id_for_key(key: tuple[Any, ...]) -> str | None:
    season_year, _league_type_code, game_date, away_fid, home_fid, dh = key
    try:
        year = int(season_year)
        away_franchise_id = int(away_fid)
        home_franchise_id = int(home_fid)
    except (TypeError, ValueError):
        return None

    away_code = _team_code_for_franchise(away_franchise_id, year)
    home_code = _team_code_for_franchise(home_franchise_id, year)
    return build_kbo_game_id(
        str(game_date or ""),
        away_code,
        home_code,
        doubleheader_no=dh,
        season_year=year,
    )


def _derive_game_franchise_ids(row: dict[str, Any]) -> dict[str, int | None]:
    game_id = str(row.get("game_id") or "")
    season_year = int(game_id[:4]) if len(game_id) >= 4 and game_id[:4].isdigit() else None
    away_fid = home_fid = None
    if game_id and season_year:
        normalized = normalize_kbo_game_id(game_id)
        if normalized and len(normalized) >= 13:
            away_fid = _franchise_id_for_code(normalized[8:10], season_year)
            home_fid = _franchise_id_for_code(normalized[10:12], season_year)

    if away_fid is None:
        away_code = resolve_team_code(str(row.get("away_team") or ""), season_year)
        away_fid = _franchise_id_for_code(away_code, season_year)
    if home_fid is None:
        home_code = resolve_team_code(str(row.get("home_team") or ""), season_year)
        home_fid = _franchise_id_for_code(home_code, season_year)

    winning_fid = None
    winning_team = row.get("winning_team")
    if winning_team and season_year:
        winning_code = resolve_team_code(str(winning_team), season_year)
        winning_fid = _franchise_id_for_code(winning_code, season_year)

    return {
        "away_franchise_id": away_fid,
        "home_franchise_id": home_fid,
        "winning_franchise_id": winning_fid,
    }


def collect_duplicate_groups(conn, tables: dict[str, Table], years: Iterable[int]) -> list[dict[str, Any]]:
    season_by_id = _season_map(conn, tables)
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in _game_rows(conn, tables, years):
        key = _logical_key(row, season_by_id)
        if key:
            grouped[key].append(row)

    groups = []
    for key, rows in grouped.items():
        if len(rows) < 2:
            continue
        game_ids = [str(row["game_id"]) for row in rows]
        counts = _child_counts(conn, tables, game_ids)
        primary_id = choose_primary_game_id(
            game_ids,
            counts,
            expected_game_id=_canonical_game_id_for_key(key),
        )
        groups.append(
            {
                "key": key,
                "season_year": key[0],
                "league_type_code": key[1],
                "game_date": key[2],
                "away_franchise_id": key[3],
                "home_franchise_id": key[4],
                "doubleheader_no": key[5],
                "game_ids": game_ids,
                "primary_game_id": primary_id,
                "child_counts": counts,
            }
        )
    return groups


def choose_primary_game_id(
    game_ids: list[str],
    child_counts: dict[str, int],
    expected_game_id: str | None = None,
) -> str:
    if expected_game_id:
        return expected_game_id

    normalized = {game_id: normalize_kbo_game_id(game_id) for game_id in game_ids}
    existing_ids = set(game_ids)
    for game_id, canonical in normalized.items():
        if canonical in existing_ids:
            return canonical
    best_source = max(game_ids, key=lambda gid: (child_counts.get(gid, 0), gid))
    return normalized[best_source]


def _comparison_payload(row: dict[str, Any], table: Table) -> dict[str, str]:
    ignored = {"id", "game_id", *TIMESTAMP_COLUMNS}
    return {
        col.name: _normalized_compare_value(row.get(col.name))
        for col in table.columns
        if col.name not in ignored
    }


def _payload_score(payload: dict[str, str]) -> int:
    score = 0
    for value in payload.values():
        if value in {"", "0", "0.0", "0.000", "False", "false", "{}", "[]"}:
            continue
        if value and value[0] in "[{":
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, dict):
                score += sum(
                    1
                    for item in parsed.values()
                    if str(item) not in {"", "0", "0.0", "0.000", "False", "false", "None"}
                )
                continue
            if isinstance(parsed, list):
                score += len(parsed)
                continue
        score += 1
    return score


def _parse_time_minutes(value: Any) -> int | None:
    if value is None:
        return None
    hour = getattr(value, "hour", None)
    minute = getattr(value, "minute", None)
    if isinstance(hour, int) and isinstance(minute, int):
        return hour * 60 + minute
    text_value = str(value).strip()
    if not text_value:
        return None
    match = re.search(r"(\d{1,2}):(\d{2})", text_value)
    if not match:
        return None
    hour_value = int(match.group(1))
    minute_value = int(match.group(2))
    if not (0 <= hour_value <= 23 and 0 <= minute_value <= 59):
        return None
    return hour_value * 60 + minute_value


def _is_round_start_time(value: Any) -> bool:
    minutes = _parse_time_minutes(value)
    if minutes is None:
        return False
    return minutes % 60 in {0, 30}


def _metadata_start_time_mergeable(source_value: str, target_value: str) -> bool:
    if _is_blank_compare_value(source_value) or _is_blank_compare_value(target_value):
        return True
    source_minutes = _parse_time_minutes(source_value)
    target_minutes = _parse_time_minutes(target_value)
    if source_minutes is None or target_minutes is None:
        return False
    return abs(source_minutes - target_minutes) <= 2


def _metadata_score(payload: dict[str, str]) -> int:
    score = _payload_score(payload)
    stadium_name = payload.get("stadium_name", "")
    if stadium_name:
        score += len(stadium_name)
    start_time = payload.get("start_time", "")
    if start_time:
        score += 1
    if _is_round_start_time(start_time):
        score += 5
    return score


def _metadata_payload_resolution(
    source_payload: dict[str, str],
    target_payload: dict[str, str],
    diff_keys: set[str],
) -> str | None:
    if not diff_keys <= {"stadium_name", "start_time", "source_payload"}:
        return None
    if "start_time" in diff_keys and not _metadata_start_time_mergeable(
        source_payload.get("start_time", ""),
        target_payload.get("start_time", ""),
    ):
        return None

    source_score = _metadata_score(source_payload)
    target_score = _metadata_score(target_payload)
    return "source" if source_score > target_score else "target"


def _blank_zero_equivalent(left: str, right: str) -> bool:
    zeroish = {"", "0", "0.0", "0.000", "0.000000"}
    return left in zeroish and right in zeroish


def _is_blank_compare_value(value: str) -> bool:
    return value in {"", "0", "0.0", "0.000", "0.000000", "False", "false", "{}", "[]"}


def _numeric_close_equivalent(left: str, right: str, *, tolerance: float = 0.001) -> bool:
    if not left or not right:
        return False
    try:
        return abs(float(left) - float(right)) <= tolerance
    except (TypeError, ValueError):
        return False


def _is_generated_player_id(value: Any) -> bool:
    try:
        return int(value) >= 900000
    except (TypeError, ValueError):
        return False


def _player_id_only_resolution(
    source_row: dict[str, Any],
    target_row: dict[str, Any],
    source_payload: dict[str, str],
    target_payload: dict[str, str],
    table: Table,
    diff_keys: set[str],
) -> str | None:
    if diff_keys != {"player_id"}:
        return None

    if table.name == "game_lineups":
        if source_payload.get("player_name") == target_payload.get("player_name"):
            return "target"
        return None

    if table.name != "game_summary":
        return None

    source_player_id = source_payload.get("player_id", "")
    target_player_id = target_payload.get("player_id", "")
    source_blank = _is_blank_compare_value(source_player_id)
    target_blank = _is_blank_compare_value(target_player_id)
    if source_blank and not target_blank:
        return "target"
    if target_blank and not source_blank:
        return "source"

    source_generated = _is_generated_player_id(source_row.get("player_id"))
    target_generated = _is_generated_player_id(target_row.get("player_id"))
    if source_generated != target_generated:
        return "target" if source_generated else "source"

    return "target"


def _mergeable_payload_diff(key: str, source_value: str, target_value: str) -> bool:
    if key in MERGE_KEEP_TARGET_COLUMNS:
        return _is_blank_compare_value(source_value) and not _is_blank_compare_value(target_value)
    if key == "extra_stats":
        return True
    if key in MERGE_SOURCE_COLUMNS:
        return (
            _is_blank_compare_value(target_value)
            or _blank_zero_equivalent(source_value, target_value)
            or _numeric_close_equivalent(source_value, target_value)
        )
    return False


def _payload_resolution(source_row: dict[str, Any], target_row: dict[str, Any], table: Table) -> str:
    source_payload = _comparison_payload(source_row, table)
    target_payload = _comparison_payload(target_row, table)
    if source_payload == target_payload:
        return "same"
    diff_keys = {
        key
        for key in set(source_payload) | set(target_payload)
        if source_payload.get(key, "") != target_payload.get(key, "")
        and not _blank_zero_equivalent(source_payload.get(key, ""), target_payload.get(key, ""))
    }
    player_id_resolution = _player_id_only_resolution(
        source_row,
        target_row,
        source_payload,
        target_payload,
        table,
        diff_keys,
    )
    if player_id_resolution:
        return player_id_resolution
    if "player_id" in diff_keys and source_row.get("player_name") and target_row.get("player_name"):
        source_generated = _is_generated_player_id(source_row.get("player_id"))
        target_generated = _is_generated_player_id(target_row.get("player_id"))
        if source_generated != target_generated:
            return "target" if source_generated else "source"
    if diff_keys == {"extra_stats"}:
        source_score = _payload_score({"extra_stats": source_payload.get("extra_stats", "")})
        target_score = _payload_score({"extra_stats": target_payload.get("extra_stats", "")})
        return "source" if source_score > target_score else "target"
    if diff_keys <= {"player_name", "extra_stats"} and source_row.get("player_id") == target_row.get("player_id"):
        source_score = _payload_score(source_payload)
        target_score = _payload_score(target_payload)
        if source_score >= target_score:
            return "source"
        return "target"
    if table.name == "game_metadata":
        metadata_resolution = _metadata_payload_resolution(source_payload, target_payload, diff_keys)
        if metadata_resolution:
            return metadata_resolution
    if diff_keys and all(
        _mergeable_payload_diff(key, source_payload.get(key, ""), target_payload.get(key, ""))
        for key in diff_keys
    ):
        return "merge"
    if all(
        source_payload.get(key) == target_payload.get(key)
        or _blank_zero_equivalent(source_payload.get(key, ""), target_payload.get(key, ""))
        for key in set(source_payload) | set(target_payload)
    ):
        return "same"

    source_score = _payload_score(source_payload)
    target_score = _payload_score(target_payload)
    if source_score > target_score:
        return "source"
    if target_score > source_score:
        return "target"
    return "conflict"


def _decode_json_object(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    decoded = value
    for _ in range(3):
        if isinstance(decoded, dict):
            return decoded
        if not isinstance(decoded, str):
            return None
        stripped = decoded.strip()
        if stripped in {"", "null"}:
            return None
        if stripped[0] not in "{\"":
            return None
        try:
            decoded = json.loads(stripped)
        except json.JSONDecodeError:
            return None
    return decoded if isinstance(decoded, dict) else None


def _merge_extra_stats(source_value: Any, target_value: Any) -> Any:
    source_dict = _decode_json_object(source_value)
    target_dict = _decode_json_object(target_value)
    if source_dict is None:
        return target_value
    if target_dict is None:
        return json.dumps(source_dict, ensure_ascii=False, sort_keys=True)
    merged = dict(target_dict)
    for key, value in source_dict.items():
        if not _is_zeroish_raw(value):
            merged[key] = value
        elif key not in merged:
            merged[key] = value
    return json.dumps(merged, ensure_ascii=False, sort_keys=True)


def _merged_child_values(source_row: dict[str, Any], target_row: dict[str, Any], table: Table) -> dict[str, Any]:
    values = {}
    for column in table.columns:
        name = column.name
        if name in {"id", "game_id", *TIMESTAMP_COLUMNS}:
            continue
        if name == "extra_stats":
            merged = _merge_extra_stats(source_row.get(name), target_row.get(name))
            if merged != target_row.get(name):
                values[name] = merged
            continue
        source_value = source_row.get(name)
        target_value = target_row.get(name)
        source_compare = _normalized_compare_value(source_value)
        target_compare = _normalized_compare_value(target_value)
        if name in MERGE_KEEP_TARGET_COLUMNS:
            if _is_blank_compare_value(target_compare) and not _is_blank_compare_value(source_compare):
                values[name] = source_value
            continue
        if name in MERGE_SOURCE_COLUMNS:
            if _is_blank_compare_value(target_compare) and not _is_blank_compare_value(source_compare):
                values[name] = source_value
            continue
        if target_value in (None, "") and source_value not in (None, ""):
            values[name] = source_value
    return values


def _event_row_quality(row: dict[str, Any]) -> int:
    score = 0
    description = str(row.get("description") or "")
    event_type = str(row.get("event_type") or "").lower()
    if row.get("batter_name"):
        score += 2
    if row.get("pitcher_name"):
        score += 3
    if row.get("result_code"):
        score += 3
    if event_type and event_type != "unknown":
        score += 2
    if description and "번타자" not in description:
        score += 2
    if row.get("bases_before") and row.get("bases_after"):
        score += 1
    if row.get("outs") is not None:
        score += 1
    if row.get("inning") is not None and row.get("inning_half"):
        score += 1
    return score


def _event_dataset_resolution(source_rows: list[dict[str, Any]], target_rows: list[dict[str, Any]]) -> str:
    source_score = sum(_event_row_quality(row) for row in source_rows)
    target_score = sum(_event_row_quality(row) for row in target_rows)
    if source_score > target_score:
        return "source"
    if target_score > source_score:
        return "target"
    return "conflict"


def _row_key(row: dict[str, Any], unique_columns: tuple[str, ...], canonical_game_id: str) -> tuple[str, ...]:
    values = []
    for column in unique_columns:
        value = canonical_game_id if column == "game_id" else row.get(column)
        values.append(_stringify(value))
    return tuple(values)


def _where_source_row(table: Table, row: dict[str, Any], unique_columns: tuple[str, ...]):
    if "id" in table.c and row.get("id") is not None:
        return table.c.id == row["id"]
    clauses = []
    for column_name in unique_columns:
        column = table.c[column_name]
        value = row.get(column_name)
        clauses.append(column.is_(None) if value is None else column == value)
    return and_(*clauses)


def _unique_columns_for_table(table: Table, spec: ChildSpec) -> tuple[str, ...]:
    if spec.unique_columns is not None:
        return tuple(col for col in spec.unique_columns if col in table.c)
    return tuple(
        col.name
        for col in table.columns
        if col.name not in {"id", *TIMESTAMP_COLUMNS}
    )


def detect_child_conflicts(
    conn,
    tables: dict[str, Table],
    source_game_id: str,
    canonical_game_id: str,
) -> list[dict[str, Any]]:
    conflicts = []
    for spec in CHILD_SPECS:
        table = tables.get(spec.table_name)
        if table is None or "game_id" not in table.c:
            continue
        unique_columns = _unique_columns_for_table(table, spec)
        source_rows = [
            dict(row)
            for row in conn.execute(select(table).where(table.c.game_id == source_game_id)).mappings()
        ]
        if not source_rows:
            continue
        target_rows = [
            dict(row)
            for row in conn.execute(select(table).where(table.c.game_id == canonical_game_id)).mappings()
        ]
        if spec.table_name == "game_events" and target_rows:
            resolution = _event_dataset_resolution(source_rows, target_rows)
            if resolution in {"source", "target", "same"}:
                continue
        target_by_key = {
            _row_key(row, unique_columns, canonical_game_id): row
            for row in target_rows
        }
        for source_row in source_rows:
            key = _row_key(source_row, unique_columns, canonical_game_id)
            target_row = target_by_key.get(key)
            if target_row is None:
                continue
            if _payload_resolution(source_row, target_row, table) == "conflict":
                conflicts.append(
                    {
                        "table_name": spec.table_name,
                        "source_game_id": source_game_id,
                        "canonical_game_id": canonical_game_id,
                        "key": "|".join(key),
                        "reason": "conflicting_child_row",
                    }
                )
    return conflicts


def _upsert_alias(conn, tables: dict[str, Table], alias_game_id: str, canonical_game_id: str) -> None:
    if alias_game_id == canonical_game_id:
        return
    alias_table = tables["game_id_aliases"]
    existing = conn.execute(
        select(alias_table).where(alias_table.c.alias_game_id == alias_game_id)
    ).mappings().first()
    values = {
        "canonical_game_id": canonical_game_id,
        "source": "repair_game_id_integrity",
        "reason": "merged_duplicate_to_kbo_legacy_game_id",
    }
    if "updated_at" in alias_table.c:
        values["updated_at"] = datetime.utcnow()
    if existing:
        conn.execute(
            alias_table.update()
            .where(alias_table.c.alias_game_id == alias_game_id)
            .values(**values)
        )
    else:
        insert_values = {"alias_game_id": alias_game_id, **values}
        if "created_at" in alias_table.c:
            insert_values["created_at"] = datetime.utcnow()
        conn.execute(alias_table.insert().values(**insert_values))


def _retarget_aliases(conn, tables: dict[str, Table], source_game_id: str, canonical_game_id: str) -> None:
    alias_table = tables.get("game_id_aliases")
    if alias_table is None or source_game_id == canonical_game_id:
        return

    source_alias_rows = [
        dict(row)
        for row in conn.execute(
            select(alias_table).where(alias_table.c.canonical_game_id == source_game_id)
        ).mappings()
    ]
    for row in source_alias_rows:
        alias_game_id = str(row["alias_game_id"])
        if alias_game_id == canonical_game_id:
            conn.execute(alias_table.delete().where(alias_table.c.alias_game_id == alias_game_id))
            continue
        values = {"canonical_game_id": canonical_game_id}
        if "updated_at" in alias_table.c:
            values["updated_at"] = datetime.utcnow()
        conn.execute(
            alias_table.update()
            .where(alias_table.c.alias_game_id == alias_game_id)
            .values(**values)
        )

    conn.execute(
        alias_table.delete().where(alias_table.c.alias_game_id == alias_table.c.canonical_game_id)
    )


def _ensure_canonical_game_row(
    conn,
    tables: dict[str, Table],
    group: dict[str, Any],
) -> None:
    game = tables["game"]
    canonical_id = group["primary_game_id"]
    existing = conn.execute(select(game).where(game.c.game_id == canonical_id)).mappings().first()
    if existing:
        return

    source_id = max(group["game_ids"], key=lambda gid: (group["child_counts"].get(gid, 0), gid))
    source = conn.execute(select(game).where(game.c.game_id == source_id)).mappings().first()
    if not source:
        return
    values = {
        col.name: source.get(col.name)
        for col in game.columns
        if col.name != "id"
    }
    values["game_id"] = canonical_id
    if "is_primary" in game.c:
        values["is_primary"] = True
    conn.execute(game.insert().values(**values))


def _merge_master_fields(conn, tables: dict[str, Table], source_game_id: str, canonical_game_id: str) -> None:
    game = tables["game"]
    if source_game_id == canonical_game_id:
        return
    source = conn.execute(select(game).where(game.c.game_id == source_game_id)).mappings().first()
    target = conn.execute(select(game).where(game.c.game_id == canonical_game_id)).mappings().first()
    if not source or not target:
        return
    updates = {}
    for column in game.columns:
        if column.name in {"id", "game_id", *TIMESTAMP_COLUMNS}:
            continue
        if column.name == "game_status":
            target_status = str(target.get(column.name) or "").upper()
            source_status = source.get(column.name)
            if target_status in MERGEABLE_MASTER_STATUSES and source_status not in (None, ""):
                updates[column.name] = source_status
            continue
        if target.get(column.name) in (None, "") and source.get(column.name) not in (None, ""):
            updates[column.name] = source.get(column.name)
    if "is_primary" in game.c:
        updates["is_primary"] = True
    if updates:
        conn.execute(game.update().where(game.c.game_id == canonical_game_id).values(**updates))


def _move_child_rows(conn, tables: dict[str, Table], source_game_id: str, canonical_game_id: str) -> None:
    for spec in CHILD_SPECS:
        table = tables.get(spec.table_name)
        if table is None or "game_id" not in table.c:
            continue
        unique_columns = _unique_columns_for_table(table, spec)
        source_rows = [
            dict(row)
            for row in conn.execute(select(table).where(table.c.game_id == source_game_id)).mappings()
        ]
        if not source_rows:
            continue
        target_rows = [
            dict(row)
            for row in conn.execute(select(table).where(table.c.game_id == canonical_game_id)).mappings()
        ]
        if spec.table_name == "game_events" and target_rows:
            resolution = _event_dataset_resolution(source_rows, target_rows)
            if resolution == "source":
                conn.execute(table.delete().where(table.c.game_id == canonical_game_id))
                conn.execute(table.update().where(table.c.game_id == source_game_id).values(game_id=canonical_game_id))
                continue
            if resolution in {"target", "same"}:
                conn.execute(table.delete().where(table.c.game_id == source_game_id))
                continue
        target_by_key = {
            _row_key(row, unique_columns, canonical_game_id): row
            for row in target_rows
        }
        move_source_ids: list[Any] = []
        delete_source_ids: list[Any] = []
        for source_row in source_rows:
            where_clause = _where_source_row(table, source_row, unique_columns)
            key = _row_key(source_row, unique_columns, canonical_game_id)
            target_row = target_by_key.get(key)
            if target_row is not None:
                resolution = _payload_resolution(source_row, target_row, table)
                if resolution == "source":
                    update_values = {
                        col.name: source_row.get(col.name)
                        for col in table.columns
                        if col.name not in {"id", "game_id", *TIMESTAMP_COLUMNS}
                    }
                    conn.execute(table.update().where(_where_source_row(table, target_row, unique_columns)).values(**update_values))
                elif resolution == "merge":
                    update_values = _merged_child_values(source_row, target_row, table)
                    if update_values:
                        conn.execute(table.update().where(_where_source_row(table, target_row, unique_columns)).values(**update_values))
                if "id" in table.c and source_row.get("id") is not None:
                    delete_source_ids.append(source_row["id"])
                else:
                    conn.execute(table.delete().where(where_clause))
            else:
                if "id" in table.c and source_row.get("id") is not None:
                    move_source_ids.append(source_row["id"])
                else:
                    conn.execute(table.update().where(where_clause).values(game_id=canonical_game_id))
        if move_source_ids:
            conn.execute(table.update().where(table.c.id.in_(move_source_ids)).values(game_id=canonical_game_id))
        if delete_source_ids:
            conn.execute(table.delete().where(table.c.id.in_(delete_source_ids)))


def apply_duplicate_group(conn, tables: dict[str, Table], group: dict[str, Any]) -> None:
    canonical_id = group["primary_game_id"]
    _ensure_canonical_game_row(conn, tables, group)

    game = tables["game"]
    for source_id in sorted(group["game_ids"]):
        if source_id == canonical_id:
            continue
        _merge_master_fields(conn, tables, source_id, canonical_id)
        _move_child_rows(conn, tables, source_id, canonical_id)
        _retarget_aliases(conn, tables, source_id, canonical_id)
        _upsert_alias(conn, tables, source_id, canonical_id)
        conn.execute(game.delete().where(game.c.game_id == source_id))

    if "is_primary" in game.c:
        conn.execute(game.update().where(game.c.game_id == canonical_id).values(is_primary=True))


def collect_conflicts(conn, tables: dict[str, Table], groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    conflicts = []
    for group in groups:
        canonical_id = group["primary_game_id"]
        for source_id in group["game_ids"]:
            if source_id == canonical_id:
                continue
            conflicts.extend(detect_child_conflicts(conn, tables, source_id, canonical_id))
    return conflicts


def collect_coverage(conn, tables: dict[str, Table], years: Iterable[int]) -> list[dict[str, Any]]:
    rows = _game_rows(conn, tables, years)
    game_ids = [str(row["game_id"]) for row in rows]
    batting_ids = _ids_with_rows(conn, tables, "game_batting_stats", game_ids)
    pitching_ids = _ids_with_rows(conn, tables, "game_pitching_stats", game_ids)
    detail_ids = set(batting_ids) | set(pitching_ids)
    by_year: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_year[int(str(row["game_id"])[:4])].append(row)
    coverage = []
    for year in sorted(by_year):
        year_rows = by_year[year]
        ids = {str(row["game_id"]) for row in year_rows}
        coverage.append(
            {
                "year": year,
                "master_games": len(ids),
                "with_batting": len(ids & batting_ids),
                "with_pitching": len(ids & pitching_ids),
                "with_any_detail": len(ids & detail_ids),
                "non_primary": sum(1 for row in year_rows if row.get("is_primary") is False or row.get("is_primary") == 0),
            }
        )
    return coverage


def _ids_with_rows(conn, tables: dict[str, Table], table_name: str, game_ids: list[str]) -> set[str]:
    table = tables.get(table_name)
    if table is None or not game_ids:
        return set()
    return {
        row[0]
        for row in conn.execute(select(table.c.game_id).where(table.c.game_id.in_(game_ids)).distinct()).all()
    }


def collect_season_mismatches(conn, tables: dict[str, Table], years: Iterable[int]) -> list[dict[str, Any]]:
    preferred = _preferred_season_ids(conn, tables)
    season_by_id = _season_map(conn, tables)
    rows = []
    for row in _game_rows(conn, tables, years):
        game_id = str(row.get("game_id"))
        season_id = row.get("season_id")
        year = int(game_id[:4])
        league_type_code = 0
        if season_id is not None and int(season_id) in season_by_id:
            _, league_type_code = season_by_id[int(season_id)]
        expected = preferred.get((year, league_type_code))
        if expected is not None and season_id != expected:
            rows.append(
                {
                    "game_id": game_id,
                    "season_id": season_id,
                    "expected_season_id": expected,
                    "season_year": year,
                    "league_type_code": league_type_code,
                }
            )
    return rows


def collect_2024_backfill_candidates(conn, tables: dict[str, Table]) -> list[dict[str, Any]]:
    rows = _game_rows(conn, tables, [2024])
    season_by_id = _season_map(conn, tables)
    game_ids = [str(row["game_id"]) for row in rows]
    batting_ids = _ids_with_rows(conn, tables, "game_batting_stats", game_ids)
    pitching_ids = _ids_with_rows(conn, tables, "game_pitching_stats", game_ids)
    candidates = []
    for row in rows:
        game_id = str(row["game_id"])
        if row.get("is_primary") is False or row.get("is_primary") == 0:
            continue
        status = row.get("game_status")
        game_date = row.get("game_date")
        is_past_game = str(game_date) < date.today().isoformat()
        if status not in {"COMPLETED", "DRAW"} and not (
            is_past_game and status not in {"CANCELLED", "POSTPONED"}
        ):
            continue
        missing_batting = game_id not in batting_ids
        missing_pitching = game_id not in pitching_ids
        if not missing_batting and not missing_pitching:
            continue
        season_id = row.get("season_id")
        league_type_code = 0
        if season_id is not None and int(season_id) in season_by_id:
            _, league_type_code = season_by_id[int(season_id)]
        if league_type_code == 9:
            classification = "site_detail_unavailable_international_league"
        elif status == "SCHEDULED":
            classification = "past_scheduled_missing_detail"
        else:
            classification = "pending_recrawl"
        candidates.append(
            {
                "game_id": normalize_kbo_game_id(game_id),
                "game_date": row.get("game_date"),
                "game_status": status,
                "league_type_code": league_type_code,
                "missing_batting": int(missing_batting),
                "missing_pitching": int(missing_pitching),
                "classification": classification,
            }
        )
    return candidates


def is_actionable_backfill_candidate(row: dict[str, Any]) -> bool:
    return str(row.get("classification") or "") in ACTIONABLE_BACKFILL_CLASSIFICATIONS


def standardize_game_season_ids(conn, tables: dict[str, Table], years: Iterable[int]) -> int:
    game = tables["game"]
    preferred = _preferred_season_ids(conn, tables)
    season_by_id = _season_map(conn, tables)
    updates = 0
    for row in _game_rows(conn, tables, years):
        game_id = str(row["game_id"])
        season_id = row.get("season_id")
        year = int(game_id[:4])
        league_type_code = 0
        if season_id is not None and int(season_id) in season_by_id:
            _, league_type_code = season_by_id[int(season_id)]
        expected = preferred.get((year, league_type_code))
        if expected is not None and season_id != expected:
            conn.execute(game.update().where(game.c.game_id == game_id).values(season_id=expected))
            updates += 1
    return updates


def standardize_game_franchise_ids(conn, tables: dict[str, Table], years: Iterable[int]) -> int:
    game = tables["game"]
    updates = 0
    for row in _game_rows(conn, tables, years):
        game_id = str(row["game_id"])
        derived = _derive_game_franchise_ids(row)
        values = {}
        for column_name in ("away_franchise_id", "home_franchise_id", "winning_franchise_id"):
            if column_name not in game.c:
                continue
            value = derived.get(column_name)
            if value is not None and row.get(column_name) != value:
                values[column_name] = value
        if values:
            conn.execute(game.update().where(game.c.game_id == game_id).values(**values))
            updates += 1
    return updates


def run(
    *,
    db_url: str,
    years: tuple[int, ...],
    output_dir: Path,
    apply: bool,
    backup: bool,
) -> int:
    engine = create_engine(db_url)
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    with engine.begin() as conn:
        if apply:
            _ensure_repair_schema(conn)
        tables = _load_tables(conn)

        coverage = collect_coverage(conn, tables, years)
        groups = collect_duplicate_groups(conn, tables, years)
        conflicts = collect_conflicts(conn, tables, groups)
        season_mismatches = collect_season_mismatches(conn, tables, years)
        backfill_2024 = collect_2024_backfill_candidates(conn, tables)

        _write_csv(
            output_dir / f"game_identity_coverage_{stamp}.csv",
            coverage,
            ["year", "master_games", "with_batting", "with_pitching", "with_any_detail", "non_primary"],
        )
        _write_csv(
            output_dir / f"game_identity_duplicate_groups_{stamp}.csv",
            [
                {
                    **{k: group[k] for k in ("season_year", "league_type_code", "game_date", "away_franchise_id", "home_franchise_id", "doubleheader_no", "primary_game_id")},
                    "game_ids": ",".join(group["game_ids"]),
                    "child_counts": repr(group["child_counts"]),
                }
                for group in groups
            ],
            ["season_year", "league_type_code", "game_date", "away_franchise_id", "home_franchise_id", "doubleheader_no", "primary_game_id", "game_ids", "child_counts"],
        )
        _write_csv(
            output_dir / f"game_identity_conflicts_{stamp}.csv",
            conflicts,
            ["table_name", "source_game_id", "canonical_game_id", "key", "reason"],
        )
        _write_csv(
            output_dir / f"game_identity_season_mismatches_{stamp}.csv",
            season_mismatches,
            ["game_id", "season_id", "expected_season_id", "season_year", "league_type_code"],
        )
        _write_csv(
            output_dir / f"game_identity_2024_backfill_manifest_{stamp}.csv",
            backfill_2024,
            ["game_id", "game_date", "game_status", "league_type_code", "missing_batting", "missing_pitching", "classification"],
        )

        if not apply:
            actionable_backfill_2024 = sum(1 for row in backfill_2024 if is_actionable_backfill_candidate(row))
            print(f"[DRY-RUN] duplicate_groups={len(groups)} conflicts={len(conflicts)} 2024_backfill={actionable_backfill_2024}")
            print(f"[DRY-RUN] reports written to {output_dir}")
            return 0 if not conflicts else 2

        if conflicts:
            print(f"[ABORT] {len(conflicts)} conflicts found. Resolve conflict CSV before --apply.")
            return 2

        if backup:
            backup_path = _backup_sqlite_database(db_url, output_dir)
            if backup_path:
                print(f"[BACKUP] {backup_path}")
            affected_backup_path = _backup_affected_rows(conn, tables, years, groups, output_dir, stamp)
            if affected_backup_path:
                print(f"[BACKUP] {affected_backup_path}")

        franchise_updates = standardize_game_franchise_ids(conn, tables, years)
        season_updates = standardize_game_season_ids(conn, tables, years)
        for group in groups:
            apply_duplicate_group(conn, tables, group)

        print(
            f"[APPLY] merged_duplicate_groups={len(groups)} "
            f"franchise_id_updates={franchise_updates} season_id_updates={season_updates}"
        )
        return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit and repair KBO game_id identity duplicates.")
    parser.add_argument("--db-url", default=None, help="SQLAlchemy database URL. Defaults to DATABASE_URL or local SQLite.")
    parser.add_argument("--oci", action="store_true", help="Use OCI_DB_URL as target database.")
    parser.add_argument("--years", default="2024,2025,2026", help="Comma-separated years to inspect/repair.")
    parser.add_argument("--output-dir", default="data/repair_game_id_integrity", help="Directory for CSV reports and backups.")
    parser.add_argument("--apply", action="store_true", help="Apply repairs. Default is dry-run only.")
    parser.add_argument("--no-backup", action="store_true", help="Skip SQLite backup before --apply.")
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()
    db_url = args.db_url
    if args.oci:
        db_url = os.getenv("OCI_DB_URL")
    if not db_url:
        db_url = os.getenv("DATABASE_URL") or os.getenv("SOURCE_DATABASE_URL") or DEFAULT_DB_URL
    years = tuple(int(part.strip()) for part in args.years.split(",") if part.strip())
    raise SystemExit(
        run(
            db_url=db_url,
            years=years,
            output_dir=Path(args.output_dir),
            apply=bool(args.apply),
            backup=not args.no_backup,
        )
    )


if __name__ == "__main__":
    main()
