#!/usr/bin/env python3
"""Collect source boxscore evidence for identity-conflict duplicate rows.

The script is read-only with respect to the database. It crawls game detail
pages from an identity-conflict manifest, writes source hitter rows, and
generates proposed row-level remaps only when a current DB row matches exactly
one source row with a concrete source player id.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.game_detail_crawler import GameDetailCrawler  # noqa: E402
from src.utils.player_positions import get_primary_position  # noqa: E402
from src.utils.team_codes import normalize_kbo_game_id  # noqa: E402

DEFAULT_DB_PATH = Path("data/kbo_dev.db")
DEFAULT_OUTPUT_DIR = Path("data/identity_conflict_evidence")

CORE_BATTING_STAT_COLUMNS = (
    "at_bats",
    "runs",
    "hits",
    "doubles",
    "triples",
    "home_runs",
    "rbi",
    "walks",
    "intentional_walks",
    "hbp",
    "strikeouts",
    "stolen_bases",
    "caught_stealing",
    "sacrifice_hits",
    "sacrifice_flies",
    "gdp",
)


@dataclass(frozen=True)
class EvidenceTarget:
    game_id: str
    game_date: str

    def as_crawler_input(self) -> dict[str, str]:
        return {"game_id": self.game_id, "game_date": self.game_date}


def _write_csv(path: Path, rows: Iterable[dict[str, Any]], fieldnames: list[str]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fieldnames})
            count += 1
    return count


def _safe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _norm_int(value: Any) -> int:
    parsed = _safe_int(value)
    return parsed if parsed is not None else 0


def _norm_text(value: Any) -> str:
    return str(value or "").strip()


def _game_date_from_manifest(row: dict[str, str]) -> str:
    raw = _norm_text(row.get("game_date"))
    if raw:
        return raw.replace("-", "")
    game_id = normalize_kbo_game_id(row.get("game_id") or "")
    return game_id[:8]


def load_manifest_targets(
    manifest_path: Path,
    *,
    limit: int | None = None,
    offset: int = 0,
) -> list[EvidenceTarget]:
    seen: set[str] = set()
    targets: list[EvidenceTarget] = []
    skipped = 0
    with manifest_path.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            game_id = normalize_kbo_game_id(row.get("game_id") or "")
            if not game_id or game_id in seen:
                continue
            seen.add(game_id)
            if skipped < offset:
                skipped += 1
                continue
            targets.append(EvidenceTarget(game_id=game_id, game_date=_game_date_from_manifest(row)))
            if limit is not None and len(targets) >= limit:
                break
    return targets


def flatten_hitter_evidence(
    payloads: Iterable[dict[str, Any]],
    *,
    player_name: str | None = None,
    team_code: str | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for payload in payloads:
        game_id = normalize_kbo_game_id(payload.get("game_id") or "")
        game_date = _norm_text(payload.get("game_date")) or game_id[:8]
        hitters = payload.get("hitters") or {}
        for team_side in ("away", "home"):
            for entry in hitters.get(team_side, []) or []:
                if player_name and _norm_text(entry.get("player_name")) != player_name:
                    continue
                if team_code and _norm_text(entry.get("team_code")) != team_code:
                    continue
                stats = entry.get("stats") or {}
                position = _norm_text(entry.get("position"))
                row = {
                    "game_id": game_id,
                    "game_date": game_date,
                    "team_side": team_side,
                    "team_code": _norm_text(entry.get("team_code")),
                    "source_player_id": _safe_int(entry.get("player_id")),
                    "player_name": _norm_text(entry.get("player_name")),
                    "uniform_no": _norm_text(entry.get("uniform_no")),
                    "batting_order": _safe_int(entry.get("batting_order")),
                    "appearance_seq": _safe_int(entry.get("appearance_seq")),
                    "position": position,
                    "standard_position": get_primary_position(position).value,
                }
                for column in ("plate_appearances", *CORE_BATTING_STAT_COLUMNS):
                    row[column] = _norm_int(stats.get(column))
                rows.append(row)
    return rows


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone() is not None


def _columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table_name})")}


def _select_expr(table_name: str, columns: set[str], *, table_alias: str = "t") -> str:
    wanted = [
        "id",
        "game_id",
        "team_side",
        "team_code",
        "player_id",
        "player_name",
        "uniform_no",
        "batting_order",
        "appearance_seq",
        "position",
        "standard_position",
        "plate_appearances",
        *CORE_BATTING_STAT_COLUMNS,
    ]
    expressions = []
    for column in wanted:
        if column in columns:
            expressions.append(f"{table_alias}.{column} AS {column}")
        else:
            expressions.append(f"NULL AS {column}")
    return ", ".join(expressions)


def _fetch_duplicate_rows(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    game_ids: list[str],
    player_name: str | None,
    team_code: str | None,
) -> list[dict[str, Any]]:
    if not game_ids or not _table_exists(conn, table_name):
        return []
    columns = _columns(conn, table_name)
    if not {"game_id", "player_id", "player_name", "team_code"}.issubset(columns):
        return []

    placeholders = ",".join("?" for _ in game_ids)
    params: list[Any] = list(game_ids)
    filters = [f"t.game_id IN ({placeholders})"]
    if player_name:
        filters.append("t.player_name = ?")
        params.append(player_name)
    if team_code:
        filters.append("t.team_code = ?")
        params.append(team_code)
    where_sql = " AND ".join(filters)
    select_sql = _select_expr(table_name, columns)

    group_columns = "game_id, player_id, team_code" if table_name == "game_lineups" else "game_id, player_id"
    group_join = (
        "d.game_id = t.game_id AND d.player_id = t.player_id AND d.team_code = t.team_code"
        if table_name == "game_lineups"
        else "d.game_id = t.game_id AND d.player_id = t.player_id"
    )
    rows = conn.execute(
        f"""
        WITH duplicate_groups AS (
            SELECT {group_columns}, COUNT(*) AS row_count
            FROM {table_name}
            WHERE player_id IS NOT NULL
            GROUP BY {group_columns}
            HAVING COUNT(*) > 1
        )
        SELECT '{table_name}' AS table_name, {select_sql}
        FROM {table_name} t
        JOIN duplicate_groups d
          ON {group_join}
        WHERE {where_sql}
        ORDER BY t.game_id, t.player_id, t.id
        """,
        tuple(params),
    ).fetchall()
    return [dict(row) for row in rows]


def _lineup_match_key(row: dict[str, Any]) -> tuple[Any, ...]:
    standard_position = _norm_text(row.get("standard_position"))
    if not standard_position or standard_position == "UNKNOWN":
        standard_position = get_primary_position(_norm_text(row.get("position"))).value
    return (
        _norm_text(row.get("game_id")),
        _norm_text(row.get("team_side")),
        _norm_text(row.get("team_code")),
        _norm_text(row.get("player_name")),
        _safe_int(row.get("batting_order")),
        _safe_int(row.get("appearance_seq")),
        standard_position,
    )


def _batting_match_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        *_lineup_match_key(row),
        *(_norm_int(row.get(column)) for column in CORE_BATTING_STAT_COLUMNS),
    )


def _match_key(table_name: str, row: dict[str, Any]) -> tuple[Any, ...]:
    if table_name == "game_batting_stats":
        return _batting_match_key(row)
    return _lineup_match_key(row)


def propose_identity_conflict_updates(
    *,
    db_path: Path,
    evidence_rows: list[dict[str, Any]],
    player_name: str | None = None,
    team_code: str | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    game_ids = sorted({_norm_text(row.get("game_id")) for row in evidence_rows if row.get("game_id")})
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    try:
        db_rows = []
        for table_name in ("game_batting_stats", "game_lineups"):
            db_rows.extend(
                _fetch_duplicate_rows(
                    conn,
                    table_name=table_name,
                    game_ids=game_ids,
                    player_name=player_name,
                    team_code=team_code,
                )
            )
    finally:
        conn.close()

    evidence_by_key: dict[tuple[str, tuple[Any, ...]], list[dict[str, Any]]] = {}
    for evidence in evidence_rows:
        for table_name in ("game_batting_stats", "game_lineups"):
            key = _match_key(table_name, evidence)
            evidence_by_key.setdefault((table_name, key), []).append(evidence)

    proposed: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    for db_row in db_rows:
        table_name = _norm_text(db_row.get("table_name"))
        key = _match_key(table_name, db_row)
        matches = evidence_by_key.get((table_name, key), [])
        base = {
            "table_name": table_name,
            "row_id": db_row.get("id"),
            "game_id": db_row.get("game_id"),
            "team_side": db_row.get("team_side"),
            "team_code": db_row.get("team_code"),
            "player_name": db_row.get("player_name"),
            "current_player_id": db_row.get("player_id"),
            "batting_order": db_row.get("batting_order"),
            "appearance_seq": db_row.get("appearance_seq"),
            "position": db_row.get("position"),
            "standard_position": db_row.get("standard_position"),
            "match_key": "|".join(str(part) for part in key),
        }
        if len(matches) != 1:
            blocked.append(
                {
                    **base,
                    "reason": "no_source_match" if not matches else "multiple_source_matches",
                    "match_count": len(matches),
                    "source_player_ids": ",".join(
                        sorted({_norm_text(match.get("source_player_id")) for match in matches if match.get("source_player_id")})
                    ),
                }
            )
            continue

        source = matches[0]
        source_player_id = _safe_int(source.get("source_player_id"))
        if source_player_id is None:
            blocked.append(
                {
                    **base,
                    "reason": "source_player_id_missing",
                    "match_count": 1,
                    "source_player_ids": "",
                }
            )
            continue

        current_player_id = _safe_int(db_row.get("player_id"))
        if current_player_id == source_player_id:
            blocked.append(
                {
                    **base,
                    "reason": "source_matches_current",
                    "match_count": 1,
                    "source_player_ids": str(source_player_id),
                }
            )
            continue

        proposed.append(
            {
                **base,
                "proposed_player_id": source_player_id,
                "confidence": "exact_source_row_match",
                "reason": "DB duplicate row matched one source row with a different concrete player id.",
            }
        )

    return proposed, blocked


async def collect_evidence_payloads(
    targets: list[EvidenceTarget],
    *,
    delay: float,
    concurrency: int | None,
) -> list[dict[str, Any]]:
    crawler = GameDetailCrawler(request_delay=delay)
    return await crawler.crawl_games(
        [target.as_crawler_input() for target in targets],
        concurrency=concurrency,
    )


async def collect_identity_conflict_evidence(
    *,
    manifest_path: Path,
    db_path: Path,
    output_dir: Path,
    player_name: str | None = None,
    team_code: str | None = None,
    limit: int | None = None,
    offset: int = 0,
    concurrency: int | None = None,
    delay: float = 1.0,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    targets = load_manifest_targets(manifest_path, limit=limit, offset=offset)
    payloads = await collect_evidence_payloads(targets, delay=delay, concurrency=concurrency)
    evidence_rows = flatten_hitter_evidence(payloads, player_name=player_name, team_code=team_code)
    proposed_rows, blocked_rows = propose_identity_conflict_updates(
        db_path=db_path,
        evidence_rows=evidence_rows,
        player_name=player_name,
        team_code=team_code,
    )

    evidence_csv = output_dir / f"identity_conflict_source_evidence_{stamp}.csv"
    proposed_csv = output_dir / f"identity_conflict_proposed_updates_{stamp}.csv"
    blocked_csv = output_dir / f"identity_conflict_blocked_rows_{stamp}.csv"
    evidence_fields = [
        "game_id",
        "game_date",
        "team_side",
        "team_code",
        "source_player_id",
        "player_name",
        "uniform_no",
        "batting_order",
        "appearance_seq",
        "position",
        "standard_position",
        "plate_appearances",
        *CORE_BATTING_STAT_COLUMNS,
    ]
    proposed_fields = [
        "table_name",
        "row_id",
        "game_id",
        "team_side",
        "team_code",
        "player_name",
        "current_player_id",
        "proposed_player_id",
        "confidence",
        "reason",
        "batting_order",
        "appearance_seq",
        "position",
        "standard_position",
        "match_key",
    ]
    blocked_fields = [
        "table_name",
        "row_id",
        "game_id",
        "team_side",
        "team_code",
        "player_name",
        "current_player_id",
        "reason",
        "match_count",
        "source_player_ids",
        "batting_order",
        "appearance_seq",
        "position",
        "standard_position",
        "match_key",
    ]
    _write_csv(evidence_csv, evidence_rows, evidence_fields)
    _write_csv(proposed_csv, proposed_rows, proposed_fields)
    _write_csv(blocked_csv, blocked_rows, blocked_fields)

    return {
        "targets": len(targets),
        "payloads": len(payloads),
        "evidence_rows": len(evidence_rows),
        "proposed_updates": len(proposed_rows),
        "blocked_rows": len(blocked_rows),
        "evidence_csv": str(evidence_csv),
        "proposed_csv": str(proposed_csv),
        "blocked_csv": str(blocked_csv),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect source evidence for identity-conflict remaps.")
    parser.add_argument("--manifest", required=True, help="identity_conflict_game_manifest CSV path.")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="SQLite database path.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="CSV output directory.")
    parser.add_argument("--player-name", default=None, help="Optional exact player name filter.")
    parser.add_argument("--team-code", default=None, help="Optional exact team code filter.")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of manifest games for a smoke run.")
    parser.add_argument("--offset", type=int, default=0, help="Skip this many unique manifest games before crawling.")
    parser.add_argument("--concurrency", type=int, default=1, help="Max concurrent game crawls.")
    parser.add_argument("--delay", type=float, default=1.0, help="Request delay passed to GameDetailCrawler.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = asyncio.run(
        collect_identity_conflict_evidence(
            manifest_path=Path(args.manifest),
            db_path=Path(args.db_path),
            output_dir=Path(args.output_dir),
            player_name=args.player_name,
            team_code=args.team_code,
            limit=args.limit,
            offset=args.offset,
            concurrency=args.concurrency,
            delay=args.delay,
        )
    )
    print(
        f"[REPORT] targets={result['targets']} payloads={result['payloads']} "
        f"evidence_rows={result['evidence_rows']} proposed_updates={result['proposed_updates']} "
        f"blocked_rows={result['blocked_rows']}"
    )
    print(f"[REPORT] evidence_csv={result['evidence_csv']}")
    print(f"[REPORT] proposed_csv={result['proposed_csv']}")
    print(f"[REPORT] blocked_csv={result['blocked_csv']}")


if __name__ == "__main__":
    main()
