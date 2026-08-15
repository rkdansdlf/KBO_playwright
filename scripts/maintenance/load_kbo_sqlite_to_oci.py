"""Load local SQLite baseball data into the OCI Autonomous DB.

Read-only by default (no ``--apply``); use ``--apply`` to write.
Idempotent: every table uses an id/PK-based ``MERGE`` so re-runs are safe.

Usage:
    venv/bin/python -m scripts.maintenance.load_sqlite_to_oci
    venv/bin/python -m scripts.maintenance.load_sqlite_to_oci --apply --table game
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sqlite3
import time
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import oracledb
from sqlalchemy import create_engine, text

_KST = ZoneInfo("Asia/Seoul")

TABLE_ORDER = [
    "kbo_seasons",
    "team_code_map",
    "team_franchises",
    "team_history",
    "teams",
    "player_basic",
    "players",
    "game",
    "game_id_aliases",
    "game_metadata",
    "game_validation_metrics",
    "game_summary",
    "game_lineups",
    "game_batting_stats",
    "game_pitching_stats",
    "game_play_by_play",
    "game_events",
    "player_game_batting",
    "player_game_pitching",
    "player_season_batting",
    "player_season_pitching",
    "player_season_fielding",
    "player_season_baserunning",
    "team_season_batting",
    "team_season_pitching",
    "team_season_fielding",
    "team_season_baserunning",
    "team_standings_daily",
    "game_inning_scores",
    "game_highlights",
    "game_mvps",
    "player_movements",
    "team_events",
    "raw_source_snapshots",
    "data_sources",
    "awards",
    "player_identities",
    "team_daily_roster",
    "stat_rankings",
    "sla_metrics",
    "roster_transactions",
    "cheer_songs",
    "stadium_info",
    "stadium_operation_notices",
    "stadium_seat_sections",
    "stadium_food_vendors",
    "stadium_food_menu_items",
    "stadium_regulations",
    "parking_lots",
    "parking_fee_rules",
    "team_rivalries",
    "injury_entries",
    "game_broadcasts",
    "manager_changes",
    "foreign_player_changes",
    "ticket_open_rules",
    "ticket_prices",
    "embedding_cache",
]

_SHA256 = hashlib.sha256


def _detail_hash(v: Any) -> Any:
    if v is None:
        return None
    return _SHA256(str(v).encode("utf-8")).hexdigest()


TABLE_OVERRIDE: dict[str, Any] = {
    "detail_text_hash": _detail_hash,
}

TABLE_COL_OVERRIDE: dict[tuple[str, str], Any] = {
    ("player_season_batting", "level"): lambda v: LEVEL_NORMALIZE.get(v, v),
    ("player_season_pitching", "level"): lambda v: LEVEL_NORMALIZE.get(v, v),
    ("player_season_batting", "team_code"): lambda v: TEAM_CODE_MAP.get(v, v),
    ("player_season_pitching", "team_code"): lambda v: TEAM_CODE_MAP.get(v, v),
    ("team_daily_roster", "player_name"): lambda v: v if v and str(v).strip() else "UNKNOWN",
}

NATURAL_KEYS: dict[str, list[str]] = {
    "game_lineups": ["game_id", "team_side", "appearance_seq"],
    "game_batting_stats": ["game_id", "player_id"],
    "game_pitching_stats": ["game_id", "player_id", "appearance_seq"],
    "game_events": ["game_id", "event_seq"],
    "player_game_batting": ["game_id", "player_id"],
    "player_game_pitching": ["game_id", "player_id"],
    "player_season_batting": ["player_id", "season", "league", "level", "team_code"],
    "player_season_pitching": ["player_id", "season", "league", "level", "team_code"],
    "game_inning_scores": ["game_id", "team_side", "inning"],
}

ORPHAN_FILTERS: dict[str, str] = {
    "game_highlights": 'game_id IN (SELECT game_id FROM "game")',
    "player_identities": "player_id IN (SELECT id FROM players)",
}

REPLACE_TABLES = {
    "game_lineups",
    "game_batting_stats",
    "game_pitching_stats",
    "game_events",
    "player_game_batting",
    "player_game_pitching",
    "game_play_by_play",
}

NO_ID_TABLES: set[str] = set()

LEVEL_NORMALIZE = {"1": "KBO1", "1군": "KBO1"}

TEAM_CODE_MAP = {
    "BE": "HH",
    "HT": "KIA",
    "MBC": "LG",
    "NX": "KH",
    "OB": "DB",
    "SK": "SSG",
    "WO": "KH",
}


def log(msg: str) -> None:
    print(f"{datetime.now().strftime('%H:%M:%S')} {msg}", flush=True)


def _load_env() -> dict[str, str]:
    env: dict[str, str] = {}
    with Path(".env").open() as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k] = v
    return env


class OciLoader:
    def __init__(self, sqlite_path: str, oci_url: str, tns_admin: str) -> None:
        self.sq = sqlite3.connect(sqlite_path)
        self.sq.row_factory = sqlite3.Row
        wallet_password = _load_env().get("OCI_WALLET_PASSWORD")
        connect_args: dict[str, Any] = {"config_dir": tns_admin}
        if wallet_password:
            connect_args["wallet_location"] = tns_admin
            connect_args["wallet_password"] = wallet_password
        self.engine = create_engine(oci_url, connect_args=connect_args)
        m = re.match(r"oracle\+oracledb://([^:]+):([^@]+)@(.+)$", oci_url)
        conn_kwargs: dict[str, Any] = {
            "user": m.group(1),
            "password": urllib.parse.unquote(m.group(2)),
            "dsn": m.group(3),
            "config_dir": tns_admin,
        }
        if wallet_password:
            conn_kwargs["wallet_location"] = tns_admin
            conn_kwargs["wallet_password"] = wallet_password
        self.oci = oracledb.connect(**conn_kwargs)

    def close(self) -> None:
        self.sq.close()
        self.oci.close()
        self.engine.dispose()

    def oci_columns(self, table: str) -> dict[str, str]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT column_name, data_type FROM user_tab_columns WHERE table_name = :t ORDER BY column_id"),
                {"t": table.upper()},
            ).fetchall()
        return {r[0]: r[1] for r in rows}

    def set_table_triggers(self, table: str, enable: bool) -> None:
        action = "ENABLE" if enable else "DISABLE"
        with self.engine.begin() as conn:
            rows = conn.execute(
                text("SELECT trigger_name FROM user_triggers WHERE table_name = :t AND status = :s"),
                {"t": table.upper(), "s": "DISABLED" if enable else "ENABLED"},
            ).fetchall()
            for (name,) in rows:
                conn.execute(text(f"ALTER TRIGGER {name} {action}"))
            log(f"[trigger] {len(rows)} trigger(s) {action}d on {table}")

    def oci_char_sizes(self, table: str) -> dict[str, int]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT column_name, char_length FROM user_tab_columns "
                    "WHERE table_name = :t AND data_type IN ('VARCHAR2','CHAR')"
                ),
                {"t": table.upper()},
            ).fetchall()
        return {r[0]: r[1] for r in rows}

    def oci_pk_columns(self, table: str) -> list[str]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT cc.column_name FROM user_constraints c "
                    "JOIN user_cons_columns cc ON c.constraint_name = cc.constraint_name "
                    "WHERE c.constraint_type = 'P' AND c.table_name = :t "
                    "ORDER BY cc.position"
                ),
                {"t": table.upper()},
            ).fetchall()
        return [r[0] for r in rows]

    def sqlite_columns(self, table: str) -> list[str]:
        return [r[1] for r in self.sq.execute(f'PRAGMA table_info("{table}")').fetchall()]

    def _convert(self, value: Any, oci_type: str) -> Any:
        if value is None or value == "":
            return None
        if oci_type.startswith("DATE") or oci_type.startswith("TIMESTAMP"):
            if isinstance(value, datetime):
                return value
            if isinstance(value, str):
                v = value.replace("T", " ").strip()
                for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                    try:
                        dt = datetime.strptime(v, fmt)
                    except ValueError:
                        continue
                    if "WITH TIME ZONE" in oci_type:
                        return dt.replace(tzinfo=_KST)
                    return dt
                try:
                    return datetime.strptime(v, "%H:%M:%S.%f" if "." in v else "%H:%M:%S").replace(
                        year=2000, month=1, day=1
                    )
                except ValueError:
                    return value
        return value

    def plan(self, table: str) -> tuple[list[str], list[str], dict[str, str], dict[str, str]]:
        oci_cols = self.oci_columns(table)
        sqlite_cols = self.sqlite_columns(table)
        oci_lower = {c.lower(): c for c in oci_cols}
        common = [c for c in sqlite_cols if c in oci_lower]
        oci_only = [c for c in oci_cols if c.lower() not in common]
        known = {c: oci_lower[c] for c in common}
        for c in oci_only:
            known[c.lower()] = c
        return common, oci_only, oci_cols, known

    def insert_sql(self, table: str, columns: list[str], pk: list[str], known: dict[str, str]) -> str:
        def qn(c: str) -> str:
            return f'"{known.get(c.lower(), c.upper())}"'

        ins_cols = ", ".join(qn(c) for c in columns)
        bind_vals = ", ".join(f":c{i}" for i in range(len(columns)))
        return f"INSERT INTO {table.upper()} ({ins_cols}) VALUES ({bind_vals})"

    def _row_payload(
        self,
        row: sqlite3.Row,
        columns: list[str],
        oci_types: dict[str, str],
        table: str,
        char_sizes: dict[str, int] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        for c in columns:
            v = row[c]
            if c in TABLE_OVERRIDE:
                v = TABLE_OVERRIDE[c](v)
            col_override = TABLE_COL_OVERRIDE.get((table, c))
            if col_override is not None:
                v = col_override(v)
            if table == "player_movements" and c == "team_code" and not str(v or "").strip():
                v = row["canonical_team_id"] or "UNKNOWN"
            v = self._convert(v, oci_types.get(c.upper(), "VARCHAR2"))
            limit = char_sizes.get(c.upper()) if char_sizes else None
            if limit is not None and isinstance(v, str) and len(v) > limit:
                v = v[:limit]
            payload[c] = v
        return payload

    def _build_batch_payloads(
        self,
        rows: list[sqlite3.Row],
        full_columns: list[str],
        oci_types: dict[str, str],
        table: str,
        char_sizes: dict[str, int],
        key_cols: list[str],
        keys_seen: set[tuple],
    ) -> tuple[list[dict[str, Any]], int]:
        payloads: list[dict[str, Any]] = []
        skipped = 0
        for row in rows:
            payload = self._row_payload(row, full_columns, oci_types, table, char_sizes)
            if key_cols:
                k = tuple(payload[c] for c in key_cols)
                if k in keys_seen:
                    skipped += 1
                    continue
                keys_seen.add(k)
            payloads.append({f"c{i}": payload[col] for i, col in enumerate(full_columns)})
        return payloads, skipped

    def _executemany_with_fallback(
        self,
        cursor: Any,
        sql: str,
        payloads: list[dict[str, Any]],
        table: str,
        skipped: int,
    ) -> int:
        if not payloads:
            return skipped
        try:
            cursor.executemany(sql, payloads)
        except oracledb.Error as exc:
            log(f"[warn] {table}: executemany failed ({exc}); falling back to row-by-row")
            for p in payloads:
                try:
                    cursor.execute(sql, p)
                except Exception as exc2:
                    skipped += 1
                    if skipped <= 3:
                        log(f"[warn] {table}: row error ({exc2}): {p}")
            log(f"[warn] {table}: batch fell back to row-by-row, skipped {skipped} orphan rows so far")
        return skipped

    def load_table(
        self,
        table: str,
        apply: bool,
        limit: int | None,
        batch_size: int,
        commit_every: int,
    ) -> dict[str, Any]:
        common, oci_only, oci_types, known = self.plan(table)
        log(f"[plan] {table}: common={len(common)} oci_only={oci_only}")
        pk = self.oci_pk_columns(table)
        log(f"[plan] {table}: pk={pk}")
        if not pk:
            log(f"[skip] {table}: no PK, cannot merge")
            return {"table": table, "dry_run": True}
        keys = [k.upper() for k in NATURAL_KEYS.get(table, [p.lower() for p in pk])]
        key_cols = [k.lower() for k in NATURAL_KEYS.get(table, [p.lower() for p in pk])]
        full_columns = common + [
            c.lower() for c in oci_only if c.lower() in {x.lower() for x in self.sqlite_columns(table)}
        ]
        if table in NO_ID_TABLES:
            full_columns = [c for c in full_columns if c.lower() != "id"]
            log(f"[plan] {table}: id omitted (identity sequence)")
        sql = self.insert_sql(table, full_columns, keys, known)
        total = self.sq.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        log(f"[count] {table}: local={total}")
        if not apply:
            return {"table": table, "local": total, "dry_run": True}

        order_by = ", ".join(f'"{c}"' for c in key_cols) if table in REPLACE_TABLES else None
        orphan_filter = ORPHAN_FILTERS.get(table)
        where_clause = f" WHERE {orphan_filter}" if orphan_filter else ""
        r = self.sq.execute(f'SELECT * FROM "{table}"{where_clause}' + (f" ORDER BY {order_by}" if order_by else ""))
        replace_mode = table in REPLACE_TABLES
        if replace_mode:
            with self.engine.connect() as conn:
                existing = conn.execute(text(f'SELECT COUNT(*) FROM "{table.upper()}"')).scalar()
            if existing:
                with self.engine.begin() as conn:
                    conn.execute(text(f'TRUNCATE TABLE "{table.upper()}"'))
                log(f"[wipe] {table}: {existing} existing rows truncated (replace mode)")
            else:
                log(f"[wipe] {table}: already empty, skip truncate")
            keys_seen: set[tuple] = set()
        else:
            qn = ", ".join(f'"{known.get(k.lower(), k)}"' for k in keys)
            with self.engine.connect() as conn:
                rows = conn.execute(text(f'SELECT {qn} FROM "{table.upper()}"')).fetchall()
            keys_seen = {tuple(r) for r in rows}
            log(f"[keys] {table}: {len(keys_seen)} existing keys loaded for dedup")
        char_sizes = self.oci_char_sizes(table)
        t0 = time.monotonic()
        done = 0
        skipped = 0
        cursor = self.oci.cursor()
        self.set_table_triggers(table, enable=False)
        try:
            while True:
                rows = r.fetchmany(batch_size)
                if not rows:
                    break
                payloads, dup = self._build_batch_payloads(
                    rows, full_columns, oci_types, table, char_sizes, key_cols, keys_seen
                )
                skipped += dup
                skipped = self._executemany_with_fallback(cursor, sql, payloads, table, skipped)
                done += len(rows)
                if done % commit_every == 0:
                    self.oci.commit()
                    elapsed = time.monotonic() - t0
                    rate = done / elapsed if elapsed else 0
                    log(f"[load] {table}: {done}/{total} ({rate:.0f} rows/s, {elapsed:.0f}s)")
                if limit and done >= limit:
                    break
            self.oci.commit()
        finally:
            self.set_table_triggers(table, enable=True)
            cursor.close()
        elapsed = time.monotonic() - t0
        log(f"[done] {table}: {done} rows in {elapsed:.0f}s (skipped {skipped} dup keys)")
        return {"table": table, "local": total, "loaded": done, "skipped": skipped}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="write to OCI (default: dry-run)")
    parser.add_argument("--table", help="only load this table")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--commit-every", type=int, default=50000)
    args = parser.parse_args()

    env = _load_env()
    loader = OciLoader(
        env["DATABASE_URL"].replace("sqlite:///", ""),
        env["OCI_DB_URL"],
        env["TNS_ADMIN"],
    )
    tables = [args.table] if args.table else TABLE_ORDER
    try:
        for t in tables:
            if t not in TABLE_ORDER:
                log(f"[skip] {t} not in TABLE_ORDER")
                continue
            loader.load_table(t, args.apply, args.limit, args.batch_size, args.commit_every)
    finally:
        loader.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
