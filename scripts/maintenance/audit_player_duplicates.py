"""Read-only audit of duplicate player identities (legacy vs modern ids).

This supplements ``scripts/maintenance/full_audit.py`` (which only detects
duplicate ``player_id`` groups) by detecting duplicate *names*: the same player
registered under both a legacy id (<10000) and a modern id (>=50000), which
fragments career stats across two master records.

Outputs two buckets:
- ``clean_pairs``: exactly one legacy id + exactly one modern id (safe auto-merge candidates).
- ``ambiguous_names``: multiple legacy/modern ids or pseudo profiles (manual review).
"""

from __future__ import annotations

import argparse
import json
import os
from sqlalchemy import Connection, create_engine, text


LEGACY_MAX_ID = 10000
MODERN_MIN_ID = 50000
MODERN_MAX_ID = 900000
PSEUDO_MIN_ID = 900000


def _classify_player_id(player_id: int) -> str:
    if player_id < LEGACY_MAX_ID:
        return "legacy"
    if MODERN_MIN_ID <= player_id < MODERN_MAX_ID:
        return "modern"
    return "pseudo"


def audit_player_duplicates(conn: Connection) -> dict:
    """Audit player_basic for names split across legacy/modern ids.

    Returns clean 1:1 merge candidates and ambiguous name groups, plus the
    count of legacy ``player_season_pitching`` rows that fall under clean pairs
    (i.e. the rows that a safe merge would re-point to the modern canonical id).
    """
    rows = conn.execute(text("SELECT player_id, name, birth_date FROM player_basic")).mappings().fetchall()

    by_name: dict[str, list[dict]] = {}
    for row in rows:
        name = row["name"]
        by_name.setdefault(name, []).append(
            {
                "player_id": row["player_id"],
                "kind": _classify_player_id(row["player_id"]),
                "has_profile": row["birth_date"] is not None,
            }
        )

    clean_pairs: list[dict] = []
    ambiguous_names: list[dict] = []
    for name, members in by_name.items():
        if len(members) < 2:
            continue
        legacy = [m for m in members if m["kind"] == "legacy"]
        modern = [m for m in members if m["kind"] == "modern"]
        pseudo = [m for m in members if m["kind"] == "pseudo"]
        if len(legacy) == 1 and len(modern) == 1 and not pseudo:
            clean_pairs.append(
                {
                    "name": name,
                    "legacy_id": legacy[0]["player_id"],
                    "modern_id": modern[0]["player_id"],
                }
            )
        else:
            ambiguous_names.append(
                {
                    "name": name,
                    "legacy_ids": [m["player_id"] for m in legacy],
                    "modern_ids": [m["player_id"] for m in modern],
                    "pseudo_ids": [m["player_id"] for m in pseudo],
                }
            )

    clean_name_set = {c["name"] for c in clean_pairs}
    mergeable_psp_rows = 0
    if clean_name_set:
        placeholders = ", ".join(f":n{i}" for i in range(len(clean_name_set)))
        params = {f"n{i}": name for i, name in enumerate(clean_name_set)}
        params["legacy"] = LEGACY_MAX_ID
        mergeable_psp_rows = conn.execute(
            text(
                "SELECT COUNT(*) FROM player_season_pitching s "
                "JOIN player_basic pb ON pb.player_id = s.player_id "
                f"WHERE pb.name IN ({placeholders}) AND pb.player_id < :legacy"
            ),
            params,
        ).fetchone()[0]

    return {
        "total_players": len(rows),
        "duplicate_name_count": len(clean_pairs) + len(ambiguous_names),
        "clean_pair_count": len(clean_pairs),
        "ambiguous_count": len(ambiguous_names),
        "mergeable_legacy_season_pitching_rows": mergeable_psp_rows,
        "clean_pairs": clean_pairs,
        "ambiguous_names": ambiguous_names,
    }


def main(argv: list[str] | None = None) -> int:
    """Run the read-only duplicate-player audit and emit a report."""
    parser = argparse.ArgumentParser(description="Audit duplicate player identities (legacy vs modern ids), read-only")
    parser.add_argument("--database-url", default=None, help="Local SQLite URL (defaults to DATABASE_URL)")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    parser.add_argument("--output-dir", type=str, default="data/audit")
    args = parser.parse_args(argv)

    database_url = args.database_url or os.getenv("DATABASE_URL")
    if not database_url:
        parser.error("database URL is required via --database-url or DATABASE_URL")

    engine = create_engine(database_url)
    with engine.connect() as conn:
        report = audit_player_duplicates(conn)

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(f"total_players={report['total_players']}")
        print(f"duplicate_name_count={report['duplicate_name_count']}")
        print(f"clean_pair_count={report['clean_pair_count']}")
        print(f"ambiguous_count={report['ambiguous_count']}")
        print(f"mergeable_legacy_season_pitching_rows={report['mergeable_legacy_season_pitching_rows']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
