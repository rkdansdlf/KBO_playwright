"""Remove non-primary duplicate games from OCI/Postgres.

Defaults to dry-run and requires --apply before deleting rows.
"""
from __future__ import annotations

import argparse
import os

import psycopg2
from dotenv import load_dotenv


DELETE_STEPS = (
    (
        "game_events",
        "DELETE FROM game_events WHERE game_id IN (SELECT game_id FROM game WHERE is_primary = false)",
    ),
    (
        "game_play_by_play",
        "DELETE FROM game_play_by_play WHERE game_id IN (SELECT game_id FROM game WHERE is_primary = false)",
    ),
    (
        "game_summary",
        "DELETE FROM game_summary WHERE game_id IN (SELECT game_id FROM game WHERE is_primary = false)",
    ),
    (
        "game_metadata",
        "DELETE FROM game_metadata WHERE game_id IN (SELECT game_id FROM game WHERE is_primary = false)",
    ),
    (
        "game_inning_scores",
        "DELETE FROM game_inning_scores WHERE game_id IN (SELECT game_id FROM game WHERE is_primary = false)",
    ),
    (
        "game_lineups",
        "DELETE FROM game_lineups WHERE game_id IN (SELECT game_id FROM game WHERE is_primary = false)",
    ),
    (
        "game_batting_stats",
        "DELETE FROM game_batting_stats WHERE game_id IN (SELECT game_id FROM game WHERE is_primary = false)",
    ),
    (
        "game_pitching_stats",
        "DELETE FROM game_pitching_stats WHERE game_id IN (SELECT game_id FROM game WHERE is_primary = false)",
    ),
    (
        "game_id_aliases",
        "DELETE FROM game_id_aliases WHERE canonical_game_id IN (SELECT game_id FROM game WHERE is_primary = false)",
    ),
    (
        "game",
        "DELETE FROM game WHERE is_primary = false",
    ),
)


def cleanup_oci_duplicates(*, database_url: str, apply: bool = False) -> dict[str, int]:
    counts: dict[str, int] = {}
    conn = psycopg2.connect(database_url)
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM game WHERE is_primary = false")
            counts["non_primary_games_before"] = cursor.fetchone()[0]

            for label, sql in DELETE_STEPS:
                cursor.execute(sql)
                counts[label] = cursor.rowcount

            if apply:
                conn.commit()
            else:
                conn.rollback()

            cursor.execute("SELECT COUNT(*) FROM game WHERE is_primary = false")
            counts["non_primary_games_after"] = cursor.fetchone()[0]
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return counts


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Clean up non-primary duplicate games in OCI/Postgres.")
    parser.add_argument("--apply", action="store_true", help="Actually delete rows. Default is dry-run rollback.")
    parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL. Defaults to OCI_DB_URL or TARGET_DATABASE_URL from environment.",
    )
    return parser


def main() -> int:
    load_dotenv()
    args = build_arg_parser().parse_args()
    database_url = args.database_url or os.getenv("OCI_DB_URL") or os.getenv("TARGET_DATABASE_URL")
    if not database_url:
        print("[ERROR] OCI_DB_URL or TARGET_DATABASE_URL is required.")
        return 1

    if not args.apply:
        print("[DRY-RUN] No changes will be committed. Pass --apply to delete rows.")

    counts = cleanup_oci_duplicates(database_url=database_url, apply=args.apply)
    for key, value in counts.items():
        print(f"{key}: {value}")

    if args.apply:
        print("[DONE] OCI duplicate cleanup committed.")
    else:
        print("[DONE] Dry-run rolled back.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
