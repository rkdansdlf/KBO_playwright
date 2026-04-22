"""Backfill missing game starting pitchers from pitching stats.

This command repairs ``game.away_pitcher`` and ``game.home_pitcher`` when
completed games already have starting pitchers in ``game_pitching_stats``.
It does not infer names from unrelated fields and does not overwrite existing
values unless explicitly requested.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from src.db.engine import SessionLocal


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _normalize_date(value: str | None) -> str | None:
    if not value:
        return None

    compact = value.strip()
    if len(compact) == 8 and compact.isdigit():
        return f"{compact[:4]}-{compact[4:6]}-{compact[6:]}"

    return compact


def _is_blank(value: Any) -> bool:
    return value is None or str(value).strip() == ""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fill missing game starting pitchers from game_pitching_stats."
    )
    parser.add_argument("--start-date", help="Start date, YYYYMMDD or YYYY-MM-DD")
    parser.add_argument("--end-date", help="End date, YYYYMMDD or YYYY-MM-DD")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show repair candidates without updating the database.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing game pitcher values from pitching stats.",
    )
    parser.add_argument(
        "--sync",
        action="store_true",
        help="Sync repaired games to OCI after local update.",
    )
    parser.add_argument(
        "--sync-target-missing",
        action="store_true",
        help="Sync games whose pitcher fields are present locally but missing in OCI.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of games to repair.",
    )
    return parser.parse_args()


def load_candidates(session, args: argparse.Namespace) -> list[dict[str, Any]]:
    start_date = _normalize_date(args.start_date)
    end_date = _normalize_date(args.end_date)
    overwrite_filter = "1 = 1" if args.overwrite else """
        (
            (coalesce(trim(g.away_pitcher), '') = '' AND coalesce(trim(s.away_start), '') <> '')
            OR
            (coalesce(trim(g.home_pitcher), '') = '' AND coalesce(trim(s.home_start), '') <> '')
        )
    """
    limit_clause = "LIMIT :limit" if args.limit else ""

    query = text(
        f"""
        WITH starts AS (
            SELECT
                game_id,
                max(
                    CASE
                        WHEN team_side = 'away'
                             AND is_starting = 1
                             AND coalesce(trim(player_name), '') <> ''
                        THEN player_name
                    END
                ) AS away_start,
                max(
                    CASE
                        WHEN team_side = 'home'
                             AND is_starting = 1
                             AND coalesce(trim(player_name), '') <> ''
                        THEN player_name
                    END
                ) AS home_start
            FROM game_pitching_stats
            GROUP BY game_id
        )
        SELECT
            g.game_id,
            g.game_date,
            g.away_pitcher AS current_away_pitcher,
            g.home_pitcher AS current_home_pitcher,
            s.away_start,
            s.home_start
        FROM game g
        JOIN starts s ON s.game_id = g.game_id
        WHERE (:start_date IS NULL OR g.game_date >= :start_date)
          AND (:end_date IS NULL OR g.game_date <= :end_date)
          AND g.game_date < date('now')
          AND coalesce(g.game_status, '') <> 'SCHEDULED'
          AND (coalesce(trim(s.away_start), '') <> '' OR coalesce(trim(s.home_start), '') <> '')
          AND {overwrite_filter}
        ORDER BY g.game_date, g.game_id
        {limit_clause}
        """
    )
    params: dict[str, Any] = {"start_date": start_date, "end_date": end_date}
    if args.limit:
        params["limit"] = args.limit

    return [dict(row) for row in session.execute(query, params).mappings().all()]


def repair_candidates(
    session,
    candidates: list[dict[str, Any]],
    overwrite: bool,
    dry_run: bool,
) -> tuple[list[str], int, int]:
    updated_game_ids: list[str] = []
    away_updates = 0
    home_updates = 0

    update_query = text(
        """
        UPDATE game
        SET
            away_pitcher = :away_pitcher,
            home_pitcher = :home_pitcher
        WHERE game_id = :game_id
        """
    )

    for row in candidates:
        current_away = row["current_away_pitcher"]
        current_home = row["current_home_pitcher"]
        away_start = row["away_start"]
        home_start = row["home_start"]

        next_away = away_start if (overwrite or _is_blank(current_away)) and not _is_blank(away_start) else current_away
        next_home = home_start if (overwrite or _is_blank(current_home)) and not _is_blank(home_start) else current_home

        away_changed = next_away != current_away
        home_changed = next_home != current_home
        if not away_changed and not home_changed:
            continue

        away_updates += 1 if away_changed else 0
        home_updates += 1 if home_changed else 0
        updated_game_ids.append(row["game_id"])

        if dry_run:
            continue

        session.execute(
            update_query,
            {
                "game_id": row["game_id"],
                "away_pitcher": next_away,
                "home_pitcher": next_home,
            },
        )

    if not dry_run:
        session.commit()

    return updated_game_ids, away_updates, home_updates


def sync_to_oci(game_ids: list[str]) -> tuple[int, int]:
    from src.sync.oci_sync import OCISync

    target_url = os.getenv("OCI_DB_URL") or os.getenv("TARGET_DATABASE_URL")
    if not target_url:
        raise RuntimeError("OCI_DB_URL or TARGET_DATABASE_URL is required for OCI sync")

    with SessionLocal() as session:
        syncer = OCISync(target_url, session)
        success = 0
        failed = 0
        for game_id in game_ids:
            try:
                if syncer.sync_specific_game(game_id):
                    success += 1
                else:
                    failed += 1
            except Exception as exc:  # noqa: BLE001
                failed += 1
                print(f"OCI sync failed for {game_id}: {exc}", file=sys.stderr)
    return success, failed


def find_target_missing_ready_games(session, args: argparse.Namespace) -> list[dict[str, Any]]:
    target_url = os.getenv("OCI_DB_URL") or os.getenv("TARGET_DATABASE_URL")
    if not target_url:
        raise RuntimeError("OCI_DB_URL or TARGET_DATABASE_URL is required for --sync-target-missing")

    start_date = _normalize_date(args.start_date)
    end_date = _normalize_date(args.end_date)

    target_engine = create_engine(target_url)
    target_query = text(
        """
        SELECT game_id
        FROM game
        WHERE (:start_date IS NULL OR game_date >= CAST(:start_date AS date))
          AND (:end_date IS NULL OR game_date <= CAST(:end_date AS date))
          AND game_date < CURRENT_DATE
          AND coalesce(game_status, '') <> 'SCHEDULED'
          AND (coalesce(trim(away_pitcher), '') = '' OR coalesce(trim(home_pitcher), '') = '')
        """
    )
    with target_engine.connect() as target_conn:
        target_missing_ids = {
            row[0]
            for row in target_conn.execute(
                target_query,
                {"start_date": start_date, "end_date": end_date},
            )
        }

    if not target_missing_ids:
        return []

    local_query = text(
        """
        SELECT
            g.game_id,
            g.away_pitcher,
            g.home_pitcher
        FROM game g
        WHERE (:start_date IS NULL OR g.game_date >= :start_date)
          AND (:end_date IS NULL OR g.game_date <= :end_date)
          AND g.game_date < date('now')
          AND coalesce(g.game_status, '') <> 'SCHEDULED'
          AND coalesce(trim(g.away_pitcher), '') <> ''
          AND coalesce(trim(g.home_pitcher), '') <> ''
        """
    )
    local_ready_rows = [
        dict(row)
        for row in session.execute(
            local_query,
            {"start_date": start_date, "end_date": end_date},
        ).mappings().all()
    ]

    return sorted(
        (row for row in local_ready_rows if row["game_id"] in target_missing_ids),
        key=lambda row: row["game_id"],
    )


def update_target_pitcher_fields(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0

    target_url = os.getenv("OCI_DB_URL") or os.getenv("TARGET_DATABASE_URL")
    if not target_url:
        raise RuntimeError("OCI_DB_URL or TARGET_DATABASE_URL is required for OCI update")

    target_engine = create_engine(target_url)
    update_query = text(
        """
        UPDATE game
        SET
            away_pitcher = CASE
                WHEN coalesce(trim(away_pitcher), '') = '' THEN :away_pitcher
                ELSE away_pitcher
            END,
            home_pitcher = CASE
                WHEN coalesce(trim(home_pitcher), '') = '' THEN :home_pitcher
                ELSE home_pitcher
            END
        WHERE game_id = :game_id
          AND (coalesce(trim(away_pitcher), '') = '' OR coalesce(trim(home_pitcher), '') = '')
        """
    )

    with target_engine.begin() as target_conn:
        result = target_conn.execute(update_query, rows)
        return int(result.rowcount or 0)


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    args = parse_args()

    with SessionLocal() as session:
        candidates = load_candidates(session, args)
        updated_game_ids, away_updates, home_updates = repair_candidates(
            session=session,
            candidates=candidates,
            overwrite=args.overwrite,
            dry_run=args.dry_run,
        )

    action = "would update" if args.dry_run else "updated"
    print(
        f"{action}: games={len(updated_game_ids)}, "
        f"away_pitcher={away_updates}, home_pitcher={home_updates}"
    )

    if args.sync_target_missing:
        with SessionLocal() as session:
            target_missing_ready_rows = find_target_missing_ready_games(session, args)
        if args.dry_run:
            print(f"target_missing_ready: games={len(target_missing_ready_rows)}")
            return 0
        updated_target_rows = update_target_pitcher_fields(target_missing_ready_rows)
        print(
            f"target_missing_pitcher_update: "
            f"candidates={len(target_missing_ready_rows)}, updated={updated_target_rows}"
        )
        return 0

    if args.sync and args.dry_run:
        print("sync skipped: dry-run mode")
        return 0

    if args.sync and updated_game_ids:
        success, failed = sync_to_oci(updated_game_ids)
        print(f"oci_sync: success={success}, failed={failed}")
        return 1 if failed else 0

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
