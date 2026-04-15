import argparse
import asyncio
import csv
import os
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

# Adjust sys.path to run from scripts/ folder easily
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.db.engine import SessionLocal
from src.models.game import Game, GameEvent, GamePlayByPlay
from src.models.season import KboSeason
from src.repositories.game_repository import (
    backfill_game_play_by_play_from_existing_events,
    save_relay_data,
)
from src.sources.relay import (
    ImportRelayAdapter,
    KboRelayAdapter,
    NaverRelayAdapter,
    RelayRecoveryOrchestrator,
    default_source_order_for_bucket,
    derive_bucket_id,
    read_manifest_entries,
)
from src.utils.safe_print import safe_print as print
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES


DEFAULT_MANIFEST_PATH = Path(project_root) / "data" / "recovery" / "source_manifest.csv"
DEFAULT_CAPABILITY_PATH = Path(project_root) / "data" / "recovery" / "source_capability.csv"


def _parse_source_order(value: str | None) -> list[str] | None:
    if not value:
        return None
    tokens = [token.strip() for token in value.split(",") if token.strip()]
    return tokens or None


def _load_game_ids_from_file(path: str | None) -> list[str]:
    if not path:
        return []
    file_path = Path(path)
    if not file_path.exists():
        print(f"[ERROR] Game ID file not found: {file_path}")
        sys.exit(1)

    game_ids: list[str] = []
    seen: set[str] = set()
    with file_path.open("r", encoding="utf-8", newline="") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            token = line.split(",", 1)[0].strip()
            if token.lower() == "game_id" or not token:
                continue
            if token in seen:
                continue
            seen.add(token)
            game_ids.append(token)
    return game_ids


def _collect_target_games(args) -> list[dict[str, Any]]:
    with SessionLocal() as session:
        requested_ids = []
        if args.game_ids_file:
            requested_ids.extend(_load_game_ids_from_file(args.game_ids_file))
        if args.game_ids:
            requested_ids.extend(gid.strip() for gid in args.game_ids.split(",") if gid.strip())

        if requested_ids:
            requested_ids = list(dict.fromkeys(requested_ids))
            found_rows = (
                session.query(Game.game_id, KboSeason.league_type_name)
                .outerjoin(KboSeason, KboSeason.season_id == Game.season_id)
                .filter(Game.game_id.in_(requested_ids))
                .all()
            )
            row_map = {game_id: league_type_name for game_id, league_type_name in found_rows}
            rows = [(game_id, row_map.get(game_id)) for game_id in requested_ids]
        elif args.date:
            try:
                target_dt = datetime.strptime(args.date, "%Y%m%d").date()
            except ValueError:
                print(f"[ERROR] Invalid date format: {args.date}. Use YYYYMMDD.")
                sys.exit(1)
            rows = (
                session.query(Game.game_id, KboSeason.league_type_name)
                .outerjoin(KboSeason, KboSeason.season_id == Game.season_id)
                .filter(Game.game_date == target_dt, Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)))
                .all()
            )
        else:
            rows = (
                session.query(Game.game_id, KboSeason.league_type_name)
                .outerjoin(KboSeason, KboSeason.season_id == Game.season_id)
                .filter(Game.game_id.like(f"{args.season}%"), Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)))
                .all()
            )

        game_ids = [row[0] for row in rows]
        if not game_ids:
            return []

        event_set = {
            row[0]
            for row in session.query(GameEvent.game_id)
            .filter(GameEvent.game_id.in_(game_ids))
            .distinct()
            .all()
        }
        pbp_set = {
            row[0]
            for row in session.query(GamePlayByPlay.game_id)
            .filter(GamePlayByPlay.game_id.in_(game_ids))
            .distinct()
            .all()
        }

    targets = []
    skipped = 0
    for game_id, league_type_name in rows:
        has_events = game_id in event_set
        has_pbp = game_id in pbp_set
        needs_event_recovery = not has_events
        needs_pbp_recovery = not has_pbp

        if args.missing_only and not (needs_event_recovery or needs_pbp_recovery):
            skipped += 1
            continue

        targets.append(
            {
                "game_id": game_id,
                "league_type_name": league_type_name,
                "bucket_id": args.bucket or derive_bucket_id(game_id, league_type_name),
                "has_events": has_events,
                "has_pbp": has_pbp,
                "needs_event_recovery": needs_event_recovery,
                "needs_pbp_recovery": needs_pbp_recovery,
            }
        )

    if args.missing_only:
        print(f"[INFO] Missing-only mode: Skipped {skipped} games already fully recovered.")
    return targets


def _write_report(report_path: str | None, rows: list[dict[str, Any]]) -> None:
    if not report_path:
        return
    path = Path(report_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "game_id",
        "bucket_id",
        "source_name",
        "status",
        "saved_rows",
        "has_event_state",
        "has_raw_pbp",
        "notes",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name) for name in fieldnames})
    print(f"[INFO] Recovery report written to {path}")


async def run_fetcher():
    parser = argparse.ArgumentParser(description="KBO relay recovery fetcher")
    parser.add_argument("--season", type=int, help="Season year to fetch (e.g. 2024)")
    parser.add_argument("--date", type=str, help="Target date to fetch (e.g. 20240924)")
    parser.add_argument("--limit", type=int, help="Limit maximum games to process")
    parser.add_argument("--game-ids", type=str, help="Specific Game IDs to fetch, comma separated")
    parser.add_argument("--game-ids-file", type=str, help="Path to a file containing Game IDs, one per line")
    parser.add_argument("--dry-run", action="store_true", help="Parse relay data but do not save to DB")
    parser.add_argument(
        "--missing-only",
        action="store_true",
        default=True,
        help="Only process games missing events or play-by-play (default: true)",
    )
    parser.add_argument("--force", action="store_true", help="Overwrite existing relay data")
    parser.add_argument("--source-order", type=str, help="Comma separated source order override")
    parser.add_argument(
        "--import-manifest",
        type=str,
        default=str(DEFAULT_MANIFEST_PATH),
        help="Manifest CSV path, or comma separated manifest CSV paths",
    )
    parser.add_argument("--bucket", type=str, help="Force one bucket ID for all selected games")
    parser.add_argument(
        "--source-timeout",
        type=float,
        default=30.0,
        help="Per-source timeout in seconds for probe/fetch attempts",
    )
    parser.add_argument(
        "--allow-derived-pbp",
        action="store_true",
        default=False,
        help="Allow game_play_by_play backfill from existing game_events",
    )
    parser.add_argument("--report-out", type=str, help="CSV report output path")
    args = parser.parse_args()

    if not args.season and not args.date and not args.game_ids and not args.game_ids_file:
        print("[ERROR] Must provide --season, --date, --game-ids, or --game-ids-file")
        sys.exit(1)

    if args.force:
        args.missing_only = False

    targets = _collect_target_games(args)
    if not targets:
        print("[INFO] No games found to process.")
        return

    if args.limit:
        targets = targets[: args.limit]

    manifest_entries = read_manifest_entries(args.import_manifest)
    manifest_dir = Path(args.import_manifest).resolve().parent
    adapters = {
        "naver": NaverRelayAdapter(),
        "kbo": KboRelayAdapter(),
        "import": ImportRelayAdapter(
            manifest_entries,
            source_name="import",
            allowed_source_types={"naver", "kbo", "html_archive", "json_archive"},
            manifest_base_dir=manifest_dir,
        ),
        "manual": ImportRelayAdapter(
            manifest_entries,
            source_name="manual",
            allowed_source_types={"manual_text"},
            manifest_base_dir=manifest_dir,
        ),
    }
    orchestrator = RelayRecoveryOrchestrator(
        adapters,
        capability_path=DEFAULT_CAPABILITY_PATH,
        timeout_seconds=args.source_timeout,
    )

    bucket_map: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for target in targets:
        bucket_map[target["bucket_id"]].append(target)

    source_order_override = _parse_source_order(args.source_order)
    report_rows: list[dict[str, Any]] = []

    print(f"[INFO] Total games to process: {len(targets)}")
    if args.dry_run:
        print("[WARN] Dry-run mode activated. No data will be saved.")

    for bucket_id, bucket_targets in bucket_map.items():
        source_order = orchestrator.source_order_for_bucket(
            bucket_id,
            source_order_override or default_source_order_for_bucket(bucket_id),
        )
        print(f"[INFO] Bucket {bucket_id}: source order = {', '.join(source_order)}")
        await orchestrator.probe_bucket(
            bucket_id,
            [target["game_id"] for target in bucket_targets],
            source_order,
        )

        for idx, target in enumerate(bucket_targets, start=1):
            game_id = target["game_id"]
            print(f"\n[PROGRESS] Bucket {bucket_id} {idx}/{len(bucket_targets)}: {game_id}")

            if args.allow_derived_pbp and target["has_events"] and target["needs_pbp_recovery"]:
                saved_rows = 0 if args.dry_run else backfill_game_play_by_play_from_existing_events(game_id)
                print(
                    f"[SUCCESS] {'Would derive' if args.dry_run else 'Derived'} "
                    f"{saved_rows if not args.dry_run else 'missing'} play_by_play rows from game_events"
                )
                report_rows.append(
                    {
                        "game_id": game_id,
                        "bucket_id": bucket_id,
                        "source_name": "derived_game_events",
                        "status": "dry_run" if args.dry_run else "saved",
                        "saved_rows": saved_rows,
                        "has_event_state": True,
                        "has_raw_pbp": True,
                        "notes": "Derived game_play_by_play from existing game_events",
                    }
                )
                continue

            result, attempts = await orchestrator.fetch_game(game_id, bucket_id, source_order)
            report_rows.extend(attempts)
            if result.is_empty:
                print(f"[SKIP] No relay data extracted for {game_id}")
                continue

            if args.dry_run:
                saved_rows = len(result.events) if result.events else len(result.raw_pbp_rows)
                print(
                    f"[DRY-RUN] Would save {saved_rows} rows from {result.source_name} "
                    f"for {game_id}"
                )
            else:
                saved_rows = save_relay_data(
                    game_id,
                    result.events,
                    raw_pbp_rows=result.raw_pbp_rows,
                    source_name=result.source_name,
                    notes=result.notes,
                    allow_derived_pbp=args.allow_derived_pbp,
                )
                print(f"[SUCCESS] Saved {saved_rows} rows for {game_id} via {result.source_name}")

            report_rows.append(
                {
                    "game_id": game_id,
                    "bucket_id": bucket_id,
                    "source_name": result.source_name,
                    "status": "dry_run" if args.dry_run else "saved",
                    "saved_rows": saved_rows,
                    "has_event_state": result.has_event_state,
                    "has_raw_pbp": result.has_raw_pbp or bool(result.raw_pbp_rows),
                    "notes": result.notes,
                }
            )
            await asyncio.sleep(1.0)

    _write_report(args.report_out, report_rows)
    print("\n[INFO] Relay recovery run completed.")


if __name__ == "__main__":
    asyncio.run(run_fetcher())
