"""CLI module for validating and importing historical boxscore manifests (2001-2009)."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from src.crawlers.legacy_game_detail_crawler import LegacyGameDetailCrawler
from src.db.engine import SessionLocal
from src.repositories.game_repository import save_game_detail
from src.services.player_id_resolver import PlayerIdResolver

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


@dataclass
class ImportContext:
    """Import context for processing historical manifest entries."""

    manifest_dir: Path
    crawler: LegacyGameDetailCrawler
    session: Session | None
    dry_run: bool
    strict: bool


def read_boxscore_manifest_entries(manifest_path: Path) -> list[dict[str, Any]]:
    """Read boxscore manifest entries from a CSV or JSON manifest file."""
    entries: list[dict[str, Any]] = []
    if manifest_path.suffix.lower() == ".json":
        with manifest_path.open(encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                entries = data
            elif isinstance(data, dict) and "entries" in data:
                entries = data["entries"]
    else:
        with manifest_path.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                entries.append(dict(row))

    normalized: list[dict[str, Any]] = []
    for entry in entries:
        game_id = str(entry.get("game_id", "")).strip()
        locator = str(entry.get("locator") or entry.get("file") or entry.get("path") or "").strip()
        if not game_id or not locator:
            continue

        season_str = str(entry.get("season", "")).strip()
        season = int(season_str) if season_str.isdigit() else (int(game_id[:4]) if game_id[:4].isdigit() else None)

        normalized.append(
            {
                "game_id": game_id,
                "locator": locator,
                "season": season,
                "captured_at": entry.get("captured_at"),
                "sha256": entry.get("sha256"),
                "format": entry.get("format", "kbo_html"),
            }
        )
    return normalized


def validate_boxscore_payload(data: dict[str, Any], *, strict: bool = True) -> tuple[bool, str | None]:
    """Validate extracted boxscore data structure and completeness."""
    if not data or "teams" not in data:
        return False, "missing_teams_metadata"

    hitters = data.get("hitters", {})
    pitchers = data.get("pitchers", {})

    away_hitters = hitters.get("away", [])
    home_hitters = hitters.get("home", [])
    away_pitchers = pitchers.get("away", [])
    home_pitchers = pitchers.get("home", [])

    if strict:
        if not away_hitters or not home_hitters:
            return False, f"incomplete_hitters (away={len(away_hitters)}, home={len(home_hitters)})"
        if not away_pitchers or not home_pitchers:
            return False, f"incomplete_pitchers (away={len(away_pitchers)}, home={len(home_pitchers)})"

    return True, None


def _process_single_entry(entry: dict[str, Any], ctx: ImportContext) -> dict[str, Any]:
    game_id = entry["game_id"]
    season = entry["season"]
    locator = entry["locator"]

    file_path = Path(locator)
    if not file_path.is_absolute():
        file_path = ctx.manifest_dir / file_path

    if not file_path.is_file():
        return {
            "game_id": game_id,
            "season": season,
            "status": "error",
            "reason": f"file_not_found: {file_path}",
        }

    raw_bytes = file_path.read_bytes()
    if entry.get("sha256"):
        actual_sha = hashlib.sha256(raw_bytes).hexdigest()
        if actual_sha.lower() != str(entry["sha256"]).lower():
            return {
                "game_id": game_id,
                "season": season,
                "status": "error",
                "reason": f"sha256_mismatch (expected {entry['sha256']}, got {actual_sha})",
            }

    html = raw_bytes.decode("utf-8", errors="replace")
    game_date = game_id[:8]

    data = ctx.crawler.extract_from_html(html, game_id=game_id, game_date=game_date, db_session=ctx.session)
    is_valid, err_reason = validate_boxscore_payload(data, strict=ctx.strict)

    if not is_valid:
        status = "empty" if "incomplete" in (err_reason or "") else "error"
        return {
            "game_id": game_id,
            "season": season,
            "status": status,
            "reason": err_reason,
        }

    if not ctx.dry_run and ctx.session:
        saved = save_game_detail(data)
        status = "saved" if saved else "save_failed"
    else:
        status = "valid"

    return {
        "game_id": game_id,
        "season": season,
        "status": status,
        "away_hitters": len(data.get("hitters", {}).get("away", [])),
        "home_hitters": len(data.get("hitters", {}).get("home", [])),
        "away_pitchers": len(data.get("pitchers", {}).get("away", [])),
        "home_pitchers": len(data.get("pitchers", {}).get("home", [])),
    }


def process_historical_manifest(
    manifest_path: Path,
    *,
    dry_run: bool = True,
    strict: bool = True,
    seasons: set[int] | None = None,
    game_ids: set[str] | None = None,
) -> dict[str, Any]:
    """Validate and optionally import historical boxscores from an archive manifest."""
    entries = read_boxscore_manifest_entries(manifest_path)
    session = SessionLocal() if not dry_run else None
    resolver = PlayerIdResolver(session) if session else None
    crawler = LegacyGameDetailCrawler(resolver=resolver)

    ctx = ImportContext(
        manifest_dir=manifest_path.parent,
        crawler=crawler,
        session=session,
        dry_run=dry_run,
        strict=strict,
    )

    results: list[dict[str, Any]] = []
    counts = {"valid": 0, "empty": 0, "errors": 0, "saved": 0}

    try:
        for entry in entries:
            game_id = entry["game_id"]
            season = entry["season"]

            if seasons and season not in seasons:
                continue
            if game_ids and game_id not in game_ids:
                continue

            try:
                res = _process_single_entry(entry, ctx)
                results.append(res)
                st = res.get("status")
                if st in ("valid", "saved"):
                    counts["valid"] += 1
                    if st == "saved":
                        counts["saved"] += 1
                elif st == "empty":
                    counts["empty"] += 1
                else:
                    counts["errors"] += 1
            except (OSError, ValueError, TypeError, KeyError, RuntimeError) as exc:
                results.append({"game_id": game_id, "season": season, "status": "error", "reason": f"exception: {exc}"})
                counts["errors"] += 1
    finally:
        if session:
            session.close()

    summary = {
        "manifest": str(manifest_path),
        "dry_run": dry_run,
        "total_entries": len(entries),
        "processed_entries": len(results),
        "valid": counts["valid"],
        "empty": counts["empty"],
        "errors": counts["errors"],
        "saved": counts["saved"],
    }

    return {"summary": summary, "games": results}


def main(argv: Sequence[str] | None = None) -> int:
    """Run historical boxscore manifest import CLI."""
    parser = argparse.ArgumentParser(description="Historical Boxscore Manifest Import CLI")
    parser.add_argument("--manifest", type=Path, required=True, help="Manifest file path (.csv or .json)")
    parser.add_argument("--dry-run", action="store_true", help="Validate without writing to database")
    parser.add_argument("--save", action="store_true", help="Write validated game boxscores to database")
    parser.add_argument(
        "--strict",
        action="store_true",
        default=True,
        help="Enforce complete hitter and pitcher rosters",
    )
    parser.add_argument("--season", action="append", type=int, dest="seasons", help="Filter by season year (e.g. 2001)")
    parser.add_argument("--game-ids", help="Comma-separated game IDs filter")
    parser.add_argument("--report-out", type=Path, help="Write JSON summary report to file")

    args = parser.parse_args(argv)

    if not args.dry_run and not args.save:
        parser.error("Must specify either --dry-run or --save")

    dry_run = args.dry_run or not args.save
    seasons_set = set(args.seasons) if args.seasons else None
    game_ids_set = {gid.strip() for gid in args.game_ids.split(",")} if args.game_ids else None

    report = process_historical_manifest(
        args.manifest,
        dry_run=dry_run,
        strict=args.strict,
        seasons=seasons_set,
        game_ids=game_ids_set,
    )

    rendered = json.dumps(report, ensure_ascii=False, indent=2)
    if args.report_out:
        args.report_out.parent.mkdir(parents=True, exist_ok=True)
        args.report_out.write_text(rendered + "\n", encoding="utf-8")
        logger.info("Boxscore import report written to %s", args.report_out)
    else:
        print(rendered)  # noqa: T201

    if report["summary"]["errors"] > 0 and args.strict:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
