"""Validate historical relay manifests without writing to the database."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import TYPE_CHECKING

from src.sources.relay.base import ManifestEntry, NormalizedRelayResult, read_manifest_entries
from src.sources.relay.importer import ImportRelayAdapter

if TYPE_CHECKING:
    from collections.abc import Sequence


def _parse_game_ids(value: str | None) -> set[str] | None:
    if not value:
        return None
    return {token.strip() for token in value.split(",") if token.strip()}


def _entry_season(entry: ManifestEntry) -> int | None:
    if entry.season is not None:
        return entry.season
    year = entry.game_id[:4]
    return int(year) if year.isdigit() else None


def _select_entries(
    entries: list[ManifestEntry],
    *,
    game_ids: set[str] | None,
    seasons: set[int] | None,
) -> list[ManifestEntry]:
    return [
        entry
        for entry in entries
        if (game_ids is None or entry.game_id in game_ids) and (seasons is None or _entry_season(entry) in seasons)
    ]


def _game_report(
    entry_group: list[ManifestEntry],
    result: NormalizedRelayResult | None,
    error: str | None = None,
) -> dict[str, object]:
    game_id = entry_group[0].game_id
    if error:
        status = "error"
    elif result is None or result.is_empty:
        status = "empty"
    else:
        status = "valid"
    return {
        "game_id": game_id,
        "season": _entry_season(entry_group[0]),
        "manifest_entries": len(entry_group),
        "status": status,
        "event_rows": len(result.events) if result else 0,
        "pbp_rows": len(result.raw_pbp_rows) if result else 0,
        "notes": result.notes if result else None,
        "error": error,
    }


async def _build_game_reports(
    entries: list[ManifestEntry],
    *,
    manifest_base_dir: Path,
) -> list[dict[str, object]]:
    grouped: dict[str, list[ManifestEntry]] = {}
    for entry in entries:
        grouped.setdefault(entry.game_id, []).append(entry)
    adapter = ImportRelayAdapter(entries, manifest_base_dir=manifest_base_dir)
    reports: list[dict[str, object]] = []
    for game_id in sorted(grouped):
        entry_group = grouped[game_id]
        try:
            result = await adapter.fetch_game(game_id)
        except (OSError, TypeError, UnicodeError, ValueError) as exc:
            reports.append(_game_report(entry_group, None, str(exc)))
        else:
            reports.append(_game_report(entry_group, result))
    return reports


def build_dry_run_report(
    manifest_path: Path,
    *,
    game_ids: set[str] | None = None,
    seasons: set[int] | None = None,
) -> dict[str, object]:
    """Parse a historical manifest and return a no-write validation report."""
    if not manifest_path.is_file():
        message = f"Manifest file not found: {manifest_path}"
        raise FileNotFoundError(message)
    entries = read_manifest_entries(manifest_path)
    selected_entries = _select_entries(entries, game_ids=game_ids, seasons=seasons)
    games = asyncio.run(_build_game_reports(selected_entries, manifest_base_dir=manifest_path.resolve().parent))
    summary = {
        "selected_games": len(games),
        "valid": sum(item["status"] == "valid" for item in games),
        "empty": sum(item["status"] == "empty" for item in games),
        "errors": sum(item["status"] == "error" for item in games),
    }
    return {
        "manifest": str(manifest_path),
        "dry_run": True,
        "manifest_entries": len(selected_entries),
        "summary": summary,
        "games": games,
    }


def main(argv: Sequence[str] | None = None) -> int:
    """Run historical manifest validation in dry-run mode."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--dry-run", action="store_true", help="Validate only; never write to the database")
    parser.add_argument("--game-ids", help="Comma-separated game IDs to include")
    parser.add_argument("--season", action="append", type=int, dest="seasons")
    parser.add_argument("--strict", action="store_true", help="Return 1 when any selected game is empty or invalid")
    parser.add_argument("--report-out", type=Path, help="Write JSON report to this path")
    args = parser.parse_args(argv)
    if not args.dry_run:
        parser.error("--dry-run is required; apply mode is not supported")

    try:
        report = build_dry_run_report(
            args.manifest,
            game_ids=_parse_game_ids(args.game_ids),
            seasons=set(args.seasons) if args.seasons else None,
        )
    except (FileNotFoundError, ValueError) as exc:
        parser.error(str(exc))

    rendered = json.dumps(report, ensure_ascii=False, indent=2)
    if args.report_out:
        args.report_out.parent.mkdir(parents=True, exist_ok=True)
        args.report_out.write_text(rendered + "\n", encoding="utf-8")
        print(f"Historical import dry-run: {args.report_out}")  # noqa: T201
    else:
        print(rendered)  # noqa: T201
    summary = report["summary"]
    if args.strict and isinstance(summary, dict) and (summary["empty"] or summary["errors"]):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
