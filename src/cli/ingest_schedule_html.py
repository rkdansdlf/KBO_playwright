"""
CLI to ingest saved schedule HTML files into the database.
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, List, Dict, Any

from src.parsers.schedule_parser import parse_schedule_html
from src.repositories.game_repository import GameRepository


def ingest_schedule_html(args: argparse.Namespace) -> None:
    fixtures_dir = Path(args.fixtures_dir)
    if not fixtures_dir.exists():
        raise SystemExit(f"Fixture directory not found: {fixtures_dir}")

    repo = GameRepository()
    all_games: List[Dict[str, Any]] = []

    files = sorted(fixtures_dir.glob("*.html"))
    if not files:
        print("ℹ️  No HTML files found. Save schedule pages as *.html first.")
        return

    for html_file in files:
        html = html_file.read_text(encoding="utf-8")
        games = parse_schedule_html(
            html,
            default_year=args.default_year,
            season_type=args.season_type,
        )
        all_games.extend(games)
        print(f"📄 Parsed {len(games)} games from {html_file.name}")

    if not all_games:
        print("ℹ️  No games parsed from fixtures.")
        return

    repo.save_schedules(all_games)
    print(f"✅ Ingested {len(all_games)} games from fixtures.")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest saved schedule HTML files.")
    parser.add_argument(
        "--fixtures-dir",
        type=str,
        default="tests/fixtures/schedules",
        help="Directory containing saved schedule HTML files.",
    )
    parser.add_argument(
        "--default-year",
        type=int,
        default=None,
        help="Fallback season year if it cannot be inferred from game_id.",
    )
    parser.add_argument(
        "--season-type",
        type=str,
        default="regular",
        choices=["preseason", "regular", "postseason"],
        help="Season type label applied to ingested games.",
    )
    return parser


def main(argv: Iterable[str] | None = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    ingest_schedule_html(args)


if __name__ == "__main__":  # pragma: no cover
    main()
