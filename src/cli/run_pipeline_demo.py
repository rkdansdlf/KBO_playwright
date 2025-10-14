"""End-to-end pipeline demo using offline fixtures."""
from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
from typing import Iterable, Optional

from src.parsers.schedule_parser import parse_schedule_html
from src.parsers.game_detail_parser import parse_game_detail_html
from src.repositories.game_repository import GameRepository


def ingest_schedule_fixtures(fixtures_dir: Path, season_type: str, default_year: Optional[int]) -> int:
    repo = GameRepository()
    total = 0
    for html_file in sorted(fixtures_dir.glob("*.html")):
        html = html_file.read_text(encoding="utf-8")
        rows = parse_schedule_html(html, default_year=default_year, season_type=season_type)
        if not rows:
            continue
        repo.save_schedules(rows)
        total += len(rows)
        print(f"✅ Schedule ingest: {html_file.name} ({len(rows)} games)")
    return total


def ingest_game_fixtures(fixtures_dir: Path) -> int:
    repo = GameRepository()
    count = 0
    for html_file in sorted(fixtures_dir.glob("*.html")):
        game_id = html_file.stem
        html = html_file.read_text(encoding="utf-8")
        payload = parse_game_detail_html(html, game_id, game_id[:8])
        if repo.save_game_detail(payload):
            count += 1
            print(f"✅ Game ingest: {game_id}")
    return count


async def run_futures(limit: Optional[int], season: int, delay: float, concurrency: int) -> None:
    from src.cli.crawl_futures import crawl_futures

    args = argparse.Namespace(
        season=season,
        concurrency=concurrency,
        delay=delay,
        limit=limit,
    )
    await crawl_futures(args)


def show_summary(game_ids: list[str]) -> None:
    repo = GameRepository()
    counts = repo.count_schedules_by_type()
    print("\n📊 Schedule totals:")
    for season_type, count in sorted(counts.items()):
        print(f"  - {season_type}: {count}")

    for game_id in game_ids:
        summary = repo.get_game_summary(game_id)
        print(f"\n🎯 Game summary: {game_id}")
        schedule = summary["schedule"]
        game = summary["game"]
        if schedule:
            print(f"  Season type: {schedule.season_type}")
            print(f"  Game date:  {schedule.game_date}")
        else:
            print("  Schedule: not found")
        if game:
            print(f"  Stored scores: away {game.away_score} / home {game.home_score}")
        else:
            print("  Game detail: not ingested")
        print(f"  Batting rows:  {summary['batting_rows']}")
        print(f"  Pitching rows: {summary['pitching_rows']}")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="End-to-end pipeline demo using saved fixtures.")
    parser.add_argument("--schedule-fixtures", type=str, default=None, help="Directory containing schedule HTML fixtures")
    parser.add_argument("--schedule-season-type", type=str, default="regular", choices=["preseason", "regular", "postseason"])
    parser.add_argument("--schedule-year", type=int, default=None)
    parser.add_argument("--game-fixtures", type=str, default=None, help="Directory containing game detail HTML fixtures")
    parser.add_argument("--report-game-id", action="append", default=[], help="Game ID to summarize after ingest")
    parser.add_argument("--run-futures", action="store_true", help="Run futures crawler after ingest")
    parser.add_argument("--futures-limit", type=int, default=None)
    parser.add_argument("--futures-season", type=int, default=None)
    parser.add_argument("--futures-delay", type=float, default=1.5)
    parser.add_argument("--futures-concurrency", type=int, default=3)
    return parser


def main(argv: Iterable[str] | None = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.schedule_fixtures:
        fixtures_dir = Path(args.schedule_fixtures)
        if not fixtures_dir.exists():
            raise SystemExit(f"Schedule fixtures directory not found: {fixtures_dir}")
        total = ingest_schedule_fixtures(fixtures_dir, args.schedule_season_type, args.schedule_year)
        print(f"\n✅ Schedule ingest complete ({total} rows processed)")

    game_ids = list(args.report_game_id)
    if args.game_fixtures:
        game_dir = Path(args.game_fixtures)
        if not game_dir.exists():
            raise SystemExit(f"Game fixtures directory not found: {game_dir}")
        ingested = ingest_game_fixtures(game_dir)
        print(f"\n✅ Game detail ingest complete ({ingested} files)")
        if ingested and not game_ids:
            game_ids = [path.stem for path in sorted(game_dir.glob("*.html"))]

    if args.run_futures:
        season = args.futures_season
        if season is None:
            from datetime import datetime

            season = datetime.now().year
        asyncio.run(run_futures(args.futures_limit, season, args.futures_delay, args.futures_concurrency))

    if game_ids:
        show_summary(game_ids)
    else:
        repo = GameRepository()
        counts = repo.count_schedules_by_type()
        print("\n📊 Schedule totals:")
        for season_type, count in sorted(counts.items()):
            print(f"  - {season_type}: {count}")


if __name__ == "__main__":  # pragma: no cover
    main()

