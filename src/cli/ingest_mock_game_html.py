"""Ingest saved GameCenter HTML fixtures into the database."""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

from src.parsers.game_detail_parser import parse_game_detail_html
from src.repositories.game_repository import GameRepository


def ingest_mock_html(args: argparse.Namespace) -> None:
    repo = GameRepository()
    fixtures_dir = Path(args.fixtures_dir)
    if not fixtures_dir.exists():
        raise SystemExit(f"Fixture directory not found: {fixtures_dir}")

    files = sorted(fixtures_dir.glob("*.html"))
    if args.limit:
        files = files[: args.limit]

    if not files:
        print("ℹ️  No HTML fixtures found. Place files like 20251001NCLG0.html in the directory.")
        return

    for html_file in files:
        game_id = html_file.stem
        with html_file.open("r", encoding="utf-8") as f:
            html = f.read()
        game_date = game_id[:8]
        payload = parse_game_detail_html(html, game_id, game_date)
        success = repo.save_game_detail(payload)
        if success:
            print(f"✅ Ingested mock game {game_id}")
        else:
            print(f"❌ Failed to ingest mock game {game_id}")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest saved GameCenter HTML fixtures")
    parser.add_argument("--fixtures-dir", type=str, default="tests/fixtures/game_details")
    parser.add_argument("--limit", type=int, default=None)
    return parser


def main(argv: Iterable[str] | None = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    ingest_mock_html(args)


if __name__ == "__main__":  # pragma: no cover
    main()

