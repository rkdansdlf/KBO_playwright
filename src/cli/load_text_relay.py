"""Load text relay CSV files into the database."""

from __future__ import annotations

import argparse
import csv
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import SessionLocal

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def _load_csv_file(csv_path: Path, session: Session) -> int:
    """
    Load a single text relay CSV into game_play_by_play.

    Return the number of rows inserted.

    Args:
        csv_path: Csv file path.
        session: Session.

    """
    from src.models.game import GamePlayByPlay

    game_id = csv_path.stem.replace("_text_relay", "")
    rows_inserted = 0

    try:
        with csv_path.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for idx, row in enumerate(reader):
                play = GamePlayByPlay(
                    game_id=game_id,
                    inning=row.get("inning"),
                    inning_half=row.get("inning_half"),
                    pitcher_name=row.get("pitcher_name"),
                    batter_name=row.get("batter_name"),
                    play_description=row.get("play_description", ""),
                    event_type=row.get("event_type"),
                    result=row.get("result"),
                    source_name="text_relay_csv",
                    source_row_index=idx,
                )
                session.add(play)
                rows_inserted += 1

        session.flush()
    except (OSError, csv.Error) as e:
        logger.warning("Failed to parse %s: %s", csv_path, e)
        session.rollback()
        return 0

    return rows_inserted


def _find_csv_files(input_dir: Path) -> list[Path]:
    """
    Find all text relay CSV files in the directory.

    Args:
        input_dir: Input Dir.

    """
    return sorted(input_dir.glob("*_text_relay.csv"))


def load_text_relays(
    input_dir: Path,
    *,
    dry_run: bool = False,
) -> dict[str, int]:
    """
    Load all text relay CSVs from a directory into the database.

    Return {game_id: rows_inserted}.

    Args:
        input_dir: Input Dir.
        dry_run: If True, performs a dry run without persisting changes.

    """
    csv_files = _find_csv_files(input_dir)

    if not csv_files:
        logger.info("No text relay CSV files found in %s", input_dir)
        return {}

    results: dict[str, int] = {}

    with SessionLocal() as session:
        for csv_path in csv_files:
            game_id = csv_path.stem.replace("_text_relay", "")
            logger.info("Loading %s...", csv_path.name)

            rows = _load_csv_file(csv_path, session)
            if rows > 0:
                results[game_id] = rows
                logger.info("  -> %d rows loaded", rows)
            else:
                logger.info("  -> skipped (0 rows or parse error)")

        if dry_run:
            session.rollback()
            logger.info("[DRY RUN] Rolled back all changes")
        else:
            try:
                session.commit()
                logger.info("Committed %d games", len(results))
            except SQLAlchemyError:
                session.rollback()
                logger.exception("DB commit failed")
                return {}

    return results


def build_arg_parser() -> argparse.ArgumentParser:
    """
    Build arg parser.

    Returns:
        The result of the operation.

    """
    parser = argparse.ArgumentParser(
        description="Load text relay CSV files into the database",
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data"),
        help="Directory containing text relay CSV files",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse files but don't write to DB",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    """
    Run the main entry point for this CLI command.

    Args:
        argv: Argv.

    """
    parser = build_arg_parser()

    args = parser.parse_args(argv)

    results = load_text_relays(args.input_dir, dry_run=args.dry_run)

    if results:
        total = sum(results.values())
        logger.info("Total: %d rows across %d games", total, len(results))
    else:
        logger.info("No data loaded")


if __name__ == "__main__":
    main()
