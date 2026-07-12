"""P1 시드 데이터를 일괄 등록하는 CLI 스크립트 (좌석 + 주차 + 먹거리)."""

from __future__ import annotations

import argparse
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence


def run_seat(*, dry_run: bool = False) -> None:
    """Run seat.

    Args:
        dry_run: If True, performs a dry run without persisting changes.

    """
    from scripts.seed_seat_sections import run as seat_run

    seat_run(dry_run=dry_run)


def run_parking(*, dry_run: bool = False) -> None:
    """Run parking.

    Args:
        dry_run: If True, performs a dry run without persisting changes.

    """
    from scripts.seed_parking import run as parking_run

    parking_run(dry_run=dry_run)


def run_food(*, dry_run: bool = False) -> None:
    """Run food.

    Args:
        dry_run: If True, performs a dry run without persisting changes.

    """
    from scripts.seed_stadium_food import run as food_run

    food_run(dry_run=dry_run)


def run_all(*, dry_run: bool = False) -> None:
    """Run all.

    Args:
        dry_run: If True, performs a dry run without persisting changes.

    """
    run_seat(dry_run=dry_run)

    run_parking(dry_run=dry_run)
    run_food(dry_run=dry_run)


def build_arg_parser() -> argparse.ArgumentParser:
    """Build arg parser.

    Returns:
        The result of the operation.

    """
    parser = argparse.ArgumentParser(description="P1 seed data (seat + parking + food)")

    parser.add_argument(
        "--type",
        choices=["seat", "parking", "food", "all"],
        default="all",
        help="Data type to seed",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    """Run the main entry point for this CLI command.

    Args:
        argv: Argv.

    """
    parser = build_arg_parser()

    args = parser.parse_args(argv)
    runner_map = {
        "seat": lambda: run_seat(dry_run=args.dry_run),
        "parking": lambda: run_parking(dry_run=args.dry_run),
        "food": lambda: run_food(dry_run=args.dry_run),
        "all": lambda: run_all(dry_run=args.dry_run),
    }
    runner_map[args.type]()


if __name__ == "__main__":
    main()
