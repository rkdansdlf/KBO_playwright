"""Repair the known 2020 regular-season season-id misclassification.

The 2020 schedule import assigned the regular-season games from 2020-05-05
through 2020-10-31 to the exhibition season row. This read-only-by-default
tool moves only that bounded set to the canonical regular-season row.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict, dataclass

from sqlalchemy import create_engine, text


@dataclass(frozen=True)
class SeasonIdRepair:
    """Describe one bounded season-id repair rule."""

    year: int
    source_season_id: int
    target_season_id: int
    start_date: str
    end_date_exclusive: str


REPAIRS = {
    2020: SeasonIdRepair(
        year=2020,
        source_season_id=230,
        target_season_id=229,
        start_date="2020-05-05",
        end_date_exclusive="2020-11-01",
    ),
}


def _repair_for_year(year: int) -> SeasonIdRepair:
    try:
        return REPAIRS[year]
    except KeyError as exc:
        message = f"No bounded season-id repair is defined for {year}"
        raise ValueError(message) from exc


def _validate_target(conn, repair: SeasonIdRepair) -> None:
    row = (
        conn.execute(
            text(
                "SELECT season_year, league_type_code FROM kbo_seasons WHERE season_id = :season_id",
            ),
            {"season_id": repair.target_season_id},
        )
        .mappings()
        .first()
    )
    if not row:
        message = f"Target season_id {repair.target_season_id} does not exist"
        raise ValueError(message)
    if row["season_year"] != repair.year or row["league_type_code"] != 0:
        message = f"Target season_id {repair.target_season_id} is not the {repair.year} regular season"
        raise ValueError(message)


def build_repair_plan(conn, year: int) -> dict[str, object]:
    """Build a read-only count of rows affected by the bounded repair."""
    repair = _repair_for_year(year)
    _validate_target(conn, repair)
    row = (
        conn.execute(
            text(
                "SELECT COUNT(*) AS total, "
                "SUM(CASE WHEN game_status IN ('COMPLETED', 'DRAW') THEN 1 ELSE 0 END) AS terminal "
                "FROM game "
                "WHERE season_id = :source_season_id "
                "AND game_date >= :start_date "
                "AND game_date < :end_date_exclusive",
            ),
            asdict(repair),
        )
        .mappings()
        .one()
    )
    return {
        **asdict(repair),
        "candidate_rows": int(row["total"] or 0),
        "candidate_terminal_rows": int(row["terminal"] or 0),
    }


def apply_repair(conn, year: int) -> dict[str, object]:
    """Apply the bounded repair and return the affected-row count."""
    plan = build_repair_plan(conn, year)
    result = conn.execute(
        text(
            "UPDATE game SET season_id = :target_season_id "
            "WHERE season_id = :source_season_id "
            "AND game_date >= :start_date "
            "AND game_date < :end_date_exclusive",
        ),
        plan,
    )
    return {**plan, "updated_rows": int(result.rowcount or 0)}


def main(argv: list[str] | None = None) -> int:
    """Plan or apply the bounded season-id repair."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL", "sqlite:///./data/kbo_dev.db"),
    )
    parser.add_argument("--apply", action="store_true", help="Persist the repair; default is dry-run")
    args = parser.parse_args(argv)

    engine = create_engine(args.database_url)
    with engine.begin() as conn:
        if args.apply:
            report = apply_repair(conn, args.year)
        else:
            report = build_repair_plan(conn, args.year)
    report["dry_run"] = not args.apply
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
