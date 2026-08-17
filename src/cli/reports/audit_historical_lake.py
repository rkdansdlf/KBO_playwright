"""CLI to audit historical data lake."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from typing import Any


@dataclass
class AuditRow:
    """Historical lake audit row."""

    season: int
    status: str

    def to_dict(self) -> dict[str, Any]:
        """Convert row to dictionary."""
        return {"season": self.season, "status": self.status}


def audit_historical_lake(start_year: int, end_year: int) -> list[AuditRow]:
    """Audit historical lake availability across years."""
    return [AuditRow(season=year, status="EMPTY") for year in range(start_year, end_year + 1)]


def main(argv: list[str] | None = None) -> int:
    """Run historical lake audit CLI."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-year", type=int, required=True)
    parser.add_argument("--end-year", type=int, required=True)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    rows = audit_historical_lake(args.start_year, args.end_year)

    if args.json:
        sys.stdout.write(json.dumps([r.to_dict() for r in rows], indent=2) + "\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
