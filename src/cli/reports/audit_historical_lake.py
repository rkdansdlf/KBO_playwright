"""CLI to audit historical data lake."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import func, select

from src.db.engine import SessionLocal
from src.models.game import Game, GameBattingStat, GameInningScore, GameMetadata, GamePitchingStat
from src.models.quarantine import QuarantinedRecord

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


@dataclass
class SeasonLakeAuditRow:
    """Historical lake audit row for a season."""

    season: int
    games: int
    unique_games: int
    official_source_games: int
    inning_score_games: int
    batting_stat_games: int
    pitching_stat_games: int
    duplicates: int
    quarantined_records: int
    status: str

    def to_dict(self) -> dict[str, Any]:
        """Convert row to dictionary."""
        return asdict(self)


def audit_historical_lake(start_year: int, end_year: int) -> list[SeasonLakeAuditRow]:
    """Audit historical lake availability across years against database."""
    rows: list[SeasonLakeAuditRow] = []

    with SessionLocal() as session:
        for season in range(start_year, end_year + 1):
            prefix = f"{season}%"

            # 1. Total games
            games_count = session.execute(select(func.count(Game.id)).where(Game.game_id.like(prefix))).scalar() or 0

            # Unique games
            unique_games_count = len(
                list(
                    session.execute(select(Game.game_id).where(Game.game_id.like(prefix)).group_by(Game.game_id))
                    .scalars()
                    .all()
                )
            )

            # 2. Metadata / Provenance
            meta_count = (
                session.execute(
                    select(func.count(GameMetadata.game_id)).where(
                        GameMetadata.game_id.like(prefix),
                        GameMetadata.source_payload.is_not(None),
                    )
                ).scalar()
                or 0
            )

            # 3. Innings, Batting, Pitching
            inn_count = len(
                list(
                    session.execute(
                        select(GameInningScore.game_id)
                        .where(GameInningScore.game_id.like(prefix))
                        .group_by(GameInningScore.game_id)
                    )
                    .scalars()
                    .all()
                )
            )
            bat_count = len(
                list(
                    session.execute(
                        select(GameBattingStat.game_id)
                        .where(GameBattingStat.game_id.like(prefix))
                        .group_by(GameBattingStat.game_id)
                    )
                    .scalars()
                    .all()
                )
            )
            pit_count = len(
                list(
                    session.execute(
                        select(GamePitchingStat.game_id)
                        .where(GamePitchingStat.game_id.like(prefix))
                        .group_by(GamePitchingStat.game_id)
                    )
                    .scalars()
                    .all()
                )
            )

            # 4. Quarantined
            qr_count = len(
                list(
                    session.execute(
                        select(QuarantinedRecord.game_id)
                        .where(QuarantinedRecord.game_id.like(prefix))
                        .group_by(QuarantinedRecord.game_id)
                    )
                    .scalars()
                    .all()
                )
            )

            duplicates = max(0, games_count - unique_games_count)

            complete = (
                games_count > 0
                and unique_games_count == games_count
                and meta_count == games_count
                and inn_count == games_count
                and bat_count == games_count
                and pit_count == games_count
                and qr_count == 0
            )
            if games_count == 0:
                status = "EMPTY"
            elif complete:
                status = "VERIFIED_COMPLETE"
            else:
                status = "PARTIAL"

            rows.append(
                SeasonLakeAuditRow(
                    season=season,
                    games=games_count,
                    unique_games=unique_games_count,
                    official_source_games=meta_count,
                    inning_score_games=inn_count,
                    batting_stat_games=bat_count,
                    pitching_stat_games=pit_count,
                    duplicates=duplicates,
                    quarantined_records=qr_count,
                    status=status,
                )
            )

    return rows


def main(argv: Sequence[str] | None = None) -> int:
    """Run historical lake audit CLI."""
    parser = argparse.ArgumentParser(description="Audit Historical Data Lake Completeness")
    parser.add_argument("--start-year", type=int, default=1982, help="Start season year")
    parser.add_argument("--end-year", type=int, default=2000, help="End season year")
    parser.add_argument("--json", action="store_true", default=False, help="Output structured JSON")
    args = parser.parse_args(argv)

    rows = audit_historical_lake(args.start_year, args.end_year)

    if args.json:
        sys.stdout.write(json.dumps([r.to_dict() for r in rows], indent=2, ensure_ascii=False) + "\n")
    else:
        header = (
            f"{'season':>6} | {'games':>5} | {'official_src':>12} | {'inn_games':>10} | "
            f"{'bat_games':>10} | {'pit_games':>10} | {'duplicates':>10} | {'quarantined':>11} | {'status':<17}\n"
        )
        sys.stdout.write(header)
        sys.stdout.write("-" * 115 + "\n")
        for r in rows:
            line = (
                f"{r.season:>6} | {r.games:>5} | {r.official_source_games:>12} | {r.inning_score_games:>10} | "
                f"{r.batting_stat_games:>10} | {r.pitching_stat_games:>10} | {r.duplicates:>10} | "
                f"{r.quarantined_records:>11} | {r.status:<17}\n"
            )
            sys.stdout.write(line)

    return 0


if __name__ == "__main__":
    sys.exit(main())
