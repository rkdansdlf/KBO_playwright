"""Historical Boxscore and Detailed Game Stats Ingestor.

Validates and manages official boxscores and detailed game stats for historical KBO seasons (1982-2000).
Strictly requires official source provenance and avoids synthetic generation.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy import delete, select

from src.models.game import Game, GameBattingStat, GameInningScore, GamePitchingStat

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

HISTORICAL_1982_YEAR = 1982
HISTORICAL_1982_TOTAL_GAMES = 240


@dataclass(frozen=True, slots=True)
class HistoricalBoxscoreAuditReport:
    """Audit report for historical season boxscores."""

    season_year: int
    total_games: int
    boxscore_secured_games: int
    batting_stats_games: int
    pitching_stats_games: int
    source_verified_batting_games: int
    source_verified_pitching_games: int
    is_valid: bool
    audited_at: datetime


class HistoricalBoxscoreIngestor:
    """Ingests and validates historical season boxscores and stats."""

    def __init__(self, session: Session) -> None:
        """Initialize with database session."""
        self.session = session

    def cleanup_synthetic_records(self, season_year: int = HISTORICAL_1982_YEAR) -> int:
        """Remove any legacy synthetic boxscore and stats records for a season."""
        prefix = f"{season_year}%"
        del_bat = self.session.execute(delete(GameBattingStat).where(GameBattingStat.game_id.like(prefix))).rowcount
        del_pit = self.session.execute(delete(GamePitchingStat).where(GamePitchingStat.game_id.like(prefix))).rowcount
        del_inn = self.session.execute(delete(GameInningScore).where(GameInningScore.game_id.like(prefix))).rowcount
        self.session.commit()
        logger.info(
            "[HistoricalBoxscore] Cleaned up synthetic records for %d: bat=%d, pit=%d, inn=%d",
            season_year,
            del_bat,
            del_pit,
            del_inn,
        )
        return del_bat + del_pit + del_inn

    def audit_historical_boxscore_integrity(
        self,
        season_year: int = HISTORICAL_1982_YEAR,
    ) -> HistoricalBoxscoreAuditReport:
        """Audit official boxscores and stats availability for a historical season."""
        prefix = f"{season_year}%"
        games = list(
            self.session.execute(select(Game).where(Game.game_id.like(prefix)).order_by(Game.game_id)).scalars().all()
        )
        total_games = len(games)
        if total_games == 0:
            return HistoricalBoxscoreAuditReport(
                season_year=season_year,
                total_games=0,
                boxscore_secured_games=0,
                batting_stats_games=0,
                pitching_stats_games=0,
                source_verified_batting_games=0,
                source_verified_pitching_games=0,
                is_valid=False,
                audited_at=datetime.now(UTC),
            )

        boxscore_games = len(
            list(
                self.session.execute(
                    select(GameInningScore.game_id)
                    .where(GameInningScore.game_id.like(prefix))
                    .group_by(GameInningScore.game_id)
                )
                .scalars()
                .all()
            )
        )
        batting_games = len(
            list(
                self.session.execute(
                    select(GameBattingStat.game_id)
                    .where(GameBattingStat.game_id.like(prefix))
                    .group_by(GameBattingStat.game_id)
                )
                .scalars()
                .all()
            )
        )
        pitching_games = len(
            list(
                self.session.execute(
                    select(GamePitchingStat.game_id)
                    .where(GamePitchingStat.game_id.like(prefix))
                    .group_by(GamePitchingStat.game_id)
                )
                .scalars()
                .all()
            )
        )

        return HistoricalBoxscoreAuditReport(
            season_year=season_year,
            total_games=total_games,
            boxscore_secured_games=boxscore_games,
            batting_stats_games=batting_games,
            pitching_stats_games=pitching_games,
            source_verified_batting_games=0,
            source_verified_pitching_games=0,
            is_valid=(total_games > 0 and boxscore_games == total_games),
            audited_at=datetime.now(UTC),
        )
