"""Statistical quality gate for KBO data."""
from __future__ import annotations

from typing import Any, Dict, List

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from src.models.game import Game, GameBattingStat, GamePitchingStat
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES


class QualityGate:
    """Validate consistency between cumulative and game-by-game records."""

    def __init__(self, session: Session):
        self.session = session

    def _get_regular_season_ids(self, year: int) -> List[int]:
        """Fetch season_ids that correspond to Regular Season (league_type_code=0)."""
        stmt = text("SELECT season_id FROM kbo_seasons WHERE season_year = :year AND league_type_code = 0")
        result = self.session.execute(stmt, {"year": year}).scalars().all()
        return [int(r) for r in result]

    def _result(
        self,
        *,
        season: int,
        league: str,
        checked_players: int = 0,
        mismatches: List[Dict[str, Any]] | None = None,
        error: str | None = None,
    ) -> Dict[str, Any]:
        mismatches = mismatches or []
        return {
            "season": season,
            "league": league,
            "checked_players": checked_players,
            "mismatches": mismatches,
            "ok": not error and len(mismatches) == 0,
            "error": error,
        }

    def validate_season_batting(self, season: int, league: str = "REGULAR") -> Dict[str, Any]:
        """
        Compare PlayerSeasonBatting (cumulative) with GameBattingStat sum (transactional).
        """
        if league != "REGULAR":
            return self._result(season=season, league=league)

        reg_season_ids = self._get_regular_season_ids(season)
        if not reg_season_ids:
            return self._result(
                season=season,
                league=league,
                error=f"No Regular Season IDs found for {season}",
            )

        # 1. Get cumulative totals per player
        cumulative_stmt = select(
            PlayerSeasonBatting.player_id,
            PlayerSeasonBatting.plate_appearances,
            PlayerSeasonBatting.hits,
            PlayerSeasonBatting.runs,
            PlayerSeasonBatting.home_runs,
        ).where(
            PlayerSeasonBatting.season == season,
            PlayerSeasonBatting.league == league,
        )
        cumulative_data = self.session.execute(cumulative_stmt).all()
        cumulative_map = {row.player_id: row for row in cumulative_data}

        # 2. Get transactional totals per player
        transactional_stmt = select(
            GameBattingStat.player_id,
            func.sum(GameBattingStat.plate_appearances).label("pa"),
            func.sum(GameBattingStat.hits).label("hits"),
            func.sum(GameBattingStat.runs).label("runs"),
            func.sum(GameBattingStat.home_runs).label("hr"),
        ).join(
            Game, Game.game_id == GameBattingStat.game_id,
        ).where(
            Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
            Game.season_id.in_(reg_season_ids),
        ).group_by(GameBattingStat.player_id)

        transactional_data = self.session.execute(transactional_stmt).all()
        mismatches = []
        for row in transactional_data:
            pid = row.player_id
            if pid not in cumulative_map:
                mismatches.append(
                    {
                        "player_id": pid,
                        "issue": "Missing cumulative record",
                        "transactional": {"pa": row.pa, "hits": row.hits},
                    }
                )
                continue

            cum = cumulative_map[pid]
            # Allow 0.5% tolerance or small absolute diff (1-2 units)
            # because KBO site sometimes has sync delay between summary and detail
            diff = (row.pa or 0) - (cum.plate_appearances or 0)
            if diff > 2 and diff / (cum.plate_appearances or 1) > 0.005:
                mismatches.append(
                    {
                        "player_id": pid,
                        "issue": "Transactional PA > Cumulative PA",
                        "cumulative": cum.plate_appearances,
                        "transactional": row.pa,
                    }
                )

        return self._result(
            season=season,
            league=league,
            checked_players=len(transactional_data),
            mismatches=mismatches,
        )

    def validate_season_pitching(self, season: int, league: str = "REGULAR") -> Dict[str, Any]:
        """
        Compare PlayerSeasonPitching (cumulative) with GamePitchingStat sum (transactional).
        """
        if league != "REGULAR":
            return self._result(season=season, league=league)

        reg_season_ids = self._get_regular_season_ids(season)
        if not reg_season_ids:
            return self._result(
                season=season,
                league=league,
                error=f"No Regular Season IDs found for {season}",
            )

        cumulative_stmt = select(
            PlayerSeasonPitching.player_id,
            PlayerSeasonPitching.innings_outs,
            PlayerSeasonPitching.innings_pitched,
            PlayerSeasonPitching.extra_stats,
            PlayerSeasonPitching.wins,
            PlayerSeasonPitching.strikeouts,
        ).where(
            PlayerSeasonPitching.season == season,
            PlayerSeasonPitching.league == league,
        )
        cumulative_data = self.session.execute(cumulative_stmt).all()
        cumulative_map = {row.player_id: row for row in cumulative_data}

        transactional_stmt = select(
            GamePitchingStat.player_id,
            func.sum(GamePitchingStat.innings_outs).label("outs"),
            func.sum(GamePitchingStat.wins).label("wins"),
            func.sum(GamePitchingStat.strikeouts).label("so"),
        ).join(
            Game, Game.game_id == GamePitchingStat.game_id,
        ).where(
            Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
            Game.season_id.in_(reg_season_ids),
        ).group_by(GamePitchingStat.player_id)

        transactional_data = self.session.execute(transactional_stmt).all()
        mismatches = []
        for row in transactional_data:
            pid = row.player_id
            if pid not in cumulative_map:
                mismatches.append(
                    {
                        "player_id": pid,
                        "issue": "Missing cumulative record",
                        "transactional": {"outs": row.outs, "wins": row.wins},
                    }
                )
                continue

            cum = cumulative_map[pid]

            # Resolve innings_outs with fallbacks
            cum_outs = cum.innings_outs
            if cum_outs is None:
                if cum.extra_stats and "innings_outs" in cum.extra_stats:
                    cum_outs = int(cum.extra_stats["innings_outs"])
                elif cum.innings_pitched is not None:
                    # 8.33 -> 8*3 + 1 = 25
                    ip = float(cum.innings_pitched)
                    whole = int(ip)
                    frac = round((ip - whole) * 100)
                    if frac == 33:
                        cum_outs = whole * 3 + 1
                    elif frac == 66:
                        cum_outs = whole * 3 + 2
                    else:
                        cum_outs = whole * 3

            diff = (row.outs or 0) - (cum_outs or 0)
            if diff > 3 and (cum_outs is None or diff / (cum_outs or 1) > 0.01):
                mismatches.append(
                    {
                        "player_id": pid,
                        "issue": "Transactional Outs > Cumulative Outs",
                        "cumulative": cum_outs,
                        "transactional": row.outs,
                    }
                )

        return self._result(
            season=season,
            league=league,
            checked_players=len(transactional_data),
            mismatches=mismatches,
        )


def run_quality_gate(session: Session, season: int) -> Dict[str, Any]:
    gate = QualityGate(session)
    batting_result = gate.validate_season_batting(season)
    pitching_result = gate.validate_season_pitching(season)

    return {
        "batting": batting_result,
        "pitching": pitching_result,
        "ok": batting_result.get("ok", False) and pitching_result.get("ok", False),
    }
