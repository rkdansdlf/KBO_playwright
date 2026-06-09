"""
Aggregate player-level fielding & baserunning stats to team-season level.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy.orm import Session

from src.models.player import PlayerSeasonBaserunning, PlayerSeasonFielding
from src.models.team import TeamSeasonBaserunning, TeamSeasonFielding

logger = logging.getLogger(__name__)


class TeamFieldingAggregator:
    def __init__(self, session: Session) -> None:
        self.session = session

    def aggregate_fielding(self, season: int, team_code: str) -> dict[str, Any]:
        rows = (
            self.session.query(PlayerSeasonFielding)
            .filter(
                PlayerSeasonFielding.year == season,
                PlayerSeasonFielding.team_id == team_code,
            )
            .all()
        )

        total = {
            "errors": 0,
            "double_plays": 0,
            "putouts": 0,
            "assists": 0,
            "innings": 0,
            "total_chances": 0,
        }

        for r in rows:
            total["errors"] += r.errors or 0
            total["double_plays"] += r.double_plays or 0
            total["putouts"] += r.putouts or 0
            total["assists"] += r.assists or 0
            total["innings"] += r.innings or 0

        total["total_chances"] = total["putouts"] + total["assists"] + total["errors"]

        fielding_pct = (
            (total["putouts"] + total["assists"]) / total["total_chances"] if total["total_chances"] > 0 else None
        )

        rg = (total["putouts"] + total["assists"]) / (total["innings"] / 9) if total["innings"] > 0 else None

        return {
            "season": season,
            "team_code": team_code,
            "errors": total["errors"],
            "double_plays": total["double_plays"],
            "putouts": total["putouts"],
            "assists": total["assists"],
            "total_chances": total["total_chances"],
            "def_innings": total["innings"],
            "fielding_pct": round(fielding_pct, 4) if fielding_pct else None,
            "range_factor_per_game": round(rg, 3) if rg else None,
        }

    def aggregate_baserunning(self, season: int, team_code: str) -> dict[str, Any]:
        rows = (
            self.session.query(PlayerSeasonBaserunning)
            .filter(
                PlayerSeasonBaserunning.year == season,
                PlayerSeasonBaserunning.team_id == team_code,
            )
            .all()
        )

        total = {
            "stolen_bases": 0,
            "caught_stealing": 0,
            "out_on_base": 0,
            "picked_off": 0,
        }

        for r in rows:
            total["stolen_bases"] += r.stolen_bases or 0
            total["caught_stealing"] += r.caught_stealing or 0
            total["out_on_base"] += r.out_on_base or 0

        sb_rate = (
            total["stolen_bases"] / (total["stolen_bases"] + total["caught_stealing"])
            if (total["stolen_bases"] + total["caught_stealing"]) > 0
            else None
        )

        return {
            "season": season,
            "team_code": team_code,
            "stolen_bases": total["stolen_bases"],
            "caught_stealing": total["caught_stealing"],
            "sb_success_rate": round(sb_rate, 3) if sb_rate else None,
            "out_on_base": total["out_on_base"],
        }

    def run_all(self, season: int, team_codes: list[str]) -> None:
        from src.models.player import PlayerSeasonBaserunning, PlayerSeasonFielding
        from src.models.team import Team

        # Filter to teams that have actual player data for this season
        fielding_teams = {
            r[0]
            for r in self.session.query(PlayerSeasonFielding.team_id)
            .filter(PlayerSeasonFielding.year == season)
            .distinct()
            .all()
        }
        baserunning_teams = {
            r[0]
            for r in self.session.query(PlayerSeasonBaserunning.team_id)
            .filter(PlayerSeasonBaserunning.year == season)
            .distinct()
            .all()
        }
        active_in_db = fielding_teams | baserunning_teams

        # Filter to only KBO franchise teams (exclude All-Star, foreign teams without franchise_id)
        kbo_teams = {t.team_id for t in self.session.query(Team.team_id).filter(Team.franchise_id.isnot(None)).all()}

        valid_teams = [code for code in team_codes if code in active_in_db and code in kbo_teams]
        logger.info(
            "[TeamFieldingAggregator] Season %s filtering: original=%d teams -> valid=%d teams (%s)",
            season,
            len(team_codes),
            len(valid_teams),
            ", ".join(valid_teams),
        )

        for code in valid_teams:
            fdata = self.aggregate_fielding(season, code)
            existing = (
                self.session.query(TeamSeasonFielding)
                .filter(
                    TeamSeasonFielding.season == season,
                    TeamSeasonFielding.team_code == code,
                )
                .first()
            )
            if existing:
                for k, v in fdata.items():
                    if k not in ("season", "team_code"):
                        setattr(existing, k, v)
            else:
                self.session.add(TeamSeasonFielding(**fdata))

            br = self.aggregate_baserunning(season, code)
            existing_br = (
                self.session.query(TeamSeasonBaserunning)
                .filter(
                    TeamSeasonBaserunning.season == season,
                    TeamSeasonBaserunning.team_code == code,
                )
                .first()
            )
            if existing_br:
                for k, v in br.items():
                    if k not in ("season", "team_code"):
                        setattr(existing_br, k, v)
            else:
                self.session.add(TeamSeasonBaserunning(**br))

            logger.info(f"  {season} {code}: fielding={fdata.get('fielding_pct')}, sb_rate={br.get('sb_success_rate')}")

        self.session.commit()


if __name__ == "__main__":
    import argparse

    from src.db.engine import SessionLocal

    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--team", type=str, nargs="+")
    args = parser.parse_args()

    session = SessionLocal()
    try:
        from src.models.team import Team

        # Select all teams regardless of active status, run_all will filter based on historical presence
        teams = [t.team_id for t in session.query(Team.team_id).all()]
        if args.team:
            teams = [t for t in args.team if t in teams]

        agg = TeamFieldingAggregator(session)
        agg.run_all(args.year, teams)
    finally:
        session.close()
