"""Aggregate player batting/pitching home/away splits from game-level stats."""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import TYPE_CHECKING

from src.constants import KST
from src.models.game import Game, GameBattingStat
from src.models.matchup import BatterHomeAwaySplit
from src.models.season import KboSeason
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class HomeAwaySplitAggregator:
    """HomeAwaySplitAggregator class."""

    def __init__(self, session: Session) -> None:
        """
        Initialize a new instance.

        Args:
            session: Session.

        """
        self.session = session

    def aggregate_batting(self, year: int) -> list[dict]:
        """
        Aggregate batting.

        Args:
            year: Season year.
            year: Season year.

        Returns:
            List of results.

        """
        rows = (
            self.session.query(GameBattingStat, Game.away_team, Game.home_team)
            .join(Game, GameBattingStat.game_id == Game.game_id)
            .join(KboSeason, Game.season_id == KboSeason.season_id)
            .filter(
                KboSeason.season_year == year,
                KboSeason.league_type_name.in_(["정규시즌", "Regular Season"]),
                Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
            )
            .all()
        )

        splits: dict = defaultdict(lambda: {"HOME": defaultdict(int), "AWAY": defaultdict(int)})

        for stat, _away_team, home_team in rows:
            if not stat.player_id or not stat.team_code:
                continue
            location = "HOME" if stat.team_code == home_team else "AWAY"
            s = splits[(stat.player_id, year)][location]

            s["games"] += 1
            s["plate_appearances"] += stat.plate_appearances or 0
            s["at_bats"] += stat.at_bats or 0
            s["hits"] += stat.hits or 0
            s["doubles"] += stat.doubles or 0
            s["triples"] += stat.triples or 0
            s["home_runs"] += stat.home_runs or 0
            s["rbi"] += stat.rbi or 0
            s["walks"] += stat.walks or 0
            s["strikeouts"] += stat.strikeouts or 0
            s["stolen_bases"] += stat.stolen_bases or 0
            s["caught_stealing"] += stat.caught_stealing or 0
            s["hbp"] += stat.hbp or 0
            s["sacrifice_flies"] += stat.sacrifice_flies or 0

        results = []
        for (pid, season), locs in splits.items():
            for location, stats in locs.items():
                ab = stats["at_bats"]
                h = stats["hits"]
                d2 = stats["doubles"]
                d3 = stats["triples"]
                hr = stats["home_runs"]
                bb = stats["walks"]
                sf = stats["sacrifice_flies"]
                hbp = stats["hbp"]
                tb = h + d2 + 2 * d3 + 3 * hr

                avg = round(h / ab, 3) if ab > 0 else 0.0
                obp = round((h + bb + hbp) / (ab + bb + hbp + sf), 3) if (ab + bb + hbp + sf) > 0 else 0.0
                slg = round(tb / ab, 3) if ab > 0 else 0.0
                ops = round(obp + slg, 3)

                results.append(
                    {
                        "player_id": pid,
                        "season_year": season,
                        "location": location,
                        "games": stats["games"],
                        "plate_appearances": stats["plate_appearances"],
                        "at_bats": ab,
                        "hits": h,
                        "doubles": d2,
                        "triples": d3,
                        "home_runs": hr,
                        "rbi": stats["rbi"],
                        "walks": bb,
                        "strikeouts": stats["strikeouts"],
                        "stolen_bases": stats["stolen_bases"],
                        "caught_stealing": stats["caught_stealing"],
                        "hbp": hbp,
                        "sacrifice_flies": sf,
                        "avg": avg,
                        "obp": obp,
                        "slg": slg,
                        "ops": ops,
                    },
                )
        return results

    def persist_batting(self, year: int) -> None:
        """
        Handle the persist batting operation.

        Args:
            year: Season year.
            year: Season year.

        """
        results = self.aggregate_batting(year)

        self.session.query(BatterHomeAwaySplit).filter(BatterHomeAwaySplit.season_year == year).delete(
            synchronize_session=False,
        )

        for r in results:
            self.session.add(BatterHomeAwaySplit(**r))
        self.session.commit()
        logger.info("[HomeAway] %s batting split rows saved for %s.", len(results), year)

    def print_report(self, year: int, top_n: int = 5) -> None:
        """
        Print print report.

        Args:
            year: Season year.
            top_n: Top N.
            year: Season year.
            top_n: Top N.

        """
        results = self.aggregate_batting(year)

        if not results:
            return

        players: dict = {}
        for r in results:
            pid = r["player_id"]
            if pid not in players:
                players[pid] = {}
            players[pid][r["location"]] = r

        logger.info("\n%s", "=" * 70)
        logger.info("  KBO %s년 홈/원정 OPS 차이 Top %s", year, top_n)
        logger.info("%s", "=" * 70)
        logger.info("%9s %7s %8s %6s", "PlayerID", "홈OPS", "원정OPS", "차이")
        logger.info("%s", "-" * 70)

        diffs = []
        for pid, locs in players.items():
            home = locs.get("HOME", {})
            away = locs.get("AWAY", {})
            home_ops = home.get("ops", 0)
            away_ops = away.get("ops", 0)
            diff = round(home_ops - away_ops, 3)
            diffs.append((pid, home_ops, away_ops, diff))

        diffs.sort(key=lambda x: abs(x[3]), reverse=True)
        diffs = [
            (pid, h, a, d)
            for pid, h, a, d in diffs
            if players[pid].get("HOME", {}).get("plate_appearances", 0) >= 50
            and players[pid].get("AWAY", {}).get("plate_appearances", 0) >= 50
        ]
        for pid, h_ops, a_ops, diff in diffs[:top_n]:
            logger.info("  %9s %7.3f %8.3f %+6.3f", pid, h_ops, a_ops, diff)


if __name__ == "__main__":
    import argparse
    from datetime import datetime

    from src.db.engine import SessionLocal

    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=datetime.now(KST).year)
    parser.add_argument("--report", action="store_true")
    parser.add_argument("--persist", action="store_true")
    args = parser.parse_args()

    with SessionLocal() as session:
        agg = HomeAwaySplitAggregator(session)
        if args.persist:
            agg.persist_batting(args.year)
        if args.report:
            agg.print_report(args.year)
