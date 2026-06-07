import logging

logger = logging.getLogger(__name__)
import argparse

from sqlalchemy import func

from src.aggregators.season_stat_aggregator import SeasonStatAggregator
from src.db.engine import SessionLocal
from src.models.game import GameBattingStat, GamePitchingStat
from src.repositories.player_season_pitching_repository import save_pitching_stats_to_db
from src.repositories.player_stats_repository import PlayerSeasonBaserunningRepository, PlayerSeasonFieldingRepository
from src.repositories.safe_batting_repository import save_batting_stats_safe


def backfill_stats(years: list[int], series: str):
    fielding_repo = PlayerSeasonFieldingRepository()
    baserun_repo = PlayerSeasonBaserunningRepository()

    with SessionLocal() as session:
        for year in years:
            logger.info(f"🛠️  Backfilling Advanced Stats for {year} {series}...")

            # 1. Resolve team_id for all players in this season (most frequent team)
            team_map = {}
            team_query = (
                session.query(
                    GameBattingStat.player_id, GameBattingStat.team_code, func.count(GameBattingStat.id).label("cnt")
                )
                .group_by(GameBattingStat.player_id, GameBattingStat.team_code)
                .all()
            )
            for pid, team, cnt in team_query:
                if pid not in team_map or cnt > team_map[pid][1]:
                    team_map[pid] = (team, cnt)

            # Pitching team map
            p_team_query = (
                session.query(
                    GamePitchingStat.player_id, GamePitchingStat.team_code, func.count(GamePitchingStat.id).label("cnt")
                )
                .group_by(GamePitchingStat.player_id, GamePitchingStat.team_code)
                .all()
            )
            for pid, team, cnt in p_team_query:
                if pid not in team_map or cnt > team_map[pid][1]:
                    team_map[pid] = (team, cnt)

            # 2. Batting Backfill
            bat_stats = SeasonStatAggregator.aggregate_batting_season_bulk(
                session, year, series, source="FALLBACK_BACKFILL"
            )
            if bat_stats:
                for stat in bat_stats:
                    pid = stat["player_id"]
                    stat["team_code"] = team_map.get(pid, (None, 0))[0]

                valid_bat = [s for s in bat_stats if s["team_code"]]
                save_batting_stats_safe(valid_bat)
                logger.info(f"   ✅ Batting: {len(valid_bat)} records saved.")

            # 3. Pitching Backfill
            pit_stats = SeasonStatAggregator.aggregate_pitching_season_bulk(
                session, year, series, source="FALLBACK_BACKFILL"
            )
            if pit_stats:
                for stat in pit_stats:
                    pid = stat["player_id"]
                    stat["team_code"] = team_map.get(pid, (None, 0))[0]

                valid_pit = [s for s in pit_stats if s["team_code"]]
                save_pitching_stats_to_db(valid_pit)
                logger.info(f"   ✅ Pitching: {len(valid_pit)} records saved.")

            # 4. Baserunning Backfill
            br_stats = SeasonStatAggregator.aggregate_baserunning_season_bulk(
                session, year, series, source="FALLBACK_BACKFILL"
            )
            if br_stats:
                for stat in br_stats:
                    pid = stat["player_id"]
                    stat["team_id"] = team_map.get(pid, (None, 0))[0]

                valid_br = [s for s in br_stats if s["team_id"]]
                cnt = baserun_repo.upsert_many(valid_br)
                logger.info(f"   ✅ Baserunning: {cnt} records saved.")

            # 5. Fielding Backfill
            fld_stats = SeasonStatAggregator.aggregate_fielding_season_bulk(
                session, year, series, source="FALLBACK_BACKFILL"
            )
            if fld_stats:
                for stat in fld_stats:
                    pid = stat["player_id"]
                    stat["team_id"] = team_map.get(pid, (None, 0))[0]

                valid_fld = [s for s in fld_stats if s["team_id"]]
                cnt = fielding_repo.upsert_many(valid_fld)
                logger.info(f"   ✅ Fielding: {cnt} records saved.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill missing advanced stats from transactions.")
    parser.add_argument("--years", type=str, default="2020-2026")
    parser.add_argument("--series", type=str, default="regular")

    args = parser.parse_args()

    if "-" in args.years:
        start, end = map(int, args.years.split("-"))
        target_years = list(range(start, end + 1))
    else:
        target_years = [int(args.years)]

    backfill_stats(target_years, args.series)
