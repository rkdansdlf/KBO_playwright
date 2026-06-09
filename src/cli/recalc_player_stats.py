"""
CLI tool to recalculate player-level season stats from game-level (transactional) data.
Aggregates GameBattingStat -> PlayerSeasonBatting
Aggregates GamePitchingStat -> PlayerSeasonPitching

Resolves mismatches detected by QualityGate:
  - "Transactional PA > Cumulative PA"
  - "Missing cumulative record"
  - "Transactional Outs > Cumulative Outs"
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence
from typing import Any

from sqlalchemy import func, text
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from src.db.engine import Engine, SessionLocal
from src.models.game import Game, GameBattingStat, GamePitchingStat
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES

logger = logging.getLogger(__name__)


def _get_regular_season_ids(session: Session, year: int) -> list[int]:
    stmt = text("SELECT season_id FROM kbo_seasons WHERE season_year = :year AND league_type_code = 0")
    result = session.execute(stmt, {"year": year}).scalars().all()
    return [int(r) for r in result]


def _get_player_teams(session: Session, season_ids: list[int], model) -> dict[int, str]:
    """Get the most common canonical_team_code per player_id."""
    rows = (
        session.query(
            model.player_id,
            model.canonical_team_code,
            func.count().label("cnt"),
        )
        .join(Game, Game.game_id == model.game_id)
        .filter(
            Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
            Game.season_id.in_(season_ids),
            model.canonical_team_code.isnot(None),
        )
        .group_by(model.player_id, model.canonical_team_code)
        .order_by(model.player_id, func.count().desc())
        .all()
    )
    result: dict[int, str] = {}
    for row in rows:
        if row.player_id not in result:
            result[row.player_id] = row.canonical_team_code
    return result


def _compute_batting_rates(row) -> dict[str, Any]:
    ab = row.at_bats or 0
    h = row.hits or 0
    bb = row.walks or 0
    hbp = row.hbp or 0
    sf = row.sacrifice_flies or 0
    k = row.strikeouts or 0
    dbl = row.doubles or 0
    triple = row.triples or 0
    hr = row.home_runs or 0

    avg = round(h / ab, 3) if ab > 0 else 0.0
    obp = round((h + bb + hbp) / (ab + bb + hbp + sf), 3) if (ab + bb + hbp + sf) > 0 else 0.0
    total_bases = h + dbl + 2 * triple + 3 * hr
    slg = round(total_bases / ab, 3) if ab > 0 else 0.0
    ops = round(obp + slg, 3)
    iso = round(slg - avg, 3)
    babip = round((h - hr) / (ab - k - hr + sf), 3) if (ab - k - hr + sf) > 0 else 0.0

    return {
        "avg": avg,
        "obp": obp,
        "slg": slg,
        "ops": ops,
        "iso": iso,
        "babip": babip,
    }


def _aggregate_batting(
    session: Session, season_ids: list[int], season: int, league: str, level: str,
) -> list[dict[str, Any]]:
    rows = (
        session.query(
            GameBattingStat.player_id,
            func.count(func.distinct(GameBattingStat.game_id)).label("games"),
            func.sum(GameBattingStat.plate_appearances).label("plate_appearances"),
            func.sum(GameBattingStat.at_bats).label("at_bats"),
            func.sum(GameBattingStat.runs).label("runs"),
            func.sum(GameBattingStat.hits).label("hits"),
            func.sum(GameBattingStat.doubles).label("doubles"),
            func.sum(GameBattingStat.triples).label("triples"),
            func.sum(GameBattingStat.home_runs).label("home_runs"),
            func.sum(GameBattingStat.rbi).label("rbi"),
            func.sum(GameBattingStat.walks).label("walks"),
            func.sum(GameBattingStat.intentional_walks).label("intentional_walks"),
            func.sum(GameBattingStat.hbp).label("hbp"),
            func.sum(GameBattingStat.strikeouts).label("strikeouts"),
            func.sum(GameBattingStat.stolen_bases).label("stolen_bases"),
            func.sum(GameBattingStat.caught_stealing).label("caught_stealing"),
            func.sum(GameBattingStat.sacrifice_hits).label("sacrifice_hits"),
            func.sum(GameBattingStat.sacrifice_flies).label("sacrifice_flies"),
            func.sum(GameBattingStat.gdp).label("gdp"),
        )
        .join(Game, Game.game_id == GameBattingStat.game_id)
        .filter(
            GameBattingStat.player_id.isnot(None),
            Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
            Game.season_id.in_(season_ids),
        )
        .group_by(GameBattingStat.player_id)
        .all()
    )

    team_map = _get_player_teams(session, season_ids, GameBattingStat)
    results = []
    for row in rows:
        rates = _compute_batting_rates(row)
        results.append(
            {
                "player_id": row.player_id,
                "season": season,
                "league": league,
                "level": level,
                "source": "AGGREGATED",
                "canonical_team_code": team_map.get(row.player_id),
                "games": row.games or 0,
                "plate_appearances": row.plate_appearances or 0,
                "at_bats": row.at_bats or 0,
                "runs": row.runs or 0,
                "hits": row.hits or 0,
                "doubles": row.doubles or 0,
                "triples": row.triples or 0,
                "home_runs": row.home_runs or 0,
                "rbi": row.rbi or 0,
                "walks": row.walks or 0,
                "intentional_walks": row.intentional_walks or 0,
                "hbp": row.hbp or 0,
                "strikeouts": row.strikeouts or 0,
                "stolen_bases": row.stolen_bases or 0,
                "caught_stealing": row.caught_stealing or 0,
                "sacrifice_hits": row.sacrifice_hits or 0,
                "sacrifice_flies": row.sacrifice_flies or 0,
                "gdp": row.gdp or 0,
                **rates,
            },
        )
    return results


def _compute_pitching_rates(total_outs: int, hits: int, bb: int, er: int, k: int, hr: int) -> dict[str, Any]:
    ip = total_outs / 3.0
    era = round(er * 9 / ip, 2) if ip > 0 else 0.0
    whip = round((bb + hits) / ip, 2) if ip > 0 else 0.0
    k9 = round(k * 9 / ip, 2) if ip > 0 else 0.0
    bb9 = round(bb * 9 / ip, 2) if ip > 0 else 0.0
    kbb = round(k / bb, 2) if bb > 0 else 0.0
    return {
        "innings_pitched": round(ip, 1),
        "era": era,
        "whip": whip,
        "k_per_nine": k9,
        "bb_per_nine": bb9,
        "kbb": kbb,
    }


def _aggregate_pitching(
    session: Session, season_ids: list[int], season: int, league: str, level: str,
) -> list[dict[str, Any]]:
    rows = (
        session.query(
            GamePitchingStat.player_id,
            func.count(func.distinct(GamePitchingStat.game_id)).label("games"),
            func.sum(GamePitchingStat.innings_outs).label("innings_outs"),
            func.sum(GamePitchingStat.hits_allowed).label("hits_allowed"),
            func.sum(GamePitchingStat.runs_allowed).label("runs_allowed"),
            func.sum(GamePitchingStat.earned_runs).label("earned_runs"),
            func.sum(GamePitchingStat.home_runs_allowed).label("home_runs_allowed"),
            func.sum(GamePitchingStat.walks_allowed).label("walks_allowed"),
            func.sum(GamePitchingStat.strikeouts).label("strikeouts"),
            func.sum(GamePitchingStat.hit_batters).label("hit_batters"),
            func.sum(GamePitchingStat.wild_pitches).label("wild_pitches"),
            func.sum(GamePitchingStat.balks).label("balks"),
            func.sum(GamePitchingStat.wins).label("wins"),
            func.sum(GamePitchingStat.losses).label("losses"),
            func.sum(GamePitchingStat.saves).label("saves"),
            func.sum(GamePitchingStat.holds).label("holds"),
            func.sum(GamePitchingStat.batters_faced).label("batters_faced"),
            func.sum(GamePitchingStat.pitches).label("pitches"),
            func.count(func.nullif(GamePitchingStat.is_starting == 1, False)).label("games_started"),
        )
        .join(Game, Game.game_id == GamePitchingStat.game_id)
        .filter(
            GamePitchingStat.player_id.isnot(None),
            Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
            Game.season_id.in_(season_ids),
        )
        .group_by(GamePitchingStat.player_id)
        .all()
    )

    team_map = _get_player_teams(session, season_ids, GamePitchingStat)
    results = []
    for row in rows:
        total_outs = row.innings_outs or 0
        rates = _compute_pitching_rates(
            total_outs=total_outs,
            hits=row.hits_allowed or 0,
            bb=row.walks_allowed or 0,
            er=row.earned_runs or 0,
            k=row.strikeouts or 0,
            hr=row.home_runs_allowed or 0,
        )
        results.append(
            {
                "player_id": row.player_id,
                "season": season,
                "league": league,
                "level": level,
                "source": "AGGREGATED",
                "canonical_team_code": team_map.get(row.player_id),
                "games": row.games or 0,
                "games_started": row.games_started or 0,
                "innings_outs": total_outs,
                **rates,
                "hits_allowed": row.hits_allowed or 0,
                "runs_allowed": row.runs_allowed or 0,
                "earned_runs": row.earned_runs or 0,
                "home_runs_allowed": row.home_runs_allowed or 0,
                "walks_allowed": row.walks_allowed or 0,
                "strikeouts": row.strikeouts or 0,
                "hit_batters": row.hit_batters or 0,
                "wild_pitches": row.wild_pitches or 0,
                "balks": row.balks or 0,
                "wins": row.wins or 0,
                "losses": row.losses or 0,
                "saves": row.saves or 0,
                "holds": row.holds or 0,
                "tbf": row.batters_faced or 0,
                "np": row.pitches or 0,
            },
        )
    return results


def _upsert_batting(session: Session, records: list[dict[str, Any]], dialect: str) -> int:
    if not records:
        return 0

    conflict_keys = ["player_id", "season", "league", "level"]
    saved = 0

    for data in records:
        try:
            if dialect == "sqlite":
                stmt = sqlite_insert(PlayerSeasonBatting).values(**data)
                stmt = stmt.on_conflict_do_update(
                    index_elements=conflict_keys,
                    set_={k: stmt.excluded[k] for k in data if k not in conflict_keys},
                )
            elif dialect == "postgresql":
                from sqlalchemy.dialects.postgresql import insert as pg_insert

                stmt = pg_insert(PlayerSeasonBatting).values(**data)
                stmt = stmt.on_conflict_do_update(
                    index_elements=conflict_keys,
                    set_={k: stmt.excluded[k] for k in data if k not in conflict_keys},
                )
            else:
                from sqlalchemy.dialects.mysql import insert as my_insert

                stmt = my_insert(PlayerSeasonBatting).values(**data)
                stmt = stmt.on_duplicate_key_update(**{k: stmt.inserted[k] for k in data if k not in conflict_keys})
            session.execute(stmt)
            saved += 1
        except SQLAlchemyError as e:
            logger.warning(f"Batting upsert failed for player {data.get('player_id')}: {e}")
            session.rollback()

    session.commit()
    return saved


def _upsert_pitching(session: Session, records: list[dict[str, Any]], dialect: str) -> int:
    if not records:
        return 0

    conflict_keys = ["player_id", "season", "league", "level"]
    saved = 0

    for data in records:
        try:
            if dialect == "sqlite":
                stmt = sqlite_insert(PlayerSeasonPitching).values(**data)
                stmt = stmt.on_conflict_do_update(
                    index_elements=conflict_keys,
                    set_={k: stmt.excluded[k] for k in data if k not in conflict_keys},
                )
            elif dialect == "postgresql":
                from sqlalchemy.dialects.postgresql import insert as pg_insert

                stmt = pg_insert(PlayerSeasonPitching).values(**data)
                stmt = stmt.on_conflict_do_update(
                    index_elements=conflict_keys,
                    set_={k: stmt.excluded[k] for k in data if k not in conflict_keys},
                )
            else:
                from sqlalchemy.dialects.mysql import insert as my_insert

                stmt = my_insert(PlayerSeasonPitching).values(**data)
                stmt = stmt.on_duplicate_key_update(**{k: stmt.inserted[k] for k in data if k not in conflict_keys})
            session.execute(stmt)
            saved += 1
        except SQLAlchemyError as e:
            logger.warning(f"Pitching upsert failed for player {data.get('player_id')}: {e}")
            session.rollback()

    session.commit()
    return saved


def _print_batting_results(records: list[dict[str, Any]]) -> None:
    for r in sorted(records, key=lambda x: x["plate_appearances"], reverse=True)[:20]:
        logger.info(
            "  PID=%-6s Team=%-4s G=%-2s PA=%-3s H=%-2s AVG=%-5s OPS=%-5s",
            r["player_id"],
            str(r.get("canonical_team_code", "")),
            r["games"],
            r["plate_appearances"],
            r["hits"],
            r["avg"],
            r["ops"],
        )


def _print_pitching_results(records: list[dict[str, Any]]) -> None:
    for r in sorted(records, key=lambda x: x["innings_pitched"] or 0, reverse=True)[:20]:
        logger.info(
            "  PID=%-6s Team=%-4s G=%-2s IP=%-5s ERA=%-5s WHIP=%-5s K=%-3s",
            r["player_id"],
            str(r.get("canonical_team_code", "")),
            r["games"],
            r["innings_pitched"],
            r["era"],
            r["whip"],
            r["strikeouts"],
        )


def run_recalc(
    season: int,
    dry_run: bool = False,
    batting_only: bool = False,
    pitching_only: bool = False,
) -> int:
    league = "REGULAR"
    level = "KBO1"

    with SessionLocal() as session:
        season_ids = _get_regular_season_ids(session, season)
        if not season_ids:
            logger.error(f"No Regular Season IDs found for season {season}")
            return 1

        dialect = Engine.dialect.name

        if not pitching_only:
            logger.info(f"Aggregating batting stats from game data for season={season}...")
            batting_records = _aggregate_batting(session, season_ids, season, league, level)
            logger.info(f"  Found {len(batting_records)} players with batting game data")

            if dry_run:
                logger.info("[DRY-RUN] Batting records that would be saved:")
                _print_batting_results(batting_records)
            else:
                saved = _upsert_batting(session, batting_records, dialect)
                logger.info(f"  Upserted {saved} batting records")

        if not batting_only:
            logger.info(f"Aggregating pitching stats from game data for season={season}...")
            pitching_records = _aggregate_pitching(session, season_ids, season, league, level)
            logger.info(f"  Found {len(pitching_records)} players with pitching game data")

            if dry_run:
                logger.info("[DRY-RUN] Pitching records that would be saved:")
                _print_pitching_results(pitching_records)
            else:
                saved = _upsert_pitching(session, pitching_records, dialect)
                logger.info(f"  Upserted {saved} pitching records")

    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Recalculate player cumulative statistics from game-level data.")
    parser.add_argument("--season", type=int, required=True, help="Season year")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print results without saving",
    )
    parser.add_argument(
        "--batting-only",
        action="store_true",
        help="Recalculate batting stats only",
    )
    parser.add_argument(
        "--pitching-only",
        action="store_true",
        help="Recalculate pitching stats only",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    return run_recalc(
        season=args.season,
        dry_run=args.dry_run,
        batting_only=args.batting_only,
        pitching_only=args.pitching_only,
    )


if __name__ == "__main__":
    sys.exit(main())
