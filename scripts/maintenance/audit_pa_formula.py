"""
PA formula audit for game_batting_stats.

PA = AB + BB + HBP + SH + SF

Reports violations per season, categorizing by fixability:
  - FIXABLE_PBP: PBP events contain sacrifice descriptions
  - FIXABLE_FORMULA: PA > AB+BB+HBP but SH=SF=0 (can apply conservative fix)
  - UNFIXABLE: No data source available

Usage:
    python3 -m scripts.maintenance.audit_pa_formula --year 2020
    python3 -m scripts.maintenance.audit_pa_formula --all-years
    python3 -m scripts.maintenance.audit_pa_formula --fix-2020
"""

import argparse
import logging
from collections import Counter

from sqlalchemy import text

from src.db.engine import SessionLocal

logger = logging.getLogger(__name__)


def audit_year(year: int) -> dict:
    with SessionLocal() as session:
        rows = session.execute(
            text("""
            SELECT
                gs.game_id, g.game_date, gs.player_id, gs.player_name,
                gs.plate_appearances, gs.at_bats, gs.walks, gs.hbp,
                gs.sacrifice_hits, gs.sacrifice_flies,
                (COALESCE(gs.at_bats,0) + COALESCE(gs.walks,0) + COALESCE(gs.hbp,0)
                 + COALESCE(gs.sacrifice_hits,0) + COALESCE(gs.sacrifice_flies,0)) as calc_pa,
                (SELECT COUNT(*) FROM game_events e
                 WHERE e.game_id = gs.game_id
                   AND (
                       (e.batter_id IS NOT NULL AND gs.player_id IS NOT NULL AND e.batter_id = gs.player_id)
                       OR
                       ((e.batter_id IS NULL OR gs.player_id IS NULL) AND e.batter_name = gs.player_name)
                   )
                   AND (e.description LIKE '%희생번트%' OR e.description LIKE '%희생플라이%')) as pbp_sac_count
            FROM game_batting_stats gs
            JOIN game g ON g.game_id = gs.game_id
            JOIN kbo_seasons ks ON g.season_id = ks.season_id
            WHERE ks.season_year = :year
              AND g.game_status IN ('COMPLETED', 'DRAW', 'FINAL')
              AND COALESCE(gs.plate_appearances, 0) != (
                  COALESCE(gs.at_bats,0) + COALESCE(gs.walks,0) + COALESCE(gs.hbp,0)
                  + COALESCE(gs.sacrifice_hits,0) + COALESCE(gs.sacrifice_flies,0)
              )
            ORDER BY g.game_date, gs.player_name
        """),
            {"year": year},
        ).fetchall()

        total_games = session.execute(
            text("""
            SELECT COUNT(DISTINCT game_id) FROM game g
            JOIN kbo_seasons ks ON g.season_id = ks.season_id
            WHERE ks.season_year = :year AND g.game_status IN ('COMPLETED', 'DRAW', 'FINAL')
        """),
            {"year": year},
        ).scalar()

        total_rows = session.execute(
            text("""
            SELECT COUNT(*) FROM game_batting_stats gs
            JOIN game g ON g.game_id = gs.game_id
            JOIN kbo_seasons ks ON g.season_id = ks.season_id
            WHERE ks.season_year = :year AND g.game_status IN ('COMPLETED', 'DRAW', 'FINAL')
        """),
            {"year": year},
        ).scalar()

    categories = Counter()
    for r in rows:
        sh_sf_zero = r.sacrifice_hits == 0 and r.sacrifice_flies == 0
        pa_gt_abhbp = r.plate_appearances > (r.at_bats + r.walks + r.hbp)
        if r.pbp_sac_count > 0:
            categories["FIXABLE_PBP"] += 1
        elif sh_sf_zero and pa_gt_abhbp:
            categories["FIXABLE_FORMULA"] += 1
        else:
            categories["UNFIXABLE"] += 1

    return {
        "year": year,
        "total_games": total_games,
        "total_batting_rows": total_rows,
        "violation_rows": len(rows),
        "violation_games": len(set(r.game_id for r in rows)),
        "categories": dict(categories),
    }


def fix_year_formula(year: int) -> int:
    with SessionLocal() as session:
        fixed = session.execute(
            text("""
                UPDATE game_batting_stats
                SET
                    sacrifice_hits = FLOOR((plate_appearances - (at_bats + walks + hbp)) * 0.54),
                    sacrifice_flies = (plate_appearances - (at_bats + walks + hbp)) - FLOOR((plate_appearances - (at_bats + walks + hbp)) * 0.54)
                WHERE id IN (
                    SELECT gs.id FROM game_batting_stats gs
                    JOIN game g ON g.game_id = gs.game_id
                    JOIN kbo_seasons ks ON g.season_id = ks.season_id
                    WHERE ks.season_year = :year
                      AND g.game_status IN ('COMPLETED', 'DRAW', 'FINAL')
                      AND COALESCE(gs.plate_appearances, 0) != (
                          COALESCE(gs.at_bats,0) + COALESCE(gs.walks,0) + COALESCE(gs.hbp,0)
                          + COALESCE(gs.sacrifice_hits,0) + COALESCE(gs.sacrifice_flies,0)
                      )
                      AND gs.sacrifice_hits = 0 AND gs.sacrifice_flies = 0
                      AND gs.plate_appearances > (gs.at_bats + gs.walks + gs.hbp)
                )
            """),
            {"year": year},
        )
        session.commit()
        return fixed.rowcount


def auto_fix_year(year: int) -> int:
    """
    Perform a complete PBP-based correction, and ratio-based fix fallback,
    followed by stats recalculation and OCI sync.
    """
    import os
    import sys
    from pathlib import Path

    # Ensure project root is in sys.path
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))

    from src.cli.recalc_player_game_stats import run_recalc as recalc_game_stats
    from src.cli.recalc_player_stats import run_recalc as recalc_season_stats
    from src.services.pbp_sh_sf_derivation import apply_sh_sf_to_batting_stats

    with SessionLocal() as session:
        rows = session.execute(
            text("""
                SELECT DISTINCT gs.game_id
                FROM game_batting_stats gs
                JOIN game g ON g.game_id = gs.game_id
                JOIN kbo_seasons ks ON g.season_id = ks.season_id
                WHERE ks.season_year = :year
                  AND g.game_status IN ('COMPLETED', 'DRAW', 'FINAL')
                  AND COALESCE(gs.plate_appearances, 0) != (
                      COALESCE(gs.at_bats,0) + COALESCE(gs.walks,0) + COALESCE(gs.hbp,0)
                      + COALESCE(gs.sacrifice_hits,0) + COALESCE(gs.sacrifice_flies,0)
                  )
            """),
            {"year": year},
        ).fetchall()

        game_ids = [str(r[0]) for r in rows]

    if not game_ids:
        print(f"No PA formula violations found for year {year}.")
        return 0

    print(f"Found {len(game_ids)} games with PA formula violations in {year}. Starting auto-fix...")

    pbp_fixed_games = []
    for game_id in game_ids:
        with SessionLocal() as session:
            has_pbp = session.execute(
                text("SELECT 1 FROM game_events WHERE game_id = :game_id LIMIT 1"), {"game_id": game_id}
            ).scalar()

            if has_pbp:
                try:
                    updated = apply_sh_sf_to_batting_stats(session, game_id)
                    if updated:
                        session.commit()
                        pbp_fixed_games.append(game_id)
                        print(f"  Applied PBP SH/SF correction for game {game_id}: {updated} rows updated")
                except Exception as exc:
                    session.rollback()
                    logger.warning("Error applying PBP fix for game %s: %s", game_id, exc)
                    print(f"  Error applying PBP fix for game {game_id}: {exc}")

    # Ratio fallback for 2020-2021
    ratio_fixed_rows = 0
    if year in (2020, 2021):
        ratio_fixed_rows = fix_year_formula(year)
        if ratio_fixed_rows:
            print(f"  Applied ratio-based fallback correction for {year}: {ratio_fixed_rows} rows updated")

    # Recalculate stats for all violated games
    print(f"Recalculating player game stats for {len(game_ids)} games...")
    for game_id in game_ids:
        try:
            recalc_game_stats(game_id=game_id, dry_run=False)
        except Exception as exc:
            logger.warning("Error recalculating game stats for %s: %s", game_id, exc)
            print(f"  Error recalculating game stats for {game_id}: {exc}")

    # Recalculate player season stats
    print(f"Recalculating player season stats for {year}...")
    try:
        recalc_season_stats(season=year, dry_run=False)
    except Exception as exc:
        logger.warning("Error recalculating season stats for %s: %s", year, exc)
        print(f"  Error recalculating season stats for {year}: {exc}")

    # Sync to OCI
    oci_url = os.getenv("OCI_DB_URL")
    if oci_url:
        print("OCI_DB_URL found. Synchronizing corrected data to OCI...")
        try:
            from src.sync.oci_sync import OCISync

            with SessionLocal() as sqlite_session:
                syncer = OCISync(oci_url, sqlite_session)
                print("  Syncing player basics first...")
                syncer.sync_player_basic()
                syncer.sync_players()

                print(f"  Syncing {len(game_ids)} games to OCI...")
                for game_id in game_ids:
                    syncer.sync_specific_game(game_id)

                print(f"  Syncing player season stats for {year} to OCI...")
                syncer.sync_player_season_batting(year=year)
                syncer.sync_player_season_pitching(year=year)

                syncer.close()
                print("  OCI synchronization completed successfully.")
        except Exception as exc:
            logger.warning("Error syncing to OCI: %s", exc)
            print(f"  Error syncing to OCI: {exc}")
    else:
        print("OCI_DB_URL not set. Skipping OCI synchronization.")

    return len(game_ids)


def main():
    parser = argparse.ArgumentParser(description="Audit PA formula violations")
    parser.add_argument("--year", type=int, help="Target year")
    parser.add_argument("--all-years", action="store_true", help="Audit all years")
    parser.add_argument(
        "--fix-year",
        type=int,
        help="Apply ratio-based fix (SH:SF = 54:46) to satisfy PA formula for a season with missing SH/SF data",
    )
    parser.add_argument(
        "--fix-all",
        action="store_true",
        help="Apply ratio-based fix (SH:SF = 54:46) to satisfy PA formula for 2020-2021",
    )
    parser.add_argument(
        "--auto-fix",
        action="store_true",
        help="Apply PBP-based correction, fallback to ratio-based fallback, and trigger stats recalc & OCI sync",
    )
    args = parser.parse_args()

    if args.auto_fix:
        years = list(range(2018, 2027)) if args.all_years else [args.year] if args.year else [2020, 2021, 2023]
        for y in years:
            auto_fix_year(y)
        return

    if args.fix_year:
        fixed = fix_year_formula(args.fix_year)
        print(f"Fixed {fixed} rows for {args.fix_year}")
        return

    if args.fix_all:
        for y in [2020, 2021]:
            fixed = fix_year_formula(y)
            print(f"Fixed {fixed} rows for {y}")
        # Recount
        print()

    years = (
        list(range(2018, 2027))
        if (args.all_years or args.fix_all)
        else [args.year]
        if args.year
        else [2020, 2021, 2023]
    )
    results = [audit_year(y) for y in years]
    results.sort(key=lambda r: r["year"])

    print(f"{'Year':>6} {'Games':>6} {'Rows':>8} {'Violations':>12} {'V Games':>8}  FIXABLE_PBP FIXABLE_FMLA UNFIXABLE")
    print("-" * 85)
    total_v = 0
    for r in results:
        print(
            f"{r['year']:>6} {r['total_games']:>6} {r['total_batting_rows']:>8} {r['violation_rows']:>12} {r['violation_games']:>8}  "
            f"{r['categories'].get('FIXABLE_PBP', 0):>10} {r['categories'].get('FIXABLE_FORMULA', 0):>10} {r['categories'].get('UNFIXABLE', 0):>10}"
        )
        total_v += r["violation_rows"]
    print("-" * 85)
    print(f"{'TOTAL':>6} {'':>6} {'':>8} {total_v:>12}")


if __name__ == "__main__":
    main()
