"""PA formula audit for game_batting_stats.

PA = AB + BB + HBP + SH + SF

Reports violations per season, categorizing by fixability:
  - FIXABLE_PBP: PBP events contain sacrifice descriptions
  - FIXABLE_FORMULA: PA > AB+BB+HBP but SH=SF=0 (can apply ratio-based fix)
  - UNFIXABLE: No data source available

Usage:
    python3 -m scripts.maintenance.audit_pa_formula --year 2020
    python3 -m scripts.maintenance.audit_pa_formula --all-years
    python3 -m scripts.maintenance.audit_pa_formula --fix-year 2020
    python3 -m scripts.maintenance.audit_pa_formula --fix-year 2020 --dry-run
    python3 -m scripts.maintenance.audit_pa_formula --all-years --json
"""

import argparse
import json
import logging
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import SessionLocal

logger = logging.getLogger(__name__)
AUDIT_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, OSError)


_GLOBAL_AUDIT_CACHE: dict[str, Any] | None = None


def _populate_audit_cache():
    global _GLOBAL_AUDIT_CACHE
    if _GLOBAL_AUDIT_CACHE is not None:
        return

    logger.info("Initializing global audit cache (single-scan table query)...")
    t0 = time.time()
    with SessionLocal() as session:
        # Bypass auxiliary statistics querying to avoid DB scan overload on OCI PostgreSQL
        total_games_map: dict[int, int] = {}
        total_rows_map: dict[int, int] = {}

        # 3. All violation rows across all seasons
        violations = session.execute(
            text("""
            SELECT
                gs.game_id, gs.player_id, gs.player_name,
                gs.plate_appearances, gs.at_bats, gs.walks, gs.hbp,
                gs.sacrifice_hits, gs.sacrifice_flies,
                (COALESCE(gs.at_bats,0) + COALESCE(gs.walks,0) + COALESCE(gs.hbp,0)
                 + COALESCE(gs.sacrifice_hits,0) + COALESCE(gs.sacrifice_flies,0)) as calc_pa
            FROM game_batting_stats gs
            WHERE COALESCE(gs.plate_appearances, 0) != (
                COALESCE(gs.at_bats,0) + COALESCE(gs.walks,0) + COALESCE(gs.hbp,0)
                + COALESCE(gs.sacrifice_hits,0) + COALESCE(gs.sacrifice_flies,0)
            )
            ORDER BY gs.game_id, gs.player_name
            """),
        ).fetchall()

        # Group violations by year
        violations_map: dict[int, list[Any]] = {}
        for r in violations:
            if r.game_id and len(r.game_id) >= 4 and r.game_id[:4].isdigit():
                y = int(r.game_id[:4])
                violations_map.setdefault(y, []).append(r)

    t1 = time.time()
    logger.info(f"Global audit cache initialized in {t1 - t0:.2f}s (cached {len(violations)} total violations)")

    _GLOBAL_AUDIT_CACHE = {
        "total_games_map": total_games_map,
        "total_rows_map": total_rows_map,
        "violations_map": violations_map,
    }


def audit_year(year: int) -> dict[str, Any]:
    t_start = time.time()
    _populate_audit_cache()
    assert _GLOBAL_AUDIT_CACHE is not None

    total_games = _GLOBAL_AUDIT_CACHE["total_games_map"].get(year, 0)
    total_rows = _GLOBAL_AUDIT_CACHE["total_rows_map"].get(year, 0)
    rows = _GLOBAL_AUDIT_CACHE["violations_map"].get(year, [])

    categories: Counter[str] = Counter()
    if rows:
        logger.info(f"  [Year {year}] Verifying PBP sacrifice counts for {len(rows)} violation rows...")
        with SessionLocal() as session:
            for idx, r in enumerate(rows):
                pbp_sac_count = (
                    session.execute(
                        text("""
                    SELECT COUNT(*) FROM game_events e
                    WHERE e.game_id = :game_id
                      AND (
                          (e.batter_id IS NOT NULL AND :player_id IS NOT NULL AND e.batter_id = :player_id)
                          OR
                          ((e.batter_id IS NULL OR :player_id IS NULL) AND e.batter_name = :player_name)
                      )
                      AND (e.description LIKE '%희생번트%' OR e.description LIKE '%희생플라이%')
                    """),
                        {
                            "game_id": r.game_id,
                            "player_id": r.player_id,
                            "player_name": r.player_name,
                        },
                    ).scalar()
                    or 0
                )

                sh_sf_zero = r.sacrifice_hits == 0 and r.sacrifice_flies == 0
                pa_gt_abhbp = r.plate_appearances > (r.at_bats + r.walks + r.hbp)
                if pbp_sac_count > 0:
                    categories["FIXABLE_PBP"] += 1
                elif sh_sf_zero and pa_gt_abhbp:
                    categories["FIXABLE_FORMULA"] += 1
                else:
                    categories["UNFIXABLE"] += 1

                if (idx + 1) % 50 == 0:
                    logger.info(f"    Processed {idx + 1}/{len(rows)} rows...")

    t_end = time.time()
    logger.info(f"  [Year {year}] Completed in {t_end - t_start:.2f}s")
    return {
        "year": year,
        "total_games": total_games,
        "total_batting_rows": total_rows,
        "violation_rows": len(rows),
        "violation_games": len({r.game_id for r in rows}),
        "categories": dict(categories),
    }


def _get_fix_candidates(year: int, session) -> list[dict]:
    """Return candidate rows for ratio-based fix."""
    rows = session.execute(
        text("""
            SELECT gs.id, gs.game_id, gs.player_id, gs.player_name,
                   gs.plate_appearances, gs.at_bats, gs.walks, gs.hbp,
                   gs.sacrifice_hits, gs.sacrifice_flies
            FROM game_batting_stats gs
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
        """),
        {"year": year},
    ).fetchall()
    return [
        {
            "id": r.id,
            "game_id": r.game_id,
            "player_id": r.player_id,
            "player_name": r.player_name,
            "plate_appearances": r.plate_appearances,
            "at_bats": r.at_bats,
            "walks": r.walks,
            "hbp": r.hbp,
            "sacrifice_hits": r.sacrifice_hits,
            "sacrifice_flies": r.sacrifice_flies,
            "expected_pa": (r.at_bats or 0) + (r.walks or 0) + (r.hbp or 0),
            "missing_pa": (r.plate_appearances or 0) - ((r.at_bats or 0) + (r.walks or 0) + (r.hbp or 0)),
        }
        for r in rows
    ]


def fix_year_formula(year: int, dry_run: bool = False) -> int:
    candidates = []
    with SessionLocal() as session:
        candidates = _get_fix_candidates(year, session)

    if not candidates:
        logger.info(f"  No fixable rows for {year}")
        return 0

    if dry_run:
        logger.info(f"  [DRY RUN] Would fix {len(candidates)} rows for {year}")
        game_ids = {c["game_id"] for c in candidates}
        player_ids = {c["player_id"] for c in candidates}
        logger.info(f"    Games affected: {len(game_ids)}")
        logger.info(f"    Players affected: {len(player_ids)}")
        missing_pa_total = sum(c["missing_pa"] for c in candidates)
        logger.info(f"    Total missing PA to allocate: {missing_pa_total}")
        sh_total = sum(int(c["missing_pa"] * 0.54) for c in candidates)
        sf_total = sum(c["missing_pa"] - int(c["missing_pa"] * 0.54) for c in candidates)
        logger.info(f"    SH to assign: {sh_total} | SF to assign: {sf_total}")
        return len(candidates)

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
        count = fixed.rowcount  # type: ignore[attr-defined]

    game_ids = {c["game_id"] for c in candidates}
    player_ids = {c["player_id"] for c in candidates}
    missing_pa_total = sum(c["missing_pa"] for c in candidates)
    sh_total = sum(int(c["missing_pa"] * 0.54) for c in candidates)
    sf_total = sum(c["missing_pa"] - int(c["missing_pa"] * 0.54) for c in candidates)

    logger.info(f"  Fixed {count} rows for {year}")
    logger.info(f"    Games affected: {len(game_ids)}")
    logger.info(f"    Players affected: {len(player_ids)}")
    logger.info(f"    Total missing PA allocated: {missing_pa_total} (SH: {sh_total}, SF: {sf_total})")
    return count


def _get_violation_game_ids(year: int) -> list[str]:
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
    return [str(r[0]) for r in rows]


def _apply_pbp_fixes(game_ids: list[str]) -> list[str]:
    from src.services.pbp_sh_sf_derivation import apply_sh_sf_to_batting_stats

    pbp_fixed: list[str] = []
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
                        pbp_fixed.append(game_id)
                        logger.info("Applied PBP SH/SF correction for game %s: %s rows updated", game_id, updated)
                except AUDIT_EXCEPTIONS:
                    session.rollback()
                    logger.exception("Error applying PBP fix for game %s", game_id)
    return pbp_fixed


def _recalc_and_sync(year: int, game_ids: list[str]) -> None:
    from src.cli.recalc_player_game_stats import run_recalc as recalc_game_stats
    from src.cli.recalc_player_stats import run_recalc as recalc_season_stats

    logger.info("Recalculating player game stats for %s games...", len(game_ids))
    for game_id in game_ids:
        try:
            recalc_game_stats(game_id=game_id, dry_run=False)
        except AUDIT_EXCEPTIONS:
            logger.exception("Error recalculating game stats for %s", game_id)
    logger.info("Recalculating player season stats for %s...", year)
    try:
        recalc_season_stats(season=year, dry_run=False)
    except AUDIT_EXCEPTIONS:
        logger.exception("Error recalculating season stats for %s", year)


def _sync_corrected_to_oci(year: int, game_ids: list[str]) -> None:
    import os

    oci_url = os.getenv("OCI_DB_URL")
    if not oci_url:
        logger.info("OCI_DB_URL not set. Skipping OCI synchronization.")
        return
    logger.info("Synchronizing corrected data to OCI...")
    try:
        from src.sync.oci_sync import OCISync

        with SessionLocal() as sqlite_session:
            syncer = OCISync(oci_url, sqlite_session)
            syncer.sync_player_basic()
            syncer.sync_players()
            for game_id in game_ids:
                syncer.sync_specific_game(game_id)
            syncer.sync_player_season_batting(year=year)
            syncer.sync_player_season_pitching(year=year)
            syncer.close()
            logger.info("OCI synchronization completed successfully.")
    except AUDIT_EXCEPTIONS:
        logger.exception("Error syncing to OCI")


def auto_fix_year(year: int, *, sync_oci: bool = False) -> int:
    game_ids = _get_violation_game_ids(year)
    if not game_ids:
        logger.info("No PA formula violations found for year %s.", year)
        return 0
    logger.info("Found %s games with PA formula violations in %s. Starting auto-fix...", len(game_ids), year)
    _apply_pbp_fixes(game_ids)
    if year in (2020, 2021):
        ratio_fixed = fix_year_formula(year)
        if ratio_fixed:
            logger.info("Applied ratio-based fallback correction for %s: %s rows updated", year, ratio_fixed)
    _recalc_and_sync(year, game_ids)
    if sync_oci:
        _sync_corrected_to_oci(year, game_ids)
    else:
        logger.info("OCI synchronization disabled; pass --sync-oci to enable it.")
    return len(game_ids)


def _setup_logging():
    """Configure structured logging to file and console."""
    log_dir = Path("logs/audit_fixes")
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"audit_pa_formula_{timestamp}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    return log_file


def _build_arg_parser():
    parser = argparse.ArgumentParser(description="Audit PA formula violations")
    parser.add_argument("--year", type=int, help="Target year")
    parser.add_argument("--all-years", action="store_true", help="Audit all years")
    parser.add_argument("--fix-year", type=int, help="Apply ratio-based fix for a season with missing SH/SF data")
    parser.add_argument("--fix-all", action="store_true", help="Apply ratio-based fix for 2020-2021")
    parser.add_argument(
        "--auto-fix", action="store_true", help="Apply PBP-based correction and trigger stats recalculation"
    )
    parser.add_argument(
        "--sync-oci",
        action="store_true",
        help="Sync auto-fixed data to OCI. Disabled by default even when OCI_DB_URL is configured.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without applying")
    parser.add_argument("--json", action="store_true", help="Output results as JSON")
    return parser


def _run_auto_fix(args, start_time: float) -> None:
    years = list(range(2018, 2027)) if args.all_years else [args.year] if args.year else [2020, 2021, 2023]
    for y in years:
        logger.info("Auto-fix for year %s", y)
        auto_fix_year(y, sync_oci=args.sync_oci)
    logger.info("Total elapsed: %.2fs", time.time() - start_time)


def _run_fix_year(args, start_time: float) -> None:
    logger.info("Fixing year %s (dry_run=%s)", args.fix_year, args.dry_run)
    fixed = fix_year_formula(args.fix_year, dry_run=args.dry_run)
    if not args.dry_run:
        logger.info("Fixed %s rows for %s", fixed, args.fix_year)
    logger.info("Total elapsed: %.2fs", time.time() - start_time)


def _print_audit_results(results: list[dict], elapsed: float, log_file: str, json_output: bool) -> None:
    for r in results:
        r["elapsed_seconds"] = round(elapsed, 2)
    if json_output:
        logger.info(json.dumps(results, indent=2, ensure_ascii=False))
        return
    logger.info(
        "%s  %s  %s  %s  %s  FIXABLE_PBP FIXABLE_FMLA UNFIXABLE",
        f"{'Year':>6}",
        f"{'Games':>6}",
        f"{'Rows':>8}",
        f"{'Violations':>12}",
        f"{'V Games':>8}",
    )
    logger.info("-" * 85)
    total_v = 0
    for r in results:
        logger.info(
            "%s %s %s %s %s  %s %s %s",
            f"{r['year']:>6}",
            f"{r['total_games']:>6}",
            f"{r['total_batting_rows']:>8}",
            f"{r['violation_rows']:>12}",
            f"{r['violation_games']:>8}",
            f"{r['categories'].get('FIXABLE_PBP', 0):>10}",
            f"{r['categories'].get('FIXABLE_FORMULA', 0):>10}",
            f"{r['categories'].get('UNFIXABLE', 0):>10}",
        )
        total_v += r["violation_rows"]
    logger.info("-" * 85)
    logger.info(f"{'TOTAL':>6} {'':>6} {'':>8} {total_v:>12}")
    logger.info("Elapsed: %.2fs | Log: %s", elapsed, log_file)


def _run_audit(args, start_time: float, log_file: str) -> None:
    years = (
        list(range(2018, 2027))
        if (args.all_years or args.fix_all)
        else [args.year]
        if args.year
        else [2020, 2021, 2023]
    )
    results = sorted([audit_year(y) for y in years], key=lambda r: r["year"])
    _print_audit_results(results, time.time() - start_time, log_file, args.json)


def main():
    parser = _build_arg_parser()
    args = parser.parse_args()
    log_file = _setup_logging()
    start_time = time.time()
    logger.info("Starting PA formula audit (dry_run=%s, json=%s)", args.dry_run, args.json)
    if args.auto_fix:
        _run_auto_fix(args, start_time)
    elif args.fix_year is not None:
        _run_fix_year(args, start_time)
    else:
        if args.fix_all:
            for y in [2020, 2021]:
                fixed = fix_year_formula(y, dry_run=args.dry_run)
                if not args.dry_run:
                    logger.info("Fixed %s rows for %s", fixed, y)
        _run_audit(args, start_time, log_file)


if __name__ == "__main__":
    main()
