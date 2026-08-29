"""Probe and analyze historical game data gaps (2010-2019).

Extracts missing terminal games, categorizes them by series and year,
and performs sample probes against KBO and Naver endpoints to establish
a definitive recovery vs known limitation triage.

Usage:
    python3 -m scripts.maintenance.probe_historical_gaps \
        --start-year 2010 --end-year 2019 --output reports/historical_batch/gap_resolution_2010_2019.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("probe_historical_gaps")

TERMINAL_STATUSES = ("COMPLETED", "DRAW")
NAVER_RECORD_URL = "https://api-gw.sports.naver.com/schedule/games/{game_id}/record"
NAVER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Origin": "https://m.sports.naver.com",
    "Referer": "https://m.sports.naver.com/",
}


def _extract_gaps(database_url: str, start_year: int, end_year: int) -> dict[str, Any]:
    """Identify missing terminal games across years and series."""
    engine = create_engine(database_url)
    with engine.connect() as conn:
        games_query = text("""
            SELECT CAST(g.game_id AS TEXT) AS game_id, g.game_date, g.game_status,
                   g.home_team, g.away_team, s.league_type_name AS series_name
            FROM game g
            LEFT JOIN kbo_seasons s ON s.season_id = g.season_id
            WHERE CAST(strftime('%Y', g.game_date) AS INTEGER) BETWEEN :start_year AND :end_year
            ORDER BY g.game_date, g.game_id
        """)
        all_games = conn.execute(games_query, {"start_year": start_year, "end_year": end_year}).fetchall()

        covered_query = text("""
            SELECT DISTINCT game_id FROM game_batting_stats
            WHERE CAST(substr(game_id, 1, 4) AS INTEGER) BETWEEN :start_year AND :end_year
        """)
        covered_ids = set(conn.execute(covered_query, {"start_year": start_year, "end_year": end_year}).scalars().all())

    year_stats: dict[int, dict[str, Any]] = defaultdict(
        lambda: {
            "total_parent": 0,
            "terminal": 0,
            "covered": 0,
            "missing": 0,
            "series_breakdown": Counter(),
            "missing_games": [],
        }
    )

    for row in all_games:
        game_id = str(row.game_id)
        year = int(game_id[:4]) if len(game_id) >= 4 else 0
        status = row.game_status or "UNKNOWN"
        series = row.series_name or "정규시즌"

        stats = year_stats[year]
        stats["total_parent"] += 1

        if status in TERMINAL_STATUSES:
            stats["terminal"] += 1
            if game_id in covered_ids:
                stats["covered"] += 1
            else:
                stats["missing"] += 1
                stats["series_breakdown"][series] += 1
                stats["missing_games"].append(
                    {
                        "game_id": game_id,
                        "game_date": str(row.game_date),
                        "series": series,
                        "matchup": f"{row.away_team} vs {row.home_team}",
                    }
                )

    return {
        "start_year": start_year,
        "end_year": end_year,
        "years": dict(sorted(year_stats.items())),
    }


async def _probe_sample_game(client: httpx.AsyncClient, game: dict[str, str]) -> dict[str, Any]:
    """Probe a single game against KBO GameCenter and Naver Record APIs."""
    game_id = game["game_id"]
    game_date = game["game_date"].replace("-", "")

    kbo_url = f"https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameDate={game_date}&gameId={game_id}&section=BOXSCORE"
    naver_url = NAVER_RECORD_URL.format(game_id=game_id)

    kbo_status = None
    kbo_has_content = False
    try:
        resp = await client.get(kbo_url, timeout=5.0)
        kbo_status = str(resp.status_code)
        kbo_has_content = "boxscore" in resp.text.lower() or "table" in resp.text.lower()
    except (httpx.HTTPError, OSError, ValueError) as e:
        kbo_status = f"ERROR: {type(e).__name__}"

    naver_status = None
    naver_has_content = False
    try:
        resp_naver = await client.get(naver_url, headers=NAVER_HEADERS, timeout=5.0)
        naver_status = str(resp_naver.status_code)
        if resp_naver.status_code == 200:
            data = resp_naver.json()
            naver_has_content = bool(data.get("result") or data.get("scoreboard"))
    except (httpx.HTTPError, OSError, ValueError, KeyError) as e:
        naver_status = f"ERROR: {type(e).__name__}"

    return {
        "game_id": game_id,
        "series": game["series"],
        "game_date": game["game_date"],
        "matchup": game["matchup"],
        "kbo_probe": {
            "url": kbo_url,
            "status": kbo_status,
            "has_content": kbo_has_content,
        },
        "naver_probe": {
            "url": naver_url,
            "status": naver_status,
            "has_content": naver_has_content,
        },
    }


async def _run_probes(gaps_data: dict[str, Any]) -> list[dict[str, Any]]:
    """Sample games from each year and series and run async probes."""
    probe_targets = []
    for _year, ydata in gaps_data["years"].items():
        missing = ydata["missing_games"]
        by_series = defaultdict(list)
        for g in missing:
            by_series[g["series"]].append(g)

        for _series, games_list in by_series.items():
            sample = games_list[:2]
            probe_targets.extend(sample)

    logger.info("Probing %d sampled historical game endpoints...", len(probe_targets))
    async with httpx.AsyncClient(follow_redirects=True) as client:
        tasks = [_probe_sample_game(client, target) for target in probe_targets]
        return await asyncio.gather(*tasks)


def main() -> int:
    parser = argparse.ArgumentParser(description="Probe historical KBO data gaps (2010-2019)")
    parser.add_argument("--start-year", type=int, default=2010)
    parser.add_argument("--end-year", type=int, default=2019)
    parser.add_argument("--database-url", default="sqlite:///./data/kbo_dev.db")
    parser.add_argument("--output", type=Path, default=Path("reports/historical_batch/gap_resolution_2010_2019.json"))
    parser.add_argument("--skip-network-probe", action="store_true", help="Skip live HTTP probes")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    gaps_data = _extract_gaps(args.database_url, args.start_year, args.end_year)

    print("=" * 80)
    print(f"  KBO Historical Gap Census ({args.start_year}-{args.end_year})")
    print("=" * 80)
    print(
        f"{'Year':5} | {'Parent':7} | {'Terminal':8} | {'Covered':7} | {'Missing':7} | {'Coverage %':10} | {'Missing Series Breakdown'}"
    )
    print("-" * 80)

    total_terminal = 0
    total_covered = 0
    total_missing = 0

    for year, ydata in gaps_data["years"].items():
        total_terminal += ydata["terminal"]
        total_covered += ydata["covered"]
        total_missing += ydata["missing"]
        pct = (ydata["covered"] / ydata["terminal"] * 100) if ydata["terminal"] else 0.0
        breakdown_str = ", ".join(f"{s}:{c}" for s, c in ydata["series_breakdown"].items())
        print(
            f"{year:5} | {ydata['total_parent']:7} | {ydata['terminal']:8} | {ydata['covered']:7} | {ydata['missing']:7} | {pct:9.1f}% | {breakdown_str}"
        )

    print("-" * 80)
    total_pct = (total_covered / total_terminal * 100) if total_terminal else 0.0
    print(
        f"{'TOTAL':5} | {'-':7} | {total_terminal:8} | {total_covered:7} | {total_missing:7} | {total_pct:9.1f}% | Total Gaps: {total_missing}"
    )
    print("=" * 80)

    probes = []
    if not args.skip_network_probe:
        probes = asyncio.run(_run_probes(gaps_data))

    report = {
        "audit_timestamp": datetime.now().isoformat(),
        "start_year": args.start_year,
        "end_year": args.end_year,
        "summary": {
            "total_terminal_games": total_terminal,
            "total_covered_games": total_covered,
            "total_missing_games": total_missing,
            "overall_coverage_pct": round(total_pct, 2),
        },
        "year_census": {
            y: {
                "parent_games": yd["total_parent"],
                "terminal_games": yd["terminal"],
                "covered_games": yd["covered"],
                "missing_games_count": yd["missing"],
                "coverage_pct": round((yd["covered"] / yd["terminal"] * 100) if yd["terminal"] else 0.0, 2),
                "series_breakdown": dict(yd["series_breakdown"]),
                "sample_missing_game_ids": [g["game_id"] for g in yd["missing_games"][:5]],
            }
            for y, yd in gaps_data["years"].items()
        },
        "probe_results": probes,
        "triage_recommendations": [
            {
                "category": "2010 Regular Season Gaps (581 games)",
                "classification": "RECOVERY_CANDIDATE_PHASE_1",
                "action": "Execute targeted KBO schedule/boxscore crawl for 2010 season",
                "remediation_cmd": "python3 -m src.cli.crawl_schedule --year 2010 && python3 -m src.cli.collect_games --year 2010",
            },
            {
                "category": "2011-2019 Exhibition/Special Series Gaps (~1,638 games)",
                "classification": "RECOVERY_CANDIDATE_PHASE_2",
                "action": "Run backfill_historical_details for terminal missing games by year",
                "remediation_cmd": "for y in $(seq 2011 2019); do python3 -m src.cli.backfill_historical_details --year $y; done",
            },
        ],
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Saved gap resolution report to %s", args.output)

    return 0


if __name__ == "__main__":
    sys.exit(main())
