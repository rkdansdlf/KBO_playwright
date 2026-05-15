#!/usr/bin/env python3
"""
Deep Statistical Audit for KBO Game Data.
Verifies logical consistency across different tables:
1. Score consistency (Innings vs Final)
2. Batting formula (PA = AB + BB + HBP + SH + SF)
3. Hit consistency (H <= AB)
4. Cross-domain consistency (Away Batting vs Home Pitching)
5. Earned Run constraint (Team ER <= Opponent Runs)
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List

from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.engine import SessionLocal

def audit_game_logic(year: int | None = None, game_id: str | None = None) -> List[Dict[str, Any]]:
    violations = []
    
    with SessionLocal() as session:
        # Filter for completed games
        base_filter = "WHERE g.game_status IN ('COMPLETED', 'DRAW')"
        params = {}
        if year:
            base_filter += " AND g.game_date LIKE :year_pattern"
            params["year_pattern"] = f"{year}%"
        if game_id:
            base_filter += " AND g.game_id = :game_id"
            params["game_id"] = game_id

        # 1. Score Consistency (Innings vs Final)
        print("   - Checking Score Consistency (Innings vs Final)...")
        score_inconsistencies = session.execute(
            text(f"""
                WITH inning_totals AS (
                    SELECT 
                        game_id, 
                        team_side, 
                        SUM(runs) as total_inning_runs
                    FROM game_inning_scores
                    GROUP BY game_id, team_side
                )
                SELECT 
                    g.game_id, 
                    g.game_date,
                    g.home_score, 
                    g.away_score,
                    it_home.total_inning_runs as home_inning_total,
                    it_away.total_inning_runs as away_inning_total
                FROM game g
                JOIN inning_totals it_home ON g.game_id = it_home.game_id AND it_home.team_side = 'home'
                JOIN inning_totals it_away ON g.game_id = it_away.game_id AND it_away.team_side = 'away'
                {base_filter}
                AND (g.home_score != it_home.total_inning_runs OR g.away_score != it_away.total_inning_runs)
            """),
            params
        ).mappings().all()

        for row in score_inconsistencies:
            violations.append({
                "game_id": row["game_id"],
                "game_date": row["game_date"],
                "reason": f"Score Mismatch: Final(H:{row['home_score']} A:{row['away_score']}) != Innings(H:{row['home_inning_total']} A:{row['away_inning_total']})"
            })

        # 2. Batting Formula (PA = AB + BB + HBP + SH + SF)
        print("   - Checking Batting Formula (PA = AB + BB + HBP + SH + SF)...")
        batting_formula_violations = session.execute(
            text(f"""
                SELECT 
                    g.game_id, 
                    g.game_date,
                    b.player_id,
                    p.name as player_name,
                    b.plate_appearances as pa, b.at_bats as ab, b.walks as bb, b.hbp, b.sacrifice_hits as sh, b.sacrifice_flies as sf
                FROM game_batting_stats b
                JOIN game g ON b.game_id = g.game_id
                LEFT JOIN player_basic p ON b.player_id = p.player_id
                {base_filter}
                AND b.plate_appearances != (b.at_bats + b.walks + b.hbp + b.sacrifice_hits + b.sacrifice_flies)
            """),
            params
        ).mappings().all()

        for row in batting_formula_violations:
            violations.append({
                "game_id": row["game_id"],
                "game_date": row["game_date"],
                "reason": f"Batting Formula: {row['player_name']}({row['player_id']}) PA({row['pa']}) != AB({row['ab']})+BB({row['bb']})+HBP({row['hbp']})+SH({row['sh']})+SF({row['sf']})"
            })

        # 3. Hit Consistency (H <= AB)
        print("   - Checking Hit Consistency (H <= AB)...")
        hit_violations = session.execute(
            text(f"""
                SELECT 
                    g.game_id, 
                    g.game_date,
                    b.player_id,
                    p.name as player_name,
                    b.hits as h, b.at_bats as ab
                FROM game_batting_stats b
                JOIN game g ON b.game_id = g.game_id
                LEFT JOIN player_basic p ON b.player_id = p.player_id
                {base_filter}
                AND b.hits > b.at_bats
            """),
            params
        ).mappings().all()

        for row in hit_violations:
            violations.append({
                "game_id": row["game_id"],
                "game_date": row["game_date"],
                "reason": f"Impossible Stats: {row['player_name']}({row['player_id']}) H({row['h']}) > AB({row['ab']})"
            })

        # 4. Cross-domain consistency (Batting H vs Pitching H Allowed)
        # Checking team totals: Away Batting H sum vs Home Pitching H_allowed sum
        print("   - Checking Cross-domain consistency (Team Totals)...")
        cross_domain_violations = session.execute(
            text(f"""
                WITH batting_totals AS (
                    SELECT 
                        game_id, 
                        team_side, 
                        SUM(hits) as total_h,
                        SUM(home_runs) as total_hr,
                        SUM(strikeouts) as total_so
                    FROM game_batting_stats
                    GROUP BY game_id, team_side
                ),
                pitching_totals AS (
                    SELECT 
                        game_id, 
                        team_side, 
                        SUM(hits_allowed) as total_h_allowed,
                        SUM(home_runs_allowed) as total_hr_allowed,
                        SUM(strikeouts) as total_so
                    FROM game_pitching_stats
                    GROUP BY game_id, team_side
                )
                SELECT 
                    g.game_id, 
                    g.game_date,
                    bt_away.total_h as away_bat_h, pt_home.total_h_allowed as home_pitch_h,
                    bt_away.total_hr as away_bat_hr, pt_home.total_hr_allowed as home_pitch_hr,
                    bt_away.total_so as away_bat_so, pt_home.total_so as home_pitch_so
                FROM game g
                JOIN batting_totals bt_away ON g.game_id = bt_away.game_id AND bt_away.team_side = 'away'
                JOIN pitching_totals pt_home ON g.game_id = pt_home.game_id AND pt_home.team_side = 'home'
                {base_filter}
                AND (bt_away.total_h != pt_home.total_h_allowed 
                     OR bt_away.total_hr != pt_home.total_hr_allowed
                     OR bt_away.total_so != pt_home.total_so)
            """),
            params
        ).mappings().all()

        for row in cross_domain_violations:
            violations.append({
                "game_id": row["game_id"],
                "game_date": row["game_date"],
                "reason": f"Batting/Pitching Mismatch (Away Bat vs Home Pitch): H({row['away_bat_h']} vs {row['home_pitch_h']}), HR({row['away_bat_hr']} vs {row['home_pitch_hr']}), SO({row['away_bat_so']} vs {row['home_pitch_so']})"
            })

        # 5. Earned Run Constraint (Team ER <= Opponent Runs)
        print("   - Checking Earned Run Constraint (Team ER <= Opponent Runs)...")
        er_violations = session.execute(
            text(f"""
                SELECT 
                    g.game_id, 
                    g.game_date,
                    gps.team_side,
                    SUM(gps.earned_runs) as team_er,
                    CASE WHEN gps.team_side = 'home' THEN g.away_score ELSE g.home_score END as opp_runs
                FROM game_pitching_stats gps
                JOIN game g ON gps.game_id = g.game_id
                {base_filter}
                GROUP BY g.game_id, gps.team_side
                HAVING team_er > opp_runs
            """),
            params
        ).mappings().all()

        for row in er_violations:
            violations.append({
                "game_id": row["game_id"],
                "game_date": row["game_date"],
                "reason": f"ER Constraint Violation ({row['team_side']} team): Total ER({row['team_er']}) > Opponent Runs({row['opp_runs']})"
            })

    return violations

def main():
    parser = argparse.ArgumentParser(description="Deep Statistical Audit for KBO Game Data")
    parser.add_argument("--year", type=int, help="Filter audit by year")
    parser.add_argument("--game-id", help="Filter audit by specific game_id")
    parser.add_argument("--fail", action="store_true", help="Exit with non-zero code if violations found")
    args = parser.parse_args()

    print(f"🕵️  Starting Deep Statistical Audit...")
    if args.year: print(f"   Filter: Year={args.year}")
    if args.game_id: print(f"   Filter: GameID={args.game_id}")

    violations = audit_game_logic(year=args.year, game_id=args.game_id)

    if not violations:
        print("✅ No logical inconsistencies found.")
        sys.exit(0)

    print(f"❌ Found {len(violations)} logical inconsistencies:")
    # Group by Game ID for better readability
    grouped = {}
    for v in violations:
        grouped.setdefault(v['game_id'], []).append(v)
    
    for gid in sorted(grouped.keys()):
        msgs = grouped[gid]
        print(f"  - [{gid}] {msgs[0]['game_date']}")
        for m in msgs:
            print(f"    * {m['reason']}")

    if args.fail:
        sys.exit(1)

if __name__ == "__main__":
    main()
