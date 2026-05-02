"""
Grand Reconciliation Audit: SQLite (Local) vs OCI (Production)
Purpose: Ensure total data parity between development and production environments.
"""
import os
import sys
from collections import defaultdict
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

load_dotenv()

def get_stats(engine, db_type="local"):
    conn = engine.connect()
    stats = {
        "tables": {},
        "game_years": {},
        "batting_years": {},
        "pitching_years": {}
    }
    
    tables = [
        "player_basic", "player_season_batting", "player_season_pitching",
        "game", "game_metadata", "game_inning_scores", "game_lineups",
        "game_batting_stats", "game_pitching_stats", "game_summary", "game_play_by_play",
        "teams", "kbo_seasons", "awards"
    ]
    
    # Table counts
    for table in tables:
        try:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()[0]
            stats["tables"][table] = count
        except Exception:
            stats["tables"][table] = -1

    # Game distribution
    game_sql = "SELECT SUBSTR(game_id, 1, 4) as year, COUNT(*) as cnt FROM game GROUP BY year"
    res = conn.execute(text(game_sql)).fetchall()
    for yr, cnt in res:
        stats["game_years"][yr] = cnt

    # Season stats distribution
    bat_sql = "SELECT season, COUNT(*) as cnt FROM player_season_batting GROUP BY season"
    res = conn.execute(text(bat_sql)).fetchall()
    for yr, cnt in res:
        stats["batting_years"][yr] = cnt

    pit_sql = "SELECT season, COUNT(*) as cnt FROM player_season_pitching GROUP BY season"
    res = conn.execute(text(pit_sql)).fetchall()
    for yr, cnt in res:
        stats["pitching_years"][yr] = cnt

    conn.close()
    return stats

def main():
    local_url = "sqlite:///data/kbo_dev.db"
    oci_url = os.getenv("OCI_DB_URL")

    if not oci_url:
        print("❌ OCI_DB_URL not found.")
        return

    print("🚀 Initializing Grand Reconciliation Audit...")
    print(f"📡 Local: {local_url}")
    print(f"📡 OCI:   {oci_url[:50]}...")

    local_engine = create_engine(local_url)
    oci_engine = create_engine(oci_url)

    local_stats = get_stats(local_engine, "local")
    oci_stats = get_stats(oci_engine, "oci")

    print("\n" + "="*80)
    print(f"{'TABLE NAME':<25} | {'LOCAL':>10} | {'OCI':>10} | {'DIFF':>8} | STATUS")
    print("-" * 80)

    all_tables = sorted(set(local_stats["tables"].keys()) | set(oci_stats["tables"].keys()))
    for table in all_tables:
        l_cnt = local_stats["tables"].get(table, 0)
        o_cnt = oci_stats["tables"].get(table, 0)
        diff = o_cnt - l_cnt
        status = "✅ OK" if diff == 0 else "⚠️ DIFF"
        if l_cnt == -1 or o_cnt == -1: status = "❌ ERR"
        print(f"{table:<25} | {l_cnt:>10} | {o_cnt:>10} | {diff:>8} | {status}")

    print("\n" + "="*80)
    print("📅 Yearly Game parity (Last 10 Years + Historic Samples)")
    print("-" * 80)
    print(f"{'YEAR':<10} | {'LOCAL':>10} | {'OCI':>10} | {'DIFF':>8} | STATUS")
    
    all_years = sorted(set(local_stats["game_years"].keys()) | set(oci_stats["game_years"].keys()), reverse=True)
    check_years = [y for y in all_years if int(y) >= 2015] + ["2010", "2001", "1990", "1982"]
    
    for yr in sorted(set(check_years), reverse=True):
        if yr not in all_years: continue
        l_cnt = local_stats["game_years"].get(yr, 0)
        o_cnt = oci_stats["game_years"].get(yr, 0)
        diff = o_cnt - l_cnt
        status = "✅" if diff == 0 else "❌"
        print(f"{yr:<10} | {l_cnt:>10} | {o_cnt:>10} | {diff:>8} | {status}")

    print("\n" + "="*80)
    print("📈 Season Stats Parity (Batting)")
    print("-" * 80)
    bat_years = sorted(set(local_stats["batting_years"].keys()) | set(oci_stats["batting_years"].keys()), reverse=True)
    for yr in bat_years[:5]: # Latest 5 years
        l_cnt = local_stats["batting_years"].get(yr, 0)
        o_cnt = oci_stats["batting_years"].get(yr, 0)
        diff = o_cnt - l_cnt
        status = "✅" if diff == 0 else "❌"
        print(f"BATTING {yr:<3} | {l_cnt:>10} | {o_cnt:>10} | {diff:>8} | {status}")

    print("\n✅ Audit complete.")

if __name__ == "__main__":
    main()
