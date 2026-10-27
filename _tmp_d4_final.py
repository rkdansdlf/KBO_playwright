from pathlib import Path
from urllib.parse import urlsplit, unquote
import psycopg

raw = [l.split('=',1)[1].strip().strip('"').strip("'") for l in Path('.env').read_text(encoding='utf-8',errors='replace').splitlines() if l.strip().startswith('DATABASE_URL=')][0]
u = urlsplit(raw)
dsn = f"host={u.hostname} port={u.port} dbname={u.path.lstrip('/')} user={u.username} password={unquote(u.password or '')} connect_timeout=8 options='-c default_transaction_read_only=on'"

with psycopg.connect(dsn) as conn, conn.cursor() as cur:
    # Section 2: KIA games in season 2026
    # game columns: game_id, game_date, home_team, away_team, home_score, away_score, season_id, game_status, is_primary
    cur.execute("""
        SELECT g.game_id, g.game_date, g.home_team, g.away_team, g.home_score, g.away_score, g.game_status, g.is_primary
        FROM game g
        WHERE g.season_id = 2026 AND (g.home_team = 'KIA' OR g.away_team = 'KIA')
        ORDER BY g.game_date
    """)
    games_data = cur.fetchall()
    
    # Section 3: KIA pitcher records
    cur.execute("""
        SELECT gps.id, gps.game_id, gps.team_side, gps.team_code, gps.is_starting, gps.innings_outs, gps.extra_stats
        FROM game_pitching_stats gps
        JOIN game g ON g.game_id = gps.game_id
        WHERE g.season_id = 2026 AND (g.home_team = 'KIA' OR g.away_team = 'KIA')
    """)
    pitching_data = cur.fetchall()
    
    # Section 4: team_code, is_starting, innings_outs NULL counts
    cur.execute("""
        SELECT gps.team_code, gps.is_starting, gps.innings_outs, COUNT(*) as null_count
        FROM game_pitching_stats gps
        JOIN game g ON g.game_id = gps.game_id
        WHERE g.season_id = 2026
        GROUP BY gps.team_code, gps.is_starting, gps.innings_outs
        HAVING gps.innings_outs IS NULL OR gps.team_code IS NULL
        ORDER BY null_count DESC, gps.team_code
    """)
    team_stats_data = cur.fetchall()
    
    # Section 5: game_status, is_primary distribution
    cur.execute("""
        SELECT g.game_status, g.is_primary, COUNT(*)
        FROM game g
        WHERE g.season_id = 2026
        GROUP BY g.game_status, g.is_primary
        ORDER BY g.game_status, g.is_primary
    """)
    game_status_data = cur.fetchall()
    
    # Section 6: player_season_pitching league, level values
    cur.execute("""
        SELECT psp.league, psp.level, COUNT(*)
        FROM player_season_pitching psp
        WHERE psp.season = 2026
        GROUP BY psp.league, psp.level
        ORDER BY psp.league, psp.level
    """)
    player_stats_data = cur.fetchall()
    
    conn.rollback()

results = []
results.append("Section 2: KIA games (season 2026)")
results.append("=" * 50)
results.append(f"Total KIA games: {len(games_data)}")
results.append("")
for row in games_data:
    g_id, g_date, home, away, h_score, a_score, gs, ip = row
    results.append(f"game_id: {g_id} | game_date: {g_date} | home_team: {home} | away_team: {away} | home_score: {h_score} | away_score: {a_score} | game_status: {gs} | is_primary: {ip}")

results.append("")
results.append("Section 3: KIA pitcher records")
results.append("=" * 50)
results.append(f"Total KIA pitcher records: {len(pitching_data)}")
results.append("")
for row in pitching_data:
    pid, gid, ts, tc, start, outs, es = row
    results.append(f"pitcher_id: {pid} | game_id: {gid} | team_side: {ts} | team_code: {tc} | is_starting: {start} | innings_outs: {outs} | extra_stats: {es}")

results.append("")
results.append("Section 4: team_code, is_starting, innings_outs NULL counts")
results.append("=" * 50)
results.append(f"Total groups with NULL values: {len(team_stats_data)}")
results.append("")
for row in team_stats_data:
    tc, start, outs, n = row
    results.append(f"team_code: {tc} | is_starting: {start} | innings_outs: {outs} | null_count: {n}")

results.append("")
results.append("Section 5: game_status, is_primary distribution")
results.append("=" * 50)
results.append(f"Total groups: {len(game_status_data)}")
results.append("")
for row in game_status_data:
    gs, ip, c = row
    results.append(f"game_status: {gs} | is_primary: {ip} | count: {c}")

results.append("")
results.append("Section 6: player_season_pitching league, level values")
results.append("=" * 50)
results.append(f"Total groups: {len(player_stats_data)}")
results.append("")
for row in player_stats_data:
    lg, lv, c = row
    results.append(f"league: {lg} | level: {lv} | count: {c}")

with open('d4_result.txt', 'w', encoding='utf-8') as f:
    f.write('\n'.join(results))

print("D4 read-only check completed successfully!")
print(f"KIA games: {len(games_data)}")
print(f"KIA pitchers: {len(pitching_data)}")
print(f"NULL value groups: {len(team_stats_data)}")
print(f"Game status groups: {len(game_status_data)}")
print(f"Player stats groups: {len(player_stats_data)}")
print("\nOutput saved to d4_result.txt")