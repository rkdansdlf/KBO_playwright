from pathlib import Path
from urllib.parse import urlsplit, unquote
import psycopg

raw = [l.split('=',1)[1].strip().strip('"').strip("'") for l in Path('.env').read_text(encoding='utf-8',errors='replace').splitlines() if l.strip().startswith('DATABASE_URL=')][0]
u = urlsplit(raw)
dsn = f"host={u.hostname} port={u.port} dbname={u.path.lstrip('/')} user={u.username} password={unquote(u.password or '')} connect_timeout=8 options='-c default_transaction_read_only=on'"

with psycopg.connect(dsn) as conn, conn.cursor() as cur:
    # Section 2: KIA games in season 2026
    cur.execute("""
        SELECT game_id, home_team, away_team, game_date, game_time, status, is_primary, inning, away_score, home_score, finishing_outs
        FROM game
        WHERE season_id = 2026 AND (home_team = 'KIA' OR away_team = 'KIA')
        ORDER BY game_date, game_time
    """)
    games_data = cur.fetchall()
    
    # Section 3: KIA pitcher records
    cur.execute("""
        SELECT gps.game_id, gps.away_pitcher, gps.home_pitcher, gps.away_score, gps.home_score, gps.status, gps.is_primary, gps.inning
        FROM game_pitching_stats gps
        JOIN game g ON g.game_id = gps.game_id
        WHERE g.season_id = 2026 AND (g.home_team = 'KIA' OR g.away_team = 'KIA')
    """)
    pitching_data = cur.fetchall()
    
    # Section 4: team_code, is_starting, innings_outs NULL counts
    cur.execute("""
        SELECT team_code, is_starting, innings_outs, COUNT(*) as null_count
        FROM game_pitching_stats
        WHERE season = 2026
        GROUP BY team_code, is_starting, innings_outs
        HAVING innings_outs IS NULL OR team_code IS NULL
        ORDER BY null_count DESC, team_code
    """)
    team_stats_data = cur.fetchall()
    
    # Section 5: game_status, is_primary distribution
    cur.execute("""
        SELECT game_status, is_primary, COUNT(*)
        FROM game
        WHERE season_id = 2026
        GROUP BY game_status, is_primary
        ORDER BY game_status, is_primary
    """)
    game_status_data = cur.fetchall()
    
    # Section 6: player_season_pitching league, level values
    cur.execute("""
        SELECT league, level, COUNT(*)
        FROM player_season_pitching
        WHERE season = 2026
        GROUP BY league, level
        ORDER BY league, level
    """)
    player_stats_data = cur.fetchall()
    
    conn.rollback()

results = []
results.append("Section 2: KIA games (season 2026)")
results.append("=" * 50)
results.append(f"Total KIA games: {len(games_data)}")
results.append("")
for row in games_data:
    g_id, home, away, date, time, st, ip, inning, ascore, hscore, outs = row
    results.append(f"game_id: {g_id} | home_team: {home} | away_team: {away} | game_date: {date} | game_time: {time} | status: {st} | is_primary: {ip} | inning: {inning} | away_score: {ascore} | home_score: {hscore} | finishing_outs: {outs}")

results.append("")
results.append("Section 3: KIA pitcher records")
results.append("=" * 50)
results.append(f"Total KIA pitcher records: {len(pitching_data)}")
results.append("")
for row in pitching_data:
    gid, ap, hp, asc, hs, st, ip = row
    results.append(f"game_id: {gid} | away_pitcher: {ap} | home_pitcher: {hp} | away_score: {asc} | home_score: {hs} | status: {st} | is_primary: {ip} | inning: {ip}")

results.append("")
results.append("Section 4: team_code, is_starting, innings_outs NULL counts")
results.append("=" * 50)
results.append(f"Total rows with NULL values: {len(team_stats_data)}")
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

print("Done. Checked successfully.")
print(f"KIA games: {len(games_data)}")
print(f"KIA pitchers: {len(pitching_data)}")
print(f"NULL value rows: {len(team_stats_data)}")
print(f"Game status groups: {len(game_status_data)}")
print(f"Player stats groups: {len(player_stats_data)}")