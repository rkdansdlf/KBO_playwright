from pathlib import Path
from urllib.parse import urlsplit, unquote
import psycopg

raw = [l.split('=',1)[1].strip().strip('"').strip("'") for l in Path('.env').read_text(encoding='utf-8',errors='replace').splitlines() if l.strip().startswith('DATABASE_URL=')][0]
u = urlsplit(raw)
dsn = f"host={u.hostname} port={u.port} dbname={u.path.lstrip('/')} user={u.username} password={unquote(u.password or '')} connect_timeout=8 options='-c default_transaction_read_only=on'"

with psycopg.connect(dsn) as conn, conn.cursor() as cur:
    # Check game_pitching_stats columns
    print("=== game_pitching_stats columns ===")
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='game_pitching_stats' ORDER BY ordinal_position")
    cols = [r[0] for r in cur.fetchall()]
    for c in cols: print(c)
    print()
    
    # Check player_season_pitching columns
    print("=== player_season_pitching columns ===")
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='player_season_pitching' ORDER BY ordinal_position")
    cols = [r[0] for r in cur.fetchall()]
    for c in cols: print(c)
    print()
    
    conn.rollback()