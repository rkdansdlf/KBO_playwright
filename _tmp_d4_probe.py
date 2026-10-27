from pathlib import Path
from urllib.parse import urlsplit, unquote
import psycopg

raw = [l.split('=',1)[1].strip().strip('"').strip("'") for l in Path('.env').read_text(encoding='utf-8',errors='replace').splitlines() if l.strip().startswith('DATABASE_URL=')][0]
u = urlsplit(raw)
dsn = f"host={u.hostname} port={u.port} dbname={u.path.lstrip('/')} user={u.username} password={unquote(u.password or '')} connect_timeout=8 options='-c default_transaction_read_only=on'"

with psycopg.connect(dsn) as conn, conn.cursor() as cur:
    cur.execute("select season_id, count(*) from game group by 1 order by 1")
    print('=== game counts by season_id ===')
    for r in cur.fetchall(): print('  ', r)
    print()

    cur.execute("select count(*) from game")
    print('=== total games ===', cur.fetchone()[0])
    print()

    cur.execute("select distinct home_team from game where season_id = 2024 order by 1")
    print('=== home_team distinct 2024 ===')
    for r in cur.fetchall(): print('  ', r[0])
    print()

    cur.execute("select distinct home_team from game where season_id = 2026 order by 1")
    print('=== home_team distinct 2026 ===')
    for r in cur.fetchall(): print('  ', r[0])
    print()

    cur.execute("select count(*) from game where season_id = 2024 and (home_team = 'HT' or away_team = 'HT')")
    print('=== HT games 2024 ===', cur.fetchone()[0])

    cur.execute("select count(*) from game where season_id = 2026 and (home_team = 'HT' or away_team = 'HT')")
    print('=== HT games 2026 ===', cur.fetchone()[0])

    cur.execute("select count(*) from game where season_id = 2024 and (home_team ILIKE '%KIA%' or away_team ILIKE '%KIA%')")
    print('=== KIA games 2024 ===', cur.fetchone()[0])

    cur.execute("select count(*) from game where season_id = 2026 and (home_team ILIKE '%KIA%' or away_team ILIKE '%KIA%')")
    print('=== KIA games 2026 ===', cur.fetchone()[0])

    cur.execute("select distinct game_status from game order by 1")
    print('=== all game_status ===')
    for r in cur.fetchall(): print('  ', r[0])
    print()

    cur.execute("select is_primary, count(*) from game group by 1 order by 1")
    print('=== is_primary distribution ===')
    for r in cur.fetchall(): print('  ', r)
    print()

    conn.rollback()