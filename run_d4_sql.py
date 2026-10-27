"""Execute d4-readonly-check-2026-10-26.sql against PostgreSQL read-only."""
from pathlib import Path
from urllib.parse import urlsplit, unquote
import psycopg

raw = [l.split('=',1)[1].strip().strip('"').strip("'")
       for l in Path('.env').read_text(encoding='utf-8',errors='replace').splitlines()
       if l.strip().startswith('DATABASE_URL=')][0]
u = urlsplit(raw)
dsn = (f"host={u.hostname} port={u.port} dbname={u.path.lstrip('/')} "
       f"user={u.username} password={unquote(u.password or '')} "
       f"connect_timeout=8 options='-c default_transaction_read_only=on'")

sql_text = Path('d4-readonly-check-2026-10-26.sql').read_text(encoding='utf-8')

# Split by semicolon (standard SQL statement terminator), then filter out pure comments and comment-only blocks
def _is_sql_block(s):
    lines = s.splitlines()
    sql_lines = [l for l in lines if l.strip() and not l.strip().startswith('--')]
    return bool(sql_lines)

raw_statements = [s.strip() for s in sql_text.split(';') if s.strip()]
filtered_queries = [s for s in raw_statements if _is_sql_block(s)]

output = []
output.append("=" * 70)
output.append("D4 READ-ONLY CHECK RESULTS (from d4-readonly-check-2026-10-26.sql)")
output.append("=" * 70)
output.append(f"Total queries executed: {len(filtered_queries)}")
output.append("Mode: READ-ONLY (default_transaction_read_only=on)")
output.append("Transaction: ROLLED BACK (no writes)")
output.append("")

with psycopg.connect(dsn) as conn, conn.cursor() as cur:
    for i, sql in enumerate(filtered_queries, 1):
        lines = sql.splitlines()
        header_lines = [l.strip() for l in lines if l.strip().startswith('--')]
        label = next((l for l in header_lines if 'Q' in l.upper()), f"Query {i}")
        
        output.append(f"--- {label} ---")
        
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            if not rows:
                output.append("  (no rows)")
            else:
                col_names = [d[0] for d in cur.description]
                for row in rows:
                    parts = [f"{col_names[j]}={val}" for j, val in enumerate(row)]
                    output.append("  " + " | ".join(parts))
        except Exception as e:
            output.append(f"  ERROR: {e}")
        output.append("")

    conn.rollback()

output.append("=" * 70)
output.append("END OF RESULTS")
output.append("Transaction rolled back. No database modifications.")

with open('d4_result.txt', 'w', encoding='utf-8') as f:
    f.write('\n'.join(output))

print(f"D4 SQL executed: {len(filtered_queries)} queries")
print(f"Output: d4_result.txt ({len(output)} lines)")
print("Transaction rolled back (read-only).")