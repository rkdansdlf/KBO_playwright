import os

import psycopg2
from dotenv import load_dotenv


def main():
    load_dotenv()
    db_url = os.getenv("OCI_DB_URL") or os.getenv("TARGET_DATABASE_URL")
    if not db_url:
        print("OCI_DB_URL not set")
        return

    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            print("--- Active/Idle in Transaction Queries ---")
            cur.execute("""
                SELECT pid, age(clock_timestamp(), query_start), state, query, client_addr
                FROM pg_stat_activity
                WHERE pid != pg_backend_pid()
                  AND (state != 'idle' OR state = 'idle in transaction')
                ORDER BY query_start ASC;
            """)
            rows = cur.fetchall()
            for r in rows:
                print(f"PID: {r[0]}, Age: {r[1]}, State: {r[2]}, Client IP: {r[4]}")
                print(f"Query: {r[3]}")
                print("-" * 40)

            print("\n--- Locks ---")
            cur.execute("""
                SELECT
                    coalesce(blockingl.relation::regclass::text, blockingl.locktype) as locked_item,
                    blockeda.pid AS blocked_pid,
                    blockeda.query as blocked_query,
                    blockedl.mode as blocked_mode,
                    blockinga.pid AS blocking_pid,
                    blockinga.query as blocking_query,
                    blockingl.mode as blocking_mode
                FROM pg_catalog.pg_locks blockedl
                JOIN pg_catalog.pg_stat_activity blockeda ON blockeda.pid = blockedl.pid
                JOIN pg_catalog.pg_locks blockingl ON blockingl.pid != blockedl.pid
                    AND (
                        (blockingl.transactionid = blockedl.transactionid AND blockedl.locktype = 'transactionid')
                        OR (blockingl.relation = blockedl.relation AND blockedl.locktype = 'relation')
                    )
                JOIN pg_catalog.pg_stat_activity blockinga ON blockinga.pid = blockingl.pid
                WHERE NOT blockedl.granted;
            """)
            locks = cur.fetchall()
            for l in locks:
                print(f"Locked Item: {l[0]}")
                print(f"Blocked PID: {l[1]} | Mode: {l[3]}")
                print(f"Blocked Query: {l[2]}")
                print(f"Blocking PID: {l[4]} | Mode: {l[6]}")
                print(f"Blocking Query: {l[5]}")
                print("-" * 40)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
