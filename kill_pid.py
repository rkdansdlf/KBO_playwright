import os
import sys
import psycopg2
from dotenv import load_dotenv

def main():
    if len(sys.argv) < 2:
        print("Usage: kill_pid.py <PID>")
        return

    pid = int(sys.argv[1])
    load_dotenv()
    db_url = os.getenv("OCI_DB_URL") or os.getenv("TARGET_DATABASE_URL")
    if not db_url:
        print("OCI_DB_URL not set")
        return

    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            print(f"Terminating PID: {pid}")
            cur.execute("SELECT pg_terminate_backend(%s);", (pid,))
            result = cur.fetchone()[0]
            print(f"Result: {result}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
