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
            cur.execute("SELECT COUNT(*) FROM rag_chunks;")
            cnt = cur.fetchone()[0]
            print(f"Total rows in rag_chunks: {cnt}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
