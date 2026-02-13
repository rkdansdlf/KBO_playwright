import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
url = os.getenv('OCI_DB_URL')
print(f"Connecting to {url}")
engine = create_engine(url)

with engine.connect() as conn:
    print("Tables in public schema:")
    res = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"))
    for row in res:
        print(f" - {row[0]}")
    
    print("\nCounts:")
    tables = ['kbo_seasons', 'player_basic', 'player_season_batting', 'game']
    for t in tables:
        try:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {t}")).fetchone()[0]
            print(f" - {t}: {count}")
        except Exception as e:
            print(f" - {t}: Error: {e}")

    print("\nSample 2001 Seasons:")
    try:
        res = conn.execute(text("SELECT * FROM kbo_seasons WHERE season_year = 2001"))
        for row in res:
            print(f" - {row}")
    except Exception as e:
        print(f" Error: {e}")
