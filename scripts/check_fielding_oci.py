import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
url = os.getenv('OCI_DB_URL')
if not url:
    print("OCI_DB_URL not set")
    exit(1)

engine = create_engine(url)
with engine.connect() as conn:
    try:
        count = conn.execute(text("SELECT COUNT(*) FROM player_season_fielding")).fetchone()[0]
        print(f"player_season_fielding count: {count}")
        
        years = conn.execute(text("SELECT year, COUNT(*) FROM player_season_fielding GROUP BY year ORDER BY year DESC")).fetchall()
        for year, cnt in years:
            print(f"  {year}: {cnt}")
    except Exception as e:
        print(f"Error: {e}")
