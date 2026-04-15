import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def migrate():
    load_dotenv()
    url = os.getenv('OCI_DB_URL')
    if not url:
        print("❌ OCI_DB_URL not found in environment")
        return

    engine = create_engine(url)
    
    columns_to_add = [
        ("photo_url", "VARCHAR(500)"),
        ("salary_original", "VARCHAR(50)"),
        ("signing_bonus_original", "VARCHAR(50)"),
        ("draft_info", "VARCHAR(100)")
    ]

    with engine.connect() as conn:
        for col_name, col_type in columns_to_add:
            try:
                print(f"Adding column {col_name}...")
                conn.execute(text(f"ALTER TABLE players ADD COLUMN IF NOT EXISTS {col_name} {col_type}"))
                print(f"✅ Added {col_name}")
            except Exception as e:
                print(f"⚠️ Could not add {col_name}: {e}")
        
        conn.commit()

if __name__ == "__main__":
    migrate()
