import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def verify_oci_data():
    load_dotenv()
    oci_url = os.getenv('OCI_DB_URL') or os.getenv('TARGET_DATABASE_URL')
    if not oci_url:
        print("❌ OCI_DB_URL is not set.")
        return

    engine = create_engine(oci_url)
    
    with engine.connect() as conn:
        print("🔍 Checking game_summary for large detail_text rows...")
        sql = """
            SELECT game_id, summary_type, LENGTH(detail_text) as len
            FROM game_summary 
            WHERE summary_type = '리뷰_WPA'
            ORDER BY len DESC
            LIMIT 5;
        """
        results = conn.execute(text(sql)).fetchall()
        for row in results:
            print(f"  Game: {row[0]}, Type: {row[1]}, Length: {row[2]} characters")

        print("\n🔍 Checking for any remaining duplicates (should be 0)...")
        sql_dup = """
            SELECT game_id, summary_type, COALESCE(player_id, 0), COALESCE(player_name, ''), md5(COALESCE(detail_text, '')), COUNT(*)
            FROM game_summary
            GROUP BY 1, 2, 3, 4, 5
            HAVING COUNT(*) > 1;
        """
        duplicates = conn.execute(text(sql_dup)).fetchall()
        if not duplicates:
            print("✅ No duplicate records found.")
        else:
            print(f"⚠️ Found {len(duplicates)} duplicate sets!")

    engine.dispose()

if __name__ == "__main__":
    verify_oci_data()
