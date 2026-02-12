#!/usr/bin/env python3
"""
Seed 2025 team history records into OCI databases using explicit IDs.
"""
import os
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def seed_2025_history(url):
    if not url: return
    print(f"🌱 Seeding 2025 history into {url}...")
    engine = create_engine(url)
    
    history_2025 = [
        (1, "SS", "삼성 라이온즈"),
        (2, "LT", "롯데 자이언츠"),
        (3, "LG", "LG 트윈스"),
        (4, "DB", "두산 베어스"),
        (5, "KIA", "KIA 타이거즈"),
        (6, "KH", "키움 히어로즈"),
        (7, "HH", "한화 이글스"),
        (8, "SSG", "SSG 랜더스"),
        (9, "NC", "NC 다이노스"),
        (10, "KT", "kt wiz"),
    ]
    
    with engine.begin() as conn:
        # Get start ID
        max_id = conn.execute(text("SELECT MAX(id) FROM team_history")).scalar() or 0
        current_id = max_id + 1
        
        for fid, code, name in history_2025:
            # Check if exists (by franchise_id and season)
            check = text("SELECT id FROM team_history WHERE franchise_id = :fid AND season = 2025")
            existing_id = conn.execute(check, {"fid": fid}).scalar()
            
            if existing_id is None:
                insert = text("""
                    INSERT INTO team_history (id, franchise_id, season, team_code, team_name, created_at, updated_at)
                    VALUES (:id, :fid, 2025, :code, :name, NOW(), NOW())
                """)
                conn.execute(insert, {"id": current_id, "fid": fid, "code": code, "name": name})
                print(f"   Added {code} for 2025 (ID: {current_id})")
                current_id += 1
            else:
                update = text("""
                    UPDATE team_history SET team_code = :code, team_name = :name, updated_at = NOW()
                    WHERE id = :id
                """)
                conn.execute(update, {"id": existing_id, "code": code, "name": name})
                print(f"   Updated {code} for 2025 (ID: {existing_id})")

if __name__ == "__main__":
    load_dotenv()
    oci_url = os.getenv("OCI_DB_URL")
    
    # Target postgres database
    seed_2025_history(oci_url)
    
    # Safely replace database name at the end of URL for bega_backend
    if oci_url and oci_url.endswith("/postgres"):
        bega_url = oci_url[:-9] + "/bega_backend"
        seed_2025_history(bega_url)
