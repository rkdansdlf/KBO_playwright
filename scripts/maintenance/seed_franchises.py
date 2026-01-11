
import os
import sys

# Ensure project root is in path
sys.path.append(os.getcwd())

from src.db.engine import SessionLocal
from src.models.franchise import Franchise

FRANCHISES = [
    {"id": 1, "name": "삼성 라이온즈", "original_code": "SS", "current_code": "SS"},
    {"id": 2, "name": "롯데 자이언츠", "original_code": "LT", "current_code": "LOT"},
    {"id": 3, "name": "LG 트윈스", "original_code": "LG", "current_code": "LG"},
    {"id": 4, "name": "두산 베어스", "original_code": "OB", "current_code": "OB"},
    {"id": 5, "name": "KIA 타이거즈", "original_code": "HT", "current_code": "KIA"},
    {"id": 6, "name": "키움 히어로즈", "original_code": "WO", "current_code": "WO"},
    {"id": 7, "name": "한화 이글스", "original_code": "HH", "current_code": "HH"},
    {"id": 8, "name": "SSG 랜더스", "original_code": "SK", "current_code": "SSG"},
    {"id": 9, "name": "NC 다이노스", "original_code": "NC", "current_code": "NC"},
    {"id": 10, "name": "kt wiz", "original_code": "KT", "current_code": "KT"},
]

def seed_franchises():
    print("🌱 Seeding Franchises...")
    with SessionLocal() as session:
        for f_data in FRANCHISES:
            f = session.get(Franchise, f_data["id"])
            if not f:
                f = Franchise(**f_data)
                session.add(f)
                print(f"   Created: {f_data['name']}")
            else:
                f.name = f_data["name"]
                f.original_code = f_data["original_code"]
                f.current_code = f_data["current_code"]
                print(f"   Updated: {f_data['name']}")
        session.commit()
    print("✅ Franchise Seeding Complete.")

if __name__ == "__main__":
    seed_franchises()
