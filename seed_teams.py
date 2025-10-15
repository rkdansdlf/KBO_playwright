"""
Seed initial KBO team data
Based on Docs/schema/KBO_teams_schema.md
"""
from src.db.engine import SessionLocal
from src.models.team import Franchise, TeamIdentity, FranchiseEvent, Ballpark, HomeBallparkAssignment
from src.utils.safe_print import safe_print as print


def seed_franchises(session):
    """Insert franchise data (idempotent)"""
    franchises_data = [
        {'key': 'SAMSUNG', 'canonical_name': '삼성 라이온즈', 'status': 'ACTIVE', 'notes': None},
        {'key': 'LOTTE', 'canonical_name': '롯데 자이언츠', 'status': 'ACTIVE', 'notes': None},
        {'key': 'LG', 'canonical_name': 'LG 트윈스', 'status': 'ACTIVE', 'notes': 'MBC 청룡의 전신'},
        {'key': 'KIA', 'canonical_name': 'KIA 타이거즈', 'status': 'ACTIVE', 'notes': '해태 타이거즈의 전신'},
        {'key': 'DOOSAN', 'canonical_name': '두산 베어스', 'status': 'ACTIVE', 'notes': 'OB 베어스의 전신'},
        {'key': 'HEROES', 'canonical_name': '키움 히어로즈', 'status': 'ACTIVE', 'notes': '삼미→청보→태평양→현대→우리→넥센→키움'},
        {'key': 'HANWHA', 'canonical_name': '한화 이글스', 'status': 'ACTIVE', 'notes': None},
        {'key': 'NC', 'canonical_name': 'NC 다이노스', 'status': 'ACTIVE', 'notes': None},
        {'key': 'SSG', 'canonical_name': 'SSG 랜더스', 'status': 'ACTIVE', 'notes': 'SK 와이번스에서 변경'},
        {'key': 'KT', 'canonical_name': 'KT 위즈', 'status': 'ACTIVE', 'notes': None},
        {'key': 'SSANG', 'canonical_name': '쌍방울 레이더스', 'status': 'DISSOLVED', 'notes': '1999년 해체'},
    ]

    inserted = 0
    skipped = 0
    for data in franchises_data:
        existing = session.query(Franchise).filter_by(key=data['key']).first()
        if not existing:
            franchise = Franchise(**data)
            session.add(franchise)
            inserted += 1
        else:
            skipped += 1

    session.commit()
    print(f"✅ Franchises: {inserted} inserted, {skipped} skipped")


def seed_team_identities(session):
    """Insert team identity (branding) data (idempotent)"""
    # Get franchise IDs
    franchises = {f.key: f.id for f in session.query(Franchise).all()}

    identities_data = [
        # Samsung/Lotte - no changes
        {'franchise_id': franchises['SAMSUNG'], 'name_kor': '삼성 라이온즈', 'short_code': 'SS', 'city_kor': '대구', 'start_season': 1982, 'end_season': None, 'is_current': 1},
        {'franchise_id': franchises['LOTTE'], 'name_kor': '롯데 자이언츠', 'short_code': 'LOT', 'city_kor': '부산', 'start_season': 1982, 'end_season': None, 'is_current': 1},

        # LG: MBC → LG
        {'franchise_id': franchises['LG'], 'name_kor': 'MBC 청룡', 'short_code': 'MBC', 'city_kor': '서울', 'start_season': 1982, 'end_season': 1990, 'is_current': 0},
        {'franchise_id': franchises['LG'], 'name_kor': 'LG 트윈스', 'short_code': 'LG', 'city_kor': '서울', 'start_season': 1990, 'end_season': None, 'is_current': 1},

        # KIA: Haitai → KIA
        {'franchise_id': franchises['KIA'], 'name_kor': '해태 타이거즈', 'short_code': 'HAI', 'city_kor': '광주', 'start_season': 1982, 'end_season': 2001, 'is_current': 0},
        {'franchise_id': franchises['KIA'], 'name_kor': 'KIA 타이거즈', 'short_code': 'KIA', 'city_kor': '광주', 'start_season': 2001, 'end_season': None, 'is_current': 1},

        # Doosan: OB → Doosan
        {'franchise_id': franchises['DOOSAN'], 'name_kor': 'OB 베어스', 'short_code': 'OB', 'city_kor': '서울', 'start_season': 1982, 'end_season': None, 'is_current': 0},
        {'franchise_id': franchises['DOOSAN'], 'name_kor': '두산 베어스', 'short_code': 'DOO', 'city_kor': '서울', 'start_season': None, 'end_season': None, 'is_current': 1},

        # Heroes chain: 삼미→청보→태평양→현대→우리→넥센→키움
        {'franchise_id': franchises['HEROES'], 'name_kor': '삼미 슈퍼스타즈', 'short_code': 'SAM', 'city_kor': None, 'start_season': 1982, 'end_season': 1985, 'is_current': 0},
        {'franchise_id': franchises['HEROES'], 'name_kor': '청보 핀토스', 'short_code': 'CB', 'city_kor': None, 'start_season': 1985, 'end_season': 1988, 'is_current': 0},
        {'franchise_id': franchises['HEROES'], 'name_kor': '태평양 돌핀스', 'short_code': 'TP', 'city_kor': None, 'start_season': 1988, 'end_season': 1995, 'is_current': 0},
        {'franchise_id': franchises['HEROES'], 'name_kor': '현대 유니콘스', 'short_code': 'HYU', 'city_kor': None, 'start_season': 1995, 'end_season': 2008, 'is_current': 0},
        {'franchise_id': franchises['HEROES'], 'name_kor': '우리 히어로즈', 'short_code': 'WO', 'city_kor': '서울', 'start_season': 2008, 'end_season': 2010, 'is_current': 0},
        {'franchise_id': franchises['HEROES'], 'name_kor': '넥센 히어로즈', 'short_code': 'NEX', 'city_kor': '서울', 'start_season': 2010, 'end_season': 2019, 'is_current': 0},
        {'franchise_id': franchises['HEROES'], 'name_kor': '키움 히어로즈', 'short_code': 'KIW', 'city_kor': '서울', 'start_season': 2019, 'end_season': None, 'is_current': 1},

        # Others
        {'franchise_id': franchises['HANWHA'], 'name_kor': '한화 이글스', 'short_code': 'HHE', 'city_kor': '대전', 'start_season': None, 'end_season': None, 'is_current': 1},
        {'franchise_id': franchises['NC'], 'name_kor': 'NC 다이노스', 'short_code': 'NC', 'city_kor': '창원', 'start_season': None, 'end_season': None, 'is_current': 1},
        {'franchise_id': franchises['SSG'], 'name_kor': 'SK 와이번스', 'short_code': 'SK', 'city_kor': '인천', 'start_season': None, 'end_season': 2021, 'is_current': 0},
        {'franchise_id': franchises['SSG'], 'name_kor': 'SSG 랜더스', 'short_code': 'SSG', 'city_kor': '인천', 'start_season': 2021, 'end_season': None, 'is_current': 1},
        {'franchise_id': franchises['KT'], 'name_kor': 'KT 위즈', 'short_code': 'KT', 'city_kor': '수원', 'start_season': None, 'end_season': None, 'is_current': 1},
        {'franchise_id': franchises['SSANG'], 'name_kor': '쌍방울 레이더스', 'short_code': 'SSANG', 'city_kor': None, 'start_season': None, 'end_season': 1999, 'is_current': 0},
    ]

    inserted = 0
    skipped = 0
    for data in identities_data:
        existing = session.query(TeamIdentity).filter_by(
            franchise_id=data['franchise_id'],
            name_kor=data['name_kor']
        ).first()
        if not existing:
            identity = TeamIdentity(**data)
            session.add(identity)
            inserted += 1
        else:
            skipped += 1

    session.commit()
    print(f"✅ Team Identities: {inserted} inserted, {skipped} skipped")


def seed_ballparks(session):
    """Insert ballpark data (idempotent)"""
    ballparks_data = [
        {'name_kor': '인천SSG랜더스필드', 'city_kor': '인천'},
        {'name_kor': '수원KT위즈파크', 'city_kor': '수원'},
        {'name_kor': '사직야구장', 'city_kor': '부산'},
        {'name_kor': '서울종합운동장 야구장', 'city_kor': '서울'},
        {'name_kor': '잠실야구장', 'city_kor': '서울'},  # 서울종합운동장 야구장의 별칭
        {'name_kor': '대구삼성라이온즈파크', 'city_kor': '대구'},
        {'name_kor': '고척스카이돔', 'city_kor': '서울'},
        {'name_kor': '광주기아챔피언스필드', 'city_kor': '광주'},
        {'name_kor': '문학야구장', 'city_kor': '인천'},  # 인천SSG랜더스필드의 구명칭
    ]

    inserted = 0
    skipped = 0
    for data in ballparks_data:
        existing = session.query(Ballpark).filter_by(name_kor=data['name_kor']).first()
        if not existing:
            ballpark = Ballpark(**data)
            session.add(ballpark)
            inserted += 1
        else:
            skipped += 1

    session.commit()
    print(f"✅ Ballparks: {inserted} inserted, {skipped} skipped")


def seed_ballpark_assignments(session):
    """Insert home ballpark assignments (idempotent)"""
    # Get IDs
    franchises = {f.key: f.id for f in session.query(Franchise).all()}
    ballparks = {b.name_kor: b.id for b in session.query(Ballpark).all()}

    assignments_data = [
        {'franchise_id': franchises['SSG'], 'ballpark_id': ballparks['인천SSG랜더스필드'], 'start_season': None, 'end_season': None, 'is_primary': 1},
        {'franchise_id': franchises['KT'], 'ballpark_id': ballparks['수원KT위즈파크'], 'start_season': None, 'end_season': None, 'is_primary': 1},
        {'franchise_id': franchises['LOTTE'], 'ballpark_id': ballparks['사직야구장'], 'start_season': None, 'end_season': None, 'is_primary': 1},
        {'franchise_id': franchises['LG'], 'ballpark_id': ballparks['잠실야구장'], 'start_season': None, 'end_season': None, 'is_primary': 1},
        {'franchise_id': franchises['SAMSUNG'], 'ballpark_id': ballparks['대구삼성라이온즈파크'], 'start_season': None, 'end_season': None, 'is_primary': 1},
        {'franchise_id': franchises['HEROES'], 'ballpark_id': ballparks['고척스카이돔'], 'start_season': None, 'end_season': None, 'is_primary': 1},
        {'franchise_id': franchises['KIA'], 'ballpark_id': ballparks['광주기아챔피언스필드'], 'start_season': None, 'end_season': None, 'is_primary': 1},
    ]

    inserted = 0
    skipped = 0
    for data in assignments_data:
        # Handle NULL start_season for primary key
        start = data['start_season'] if data['start_season'] is not None else -1
        existing = session.query(HomeBallparkAssignment).filter_by(
            franchise_id=data['franchise_id'],
            ballpark_id=data['ballpark_id'],
            start_season=start
        ).first()
        if not existing:
            assignment = HomeBallparkAssignment(
                franchise_id=data['franchise_id'],
                ballpark_id=data['ballpark_id'],
                start_season=start,
                end_season=data['end_season'],
                is_primary=data['is_primary']
            )
            session.add(assignment)
            inserted += 1
        else:
            skipped += 1

    session.commit()
    print(f"✅ Ballpark Assignments: {inserted} inserted, {skipped} skipped")


def main():
    """Run all seed operations"""
    print("\n" + "🌱" * 30)
    print("Seeding KBO Team Data")
    print("🌱" * 30 + "\n")

    with SessionLocal() as session:
        try:
            seed_franchises(session)
            seed_team_identities(session)
            seed_ballparks(session)
            seed_ballpark_assignments(session)

            print("\n" + "✅" * 30)
            print("Team Data Seeding Complete!")
            print("✅" * 30)

        except Exception as e:
            session.rollback()
            print(f"\n❌ Error seeding data: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    main()
