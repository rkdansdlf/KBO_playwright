#!/usr/bin/env python3
"""
누락된 팀 코드 확인 및 해결 스크립트
"""
from src.db.engine import SessionLocal
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching
from sqlalchemy import text, distinct


def check_missing_team_codes():
    """SQLite 데이터에서 누락된 팀 코드 확인"""
    with SessionLocal() as session:
        print("🔍 SQLite 데이터에서 팀 코드 분석 중...")
        
        # 타자 데이터의 모든 팀 코드
        batting_teams = session.execute(text("""
            SELECT DISTINCT team_code, COUNT(*) as count, MIN(season) as first_year, MAX(season) as last_year
            FROM player_season_batting 
            WHERE team_code IS NOT NULL
            GROUP BY team_code
            ORDER BY first_year, team_code
        """)).fetchall()
        
        print(f"\n📊 타자 데이터 팀 코드: {len(batting_teams)}개")
        for team_code, count, first_year, last_year in batting_teams:
            print(f"  - {team_code}: {count}명 ({first_year}-{last_year})")
        
        # 투수 데이터의 모든 팀 코드
        pitching_teams = session.execute(text("""
            SELECT DISTINCT team_code, COUNT(*) as count, MIN(season) as first_year, MAX(season) as last_year
            FROM player_season_pitching 
            WHERE team_code IS NOT NULL
            GROUP BY team_code
            ORDER BY first_year, team_code
        """)).fetchall()
        
        print(f"\n📊 투수 데이터 팀 코드: {len(pitching_teams)}개")
        for team_code, count, first_year, last_year in pitching_teams:
            print(f"  - {team_code}: {count}명 ({first_year}-{last_year})")
        
        # 모든 고유 팀 코드
        all_teams = set()
        for team_code, _, _, _ in batting_teams:
            all_teams.add(team_code)
        for team_code, _, _, _ in pitching_teams:
            all_teams.add(team_code)
        
        print(f"\n🎯 전체 고유 팀 코드: {len(all_teams)}개")
        sorted_teams = sorted(all_teams)
        for i, team in enumerate(sorted_teams, 1):
            print(f"  {i:2d}. {team}")
        
        return sorted_teams


def identify_team_codes():
    """팀 코드 식별 및 설명"""
    team_mapping = {
        # 현재 팀들
        'LG': 'LG 트윈스 (1990-현재, 이전 MBC청룡)',
        'KIA': 'KIA 타이거즈 (2002-현재, 이전 해태타이거즈)',
        'DOOSAN': '두산 베어스 (1999-현재, 이전 OB베어스)',
        'SSG': 'SSG 랜더스 (2021-현재, 이전 SK와이번스)',
        'LOTTE': '롯데 자이언츠 (1982-현재)',
        'SAMSUNG': '삼성 라이온즈 (1982-현재)',
        'HANWHA': '한화 이글스 (1986-현재, 이전 빙그레)',
        'KT': 'KT 위즈 (2015-현재)',
        'NC': 'NC 다이노스 (2013-현재)',
        'KIWOOM': '키움 히어로즈 (2019-현재, 이전 넥센/우리)',
        
        # 역사적 팀들 (추정)
        'PC': '태평양 돌핀스 (1988-1995) 또는 태평양 클럽 관련',
        'MBC': 'MBC 청룡 (1982-1989, 현재 LG)',
        'OB': 'OB 베어스 (1982-1998, 현재 두산)',
        'HAITAI': '해태 타이거즈 (1982-2001, 현재 KIA)',
        'SK': 'SK 와이번스 (2000-2020, 현재 SSG)',
        'BINGGRAE': '빙그레 이글스 (1986-1993, 현재 한화)',
        'CHUNGBO': '청보 핀토스 (1982-1985)',
        'NEXEN': '넥센 히어로즈 (2008-2018, 현재 키움)',
        'WOORI': '우리 히어로즈 (2007, 현재 키움)',
        'HYUNDAI': '현대 유니콘스 (1982-2007, 해체)',
        'PACIFIC': '태평양 돌핀스 (1988-1995)',
    }
    
    print("\n🏟️ 팀 코드 식별 정보:")
    print("=" * 50)
    
    missing_teams = check_missing_team_codes()
    
    for team_code in missing_teams:
        if team_code in team_mapping:
            print(f"✅ {team_code}: {team_mapping[team_code]}")
        else:
            print(f"❓ {team_code}: 미식별 팀 코드")


def generate_missing_teams_sql():
    """누락된 팀들을 위한 SQL 생성"""
    missing_teams_data = [
        # PC = 태평양 돌핀스 (1988-1995)
        {
            'team_code': 'PC',
            'team_name': '태평양 돌핀스',
            'team_name_en': 'Pacific Dolphins',
            'city': '인천',
            'founded_year': 1988,
            'disbanded_year': 1995,
            'current_team': None,
            'description': '1988-1995년 운영된 프로야구단, 인천 연고'
        },
        # PACIFIC = 태평양 돌핀스 (정식명)
        {
            'team_code': 'PACIFIC', 
            'team_name': '태평양 돌핀스',
            'team_name_en': 'Pacific Dolphins',
            'city': '인천',
            'founded_year': 1988,
            'disbanded_year': 1995,
            'current_team': None,
            'description': '1988-1995년 운영된 프로야구단'
        },
        # MBC = MBC 청룡 (1982-1989)
        {
            'team_code': 'MBC',
            'team_name': 'MBC 청룡',
            'team_name_en': 'MBC Blue Dragons', 
            'city': '서울',
            'founded_year': 1982,
            'disbanded_year': 1989,
            'current_team': 'LG',
            'description': '1982-1989년 운영, 1990년 LG 트윈스로 인수'
        },
        # OB = OB 베어스 (1982-1998)
        {
            'team_code': 'OB',
            'team_name': 'OB 베어스',
            'team_name_en': 'OB Bears',
            'city': '서울',
            'founded_year': 1982,
            'disbanded_year': 1998,
            'current_team': 'DOOSAN',
            'description': '1982-1998년 운영, 1999년 두산 베어스로 인수'
        },
        # HAITAI = 해태 타이거즈 (1982-2001)
        {
            'team_code': 'HAITAI',
            'team_name': '해태 타이거즈', 
            'team_name_en': 'Haitai Tigers',
            'city': '광주',
            'founded_year': 1982,
            'disbanded_year': 2001,
            'current_team': 'KIA',
            'description': '1982-2001년 운영, 2002년 KIA 타이거즈로 인수'
        },
        # CHUNGBO = 청보 핀토스 (1982-1985)
        {
            'team_code': 'CHUNGBO',
            'team_name': '청보 핀토스',
            'team_name_en': 'Chungbo Pintos',
            'city': '청주',
            'founded_year': 1982,
            'disbanded_year': 1985,
            'current_team': None,
            'description': '1982-1985년 운영된 프로야구단'
        },
        # BINGGRAE = 빙그레 이글스 (1986-1993)
        {
            'team_code': 'BINGGRAE',
            'team_name': '빙그레 이글스',
            'team_name_en': 'Binggrae Eagles',
            'city': '대전',
            'founded_year': 1986,
            'disbanded_year': 1993,
            'current_team': 'HANWHA',
            'description': '1986-1993년 운영, 1994년 한화 이글스로 인수'
        }
    ]
    
    print("\n📝 누락된 팀들을 위한 SQL:")
    print("=" * 50)
    print("-- Supabase에서 실행할 SQL")
    print()
    
    for team in missing_teams_data:
        print(f"""
-- {team['team_name']} ({team['team_code']})
INSERT INTO public.teams (
    team_code, team_name, team_name_en, city, 
    founded_year, disbanded_year, is_active, 
    description, created_at, updated_at
) VALUES (
    '{team['team_code']}', 
    '{team['team_name']}', 
    '{team['team_name_en']}', 
    '{team['city']}',
    {team['founded_year']}, 
    {team['disbanded_year'] if team['disbanded_year'] else 'NULL'}, 
    false,
    '{team['description']}',
    NOW(), 
    NOW()
) ON CONFLICT (team_code) DO NOTHING;""")
    
    print("\n💡 이 SQL을 Supabase 대시보드 → SQL Editor에서 실행하세요!")


def main():
    try:
        print("🔍 KBO 팀 코드 분석 및 누락 팀 식별")
        print("=" * 50)
        
        # 1. 팀 코드 식별
        identify_team_codes()
        
        # 2. SQL 생성
        generate_missing_teams_sql()
        
        print("\n🎯 다음 단계:")
        print("1. 위의 SQL을 Supabase에서 실행")
        print("2. ./venv/bin/python3 -m src.sync.supabase_sync 재시도")
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")


if __name__ == "__main__":
    main()