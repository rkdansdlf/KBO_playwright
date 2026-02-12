"""
KBO 팀명 매핑 유틸리티
Supabase team_history 테이블과 연동하여 동적 매핑 제공
"""
import os
from typing import Dict, Optional, List, Tuple
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from src.utils.team_codes import resolve_team_code


class TeamMapper:
    """팀명 매핑 관리 클래스"""
    
    def __init__(self):
        self.static_mapping = self._get_static_mapping()
        self.supabase_mapping = {}
        self.year_specific_mapping = {}
        self._supabase_loaded = False
    
    def _get_static_mapping(self) -> Dict[str, str]:
        """기본 정적 매핑 (현재 팀들)"""
        return {
            'LG': 'LG',
            'NC': 'NC', 
            'KT': 'KT',
            '삼성': 'SS',
            '롯데': 'LT',
            '두산': 'DB',
            'KIA': 'KIA',
            '한화': 'HH',
            '키움': 'KH',
            'SSG': 'SSG',
            # 추가 변형들
            'LG트윈스': 'LG',
            'NC다이노스': 'NC',
            'KT위즈': 'KT',
            '삼성라이온즈': 'SS',
            '롯데자이언츠': 'LT',
            '두산베어스': 'DB',
            'KIA타이거즈': 'KIA',
            '한화이글스': 'HH',
            '키움히어로즈': 'KH',
            'SSG랜더스': 'SSG',
        }
    
    def load_supabase_mapping(self) -> bool:
        """Supabase team_history 테이블에서 매핑 데이터 로드"""
        supabase_url = os.getenv('SUPABASE_DB_URL')
        if not supabase_url:
            print("⚠️ SUPABASE_DB_URL 환경변수가 설정되지 않음. 정적 매핑만 사용.")
            return False
        
        try:
            engine = create_engine(supabase_url)
            Session = sessionmaker(bind=engine)
            session = Session()
            
            # team_history 테이블에서 역대 팀 정보 조회
            # 먼저 테이블 구조 확인
            try:
                structure_query = text("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'team_history'
                    ORDER BY ordinal_position
                """)
                columns = session.execute(structure_query).fetchall()
                print(f"📋 team_history 테이블 컬럼: {[col[0] for col in columns]}")
            except Exception as e:
                print(f"⚠️ 테이블 구조 확인 실패: {e}")
            
            # 가능한 컬럼명으로 쿼리 시도
            possible_queries = [
                # 컬럼명 패턴 0 (KBO_playwright 표준)
                """
                SELECT 
                    team_name,
                    team_code,
                    season,
                    season as end_year,
                    team_name as franchise_name
                FROM team_history 
                WHERE team_name IS NOT NULL 
                AND team_code IS NOT NULL
                ORDER BY season
                """,
                # 컬럼명 패턴 1
                """
                SELECT 
                    team_name_kor,
                    team_code,
                    start_year,
                    end_year,
                    franchise_name
                FROM team_history 
                WHERE team_name_kor IS NOT NULL 
                AND team_code IS NOT NULL
                ORDER BY start_year
                """,
                # 컬럼명 패턴 2
                """
                SELECT 
                    name_kor,
                    code,
                    start_year,
                    end_year,
                    franchise
                FROM team_history 
                WHERE name_kor IS NOT NULL 
                AND code IS NOT NULL
                ORDER BY start_year
                """,
                # 컬럼명 패턴 3 (실제 Supabase 일부 구조)
                """
                SELECT 
                    team_name,
                    team_code,
                    start_season,
                    end_season,
                    team_name as franchise_name
                FROM team_history 
                WHERE team_name IS NOT NULL 
                AND team_code IS NOT NULL
                ORDER BY start_season
                """,
                # 기본 모든 컬럼 조회
                """
                SELECT * FROM team_history LIMIT 5
                """
            ]
            
            query_result = None
            for i, query_sql in enumerate(possible_queries):
                try:
                    # 트랜잭션 롤백 후 새로 시작
                    session.rollback()
                    query = text(query_sql)
                    query_result = session.execute(query).fetchall()
                    print(f"✅ 쿼리 패턴 {i+1} 성공: {len(query_result)}개 행 조회")
                    break
                except Exception as e:
                    print(f"⚠️ 쿼리 패턴 {i+1} 실패: {e}")
                    session.rollback()  # 실패시 트랜잭션 롤백
                    continue
            
            if not query_result:
                print("❌ 모든 쿼리 패턴 실패")
                return False
            
            results = query_result
            
            if not results:
                print("⚠️ team_history 테이블에서 데이터를 찾을 수 없음")
                return False
            
            # 매핑 데이터 구성
            for row in results:
                team_name = row[0]
                team_code = row[1]
                try:
                    start_year = int(row[2])
                    end_year = int(row[3]) if row[3] is not None else 9999
                except (ValueError, TypeError):
                    continue
                franchise = row[4]
                
                # 기본 매핑
                self.supabase_mapping[team_name] = team_code
                
                # 년도별 매핑
                for year in range(start_year, end_year + 1):
                    if year not in self.year_specific_mapping:
                        self.year_specific_mapping[year] = {}
                    self.year_specific_mapping[year][team_name] = team_code
                
                # 프랜차이즈명도 매핑에 추가
                if franchise and franchise != team_name:
                    self.supabase_mapping[franchise] = team_code
                    for year in range(start_year, end_year + 1):
                        if year not in self.year_specific_mapping:
                            self.year_specific_mapping[year] = {}
                        self.year_specific_mapping[year][franchise] = team_code
            
            session.close()
            engine.dispose()
            
            self._supabase_loaded = True
            print(f"✅ Supabase에서 {len(results)}개 팀 매핑 로드 완료")
            return True
            
        except Exception as e:
            print(f"⚠️ Supabase 팀 매핑 로드 실패: {e}")
            return False
    
    def get_team_code(self, team_name: str, year: Optional[int] = None) -> Optional[str]:
        """팀명으로 팀 코드 조회 (년도 고려)"""
        if not team_name:
            return None
        
        team_name = team_name.strip()
        
        # 1. 년도별 매핑 우선 확인 (Supabase 등의 외부 소스)
        if year and self._supabase_loaded and year in self.year_specific_mapping:
            year_mapping = self.year_specific_mapping[year]
            if team_name in year_mapping:
                # [REFINED] If we have a year, ensure it follows historical resolution
                # to avoid Supabase legacy overrides for modern years or vice versa.
                canonical_code = resolve_team_code(team_name, year)
                if canonical_code:
                    return canonical_code
                return year_mapping[team_name]
        
        # 1.5 Standard Resolution via team_codes (Superior to static/fuzzy)
        resolved = resolve_team_code(team_name, year)
        if resolved:
            return resolved
        
        # 2. Supabase 매핑 확인
        if self._supabase_loaded and team_name in self.supabase_mapping:
            return self.supabase_mapping[team_name]
        
        # 3. 정적 매핑 확인
        if team_name in self.static_mapping:
            return self.static_mapping[team_name]
        
        # 4. 부분 매칭 시도 (역대 팀명 변화 고려)
        return self._fuzzy_match(team_name, year)
    
    def _fuzzy_match(self, team_name: str, year: Optional[int] = None) -> Optional[str]:
        """퍼지 매칭으로 팀 코드 찾기"""
        # 역대 팀명 변화 패턴
        historical_patterns = {
            # OB 계열
            'OB': 'OB', 'OB베어스': 'OB', '두산': 'DB', '두산베어스': 'DB',
            # 삼성 계열  
            '삼성': 'SS', '삼성라이온즈': 'SS',
            # LG 계열
            'LG': 'LG', 'LG트윈스': 'LG', 'MBC': 'MBC', 'MBC청룡': 'MBC',
            # 롯데 계열
            '롯데': 'LT', '롯데자이언츠': 'LT',
            # 한화 계열
            '한화': 'HH', '한화이글스': 'HH', '빙그레': 'BE', '빙그레이글스': 'BE',
            # 해태/KIA 계열
            '해태': 'HT', '해태타이거즈': 'HT', 'KIA': 'KIA', 'KIA타이거즈': 'KIA',
            # 현대/키움 계열
            '현대': 'HU', '현대유니콘스': 'HU', '키움': 'KH', '키움히어로즈': 'KH', '넥센': 'NX', '넥센히어로즈': 'NX',
            # SK/SSG 계열
            'SK': 'SK', 'SK와이번스': 'SK', 'SSG': 'SSG', 'SSG랜더스': 'SSG',
            # 기타 초창기 팀들
            '청보': 'CB', '청보핀토스': 'CB',
            '삼미': 'SM', '삼미슈퍼스타즈': 'SM',
            '태평양': 'TP', '태평양돌핀스': 'TP',
            '쌍방울': 'SL', '쌍방울레이더스': 'SL',
            'NC': 'NC', 'NC다이노스': 'NC',
            'KT': 'KT', 'KT위즈': 'KT',
        }
        
        # 직접 매칭
        if team_name in historical_patterns:
            return historical_patterns[team_name]
        
        # 부분 문자열 매칭
        for pattern, code in historical_patterns.items():
            if pattern in team_name or team_name in pattern:
                return code
        
        # 년도별 특수 케이스
        if year:
            if year <= 1985:  # 초창기
                if 'MBC' in team_name or '청룡' in team_name:
                    return 'LG'
                elif '해태' in team_name or '타이거즈' in team_name:
                    return 'HT'
                elif '삼미' in team_name:
                    return 'SM'
                elif '청보' in team_name:
                    return 'CB'
            elif year <= 1995:  # 90년대
                if '빙그레' in team_name:
                    return 'BE'
                elif '태평양' in team_name:
                    return 'TP'
            elif year <= 2000:  # 90년대 후반
                if '현대' in team_name:
                    return 'HU'
                elif '쌍방울' in team_name:
                    return 'SL'
        
        return None
    
    def get_all_teams_for_year(self, year: int) -> Dict[str, str]:
        """특정 년도의 모든 팀 매핑 반환"""
        if year in self.year_specific_mapping:
            return self.year_specific_mapping[year].copy()
        else:
            return self.static_mapping.copy()
    
    def validate_team_code(self, team_code: str, year: Optional[int] = None) -> bool:
        """팀 코드 유효성 검증"""
        if not team_code:
            return False
        
        # 현재 유효한 팀 코드들
        valid_codes = {'LG', 'NC', 'KT', 'SS', 'LT', 'DB', 'KIA', 'HH', 'KH', 'SSG'}
        
        # 역대 팀 코드들 (해체된 팀 포함)
        historical_codes = {'CB', 'SM', 'TP', 'SL', 'OB', 'HT', 'WO', 'SK', 'NX', 'HU', 'MBC', 'BE'}
        
        return team_code in valid_codes or team_code in historical_codes


# 전역 인스턴스
_team_mapper = None

def get_team_mapper() -> TeamMapper:
    """TeamMapper 싱글톤 인스턴스 반환"""
    global _team_mapper
    if _team_mapper is None:
        _team_mapper = TeamMapper()
        # 처음 생성시 Supabase 매핑 시도
        _team_mapper.load_supabase_mapping()
    return _team_mapper

def get_team_code(team_name: str, year: Optional[int] = None) -> Optional[str]:
    """간편 함수: 팀명으로 팀 코드 조회"""
    mapper = get_team_mapper()
    return mapper.get_team_code(team_name, year)

def get_team_mapping_for_year(year: int) -> Dict[str, str]:
    """간편 함수: 특정 년도의 팀 매핑 반환"""
    mapper = get_team_mapper()
    return mapper.get_all_teams_for_year(year)

def refresh_supabase_mapping() -> bool:
    """Supabase 매핑 갱신"""
    mapper = get_team_mapper()
    return mapper.load_supabase_mapping()


if __name__ == "__main__":
    # 테스트 코드
    mapper = TeamMapper()
    mapper.load_supabase_mapping()
    
    # 테스트 케이스들
    test_cases = [
        ("삼성", 2025),
        ("해태", 1985),
        ("MBC", 1983),
        ("현대", 1998),
        ("키움", 2020),
        ("SSG", 2021),
        ("삼미", 1983),
        ("청보", 1983),
    ]
    
    print("🔍 팀 매핑 테스트:")
    for team_name, year in test_cases:
        code = mapper.get_team_code(team_name, year)
        print(f"  {year}년 '{team_name}' → '{code}'")