"""
KBO 연도별 시리즈 존재 여부 검증 유틸리티
"""
from typing import List, Dict


def get_available_series_by_year(year: int) -> List[str]:
    """
    연도별 존재하는 시리즈 목록 반환
    
    Args:
        year: 조회할 연도
        
    Returns:
        해당 연도에 존재하는 시리즈 키 목록
    """
    # 기본적으로 모든 연도에 존재하는 시리즈
    base_series = ['regular']
    
    # 연도별 추가 시리즈
    if year >= 1982:  # KBO 창설 이후
        if year <= 1985:
            # 1982-1985: 정규시즌 + 한국시리즈만
            return base_series + ['korean_series']
            
        elif year <= 1988:
            # 1986-1988: 정규시즌 + 한국시리즈 + 시범경기
            return base_series + ['korean_series', 'exhibition']
            
        elif year <= 1999:
            # 1989-1999: 현재와 유사하지만 플레이오프 체계 다름
            return base_series + ['korean_series', 'exhibition']
            
        elif year <= 2001:
            # 2000-2001: 플레이오프 없음, 정규시즌 1위가 직접 한국시리즈
            return base_series + ['korean_series', 'exhibition']
            
        elif year <= 2006:
            # 2002-2006: 플레이오프 도입
            return base_series + ['korean_series', 'playoff', 'exhibition']
            
        elif year <= 2014:
            # 2007-2014: 플레이오프 확장
            return base_series + ['korean_series', 'playoff', 'semi_playoff', 'exhibition']
            
        else:
            # 2015-현재: 와일드카드 도입
            return base_series + ['korean_series', 'playoff', 'semi_playoff', 'wildcard', 'exhibition']
    
    else:
        # 1982년 이전은 KBO 창설 전
        return []


def is_series_available(year: int, series_key: str) -> bool:
    """
    특정 연도에 특정 시리즈가 존재하는지 확인
    
    Args:
        year: 연도
        series_key: 시리즈 키
        
    Returns:
        존재 여부
    """
    available_series = get_available_series_by_year(year)
    return series_key in available_series


def filter_series_for_year(year: int, requested_series: List[str]) -> List[str]:
    """
    요청된 시리즈 목록에서 해당 연도에 존재하지 않는 시리즈 제거
    
    Args:
        year: 연도
        requested_series: 요청된 시리즈 목록
        
    Returns:
        필터링된 시리즈 목록
    """
    available_series = get_available_series_by_year(year)
    filtered = [series for series in requested_series if series in available_series]
    
    # 제거된 시리즈가 있으면 알림
    removed = [series for series in requested_series if series not in available_series]
    if removed:
        print(f"⚠️ {year}년에는 다음 시리즈가 존재하지 않아 제외됩니다: {', '.join(removed)}")
    
    return filtered


def get_series_info() -> Dict[str, Dict]:
    """
    시리즈별 상세 정보 반환
    """
    return {
        'regular': {
            'name': 'KBO 정규시즌',
            'description': '4월-10월 정규 경기',
            'since': 1982
        },
        'exhibition': {
            'name': 'KBO 시범경기', 
            'description': '시즌 전 연습 경기',
            'since': 1986
        },
        'korean_series': {
            'name': 'KBO 한국시리즈',
            'description': '시즌 최종 우승 결정전',
            'since': 1982
        },
        'playoff': {
            'name': 'KBO 플레이오프',
            'description': '포스트시즌 결승',
            'since': 2002
        },
        'semi_playoff': {
            'name': 'KBO 준플레이오프',
            'description': '포스트시즌 준결승',
            'since': 2007
        },
        'wildcard': {
            'name': 'KBO 와일드카드',
            'description': '포스트시즌 진출 결정전',
            'since': 2015
        }
    }


def validate_year_series_combination(year: int, series_key: str) -> tuple:
    """
    연도-시리즈 조합 유효성 검증
    
    Args:
        year: 연도
        series_key: 시리즈 키
        
    Returns:
        (is_valid: bool, message: str)
    """
    if year < 1982:
        return False, "KBO는 1982년에 창설되었습니다."
    
    if not is_series_available(year, series_key):
        series_info = get_series_info()
        if series_key in series_info:
            since_year = series_info[series_key]['since']
            return False, f"{series_info[series_key]['name']}는 {since_year}년부터 시작되었습니다."
        else:
            return False, f"알 수 없는 시리즈: {series_key}"
    
    return True, "유효한 조합입니다."


def get_recommended_series_for_period(start_year: int, end_year: int) -> List[str]:
    """
    특정 기간에 대한 권장 시리즈 목록
    
    Args:
        start_year: 시작 연도
        end_year: 끝 연도
        
    Returns:
        권장 시리즈 목록 (가장 많은 연도에 공통으로 존재하는 순)
    """
    # 전체 기간에서 각 시리즈가 존재하는 연도 수 계산
    series_counts = {}
    total_years = end_year - start_year + 1
    
    for year in range(start_year, end_year + 1):
        available = get_available_series_by_year(year)
        for series in available:
            series_counts[series] = series_counts.get(series, 0) + 1
    
    # 존재 비율이 높은 순으로 정렬
    sorted_series = sorted(
        series_counts.items(), 
        key=lambda x: x[1], 
        reverse=True
    )
    
    # 50% 이상의 연도에 존재하는 시리즈만 권장
    recommended = []
    for series, count in sorted_series:
        if count / total_years >= 0.5:  # 50% 이상
            recommended.append(series)
    
    return recommended


if __name__ == "__main__":
    # 테스트 코드
    print("=== KBO 시리즈 연도별 검증 테스트 ===")
    
    test_years = [1990, 2000, 2001, 2002, 2010, 2015, 2025]
    
    for year in test_years:
        available = get_available_series_by_year(year)
        print(f"{year}년: {', '.join(available)}")
    
    print("\n=== 특정 조합 검증 ===")
    test_cases = [
        (2001, 'playoff'),
        (2000, 'korean_series'),
        (1990, 'wildcard'),
        (2015, 'wildcard')
    ]
    
    for year, series in test_cases:
        valid, msg = validate_year_series_combination(year, series)
        status = "✅" if valid else "❌"
        print(f"{status} {year}년 {series}: {msg}")
    
    print("\n=== 기간별 권장 시리즈 ===")
    periods = [(1990, 2000), (2000, 2010), (2010, 2025)]
    
    for start, end in periods:
        recommended = get_recommended_series_for_period(start, end)
        print(f"{start}-{end}년: {', '.join(recommended)}")