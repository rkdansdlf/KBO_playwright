#!/usr/bin/env python3
"""
1982-2001년 기존 크롤링 데이터의 league 필드 업데이트
REGULAR로 저장된 데이터를 시리즈별로 정확히 매핑
"""
import argparse
from src.db.engine import SessionLocal
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching
from sqlalchemy import text, and_


def analyze_legacy_data():
    """기존 레거시 데이터 분석"""
    with SessionLocal() as session:
        print("📊 1982-2001년 기존 데이터 분석 중...")
        print("=" * 50)
        
        # 타자 데이터 분석
        batting_stats = session.execute(text("""
            SELECT 
                season,
                league,
                COUNT(*) as count
            FROM player_season_batting 
            WHERE season BETWEEN 1982 AND 2001
            GROUP BY season, league
            ORDER BY season, league
        """)).fetchall()
        
        print("📊 타자 데이터 현황:")
        for row in batting_stats:
            print(f"  {row.season}년 {row.league}: {row.count}명")
        
        print()
        
        # 투수 데이터 분석
        pitching_stats = session.execute(text("""
            SELECT 
                season,
                league,
                COUNT(*) as count
            FROM player_season_pitching 
            WHERE season BETWEEN 1982 AND 2001
            GROUP BY season, league
            ORDER BY season, league
        """)).fetchall()
        
        print("📊 투수 데이터 현황:")
        for row in pitching_stats:
            print(f"  {row.season}년 {row.league}: {row.count}명")
        
        print()
        
        # 업데이트 대상 확인
        update_candidates = session.execute(text("""
            SELECT 
                'batting' as data_type,
                season,
                COUNT(*) as count
            FROM player_season_batting 
            WHERE season BETWEEN 1982 AND 2001 
            AND league = 'REGULAR'
            AND source != 'LEGACY_CRAWLER'
            GROUP BY season
            
            UNION ALL
            
            SELECT 
                'pitching' as data_type,
                season,
                COUNT(*) as count
            FROM player_season_pitching 
            WHERE season BETWEEN 1982 AND 2001 
            AND league = 'REGULAR'
            AND source != 'LEGACY_CRAWLER'
            GROUP BY season
            
            ORDER BY data_type, season
        """)).fetchall()
        
        print("🎯 업데이트 대상 (REGULAR로 저장된 구 크롤러 데이터):")
        for row in update_candidates:
            print(f"  {row.season}년 {row.data_type}: {row.count}명")
        
        return update_candidates


def detect_series_from_data(session, year: int, table_name: str):
    """
    데이터 패턴으로 시리즈 추정
    - 10월 이후 소수 데이터 = 한국시리즈
    - 대량 데이터 = 정규시즌
    """
    result = session.execute(text(f"""
        SELECT 
            COUNT(*) as player_count,
            AVG(games) as avg_games,
            MIN(games) as min_games,
            MAX(games) as max_games
        FROM {table_name}
        WHERE season = :year 
        AND league = 'REGULAR'
        AND source != 'LEGACY_CRAWLER'
    """), {"year": year}).fetchone()
    
    if not result or result.player_count == 0:
        return []
    
    player_count = result.player_count
    avg_games = result.avg_games or 0
    
    # 시리즈 추정 로직
    estimated_series = []
    
    if avg_games > 50:  # 정규시즌 (많은 경기)
        estimated_series.append(('REGULAR', player_count))
    elif avg_games < 10:  # 한국시리즈 (적은 경기)
        estimated_series.append(('KOREAN_SERIES', player_count))
    else:
        # 혼재된 경우 - 게임 수로 분류
        game_distribution = session.execute(text(f"""
            SELECT 
                CASE 
                    WHEN games > 50 THEN 'REGULAR'
                    WHEN games < 10 THEN 'KOREAN_SERIES'
                    ELSE 'UNKNOWN'
                END as estimated_league,
                COUNT(*) as count
            FROM {table_name}
            WHERE season = :year 
            AND league = 'REGULAR'
            AND source != 'LEGACY_CRAWLER'
            AND games IS NOT NULL
            GROUP BY estimated_league
        """), {"year": year}).fetchall()
        
        for row in game_distribution:
            if row.estimated_league != 'UNKNOWN':
                estimated_series.append((row.estimated_league, row.count))
    
    return estimated_series


def update_league_fields(dry_run: bool = True, start_year: int = 1982, end_year: int = 2001):
    """league 필드 업데이트 실행"""
    with SessionLocal() as session:
        total_updated = 0
        
        print(f"🔄 {start_year}-{end_year}년 league 필드 업데이트 {'(시뮬레이션)' if dry_run else '(실제 적용)'}")
        print("=" * 60)
        
        for year in range(start_year, end_year + 1):
            print(f"\n📅 {year}년 처리 중...")
            
            # 타자 데이터 분석
            batting_series = detect_series_from_data(session, year, 'player_season_batting')
            for estimated_league, count in batting_series:
                print(f"  📊 타자 {estimated_league}: {count}명")
                
                if not dry_run:
                    if estimated_league == 'REGULAR':
                        # 게임 수가 많은 데이터만 REGULAR 유지
                        updated = session.execute(text("""
                            UPDATE player_season_batting 
                            SET league = 'REGULAR'
                            WHERE season = :year 
                            AND league = 'REGULAR'
                            AND source != 'LEGACY_CRAWLER'
                            AND (games > 50 OR games IS NULL)
                        """), {"year": year}).rowcount
                    else:
                        # 게임 수가 적은 데이터를 KOREAN_SERIES로 변경
                        updated = session.execute(text("""
                            UPDATE player_season_batting 
                            SET league = 'KOREAN_SERIES'
                            WHERE season = :year 
                            AND league = 'REGULAR'
                            AND source != 'LEGACY_CRAWLER'
                            AND games < 10
                        """), {"year": year}).rowcount
                    
                    total_updated += updated
                    print(f"    ✅ {updated}명 업데이트")
            
            # 투수 데이터 분석
            pitching_series = detect_series_from_data(session, year, 'player_season_pitching')
            for estimated_league, count in pitching_series:
                print(f"  ⚾ 투수 {estimated_league}: {count}명")
                
                if not dry_run:
                    if estimated_league == 'REGULAR':
                        updated = session.execute(text("""
                            UPDATE player_season_pitching 
                            SET league = 'REGULAR'
                            WHERE season = :year 
                            AND league = 'REGULAR'
                            AND source != 'LEGACY_CRAWLER'
                            AND (games > 10 OR games IS NULL)
                        """), {"year": year}).rowcount
                    else:
                        updated = session.execute(text("""
                            UPDATE player_season_pitching 
                            SET league = 'KOREAN_SERIES'
                            WHERE season = :year 
                            AND league = 'REGULAR'
                            AND source != 'LEGACY_CRAWLER'
                            AND games < 5
                        """), {"year": year}).rowcount
                    
                    total_updated += updated
                    print(f"    ✅ {updated}명 업데이트")
        
        if not dry_run:
            session.commit()
            print(f"\n✅ 총 {total_updated}명의 league 필드 업데이트 완료")
        else:
            print(f"\n💡 실제 적용하려면 --apply 옵션을 사용하세요")


def simple_update_all_to_regular(dry_run: bool = True, start_year: int = 1982, end_year: int = 2001):
    """간단한 방법: 모든 레거시 데이터를 REGULAR로 통일"""
    with SessionLocal() as session:
        print(f"🔄 {start_year}-{end_year}년 모든 데이터를 REGULAR로 통일 {'(시뮬레이션)' if dry_run else '(실제 적용)'}")
        print("=" * 60)
        
        # 타자 데이터 업데이트
        if not dry_run:
            batting_updated = session.execute(text("""
                UPDATE player_season_batting 
                SET league = 'REGULAR'
                WHERE season BETWEEN :start_year AND :end_year
                AND source != 'LEGACY_CRAWLER'
            """), {"start_year": start_year, "end_year": end_year}).rowcount
            
            pitching_updated = session.execute(text("""
                UPDATE player_season_pitching 
                SET league = 'REGULAR'
                WHERE season BETWEEN :start_year AND :end_year
                AND source != 'LEGACY_CRAWLER'
            """), {"start_year": start_year, "end_year": end_year}).rowcount
            
            session.commit()
            
            print(f"✅ 타자 데이터: {batting_updated}명 업데이트")
            print(f"✅ 투수 데이터: {pitching_updated}명 업데이트")
            print(f"✅ 총 {batting_updated + pitching_updated}명 업데이트 완료")
        else:
            # 시뮬레이션
            batting_count = session.execute(text("""
                SELECT COUNT(*) 
                FROM player_season_batting 
                WHERE season BETWEEN :start_year AND :end_year
                AND source != 'LEGACY_CRAWLER'
            """), {"start_year": start_year, "end_year": end_year}).scalar()
            
            pitching_count = session.execute(text("""
                SELECT COUNT(*) 
                FROM player_season_pitching 
                WHERE season BETWEEN :start_year AND :end_year
                AND source != 'LEGACY_CRAWLER'
            """), {"start_year": start_year, "end_year": end_year}).scalar()
            
            print(f"📊 업데이트 대상:")
            print(f"  - 타자: {batting_count}명")
            print(f"  - 투수: {pitching_count}명")
            print(f"  - 총합: {batting_count + pitching_count}명")
            print(f"\n💡 실제 적용하려면 --apply 옵션을 사용하세요")


def main():
    parser = argparse.ArgumentParser(description="1982-2001년 레거시 데이터 league 필드 업데이트")
    parser.add_argument("--analyze", action="store_true", help="기존 데이터 분석만 수행")
    parser.add_argument("--apply", action="store_true", help="실제 업데이트 적용 (기본값: 시뮬레이션)")
    parser.add_argument("--simple", action="store_true", help="모든 데이터를 REGULAR로 통일")
    parser.add_argument("--start", type=int, default=1982, help="시작 년도")
    parser.add_argument("--end", type=int, default=2001, help="끝 년도")
    
    args = parser.parse_args()
    
    try:
        if args.analyze:
            analyze_legacy_data()
        elif args.simple:
            simple_update_all_to_regular(not args.apply, args.start, args.end)
        else:
            update_league_fields(not args.apply, args.start, args.end)
            
    except KeyboardInterrupt:
        print("\n❌ 사용자가 중단했습니다.")
    except Exception as e:
        print(f"❌ 오류 발생: {e}")


if __name__ == "__main__":
    main()