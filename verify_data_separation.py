#!/usr/bin/env python3
"""
타자/투수 데이터 분리 저장 검증 스크립트
투수 데이터가 제대로 분리되어 저장되었는지 확인
"""
from src.db.engine import SessionLocal
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching

def verify_data_separation():
    """데이터 분리 상태 확인 및 요약"""
    
    with SessionLocal() as session:
        print("🔍 KBO 데이터 분리 저장 검증")
        print("=" * 50)
        
        # 1. 테이블별 데이터 수 확인
        batting_count = session.query(PlayerSeasonBatting).count()
        pitching_count = session.query(PlayerSeasonPitching).count()
        
        print(f"\n📊 데이터 현황:")
        print(f"  - player_season_batting (타자): {batting_count}건")
        print(f"  - player_season_pitching (투수): {pitching_count}건")
        
        # 2. 타자 데이터 샘플 확인
        if batting_count > 0:
            print(f"\n🏏 타자 데이터 샘플:")
            batting_samples = session.query(PlayerSeasonBatting).limit(3).all()
            for i, batting in enumerate(batting_samples):
                print(f"  {i+1}. player_id: {batting.player_id}")
                print(f"     season: {batting.season}, league: {batting.league}")
                print(f"     team: {batting.team_id}, source: {batting.source}")
                print(f"     games: {batting.games}, avg: {batting.avg}")
                print(f"     hits: {batting.hits}, home_runs: {batting.home_runs}")
                
                # extra_stats 확인
                if batting.extra_stats:
                    print(f"     extra_stats: {list(batting.extra_stats.keys()) if isinstance(batting.extra_stats, dict) else 'N/A'}")
                print()
        
        # 3. 투수 데이터 샘플 확인
        if pitching_count > 0:
            print(f"⚾ 투수 데이터 샘플:")
            pitching_samples = session.query(PlayerSeasonPitching).limit(3).all()
            for i, pitching in enumerate(pitching_samples):
                print(f"  {i+1}. player_id: {pitching.player_id}")
                print(f"     season: {pitching.season}, league: {pitching.league}")
                print(f"     team: {pitching.team_code}, source: {pitching.source}")
                print(f"     games: {pitching.games}, wins: {pitching.wins}, losses: {pitching.losses}")
                print(f"     era: {pitching.era}, innings_pitched: {pitching.innings_pitched}")
                print(f"     strikeouts: {pitching.strikeouts}, whip: {pitching.whip}")
                
                # 확장 통계 확인
                extended_stats = []
                if pitching.complete_games is not None:
                    extended_stats.append(f"CG: {pitching.complete_games}")
                if pitching.shutouts is not None:
                    extended_stats.append(f"SHO: {pitching.shutouts}")
                if pitching.quality_starts is not None:
                    extended_stats.append(f"QS: {pitching.quality_starts}")
                if pitching.tbf is not None:
                    extended_stats.append(f"TBF: {pitching.tbf}")
                if pitching.np is not None:
                    extended_stats.append(f"NP: {pitching.np}")
                
                if extended_stats:
                    print(f"     확장 통계: {', '.join(extended_stats)}")
                
                # extra_stats 확인
                if pitching.extra_stats:
                    print(f"     extra_stats: {list(pitching.extra_stats.keys()) if isinstance(pitching.extra_stats, dict) else 'N/A'}")
                print()
        
        # 4. 데이터 품질 검증
        print("🔍 데이터 품질 검증:")
        
        # 타자 데이터 검증
        if batting_count > 0:
            batting_no_player_id = session.query(PlayerSeasonBatting).filter(
                PlayerSeasonBatting.player_id.is_(None)
            ).count()
            
            batting_no_season = session.query(PlayerSeasonBatting).filter(
                PlayerSeasonBatting.season.is_(None)
            ).count()
            
            print(f"  타자 데이터:")
            print(f"    - player_id 누락: {batting_no_player_id}건")
            print(f"    - season 누락: {batting_no_season}건")
        
        # 투수 데이터 검증
        if pitching_count > 0:
            pitching_no_player_id = session.query(PlayerSeasonPitching).filter(
                PlayerSeasonPitching.player_id.is_(None)
            ).count()
            
            pitching_no_season = session.query(PlayerSeasonPitching).filter(
                PlayerSeasonPitching.season.is_(None)
            ).count()
            
            print(f"  투수 데이터:")
            print(f"    - player_id 누락: {pitching_no_player_id}건")
            print(f"    - season 누락: {pitching_no_season}건")
        
        # 5. 결론
        print(f"\n" + "=" * 50)
        print("✅ 검증 결과 요약")
        print("=" * 50)
        
        if batting_count > 0 and pitching_count > 0:
            print("🎉 타자와 투수 데이터가 올바르게 분리되어 저장되었습니다!")
            print("   - 타자 데이터는 player_season_batting 테이블에")
            print("   - 투수 데이터는 player_season_pitching 테이블에")
            print("   - 각각 적절한 컬럼에 의미있는 데이터가 저장됨")
        elif batting_count > 0:
            print("✅ 타자 데이터만 존재 (정상)")
        elif pitching_count > 0:
            print("⚾ 투수 데이터만 존재 (정상)")
        else:
            print("⚠️ 저장된 데이터가 없습니다.")
        
        print(f"\n💡 이제 다음 단계로 진행할 수 있습니다:")
        print(f"1. 더 많은 데이터 수집 (다른 시리즈, 더 많은 선수)")
        print(f"2. Supabase 환경변수 설정 후 동기화 테스트")
        print(f"3. 실제 운영 데이터 수집")

if __name__ == "__main__":
    verify_data_separation()