from src.db.engine import SessionLocal
from src.models.game import Game, GameBattingStat, GamePitchingStat, GameInningScore
from sqlalchemy import func

def run_verification():
    session = SessionLocal()
    years = ['2024', '2025', '2026']
    
    print("=== KBO Box Score Data Verification (2024-2026) ===")
    
    for year in years:
        # 1. 수집 상태 확인
        total = session.query(Game).filter(func.strftime('%Y', Game.game_date) == year).count()
        if total == 0:
            print(f"\n[Year {year}] No games found in DB.")
            continue
            
        finalized = session.query(Game).filter(
            func.strftime('%Y', Game.game_date) == year,
            Game.game_status == 'COMPLETED'
        ).count()
        
        print(f"\n[Year {year}]")
        print(f"  - Total games: {total}")
        print(f"  - Finalized (Detailed): {finalized} ({finalized/total*100:.1f}%)")
        
        # 2. 상세 데이터 누락 확인 (FINAL 인데 스탯이 없는 경우)
        if finalized > 0:
            # Subquery to get game_ids with stats
            stat_exists_subquery = session.query(GameBattingStat.game_id).distinct()
            
            missing_stats = session.query(Game).filter(
                func.strftime('%Y', Game.game_date) == year,
                Game.game_status == 'COMPLETED'
            ).filter(
                ~Game.game_id.in_(stat_exists_subquery)
            ).count()
            print(f"  - Finalized games missing batting stats: {missing_stats}")

            # 3. 데이터 일치성 샘플 검증 (Score vs Batting Stats)
            # Pick a non-zero score game for better verification
            sample_game = session.query(Game).filter(
                func.strftime('%Y', Game.game_date) == year,
                Game.game_status == 'FINAL',
                (Game.home_score + Game.away_score) > 0
            ).first()
            
            if sample_game:
                batting_score = session.query(func.sum(GameBattingStat.runs)).filter(
                    GameBattingStat.game_id == sample_game.game_id
                ).scalar() or 0
                
                total_score = (sample_game.home_score or 0) + (sample_game.away_score or 0)
                
                status = "PASS" if int(batting_score) == int(total_score) else "FAIL"
                print(f"  - Sample Integrity ({sample_game.game_id}): {status} (Total Score: {total_score}, Batting Sum: {batting_score})")

    session.close()

if __name__ == "__main__":
    run_verification()
