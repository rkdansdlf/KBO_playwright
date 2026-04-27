from src.db.engine import SessionLocal
from src.models.player import PlayerSeasonBatting, Player
from sqlalchemy import func

def verify_1982_ob():
    session = SessionLocal()
    # 1982년 OB 베어스(OB) 타자 기록 확인
    # 실제 1982년 OB 타자 명단은 약 15-20명 내외입니다.
    stats = session.query(PlayerSeasonBatting).filter(
        PlayerSeasonBatting.season == 1982,
        PlayerSeasonBatting.team_code == 'OB'
    ).all()
    
    print(f"--- 1982 OB Bears Verification ---")
    print(f"Number of players found: {len(stats)}")
    
    if stats:
        total_hits = sum(s.hits or 0 for s in stats)
        total_hr = sum(s.home_runs or 0 for s in stats)
        print(f"Total Team Hits (Aggregated): {total_hits}")
        print(f"Total Team Home Runs (Aggregated): {total_hr}")
        
        # 샘플 선수 확인
        for s in stats[:3]:
            player = session.query(Player).filter(Player.id == s.player_id).first()
            name = "Unknown"
            if player:
                # identity 정보를 가져올 수 있다면 이름 출력 (생략 가능)
                pass
            print(f"  Player ID {s.player_id}: {s.avg} AVG, {s.hits} H, {s.home_runs} HR")

    session.close()

if __name__ == "__main__":
    verify_1982_ob()
