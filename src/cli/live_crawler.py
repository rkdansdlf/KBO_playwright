"""
실시간 KBO 데이터 크롤링 데몬 (Live Crawler)
설정된 매 N분 주기마다 오늘 열리는 경기의 문자 중계(PBP)와 스코어보드를 가볍게 가져와 OCI로 초고속 동기화합니다.
"""
from __future__ import annotations

import argparse
import asyncio
import time
from datetime import datetime
import pytz

from src.crawlers.schedule_crawler import ScheduleCrawler
from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.crawlers.naver_relay_crawler import NaverRelayCrawler
from src.repositories.game_repository import save_game_detail, save_relay_data
from src.sync.oci_sync import OCISync
from src.utils.safe_print import safe_print as print

async def run_live_crawler_cycle() -> bool:
    """단일 라이브 크롤링 사이클을 돕니다. 현재 진행 중인 게임이 없으면 False를 반환합니다."""
    seoul_tz = pytz.timezone("Asia/Seoul")
    now = datetime.now(seoul_tz)
    
    print(f"\n[{now.strftime('%Y-%m-%d %H:%M:%S')}] 🚨 Live Crawl Cycle Started")
    
    # 1. 오늘의 스케줄 수집
    sched_crawler = ScheduleCrawler()
    games = await sched_crawler.crawl_schedule(now.year, now.month)
    
    today_str = now.strftime('%Y%m%d')
    today_games = [g for g in games if g["game_date"].replace("-", "") == today_str]
    
    if not today_games:
        print("[INFO] No games scheduled for today.")
        return False
        
    live_game_ids = set()
    
    # 라이브 수집기 (안정적인 Naver API 활용)
    relay_crawler = NaverRelayCrawler()
    detail_crawler = GameDetailCrawler(request_delay=0.1)
    
    for g in today_games:
        game_id = g["game_id"]
        
        # Naver API로부터 PBP 데이터 수집
        relay_data = await relay_crawler.crawl_game_events(game_id)
        
        if relay_data and relay_data.get('events'):
            live_game_ids.add(game_id)
            
            # 이벤트 DB 저장
            flat_events = relay_data.get('events', [])
            save_relay_data(game_id, flat_events)
            print(f"[LIVE] 📝 Synced {len(flat_events)} PBP events via Naver for {game_id}")
                
            # 라이트웨이트 스코어보드 추출 (점수/상태 업데이트용)
            detail = await detail_crawler.crawl_game(game_id, today_str, lightweight=True)
            if detail:
                save_game_detail(detail)
                print(f"[LIVE] 📊 Updated Scoreboard for {game_id}")
                
    if not live_game_ids:
        print("[INFO] No LIVE games currently active right now.")
        return False
        
    # 2. OCI 고속 동기화
    print(f"[SYNC] Triggering high-speed Delta Sync for Games: {live_game_ids}")
    try:
        sync_engine = OCISync()
        for gid in live_game_ids:
            sync_engine.sync_specific_game(gid)
            print(f"[SYNC] ✅ Synced {gid} to OCI.")
    except Exception as e:
         print(f"[ERROR] Live Sync Failed: {e}")
         
    return True

async def main_loop(interval_minutes: int):
    while True:
        try:
            active = await run_live_crawler_cycle()
            if not active:
                # 경기가 없거나 모두 끝났을 경우
                # 현실적으로 22시부터 새벽까지는 10분-30분 단위 대기
                seoul_tz = pytz.timezone("Asia/Seoul")
                now = datetime.now(seoul_tz)
                if now.hour < 14 or now.hour >= 23:
                    print("[SLEEP] Midnight/Morning detected. Sleeping deeply for 1 hour.")
                    await asyncio.sleep(3600)
                else:
                    print("[WAIT] No live games. Next check in 5 minutes.")
                    await asyncio.sleep(300)
            else:
                print(f"[WAIT] Waiting {interval_minutes} minutes for next live cycle...")
                await asyncio.sleep(interval_minutes * 60)
        except Exception as e:
            print(f"[CRITICAL ERROR] Live loop crashed: {e}")
            await asyncio.sleep(60)

def main():
    parser = argparse.ArgumentParser(description="KBO Live Score & PBP Daemon")
    parser.add_argument("--interval", type=int, default=2, help="Crawling polling interval in minutes")
    parser.add_argument("--run-once", action="store_true", help="Run precisely one cycle and exit")
    args = parser.parse_args()

    if args.run_once:
        asyncio.run(run_live_crawler_cycle())
    else:
        print(f"🚀 Starting Real-time Daemon... Polling every {args.interval}m.")
        asyncio.run(main_loop(args.interval))

if __name__ == "__main__":
    main()
