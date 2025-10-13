
초기 데이터 수집 크롤링 로직 

- **선수 목록 크롤러** - 팀별 타자/투수 목록 수집 
- **프로필 크롤러** - playerID, player_basic 즉 선수 신체정보, 포지션 등
선수데이터 저장

현역 외 선수 기록페이지 (투수)
현역 외 선수 기록페이지 (타자)
크롤링후 저장

선수 프로필 페이지에서 퓨처스리그 (프로필 기반 수집)

- 2025 시즌 경기 일정 크롤러 예)game_id=20251013SKSS0
경기 기록 크롤링
 1. 전체 시즌 경기 ID 수집
 2. https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameDate={date}&gameId={game_id}&section=REVIEW
 **크롤링 요소:**
 box-score-area
 예시)
 구장 : 문학 관중 : 22,500 개시 : 14:00 종료 : 16:58 경기시간 : 2:58
 승리팀 이닝 스코어
 패배팀 이닝 스코어
 
- 원정 타자: `.tblAwayHitter1, .tblAwayHitter2, .tblAwayHitter3`
- 홈 타자: `.tblHomeHitter1, .tblHomeHitter2, .tblHomeHitter3`
- 투수: `.pitcher-record-area`
 
 
경기 기록 저장

위 순서대로 초기데이터 크롤링 및 저장순서 수행