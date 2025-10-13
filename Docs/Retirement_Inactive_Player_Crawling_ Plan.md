좋아요—“현역외(은퇴/비활성) 선수” 페이지를 안정적으로 긁어서 DB에 넣는 걸, 지금 KBO_Data(PlaywrightKBO) 레포 구조와 v2 스키마에 맞춰 한 번에 구현하는 로드맵으로 정리해 드릴게요. 아래 순서대로 진행하면 됩니다.

# 1) 무엇을 어디에 저장할지 (스키마 정렬)

* **프로필**: `kbo_player_profiles.player_status='Retired'`로 마킹하고, 동일 `player_id` 키로 현역/은퇴를 통합 관리합니다. (팀, 키/몸무게, 투/타, 등번호 등은 그대로 재사용) 
* **시즌별 기록**: 정규시즌 기준의 타격/투구/수비는 기존 v2 테이블에 그대로 **UPSERT**

  * 타격: `kbo_season_batting_stats` (BB/HBP/SO/OBP/SLG/OPS 포함—이미 2테이블 병합 방식으로 수집 가능)  
  * 투구: `kbo_season_pitching_stats` (ERA, WHIP 등 17컬럼) 
  * 수비: `kbo_season_fielding_stats` (필요시 포지션별 집계로 저장) 
* **포스트시즌/시범경기 등 탭(경기유형별)**: **보조 테이블(예: `player_game_type_stats`)**로 JSON 컬럼에 저장하는 경량 확장을 권장합니다. v2가 이미 보조 테이블을 추가해온 패턴(리그 평균, 베이스러닝 등)을 그대로 따르는 확장입니다. (ISO/BABIP 등 파생지표는 v2 가이드의 공식을 활용해 저장 후 계산 가능) 

> 결과적으로 “정규시즌=핵심 테이블”, “경기유형별/Top10=보조 테이블” 구조로 가면, 기존 분석·대시보드·쿼리와 깔끔하게 맞물립니다. 

# 2) 크롤링 소스·모듈 배치

레포의 표준 구조에 맞춰 “retire” 전용 모듈을 추가하세요. (아래 경로는 README_KR의 실제 레이아웃과 호환) 

```
src/
  crawlers/
    retire/
      listing.py        # 비활성(현역외) 후보 ID 뽑기
      detail_hitter.py  # /Record/Retire/Hitter.aspx 파서
      detail_pitcher.py # /Record/Retire/Pitcher.aspx 파서
  parsers/
    retire_parser.py    # 공통 파서(테이블 → dict 리스트)
  database/
    upsert.py           # UPSERT 헬퍼 재사용
  cli/
    crawl_retire.py     # CLI 진입점
```

# 3) 대상 URL & 셀렉터(안전지대)

* **현역외 타자**: `/Record/Retire/Hitter.aspx?playerId={id}`
* **현역외 투수**: `/Record/Retire/Pitcher.aspx?playerId={id}`
* **연도별/팀별 목록(역대 선수 ID 뽑기)**: `HitterBasic/Basic1.aspx`, `PitcherBasic/Basic1.aspx` (연도/팀 드롭다운 + 표의 2번째 열 링크에서 `playerId` 추출)  

> 이미 “2개 테이블 분리 노출 → 합쳐서 저장” 패턴이 문서화되어 있으니, 현역외 상세 페이지에서도 **동일 로직**(table[0] + table[1])을 우선 시도합니다. (BB/HBP/SO/OBP/SLG/OPS 유실 방지)  

# 4) 수집 순서(비활성 전용 파이프라인)

1. **현역 세트**: `/Player/RegisterAll.aspx`에서 현재 **현역 선수 `player_id` 집합**을 수집
2. **역대 세트**: 기록실(타자/투수) 목록 페이지를 **1982~현재** 연도×10개 팀으로 순회하며 **전체 선수 `player_id` 집합** 추출
3. **차집합**: `전체 - 현역 = 비활성(현역외)` ID 목록
4. **상세 수집**: 각 ID에 대해

   * Hitter/ Pitcher **둘 다 요청** → 200 응답 오는 쪽만 파싱(둘 다 존재하면 둘 다 저장)
   * **정규시즌** 내역은 v2 핵심 테이블(타/투/수비)로 UPSERT
   * **경기유형별** 탭(시범/포스트시즌)은 보조 테이블(JSON)로 저장
   * “연도별 TOP10”은 `player_rankings` 또는 경량 보조 테이블로 저장
5. **프로필 보강**: `kbo_player_profiles.player_status='Retired'`로 갱신(이적/외국인 포함)
6. **로그/리트라이**: 404/타임아웃 등은 재시도 큐에 넣고, 1–2초 딜레이로 예의 준수(robots, rate limit)  

# 5) 구현 디테일(Playwright 중심)

* **네트워크-퍼스트 전략**: 최초 1회는 **XHR/Fetch 모니터링**으로 실제 응답 JSON 엔드포인트가 있나 탐색(HAR/JSONL 저장→테스트 픽스처로 재생). 없거나 불안정하면 **HTML 파싱** 경로로 폴백. (레포에 HTML 구조 디버거/검증 도구가 있으니 같이 활용) 
* **두 테이블 결합(타자)**:
  `tables = page.query_selector_all('table.tbl.tt')` → `tables[0]`(기본 16컬럼) + `tables[1]`(고급 13컬럼)을 **연도키**로 머지 후 INSERT. 검증 예시는 문서의 샘플값 참고.  
* **투수 테이블(단일 표)**: ERA, IP, BB, SO, WHIP 등 17컬럼 맵핑 유지. 
* **셀렉터/구조 변경 대응**: `tools/debug_page_structure.py`, `tools/verify_html_structure.py`로 사전 점검 후 본수집. CI 전 단계에서 셀렉터 유효성 확인 추천.  
* **지표 보강**: 저장 후 파이프라인에서 **ISO/BABIP** 자동 계산(간단식) → 컬럼에 반영. 복잡한 wOBA/wRC+/WAR은 나중에(외부 컨텍스트/리그평균 테이블과 함께)  

# 6) UPSERT·트랜잭션 & 성능

* **DB 접근**: 파라미터 바인딩으로 **SQL Injection 차단**(이미 프로젝트 가이드에 반영). 대량 저장은 **배치/트랜잭션** 단위로 커밋. 
* **인덱스**: 연도/팀/핵심 지표 기준의 v2 인덱스 권장안을 적용(연도 조회, 팀별 랭킹, OPS/wRC+ 순위 등). 
* **증분/일일 업데이트**: 은퇴 페이지는 빈도 낮음—**전수 백필 작업** 후엔 필요 시 **주간/월간** 갱신으로 충분. 일일잡은 현역 중심 스케줄에 보조로 붙이면 됩니다.  

# 7) CLI 워크플로우(예시)

* **초기화 & 마스터 삽입**

  ```bash
  docker-compose run --rm kbo-crawler python src/database/init_db.py
  docker-compose run --rm kbo-crawler python src/database/insert_master_data.py
  ```


* **비활성 전수 수집**

  ```bash
  docker-compose run --rm kbo-crawler python src/cli/crawl_retire.py --years 1982-2025 --concurrency 3
  ```
* **일일 업데이트(참고)**

  ```bash
  docker-compose run --rm kbo-crawler python daily_update.py
  ```



# 8) 테스트 & 픽스처

* 첫 주기 때 **HAR/HTML 스냅샷**을 `tests/fixtures/kbo_retire/{player_id}/`로 보관 → 파서 단위테스트에서 재생.
* `verify_html_structure.py`로 배포 전 셀렉터 변동 검사(실패 시 경고/중단). 

# 9) 윤리/에티켓 & 한계

* **1–2초 딜레이**, 하루 1–2회 권장, robots.txt 준수(비상업/교육용) 가이드라인을 반드시 따르세요.  
* KBO 사이트가 제공하지 않는 지표들(WAR, wOBA 등)은 **내부 계산 또는 외부 소스 연동**으로 보강합니다. 

---

## 한 줄 요약

* **현역외 페이지는 “두 테이블 병합” 원칙 + 정규시즌은 v2 핵심 테이블 UPSERT, 나머지 탭은 보조 테이블(JSON)로 저장**이 가장 안전하고, 이미 문서화된 크롤링/업데이트/도구 흐름과 100% 호환됩니다. (배치 저장·인덱스·검증 도구까지 모두 기존 자산 재사용)   
