(“은퇴,비활성화선수크롤링계획.md”)를 **현재 파이프라인/스키마/SQLAlchemy 전략**

---

# 은퇴/비활성 선수 크롤링 계획 

## 0) 목적

* KBO “현역외(은퇴/비활성)” 선수의 **프로필 + 시즌 누적 기록(타/투/수비)** 을 안정적으로 수집·정규화·저장한다.
* 파이프라인·DB는 **SQLAlchemy 2.x(ORM+Core 혼합)**, **로컬 SQLite↔운영 MySQL 스위치**를 기본 전제.

---

## 1) 저장 대상 및 스키마 매핑

* **프로필**: `players`/`player_identities`에 통합 보관, 상태는 `players.is_active=False` 또는 `player_status='Retired'`로 표준화. (기존 “은퇴=프로필 마킹” 원칙 유지) 
* **정규시즌 기록**: 타격/투구/수비는 **핵심 시즌 테이블**(예: `player_season_batting/pitching/fielding`)에 **UPSERT**. 
* **기타 탭(시범/포스트/Top10)**: 경량 **보조 테이블**(예: `player_game_type_stats`)의 JSON 필드에 저장(필요 시 별도 테이블로 승격). 

> 설계 철학: “정규시즌=핵심 / 기타=보조” 구조를 유지해 기존 분석·대시보드와 호환. 

---

## 2) 소스·모듈 구조

```
src/
  crawlers/
    retire/
      listing.py         # 후보 player_id 수집 (현역/역대)
      detail_hitter.py   # /Record/Retire/Hitter.aspx?playerId={id}
      detail_pitcher.py  # /Record/Retire/Pitcher.aspx?playerId={id}
  parsers/
    retire_parser.py     # 표 → dict list(타자 2표 병합, 투수 1표 파싱)
  repository/
    game_repo.py         # 공용 세션, 트랜잭션
    upserts.py           # SQLite/MySQL 겸용 업서트(SQLAlchemy Core)
  cli/
    crawl_retire.py      # 엔트리포인트 (전수/증분)
```

(기존 경로 제안과 호환되도록 유지함) 

---

## 3) 타겟 URL/패턴

* 현역외 타자: `/Record/Retire/Hitter.aspx?playerId={id}`
* 현역외 투수: `/Record/Retire/Pitcher.aspx?playerId={id}`
* 역대 선수 ID 후보: 기록실(타자/투수) **연도×팀 목록**에서 2열 링크의 `playerId` 추출(1982~현재). 

---

## 4) 수집 절차(비활성 파이프라인)

1. **현역 집합**: `/Player/RegisterAll.aspx`로 현재 현역 `player_id` 집합 수집.
2. **역대 집합**: 기록실 목록 페이지를 **1982~현재 × 팀** 순회해 전체 `player_id` 집합 수집.
3. **차집합**: `역대 - 현역 = 비활성(현역외)` 대상 확정.
4. **상세 병행 수집**(ID별):

   * 타자/투수 **양쪽 요청** 후 **200 OK**만 파싱(둘 다 존재하면 둘 다 저장).
   * **타자**: 페이지 내 **2개 표를 연도키로 병합** 후 레코드화. 
   * **투수**: 단일 표 매핑(ERA/IP/BB/SO/WHIP 등). 
   * **정규시즌** → 핵심 시즌 테이블 UPSERT / **시범·포스트·Top10** → 보조(JSON) 저장. 
5. **프로필 상태 업데이트**: 은퇴/비활성으로 마킹. 

---

## 5) 크롤링 전략(네트워크 우선)

* **XHR/Fetch 우선 탐지**: 최초 1회 **네트워크 패널 관찰(HAR/JSONL 보관)** → API 응답이 안정적이면 JSON 파싱, 없거나 불안정하면 **HTML 파싱 폴백**(셀렉터 검증 스크립트로 사전 점검). 
* **타자 2테이블 병합 규칙**: `tables = page.query_selector_all('table.tbl.tt')` → `tables[0]`(기본) + `tables[1]`(고급) **연도 기준 병합**. 
* **지표 보강**: 저장 후 파이프라인에서 **ISO/BABIP** 계산(간단식)만 즉시 반영(고급 지표는 후순위). 

---

## 6) 저장/트랜잭션( SQLAlchemy 2.x )

* **엔진 스위치**: `DATABASE_URL`로 SQLite↔MySQL 전환(개발은 SQLite WAL).
* **업서트**: `sqlalchemy.dialects.sqlite.insert(...).on_conflict_do_update` / `mysql.insert(...).on_duplicate_key_update` 캡슐화.
* **배치 커밋**: 선수 N명 단위 트랜잭션, 실패 시 전체 롤백.
* **원문 보관(옵션)**: 주요 HTML/XHR 응답은 `raw_fetches`에 해시와 함께 저장(회귀·감사 추적).

---

## 7) 검증(불변식/무결성)

* **타격**: `H == (1B+2B+3B+HR)`, `PA == AB+BB+HBP+SH+SF`
* **투구**: `ER ≤ R`, `IP_outs % 3 == 0`
* **합계 교차검증**: 연도별 선수합 ↔ 팀합(선택), 레코드 수·연도 범위 확인.
* **셀렉터 변동 감시**: 배포 전 `verify_html_structure.py`로 검사하고 실패 시 중단. 

---

## 8) 재시도/예절/빈도

* **레이트리밋**: 요청 간 1–2초, 404/타임아웃은 지수 백오프 재시도 큐로 이월. 
* **주기**: 은퇴 페이지는 변경 빈도 낮음 → **전수 백필 후 주간/월간 갱신**으로 충분. 

---

## 9) CLI 워크플로우

```bash
# 초기화 & 마스터 삽입(선택)
docker-compose run --rm kbo-crawler python src/database/init_db.py
docker-compose run --rm kbo-crawler python src/database/insert_master_data.py

# 전수 수집(연도 범위/동시성 조절)
DATABASE_URL=sqlite:///./data/kbo_dev.db \
docker-compose run --rm kbo-crawler \
  python src/cli/crawl_retire.py --years 1982-2025 --concurrency 3
```

(기존 예시를 유지·보강) 

---

## 10) 테스트/픽스처

* **픽스처**: 최초 수집 시 **HAR/HTML 스냅샷**을 `tests/fixtures/kbo_retire/{player_id}/`에 저장, 파서 단위테스트에서 재생.
* **회귀 테스트**: 주요 선수 10명 고정 케이스 + “셀 헤더/순서 변경” 변형 케이스. 

---

## 11) 운영 체크리스트

* 크롤러 동시성(3 이하)·요청 딜레이 준수, robots 준수. 
* 스케줄러(APScheduler/Airflow)에서 **현역 파이프라인**의 야간 잡에 **보조 태스크**로 연결(주간/월간).
* 실패율·응답포맷 변동 감시(픽스처 비교/해시).
* 저장 후 **스모크 검증**(연도별 레코드 수/지표 불변식/Null 비율) 통과 시 완료.

---

### 이번 개정에서 바뀐 핵심

* 저장/업서트 경로를 **SQLAlchemy 2.x**로 표준화(로컬 SQLite와 운영 MySQL 모두 동일 코드).
* **원문 보관(`raw_fetches`)**과 **셀렉터 사전검증**을 공식 절차로 편입.
* **검증 규칙**(불변식/교차합)을 명문화하고, 실패 시 롤백 전략 권장.
* 문서 구조를 “**핵심(정규시즌)** / **보조(기타 탭)**”로 통합(기존 합의 계승). 
