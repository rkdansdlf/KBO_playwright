# 수집 파이프라인 · 품질 게이트 현재 상태 지도

> **기준 커밋**: `4008a86f` (2026-11-19) · **작성**: 2026-11-19
> **갱신 규칙**: 아래 계약이 바뀌면 **같은 PR에서 이 문서도 갱신**한다.
> **문서 위계**: `AGENTS.md` = 정책·명령어·시간순 "왜"(Anchored Summary), `Docs/runbooks/` = 운영 절차, 이 문서 = **현재 코드가 무엇을 하는가**에 대한 코드 레벨 지도.
> 라인 번호는 기준 커밋 스냅샷이므로, 이동했으면 심볼 이름으로 다시 찾는다.

---

## 0. 한 장 요약

| 질문 | 답 | 근거 |
|---|---|---|
| 언제 시작? | 00:00 KST, 전날 경기 | `src/scheduler/jobs/daily.py:378`, `_previous_day_kst()` (`src/scheduler/jobs/live.py:59`) |
| 누가 오케스트레이션? | Master DAG 6단계 (기본) → ingestion이 `run_daily_update` 17단계 | `src/orchestration/master.py:266`, `src/cli/pipelines/run_daily_update.py:1629` |
| 어디서 실패가 기록? | `crawl_execution_runs` 원장 + `crawl_dead_letters` + notification incident 원장 | `src/services/crawl_run_ledger.py`, `src/models/crawl_dead_letter.py` |
| 나쁜 데이터는 어디로? | 저장 직전 **quarantine** (DB에 안 씀) | `src/repositories/game_save.py:943` |
| 품질 검증은 몇 겹? | L1 실시간 → L2 통계 7종 → L3 SQL 불변식 20종 + 기준선 대조 | `game_save.py:943`, `quality_gate.py:1271`, `data_quality_regression_pack.py:622` |
| 취소 상태인데 경기가 끝났다면? | **릴레이 종료 마커가 상태를 이긴다** | `game_collection_service.py:416`, `game_helpers.py:816` |
| 정착된 DLQ는? | 재시도하지 않음. `requeue`만 되돌아오는 문 | `crawl_dead_letter.py:47`, `crawl_dead_letter_state.py:12` |

---

## 1. 실행 진입 경로

```
Windows APScheduler 00:00 KST   crawl_daily_games        [DAILY_LOCK = ForceProcessLock]
  └─ src/scheduler/jobs/daily.py:378
     ├─ DAILY_USE_DAG_ORCHESTRATOR=1 (기본)
     │   └─ MasterWorkflowOrchestrator.build_daily_sync_workflow()
     │      src/orchestration/master.py:266
     │      ingestion → processing → analytics → quality_gate → cloud_sync → notification
     │      ingestion = run_daily_update_main(--date 어제, --seed-tomorrow-preview, acquire_lock=False)
     └─ (구 경로) _run_legacy_daily_update
```

- `acquire_lock=False`: 스케줄러가 이미 `DAILY_LOCK`을 쥐고 있으므로 내부 자체 락을 생략(중첩 락 회피).
- DAG의 각 stage는 실패를 격리하고, `quality_gate` 실패는 `crawl_daily_games`를 FAILURE가 아니라 **WARNING**으로 끝낸다(`daily.py:438~456`). 이유: 품질 게이트 하나가 의존성 체인 전체를 멈추게 하면 데이터 갱신 자체가 중단된다.
- `quality:daily` 인시던트는 `_publish_quality_incident`(`daily.py:231`)가 발행한다. SKIPPED(상류 실패)와 FAILED(게이트 실행 실패)를 구분하고, PASS여도 기준선이 없으면 "기준선 미기록" 경고를 낸다.

### `run_daily_update` 17단계 (`src/cli/pipelines/run_daily_update.py:1629`)

| Step | 내용 | Step | 내용 |
|---|---|---|---|
| 0 | auto-healer | 6.5 | 선발투수 백필 + 시즌 통계 감사 |
| 1 | 일정 크롤 | 7 | 로스터/선수 이동 |
| 2 | **상세(박스스코어) 크롤** + 복구 라운드(최대 3회) | 7.5 | P0 비경기(이벤트/티켓) |
| 3 | 상태 새로고침(전이 요약) | 8 | 파생: standings → matchups → rankings → sabermetrics |
| 4 | 릴레이 복구 | 10.7 | 프로필 보강 + 통계 감사 |
| 4.5 | 사전 릴레이 복구(최근 30일) | 12 | 콘텐츠 히스토리 |
| 5 | 리뷰/WPA·하이라이트·스토리 | 14 | 익일 프리뷰 |
| 6 | 시즌 타격/투구 크롤 (+6.1 팀시즌, 6.2 수비/주루, 6.3 팀수비) | | |

**모든 스텝이 `allow_failure=True`** — 실패해도 파이프라인은 끝까지 돌고, 결과는 `logs/daily_update_summary/`와 알림으로 남는다.

> **중복 계산 해소**: step 8이 standings/matchups/rankings/sabermetrics를 계산하면 그 결과가 `derived_refresh`로 반환되고, DAG의 `processing`/`analytics`는 이미 끝난 standings/rankings를 다시 계산하지 않는다(`master.py`의 `derived_refresh_completed`). step 8이 실패해 목록이 비면 DAG가 폴백으로 직접 계산한다 — 모든 스텝이 `allow_failure=True`이므로 이 2차 경로가 없으면 시즌 테이블이 갱신되지 않은 채 남을 수 있다. 계약 테스트: `tests/orchestration/test_master_daily_pipeline.py::test_derived_stats_are_not_recomputed_when_ingestion_did_them` 및 `...::test_missing_derived_refresh_keeps_the_dag_fallback`.

---

## 2. 상세 수집 (game detail)

### 2.1 대상 선정 — 3개 예외 규칙

`load_game_targets_from_db()` (`src/services/game_collection_service.py:416`)

```python
or_(status IS NULL, status NOT IN (CANCELLED, POSTPONED), finished_relay)
or_(~settled_dead_letter, finished_relay)
```

| 규칙 | 의미 | 근거 사건 |
|---|---|---|
| `finished_relay` 우선 | PBP에 종료 마커가 있으면 상태가 CANCELLED여도 수집 대상 | `20261026HTSS0` — 상세 실패 → 취소 → 취소라서 재시도 안 함이라는 순환 |
| `DETAIL_IMPOSSIBLE_STATUSES` 제외 | CANCELLED/POSTPONED는 박스스코어가 없어 실패하면 DLQ가 쌓임 | 2026-10-19 `FETCH_HTTP_ERROR` 4건 |
| `SETTLED_DLQ_STATUSES` 제외 | `exhausted`/`ignored`는 이미 답난 질문 | 2024 프리미어12 4경기가 하루 뒤 다시 5회 재시도 |

**리스트 입력용 동일 규칙**(11-09 추가):
- `settled_game_ids()` `:319` — 정착된 letter의 id
- `relay_finished_game_ids()` `:357` — 종료 마커가 있는 id
- `settled_unless_relay_finished()` `:388` — "정착 + 릴레이 미종료" = 제외 대상
- 소비자: `run_daily_update.py:444,1355`(heal 타깃), `src/cli/backfill/auto_healer.py:372`(이상 행 타깃)

### 2.2 "이미 수집됨" 판정

`inspect_existing_game_data()` (`:523`)
- `has_detail = 타격(양팀 완비) AND 투구(양팀 완비)` — 한쪽만 있으면 재수집
- `has_relay = game_events OR game_play_by_play 행 존재`

### 2.3 배치·원장·typed outcome

- `crawl_and_save_game_details()` `:557` → `_collect_detail_phase()` `:636`
- 배치 크기 기본 **20**, 동시성 기본 **3**(`KBO_GAME_DETAIL_CONCURRENCY`)
- 배치마다 원장 run을 **먼저 열고**(fetch 시간 포함) 저장 후 닫아서 저장 실패도 같은 run에 기록
- `_crawl_detail_batch()` `:708`: 크롤러가 `crawl_game_attempts`를 제공하면 **typed outcome** 사용. 구 크롤러/테스트 더블은 `crawl_games`(실패 필터링) 경로 유지. `lightweight` 인자도 시그니처에 있을 때만 전달.

### 2.4 크롤러: Naver 1차 + KBO 2차 (`src/crawlers/game_detail_crawler.py`)

| 경로 | URL / 방법 | 비고 |
|---|---|---|
| 1차 | `NAVER_RECORD_API_URL` (`:64`) `api-gw.sports.naver.com/schedule/games/{naver_id}/record` | KBO ID → Naver gameId 계산은 `{kbo_id}{연도}` |
| ID 재해석 | `_resolve_naver_game_id()` `:915` | 포스트시즌은 prefix가 시리즈 코드(3333/4444/7777) → 404. schedule API를 날짜 ±3일(`SCHEDULE_RESOLUTION_WINDOW_DAYS`) 스캔, 팀코드(양방향) + MMDD 매칭 |
| 2차 | KBO GameCenter (Playwright) | `_kbo_fallback_allowed()` 게이트; `_sources_for()`는 **페이로드를 만든 쪽**을 소스로 기록 |

---

## 3. 저장 경로 + 실시간 게이트

`save_game_detail()` (`src/repositories/game_save.py:996`) 순서:

1. game_id 정규화 + alias 기록
2. **`_execute_realtime_quality_gate()`** (`:943`)
   - `validate_game_detail_comprehensive()` (구조 + 타격/투구 stat 룰)
   - blocking error → `QuarantineService`에 격리하고 **저장 안 함**
   - `DISABLE_REALTIME_GATE=1`로 비활성화 가능
3. game 행 upsert → 코어 필드 → 상태/승리팀 → 선발투수 → 시즌 id → 자식 데이터
4. 상태 도출 `_derive_game_status()` (`src/repositories/game_helpers.py:816`)
   - 점수 양쪽 + (타격 | 투구 | **finished relay** | 과거+이닝점수) → COMPLETED/DRAW
   - 미래 날짜 → SCHEDULED
   - 진행 중 `_derive_in_progress_status()` (`:849`): `game_date == today`에서만 저장된 LIVE 상태 보존. 상세 증거 없으면 CANCELLED, 그 외 `UNRESOLVED_MISSING`

> **주의**: 자정을 넘기면 `game_date == today` 보호가 사라진다(AGENTS.md 2026-11-02 항목). 00:00 finalize는 이 경계에 있다.

### 릴레이 lifecycle

`save_relay_data()` → `_apply_relay_lifecycle_state()` (`src/repositories/game_relay.py:776`)
- `game_state.validate_transition()` (`src/utils/game_state.py:44`)으로 검증, 불법 전이는 경고 후 무시
- 상태: before / running / delayed / suspended / result_pending_stabilization / final / cancelled
- `running → final`은 불허, 정본 경로는 `running → result_pending_stabilization → final`

---

## 4. 품질 게이트 3계층

### L1 — 저장 시점 (quarantine)

`src/validators/game_data_validator.py` — `validate_game_detail_comprehensive()`
- STRUCT-001/002, batting/pitching stat 룰
- blocking error = 저장 거부 + 격리행 생성(나중에 원인 분석용)

### L2 — 시즌 통계 게이트 (`src/validators/quality_gate.py:1271`)

7종: `batting` · `pitching` · `pa_formula` · `team_batting` · `team_pitching` · `futures_batting` · `futures_pitching`

| 상수 | 값 | 의미 |
|---|---|---|
| `BATTING_PA_ABSOLUTE_TOLERANCE` | 2 | PA 절대 허용오차 |
| `BATTING_PA_RELATIVE_TOLERANCE` | 0.005 | PA 상대 허용오차 |
| `TEAM_STAT_ABSOLUTE_TOLERANCE` | 5 | 팀↔선수합 허용오차 |
| `OFFICIAL_SOURCE_SEMANTICS_EXEMPT_FIELDS` | earned_runs, wins, losses, stolen_bases, caught_stealing | 공식 표 집계 의미론 차이로 면제 |
| `PLAYER_SEASON_SOURCE_PRIORITY` | OFFICIAL_ARCHIVE > CRAWLER > … > FALLBACK_* | 중복 행 중 정본 선택 |

`quality_gate_check` exit code: ok → 0, 아니면 1.

### L3 — SQL 불변식 회귀 팩 (`src/validators/data_quality_regression_pack.py`)

20개 체크:
- PA 5종: `game_batting_pa_formula`, `player_season_batting_{pa_formula,unaccounted_pa,unreconciled_pa,missing_pa}`
- NULL player_id 3종: game_batting / game_pitching / game_lineups
- 불가능값·범위: hits≤AB, ER≤RA, AVG 범위, ERA 범위, 시즌 hits≤AB
- 경기 정합: score-missing-with-innings, score-disagrees-with-innings, final-too-few-innings, cancelled-with-scores, **cancelled-with-finished-relay**, result-without-body, duplicate-identifiers

`_SqlCheck.informational`(`:622`) = 위반을 `warn`로만 보고:
- `game_cancelled_with_finished_relay` — 과거 5건 등 역사 정책이 쓰기를 거부 → fail이면 영구 red
- `game_result_without_body` — 소스 한계(1982~2000 나무위키는 결과만 제공)이므로 회귀 아님

**기준선은 DB에 저장**(`pa_violation_baseline` / `pa_unrecoverable` 테이블). 파일이 아닌 이유는 스케줄러가 DB 호스트와 다른 머신에서 돌기 때문. `--write-baseline`으로 현재 실패 행을 기록하면 이후 실행은 **신규 손상**만 별도 보고한다. `QualityHub`(`src/services/quality_hub.py`)이 L2+L3+freshness를 묶어 `run_full_audit()`를 만든다.

---

## 5. 날짜별 무결성 검사 (`src/cli/reports/data_integrity_checker.py:1082`)

`CHECKS` 13개 + `check_season_stat_team_code` 1개 = **14개**:
games_exist, game_status_populated, all_terminal_status, scores_populated, child_stats_exist, no_null_player_ids, winning_team_consistency, duplicate_games, futures_daily_integrity, pa_formula_integrity, game_season_id, season_classification, boxscore_totals_consistency, season_stat_team_code

- 개별 검사가 예외를 던지면 그 검사는 실패로 기록(조용한 통과 없음)
- 01:45 KST `data_integrity_check` 잡으로 야간 체인에 포함

---

## 6. DLQ · 재시도 · 인시던트

### 상태 기계 (`src/services/crawl_dead_letter_state.py:12`)

```
pending ──► retrying ──► resolved
   │          ├────────► pending
   │          └────────► exhausted ──► ignored
   └─────────► ignored
resolved / ignored = terminal
```

- `RETRY_SCHEDULE = (60, 300, 900, 3600)`, `DEFAULT_MAX_RETRIES = 5`
- `REQUEUEABLE = {ignored, exhausted}` (운영자forced 복귀, `retry_count` 보존)
- `IGNORABLE`은 전이표에서 **유도** — CLI와 서비스의 규칙 드리프트 방지
- `SETTLED_DLQ_STATUSES = (exhausted, ignored)` (`src/models/crawl_dead_letter.py:47`)

### 정착 후 새 실패 흡수 (11-09 추가)

`CrawlDeadLetterRepository.find_open_for_target()` (`src/repositories/crawl_dead_letter_repository.py:153`)
- 유니크 키에 `original_run_id`가 있어, 계속 실패하는 대상이 새 run id로 `pending` 행을 무한 생성 → 재시도 큐 재보충 + `MAINTENANCE_LOCK` 점유
- `status != resolved`인 기존 letter가 있으면 새 실패를 그 행에 흡수(`crawl_dead_letter_service.py:108`)
- `requeue`는 정착 행을 `pending`으로 되돌려 다시 소유권을 갖게 함

### 실패 분류 (`src/crawlers/failure_taxonomy.py:40`)

`FETCH_{TIMEOUT,HTTP_ERROR,NOT_FOUND,BLOCKED,RATE_LIMITED}` · `PARSE_{SELECTOR_MISSING,INVALID_FORMAT,EMPTY}` · `VALIDATION_{SCHEMA,QUALITY}` · `PERSIST_{CONSTRAINT,CONNECTION,TIMEOUT}` · `REPLAY_{INTERRUPTED,RUN_MISSING}` · `RUN_INTERRUPTED` · `SOURCE_PARTIAL` · `UNKNOWN`

- **404 → `FETCH_NOT_FOUND`(non-retryable)**: 없는 자원을 5번 재확인하지 않음

### 인시던트

- `apply_incidents()` (`src/notifications/bridge.py`)가 `quality:daily` 등 키로 원장을 갱신
- `alert_warning` / `alert_success` 짝이 필수(AST 계약 테스트가 강제)

---

## 7. 스케줄러 배선 (`src/scheduler/registry.py`)

| 시각(KST) | 잡 | 락 |
|---|---|---|
| 00:00 | `crawl_daily_games` | DAILY_LOCK |
| 00:30 standings → 00:45 team defense → 01:00 rankings → 01:15 auto_heal → 01:30 heal_pbp → 01:45 integrity | 야간 체인 | MAINTENANCE_LOCK |
| 02:00 RAG publish → 02:10 source_coverage → 02:30 rag_vector_backfill → 02:40 sparse_terms_catchup | RAG 체인 | MAINTENANCE_LOCK |
| 06:05~06:45 | rag_audit / coverage / identity / schema_drift / snapshot_drift | MAINTENANCE_LOCK |
| 10분마다 | `heartbeat_ping` (`:197`, 외부 dead-man's switch) | 없음(의도적) |
| 15분마다 | `db_reachability_check` | 없음 |

- **간격은 계약**: 티어 락은 60초 안에 못 잡으면 스킵(큐잉 아님) → 겹친 잡은 밀리는 게 아니라 사라진다.
- `_scheduled_job()` (`registry.py:382`)이 등록 시 모든 잡에 `CrawlRunOrigin.SCHEDULER`를 태깅. innermost claim이 이기므로 데일리 파이프라인 내부는 `daily_update`로 남는다.
- `heartbeat_ping_job`은 DB fail-fast 가드를 **일부러 붙이지 않는다** — DB가 죽었을 때 하트비트까지 멈추면 정상 호스트가 죽은 것으로 오인된다.

### 운영 컨텍스트 (문서 기준, 실시간 상태 미검증)

`AGENTS.md` 2026-11-15 항목: 11-07부터 Windows DB 호스트 정지(≈8일), 감시 스택이 피감시자와 같은 호스트에 있어 탐지 공백 → 호스트 밖 `db_availability_watchdog.yml`(30분, **전이만 알림**, 상태는 Actions 캐시 2종) 신설. 첫 버전이 자체 고장 경로에 상태가 없어 약 300건 스팸을 보낸 사고 포함. 라이브 상태는 `Docs/runbooks/WINDOWS_DB_HOST_RECOVERY.md` §17 참조.

---

## 8. 문서 위계 / 갱신 체크리스트

이 문서는 **현재 코드 지도**다. 바뀌면 다음을 함께 본다:

1. 계약 자체가 바뀌면 → 이 문서 + 회귀 테스트
2. "왜 그랬는가"가 바뀌면 → `AGENTS.md` Anchored Summary에 시간순 항목 추가
3. 운영 절차가 바뀌면 → `Docs/runbooks/` 해당 문서
4. 상태/사건 기록은 이 문서에 쓰지 않는다(여기는 현재 상태만)

**검증 명령** (기준): `./venv/bin/python -m pytest tests/ -q -m "not integration and not slow and not oci" -n 2` + `ruff check src/ tests/ scripts/`. 터치 영역에 integration 마커가 있으면 `-m integration`도 함께 돌린다(AGENTS.md 2026-11-15 교훈).

**기준 커밋 실측 (2026-11-19)**: 비통합 **15,643 passed / 20 skipped** (95.69s), 통합 **242 passed / 6 skipped** (13.60s), `ruff check src/ tests/ scripts/` 0 errors, `ruff format --check` 2,305 files clean. AGENTS.md 11-15 기록의 비통합 15,648과 5건 차이는 그 뒤 커밋(DLQ 정착 흡수·재시도 배치 제한 등) 반영분.
