# BH0/BH1/BH2 — 크롤러 신뢰성 계약 목록화 및 경계 공격

> 상태: BH0·BH1·BH2·BH9(규칙 부분)·BH11 완료. BH3~BH8, BH10, BH12~BH14 미시작. BUG-011 수정 완료(2026-10-07, 운영 빈도 미측정). BUG-001 탐지 규칙 추가(2026-10-07); 임계값 실측 완료(2026-10-09) — `partial` 0건이라 보정 불가, 규칙은 '미검증' 상태. BH9: 24h 유지·미보정. **BUG-002는 2026-10-09에 종결**(c387f8b3: `dlq:` 인시던트 3키 + heartbeat 2종 + 규칙 2종 추가). EXHAUSTED 신규 발생률 감시는 실측 후 설계 보류로 남음.
> 범위: 공유 인프라 6계층(transport → persist → ledger → DLQ → replay → incident/notification/metrics).
> 전제: 전부 로컬 SQLite. 운영 DB 무접촉.

## 1. 계층 경계와 그 경계가 지켜야 하는 계약

| # | 경계 | 계약 |
|---|---|---|
| BOUND-01 | HTTP → `CrawlResult` | 실패는 `CrawlResult`로 표현된다. `None`으로 붕괴하지 않는다. |
| BOUND-02 | result → ledger | 실행은 반드시 원장 행을 남긴다. |
| BOUND-03 | ledger → DLQ | 실패한 단위는 DLQ 레터가 된다. |
| BOUND-04 | DLQ → replay | RESOLVED는 실제 성공에서만 나온다. |
| BOUND-05 | DLQ/replay → incident | (현재 계약 없음 — BUG-002 참조) |
| BOUND-06 | ledger → metrics | 메트릭은 원장의 투영이다. 역방향은 없다. |

## 2. 상태별 계약 (BH0 확정)

사용자 확정 방향을 그대로 고정한다. **PARTIAL은 usable completion이지 full success가 아니다.**

```
SUCCESS   → usable payload 존재, records_failed == 0
          → full-success freshness 갱신 가능

PARTIAL   → usable payload 존재
          → error_code / failure evidence 존재
          → full success로 간주하지 않음
          → crawler liveness/freshness는 갱신 가능

FAILED    → 성공 payload로 소비 불가
          → success freshness 갱신 불가
```

### BH0에서 실측한 현재 구현 상태

`CrawlRunService`/`CrawlExecutionRepository`는 **의도적으로 관대**하다. 강제하는 곳이 없다:

- `mark_success(run)`에 `records_written >= 1` 가드 없음 → `written=0, failed=0`인 SUCCESS 허용 (probe 통과)
- `mark_partial(run)`에 `error_code` 필수 제약 없음 → 근거 없는 PARTIAL 허용 (probe 통과)
- `mark_failed(run, ..., records_written=99)` 허용 → 실패 실행이 쓰기 수치를 가질 수 있음 (probe 통과)

이것은 **버그로 판정하지 않는다.** 레이어는 "크롤러가 자기 결과를 정직하게 보고한다"를 가정하고 있고, 실제 강제 지점은 각 크롤러의 `record_dead_letters` 경로다. 다만 이 가정은 검증되지 않은 채 전 계층에 전파되어 있으므로 BH2에서 크롤러별 확인 대상으로 남긴다.

## 3. BH0에서 확정된 버그

### BUG-001 — 지속 PARTIAL을 정상과 구별할 수 없는 관측 공백

- **영역**: metrics / alert rules
- **발견 방식**: 계약 위반 탐지 (INV-METRIC-02 반례)
- **조건**: 크롤러가 `success` 대신 `partial`을 계속 기록
- **기대**: 탐지 가능한 degradation 신호
- **실제**: 없음. 5개 규칙 중 어느 것도 `status="partial"`을 읽지 않는다.

확인된 사실:
1. `SUCCESS_STATUSES = frozenset({"success", "partial"})` (`src/monitoring/crawler_metrics.py:108`) — partial이 `kbo_crawl_last_success_timestamp`를 갱신한다.
2. `kbo_crawl_runs_total{status="partial"}` 시계열은 **존재한다** (INV-01은 통과 — SUCCESS와 관측상 구별된다).
3. 그 시계열을 읽는 **규칙이 하나도 없다** (INV-02 실패).
4. `kbo_crawl_failures_total`은 partial에서 증가하지 않는다 (INV-03 통과 — 단일 partial이 outage이 되지는 않는다).

따라서 정확히 이 상태가 된다:

```
PARTIAL PARTIAL PARTIAL ... (계속)
AND KboCrawlerNoRecentSuccess = 정상   (last_success가 갱신되므로)
AND KboCrawlerWriteDrop       = 정상   (records_written가 정상이라면)
AND degradation alert         = 없음   (규칙이 partial을 읽지 않음)
```

- **영향**: 소스 3개 중 1개가 조용히 실패하는 상황이 장기간 감지되지 않는다. "소스 1개 중 1개 실패"는 `records_written`가 정상이라 WriteDrop도 잡지 못한다 — 이것이 가장 위험한 형태다.
- **심각도**: P1. 데이터를 오염시키지는 않지만 **데이터가 덜 쌓인 상태를 정상으로 보고**한다. hunt 정책상 즉시 수정하지 않고 보고만 한다.
- **권장 형태** (BH9 실측 후 확정, 운영 critical 의미는 변경하지 않음):

```
kbo_crawl_last_success_timestamp  = SUCCESS만 갱신
kbo_crawl_last_usable_timestamp   = SUCCESS + PARTIAL  (신규)
kbo_crawl_partial_runs_total      = PARTIAL 누적      (신규)
KboCrawlerNoRecentUsableRun       = crawler 정지/완전 실패
KboCrawlerPartialFailures         = 지속 PARTIAL 탐지 (신규, warning)
```

- **회귀 테스트**: `INV-METRIC-02` — "sustained PARTIAL은 정상으로 무기한 취급되지 않아야 한다"
- **Mutation 증명**: 미수행 (mutation은 이번 hunt 범위 밖 — BH12 제외 결정)
- **수정 (2026-10-07)**: `KboCrawlerSustainedPartial`(warning)을 `alert_rules_crawler.yml`에 추가했다 — `kbo_crawl_runs_total{status="partial"}`가 24시간 창에서 증가했는데 같은 창에 `success` 증가가 없으면 발동한다. `last_success` 의미(partial 포함)와 `SUCCESS_STATUSES`는 **의도적으로 그대로** 두었다: 위 "권장 형태"의 `last_success`/`last_usable` 분리는 운영 critical의 의미를 바꾸므로 보류하고, 부분 성공 탐지는 기존 시리즈를 읽는 별도 warning으로 해결했다. 임계값 24h는 일일 파이프라인 주기(03:00~06:45 KST)에서 도출했으며 운영 데이터로 보정되지 않았다.
- **검증 (2026-10-07)**: `INV-METRIC-02`가 `xfail(strict)`에서 활성 계약으로 전환됐고(1 passed + 1 skip), 발동·조용은 `monitoring/prometheus/tests/crawler_alert_partial_test.yml`가 promtool로 고정한다 — 연속 partial 발동(성공 시계열 고정 1종 + 성공 시계열 부재 1종), `for:` 미경과 조용, mixed/healthy 조용. Mutation: `unless` 제거·창 축소(`[1m]`)·규칙 삭제가 각각 픽스처/계약 3건을 실패시킨다.

### BUG-002 — DLQ에 알림 규칙도 인시던트 원장도 없다 (BH9에서 규칙 2종 추가)

- **영역**: metrics / alert rules / incident
- **발견 방식**: 계층 공백 탐지
- **상태**: **규칙 부분 해결(P1 → mitigated). 인시던트 부분은 미해결.**

#### 실측된 사실 (수정 전)

1. `src/utils/metrics.py:105-147`에 DLQ 메트릭 9종이 실존한다.
2. `monitoring/prometheus/alert_rules*.yml` 어디에도 `kbo_crawl_dlq_*`를 읽는 규칙이 **0개**였다.
3. DLQ 잡은 `alert_warning()`으로만 알린다. `apply_incidents`를 호출하지 않으므로 **인시던트 원장에 남지 않는다**. `Docs/runbooks/NOTIFICATIONS.md` §1.4가 이미 이 공백을 명시하고 있다.
4. **두 계약 테스트 모두 이 메트릭을 보지 못했다 — 사각지기가 구조적이었다.**
   - `test_crawler_alert_rules_contract.py`는 `crawler_metrics` 모듈만 스캔.
   - `test_notification_alert_rules_contract.py`는 `utils.metrics`를 **스캔하지만** `kbo_notification` 접두어로 필터링.
   - DLQ 계열은 `utils.metrics`에 있으면서 두 필터를 모두 벗어났다. 실측 교집합 0건.

#### 수정 내용 (BH9)

**규칙 2종 추가** — `alert_rules_crawler.yml`:

| 규칙 | 조건 | 심각도 | 임계값 근거 |
|---|---|---|---|
| `KboDlqRecoveryStalled` | `kbo_crawl_dlq_stale_retrying_letters > 0` | critical | **논쟁 없음.** 정체된 레터 1건이 이미 실패다. 정상적인 정체량은 존재하지 않는다. |
| `KboDlqBacklogAgeHigh` | `kbo_crawl_dlq_oldest_due_age_seconds > 86400` | warning | **재시도 스케줄에서 도출**(60/300/900/3600s, 5회 예산). 운영 데이터 아님. |

`stale_retrying > 0`을 택한 이유: 다른 규칙은 전부 "얼마나 많이"를 보지만 이건 **"얼마나 있는가"**가 맞습니다. 빈도가 기준이 필요한데, 그 기준이 조사 대상 자체입니다.

#### `KboDlqBacklogAgeHigh`가 보는 범위와 안 보는 범위 (2026-10-09 정정)

이 규칙의 유지 근거를 설명하면서 **"EXHAUSTED 적체를 함께 감시한다"는 논리를 잘못 세운 것이 확인됐습니다.** 실제 쿼리는 `crawl_dead_letter_repository.py:281-285`이고, `status == PENDING`만 조회합니다.

| DLQ 상태 | `oldest_due_age_seconds` |
|---|---|
| `pending`, 재시도 시각 전 | 제외 |
| `pending`, 재시도 시각 경과 | **포함** |
| `retrying`, 실행 중 | 제외 |
| `retrying`, 정체 | 제외 — `KboDlqRecoveryStalled` 담당 |
| `exhausted`, 재시도 소진 | **제외** |
| `ignored` / `resolved` | 제외 |

즉 이 규칙은 **PENDING 적체 전용**입니다. 예산 소진으로 `EXHAUSTED`에 닫힌 레터는 어떤 `KboDlq*` 규칙도 세지 않습니다.

관측 수단 자체는 있습니다 — `crawl_dead_letter_stats.py`가 `exhausted`를 집계하고 `metrics.py`가 `kbo_crawl_dlq_letters{status="exhausted"}`로 내보냅니다. **경보가 없을 뿐 값은 보인다.** 그래서 EXHAUSTED 감시는 별도 규칙 설계 과제로 남깁니다.

과거 EXHAUSTED 1건이 영구히 같은 경보를 울리는 구조는 피해야 합니다. 누적 건수보다 **신규 발생률**이 알맞고, 그것은 대개 DB를 못 읽은 구간과 정상 표본을 구분해야만 얻을 수 있습니다. 운영 실측 전에는 설계하지 않습니다.

**사각지기 제거** — 계약 테스트가 `src.utils.metrics`도 스캔하도록 확장. 이 작업이 즉시 **진짜 미참조 메트릭 6종을 드러냈습니다**(`_due_letters`, `_letters`, `_failures_total`, `_retry_attempts_total`, `_retry_outcomes_total`, `_recovery_actions_total`). 원래도 조용했지만 스캔이 도달하지 않아 아무도 몰랐던 것입니다. 각각 사유를 명시한 예외로 등록.

**발동 증명** — promtool 픽스처 2종 추가(`crawler_alert_dlq_test.yml` 30s interval / `crawler_alert_dlq_backlog_test.yml` 30m interval). promtool은 per-test `evaluation_interval`을 받지 않으므로 `for` 길이가 다른 규칙은 파일을 분리해야 합니다(기존 freshness/write-drop도 같은 이유로 분리됨).

**mutation 증명**: 게이지 이름 오타(`..._letters` → `..._letter`)를 주입하니 계약 3건이 동시에 실패했습니다 — 메트릭 존재 검사, orphan 검사, **발동 픽스처**. 세 번째가 없었으면 이 규칙은 조용히 죽은 채로 갈 수 있었습니다.

#### 남은 공백 (정직하게 기록)

- **인시던트 원장은 여전히 미사용.** DLQ 상태는 이제 Prometheus로 잡히지만 `kbo incidents list`에는 여전히 안 나옵니다. 두 경로가 병존합니다.
- **24h 임계값은 미보정.** 운영 실측 데이터가 없어 스케줄에서 도출했습니다. 규칙 주석과 `DATA_RELIABILITY.md` §3.2b에 "재측정 후 보정하라"고 명시해 두었습니다.
- **나머지 6종은 여전히 무음.** 사유를 명시한 예외로 등록해 "의도적"으로 만들었지만, 경보가 필요해지면 그때 규칙을 추가해야 합니다.
- **EXHAUSTED 누적은 무음.** 위 표대로 `KboDlq*` 어느 규칙도 세지 않습니다. 지표 값은 존재하므로 규칙 추가 자체는 가능하지만, 누적 건수는 과거 1건을 영구히 반복 보고하니 **신규 발생률**로 설계해야 합니다.

#### BH9 최종 결정 (2026-10-09, 사용자 승인)

| 항목 | 판단 |
|---|---|
| `KboDlqBacklogAgeHigh` 유지 | **유지** |
| 86400초(24h) 임계값 | **변경하지 않음** |
| 경보 활성화 | **유지** (warning) |
| 임계값 적정성 검증 | **미완료** — 운영 실측 대기 |
| BH9 처리 상태 | 구조적 안전장치 완료, 운영 보정 보류 |
| BUG-002 | 측정 근거 확보 전까지 미종결 |

유지 근거는 "24시간이 최적이라는 증거"가 아니라 **"재시도 스케줄(60/300/900/3600s)에서 하루가 지난 것은 조사할 가치가 있다는 구조적 추론"**입니다. 반대로 운영 데이터로 보정된 적정값이라는 확인도 없습니다. 따라서 "유지했다"와 "검증했다"를 문서에서 구분해 남깁니다.

**오탐률을 낮다고 단정하지 않습니다.** 현재 근거는 재시도 스케줄이지 관측이 아닙니다.

**후속 (운영 DB 복구 후):** 7일 측정 — 30분 단위 PENDING·RETRYING·EXHAUSTED 건수, oldest due age, 실제 재시도 성공률, 스위프 실패. DB 연결이 끊긴 구간은 정상 표본에서 제외. 24h 초과 사례가 실제 조치 대상이었는지 정상 처리된 지연이었는지 구분해 재판정합니다. 유효 표본이 거의 없으면 보정 기간을 연장합니다.

#### 실측 실패 기록 (중요)

BH9의 원래 목표는 운영 데이터로 임계값을 확정하는 것이었으나 **데이터가 없었습니다**:

| 수단 | 결과 |
|---|---|
| 로컬 `data/kbo_dev.db` | ledger/DLQ 테이블 없음(9월 3일자, 그 계층 이전) |
| `logs/scheduler.launchd.err.log` (19MB) | DLQ 관련 1246건 — 전부 **DB 장애 기록**(psycopg2 타임아웃) |
| `logs/daily_update_summary/` | ledger 계층 이전 스키마 |
| `data/crawl_evidence/` | 380개 파일, 9월 18일 이후 공백 |
| 운영 PG (읽기 전용 소켓 5초) | UNREACHABLE |

추측으로 임계값을 정하지 않고, 논쟁이 없는 `stale_retrying > 0`만 먼저 적용했습니다. 나머지는 DB 복구 후 재측정 대상으로 명시했습니다.

> 조사 중 오진 2건을 바로잡았습니다. ① `processed` summary 로그가 0건인 것을 버그로 단락했으나, `@_with_db_fail_fast_guard`가 조용히 반환하기 때문이었습니다(설계대로 동작). ② `86400 > 86400`이 거짓이라 정확히 24시간인 레터가 안 걸린 것을 promtool이 알려줬습니다 — 경계값 테스트로 고정했습니다.

### BUG-012 — 사각지대는 DLQ만의 문제가 아니었다 (분류 완료)

- **영역**: metrics / alert rules
- **발견 방식**: BUG-002의 "같은 사각지대에 다른 메트릭이 더 있는가"라는 질문
- **상태**: **분류 완료 + 사각지대 폐쇄**

#### 실측

BH9는 DLQ 계열(`kbo_crawl_dlq_*`)이 두 계약의 접두어 필터를 모두 벗어난 것을 고쳤습니다. 그 수정은 `kbo_crawl` 접두어가 있었기에 가능했고, **접두어가 다른 메트릭은 여전히 벗어나 있었습니다.**

`scheduler` 계약(`test_scheduler_alert_rules_contract.py`)은 docstring에서 이렇게 위임합니다:

> metric-name existence in both directions — `test_crawler_alert_rules_contract.py` already spans `BASE_RULES`

그런데 crawler 계약의 스캔은 `kbo_crawl` 접두어로 필터링했습니다. 즉 **위임을 받는 쪽이 위임 대상이 아닌 것만 보고 있었습니다.**

실측(3개 규칙 파일 전체를 참조 소스로 스캔):

| 네임스페이스 | 미참조 | 소유 계약 |
|---|---|---|
| `kbo_crawl_*` | 10종 | crawler 계약 (기존 예외) |
| `kbo_notification_*` | 5종 | notification 계약 (기존 예외) |
| **그 외** | **4종** | **없음 — 이 버그** |

드러난 4종:

| 메트릭 | 분류 | 사유 |
|---|---|---|
| `kbo_scheduler_job_duration_seconds` | 예외 | 잡별 정상 범위가 자릿수 단위로 다름(2시간 vs 수 초). 단일 임계값 부적합, 잡별 상한은 registry의 timeout/misfire가 이미 강제 |
| `kbo_api_cache_requests_total` | 예외 | hit/miss는 성능 신호. miss는 캐시 없이 계산했다는 뜻이고 기능은 정상 — outage로 페이지하면 오진 |
| `kbo_auto_healer_recovered_total` | 예외 | 진단용 진행 카운터 |
| `kbo_auto_healer_unresolved_total` | 예외 | **이미 `auto_healer:unresolved` 인시던트가 보고**(0이면 resolve). DLQ와 다름 — 저기는 인시던트가 없어 규칙이 필요했음 |

#### 수정 내용

`test_crawler_alert_rules_contract.py`의 접두어 필터를 **소유권**으로 교체했습니다:

- `kbo_` 전체를 스캔한다.
- `OWNED_ELSEWHERE`로 notification 접두어를 **명시적으로 이관**하고, 어느 테스트가 그것을 덮는지 함께 적었다. 이관은 면제가 아니며, `test_the_handed_off_prefix_is_really_covered_elsewhere`가 그 계약이 여전히 자기 접두어를 스캔하는지 확인한다.
- 동적 collector 메트릭 2종(`kbo_db_available`, `kbo_db_ping_latency_seconds`)은 모듈 속성 스캔에 안 잡히므로 `COLLECTOR_SERIES`로 선언하고, `test_the_collector_series_are_still_yielded`가 소스와 대조한다.
- 참조 소스를 3개 규칙 파일 전체로 넓혔다.

이로써 새 메트릭이 두 접두어 필터 사이로 떨어지는 실패가 **구조적으로 불가능**해졌습니다. 네임스페이스를 새로 만들면 `kbo_` 스캔에 자동으로 걸린다.

#### mutation 증명

| 주입 | 결과 |
|---|---|
| `kbo_api_cache_requests_total` 예외 제거 | orphan 검사 실패: `assert not ['kbo_api_cache_requests_total']` |
| `db_availability.py`에서 `kbo_db_available` → `kbo_db_reachable` | `test_the_collector_series_are_still_yielded` 실패 |

두 번째가 없었으면 collector 메트릭을 목록에 넣어두고도 이름 변경을 놓쳤을 것입니다 — 스캔이 도달하지 못하는 메트릭을 목록이 가려주는 형태입니다.

#### 회귀 테스트

`tests/monitoring/test_crawler_alert_rules_contract.py` — 16 → **18 passed** (신규 2건: collector 대조, 이관 검증)

#### 남은 공백 (정직하게 기록)

- 이 계약은 이제 `kbo_` 접두어를 갖는 모든 메트릭을 덮지만, **접두어가 없는 메트릭은 덮지 않습니다.** 현재 그런 메트릭은 없습니다.
- 4종은 **분류만** 했고 규칙을 추가하지 않았습니다. `kbo_auto_healer_unresolved_total`처럼 이미 다른 경로가 알리는 것은 중복 페이지를 피한 것이고, 나머지는 사유가 유효한 동안은 무음이 맞습니다. 사유가 바뀌면(예: 캐시를 SLO로 삼으면) 그때 규칙을 추가해야 합니다.

### BUG-004 — ~~PARTIAL 실행의 error_code가 비어 있음~~ → **기각. 의도된 계약이다**

- **영역**: ledger / crawler 계층 간
- **발견 방식**: BH0 계약("PARTIAL → error_code 존재") 대조
- **최초 판정**: P2 보고. **재검토 결과 결함이 아니다.**

| 크롤러 | partial 분기 | error_code | 계약 |
|---|---|---|---|
| `award_crawler` | 864행 | `SOURCE_PARTIAL` | aggregate 실패를 명명하는 코드 |
| `food_crawler` | 197행 | 비움 | 아래 근거 |
| `parking_crawler` | 202행 | 비움 | 아래 근거 |
| `kbo_event_crawler` | 379행 | 비움 | 아래 근거 |
| `player_movement_crawler` | 202행 | 비움 | 아래 근거 |

#### 왜 결함이 아닌가

**1. 이미 거부된 설계에 대한 명시적 계약이 존재한다.**
`tests/crawlers/test_food_persist_failure_contracts.py::test_the_run_leaves_the_code_to_the_queue_when_it_is_only_partial`
은 partial 실행의 `error_code is None`을 **단언한다**. 그 테스트의 docstring이 논리를 명시한다:

> One team timed out and two wrote cleanly. Putting that code on the run would
> imply the whole run failed for that reason. The queue is where a per-team code
> belongs, and the run is where the tally belongs.

 hunt가 처음에 이 테스트를 읽고 그 계약을 **버그로 재분류**했다. 계약이 존재한다는 사실 자체가 결함이 아니라는 증거였다.

**2. 수정은 `error_message`를 오염시킨다.** BH0은 "계산상 낭비이며 진단 가능성 문제"라고 적었으나,
4개 크롤러는 이미 `run.error_message`에 무엇이 실패했는지 팀·페이지·연도 단위로 적는다
(`teams failed: [...]` / `pages failed: [...]` / `years failed: [...]`).
`error_code`는 분류이고 `error_message`는 열거다. 둘을 같은 칸에 넣으면 어느 쪽이 불명확해진다.

**3. `award_crawler`가 유일한 예외가 아니라 다른 집계다.** aggregate 실패에는
`SOURCE_PARTIAL`(retryable)이 정확한 코드이고, 단일 대상 replay에는 그 코드가
실제 원인을 대체하므로 `failed`가 된다 — 그 주석이 이미 이유를 적고 있다.
즉 "빈 error_code"가 아니라 "`SOURCE_PARTIAL` 미사용"이 선택 문제지,
`SOURCE_PARTIAL`로 통일하는 것이 올바른 수정은 아니다. **partial이 단일 원인이
없다는 사실 자체가 contract다.**

**4. DLQ는 복구 정보를 잃지 않는다.** 각 실패는 개별 DLQ 레터로 적재되므로
`failure_stage`와 `error_code`가 큐에 그대로 있다. 실행 행은 집계다.

- **재판정**: **기각 (설계 의도, 계약으로 고정됨)**
- **회귀 테스트**: 기존 — `test_food_persist_failure_contracts.py`가 partial의
  `error_code is None`을 이미 고정한다. 신규 테스트는 "이 결정을 하지 말라"를
  막는 것일 뿐이며, 계약 테스트로 충분하다.

## 2-BH1. transport/parser 경계 공격 결과

BH1의 목표는 "200 + 빈 body"와 "200 + 마크업 변경"이 실제로 구별되는지였다. 계획에서 세운 2a/2b/2c 중 **2a와 2c는 이미 방어되어 있었고, 2b(호출부 어긋남)에서만 실제 발견이 나왔다.**

### 2c (실경로 재현) — 버그 아님

`read_kbo_event_page(html: str)`는 HTML 문자열을 인자로 받아 **브라우저 없이** 픽스처를 실경로에 주입할 수 있었다. 네 가지 케이스를 `run()`까지 흘려 관찰했다:

| 케이스 | run.status | error_code | DLQ |
|---|---|---|---|
| frame 있고 이벤트 있음 | `success` | None | 0 |
| frame 있고 이벤트 0개 | `success` | None | 0 |
| frame 전부 소실 | `failed` | `PARSE_SELECTOR_MISSING` | 7건 |
| 유지보수 페이지 | `failed` | `PARSE_SELECTOR_MISSING` | 7건 |

**기대대로 동작한다.** frame 소실은 `SCHEMA_CHANGED`로 분류되고 원장에 `failed`로 남으며 페이지 단위 DLQ가 적재된다. 기존 `test_kbo_event_reliability_canary.py`가 이미 드리프트를 부분 실패·증거 보존·재시도 불가까지 검증하고 있어 새 테스트를 추가하지 않았다.

> 참고: 첫 probe에서 "frame이 있는데 이벤트 0개"가 `success`로 나와 의심했지만, 그건 `has_kbo_event_frame`이 `any(header, nav, footer)`라 `<header>` 하나만 포함해도 frame이 있다고 판정하기 때문이었다. **픽스처가 실제보다 약했던 것이지 버그가 아니었다.**

### BUG-005 — typed outcome 3개 모듈의 shape 불일치 (`kbo_event`만 4튜플)

- **영역**: crawler outcome 어휘
- **발견 방식**: 호출부 ↔ 분류 어긋남 (BH1-2b)

| 모듈 | reason 테이블 형태 |
|---|---|
| `team_history_outcome` | 3튜플 `(code, text, terminal)` |
| `player_movement_outcome` | 3튜플 |
| `kbo_event_outcome` | **4튜플** + `absence` 필드 |

`kbo_event`의 4번째 필드는 **모든 entry에서 `False`**이고 **어떤 호출부도 읽지 않는다.** `classify_page_failure`가 `code, _explanation, terminal, _absence = entry`로 언패킹하고 버린다.

문서까지 그 필드가 "구현된 기능처럼" 보입니다. 모듈 docstring은 길게 논증합니다 — "빈 안내 페이지와 읽을 수 없는 페이지는 밖에서 보면 동일하다" — 그리고 바로 그 구분 근거를 이름 붙여 두었다가 **읽지 않습니다.** 상수인 필드는 근거가 아닙니다.

정리는 한쪽만 끝난 상태입니다. 나머지 두 모듈은 이미 3튜플입니다.

- **영향**: 계산상 무해하나, **"absence 추적 중"으로 오독을 유발**한다. docstring이 길게 그 구분이 중요하다고 주장하는데 코드가 구현하지 않은 의도를 남겨 둔 것.
- **심각도**: P2 → **해결됨**.
- **수정**: 4번째 컬럼 제거, `classify_page_failure`의 언패킹도 3개로 축소.
  나머지 두 모듈과 shape가 일치한다. 구분이 없다는 뜻이 아니라, **상수 컬럼은
  근거가 아니었다**는 판단이다 — 실제 구분은 `is_terminal`과 호출자의
  `EMPTY`/`SCHEMA_CHANGED` 선택이 이미 담당한다.
- **회귀 테스트**: `tests/crawlers/test_page_outcome_agreement.py`.
  strict xfail이라 해결 시 XPASS 실패가 되도록 설계되어 있었고, 해결과 동시에
  표시를 제거했다. 좁은 `width == 3` 단언에 더해 **모든 entry의 폭이 같은지**
  도 보는데, 표 전체가 3폭이어도 어떤 entry만 다른 폭인 경우가 있기 때문이다.

### BUG-006 — `FETCH_FAILED` 상태를 아무도 생산하지 않음

- **영역**: crawler outcome 어휘
- **발견 방식**: BH1-2b

세 outcome 모듈 모두 `FETCH_FAILED`를 선언합니다. 그런데 **어떤 크롤러도 그 상태를 생성하지 않습니다.** 생산해야 할 경로가 아예 존재하지 않습니다:

```python
try:
    html, final_url = await self._fetch_html(url)
except KBO_EVENT_CRAWL_EXCEPTIONS as exc:
    ...
    self._page_failures.append((url, code.value, str(exc)))
    return          # ← 예외로 처리. FETCH_FAILED 상태로 만들지 않음
```

부수 효과로 `_REASON_FAILURES["page_fetch_failed"]`가 **호출 없는 도달 가능해 보이는 행**이 됩니다. 즉 enum이 처리하는 것처럼 보이는 경우가 실제로는 다른 분기에서 처리되고 있습니다.

- **영향**: 데이터 결함이 아니라 **어휘 함정**. 실패 자체는 taxonomy code를 단 DLQ 레터로 정상 기록된다. 비용은 enum이 크롤러가 구별할 수 있는 것을 과장한다는 점.
- **심각도**: P2 → **해결됨**.
- **수정**: 크롤러가 `_page_reads`를 갖고 예외 분기에서 `fetch_failed_read(url)`을
  기록한다. **DLQ 경로는 그대로**다 — 재시도 정책을 구동하는 코드는 예외를
  `classify_failure`로 분류한 값이지 reason 표의 값이 아니므로, 상태 기록은 그 위에
  얹히는 보고이고 대체가 아니다. `read_kbo_event_page`는 이제 최종 URL을 read에
  담는데, 7개 페이지의 read 목록이 어느 페이지인지 말하지 못하면 담는 정보가
  없기 때문이다.
- **수정 중 발견한 실제 버그**: `_page_failures`는 `run()` 안에서 초기화되지만
  크롤러 객체는 한 번의 스윕보다 오래 산다. `_page_reads`를 추가하면서 같은
  클래스의 버그가 한 줄 차이로 다시 나타날 뻔했고, 초기화도 함께 넣었다.
- **회귀 테스트**: `tests/crawlers/test_page_outcome_producers.py`.
  **기존 테스트가 놓치고 있던 것이 이번 수정의 핵심이다**: 기존은 outcome 모듈이
  상태를 *구성할 수 있는지*만 봤고, 그 테스트는 `FETCH_FAILED`가 도달 불가능한
  동안에도 통과했다. 크롤러가 그 생성자를 *호출하는지*가 별개의 질문이고 그것이
  버그였다. producer 배선을 실제로 구동해 검증하는 테스트를 추가했다.

> 이 테스트의 초안은 크롤러 소스를 `Enum.MEMBER`로 grep했더니 오탐 2건이 나왔습니다. `team_history`는 상태를 `read_team_history()` **함수 안에서** 만들기 때문입니다. 상태에 도달 가능한지는 이름이 아니라 **호출**로 확인해야 하고, 텍스트 검색으로는 답할 수 없습니다.

---

## 3-BH2. persist 경계 공격 결과

### 공격 대상 재조정

계획대로 persist 경계를 공격하려 했으나, **그곳은 이미 촘촘했다.** `food`/`parking`에 30여 건이 이미 있다:

- 팀 단위 격리 (`test_parking_reliability_canary.py::TestPersistenceFailures`)
- 실제 `IntegrityError` 주입 (`test_food_persist_failure_contracts.py::TestTheTransactionBoundaryIsReal`)
- 다른 팀 커밋 보존, 재실행 중복 없음 (`test_persist_is_idempotent`)
- 스냅샷 실패가 도메인 행을 막지 않음 (`parking canary:314`)
- `raise_on_persist_error` replay 플래그 계약

여기를 공격하면 이미 잡힌 것을 다시 잡는다. 실측으로 공격 표면을 옮겼다:

```
grep -rln "exit(137)|os._exit|commit.*crash" tests/crawlers/ tests/services/
→ test_rag_rekey_safety_gate3.py   (RAG 계층, 다른 문제)

= persist 경계의 crash 시나리오는 저장소 전체에 미커버
```

### BUG-007 — 마지막 커밋과 상태 갱신 사이의 crash 창

- **영역**: persist / snapshot
- **발견 방식**: fault injection (커밋 경계 시뮬레이션)
- **조건**: `persist_parsed_records`의 N번째(마지막) 레코드 커밋 직후, `persist_snapshot`의 `_mark_status` 호출 전에 프로세스 종료

`src/services/snapshot_persist.py:179-190`이 레코드별 독립 커밋을 한다:

```python
for record in records:
    with factory() as session:
        outcome = save_parsed(session, target_domain, [record])
        session.commit()          # ← N번
# 반환 후 persist_snapshot()가 _mark_status(... "done") 호출  ← 별도 트랜잭션
```

실측 결과:

| 관측 | 값 |
|---|---|
| crash 직후 `parse_status` | `pending` |
| crash 직후 도메인 행 | 4행 (커밋됨) |
| 같은 배치 재실행 | `saved=4`, **도메인 행 4행 그대로 — 중복 없음** |
| 이후 `persist_snapshot` 재실행 | `parse_status='done'`로 복구 |

- **데이터 결함 아님**: upsert 덕분에 재실행이 중복을 만들지 않는다. `test_persist_is_idempotent`가 이미 검증한다.
- **실제 공백**: 남는 상태는 **도메인 행은 완성됐는데 스냅샷이 `pending`인 것**이고, 이 상태를 걸러내는 곳이 없다. `_recent_snapshot_ids`는 `parse_status`로 필터하지 않고 최신 N개를 뽑으므로 이 스냅샷은 드리프트 게이트에 **들어간다**. 그리고 기준선이 없어(§3-BH2-c) 무조건 통과한다.
- **영향**: 데이터 유실/중복 없음. `pending` 스냅샷이 쌓이면 그분은 드리프트 판정을 **영원히 받지 못하는 사각지대**가 된다. 일일 요약의 `with_baseline=0`으로 집계되지만 `SNAPSHOT_DRIFT_MAX=0` 판정에는 쓰이지 않는다.
- **심각도**: P2.
- **회귀 테스트**: `tests/services/test_snapshot_persist_crash_window.py` (5건)

### BH2-c — `parsed_records` 기준선의 귀결 (의도된 설계, 문서화 공백)

- **영역**: metrics / drift gate
- **발견 방식**: 계약 위반 탐지

드리프트 게이트의 기준선은 컬럼이 아니라 `capture_metadata["parsed_records"]`이며(`_baseline_count`, `snapshot_replay.py:212`), **크롤 시점에만** 기록됩니다(`award_crawler.py:359`). `_mark_status`는 `parser_version`과 `error_message`만 씁니다.

결정적 대조 실험:

| `capture_metadata.parsed_records` | replayed | delta | drifted |
|---|---|---|---|
| 없음 | 4 | `None` | **False** |
| 4 (일치) | 4 | 0 | False |
| 7 (불일치) | 4 | −3 | **True** |
| `"many"` (비정수) | 4 | `None` | **False** |

게이트는 **기준선이 있을 때만** 작동합니다. 따라서 `kbo snapshot replay --persist`로 생성된 스냅샷은 구조적으로 기준선이 없고, 이후 어떤 파서가 나더라도 드리프트 판정을 받지 않습니다.

- **판정**: **의도된 설계다.** 재파싱 경로에서 "오늘 파서 출력"을 기준선으로 기록하면 파서가 자기 자신과 비교하게 되어 드리프트를 원리적으로 검출할 수 없다. 판단하지 않는 것이 정직한 답이다. (fail-closed로 바꾸면 비교 대상 없는 스냅샷이 게이트를 매일 잠갔을 것이다.)
- **실제 문제**: **문서화되지 않았다.** `parse_status='done'`인데 기준선 없는 스냅샷이 대량 생산된다는 사실을 코드에서 읽어내야만 알 수 있다. `parsed_records`를 "파싱 성공 시 항상 기록"으로 오해하면 게이트가 조용히 무력화된다.
- **심각도**: P2 (문서화 공백).
- **회귀 테스트**: `tests/services/test_snapshot_drift_baseline.py` (6건)

---

## 5-BH11. replay 핸들러 계약 대조

hunt 조사 당시 `REPLAY_HANDLERS`를 대조했다. 당시 목록은 10개였고, 사용자 병행 작업으로 `realtime_issue`가 추가되어 11개가 됐다. 이후 `preview`도 추가되어 현재 목록은 12개다. 이번 회귀 테스트는 `run(save=False)` 기본값을 쓰는 핸들러 경로와 BUG-010을 대상으로 하며, 별도 저장 경로를 쓰는 모든 핸들러의 저장 계약을 전수 검증한 것은 아니다.

### 확인된 사실

BUG-010 발견 당시 `_execute_schedule_replay`만 `crawl_schedule`의 `save=True`를 빠뜨렸다. `game_detail`, `relay`, `player_movement`, 그리고 이후 추가된 `preview`는 별도 저장 경로를 사용하므로 단순히 `run(save=True)` 호출 여부로 평가할 수 없다.

### BUG-010 — schedule replay가 저장하지 않고 성공을 보고 (P1, 수정 완료)

- **영역**: replay → ledger → DLQ
- **발견 방식**: 핸들러 계약 일괄 대조

발견 당시 `src/services/crawl_replay_dispatcher.py`의 호출:

```python
await crawler.crawl_schedule(year, month, run_spec=spec, record_dead_letters=False)
# save=True 누락으로 기본값 False 사용
```

`crawl_schedule(save=False)`가 기본값이므로:

| 단계 | 결과 |
|---|---|
| `_execute_schedule_replay` | 월을 **fetch** 함 |
| 저장 | **하지 않음** (`save` 미전달) |
| `run.records_written` | `0` |
| `run.status` | `success` (저장 실패가 아니므로) |
| `_outcome_from_persisted_run` | `success=True` |
| `finalize_retry` | 레터가 **`resolved`** 로 종료 |

**DLQ 레터가 다시 열리지 않고 닫힙니다.** 이 시도 자체는 retry count에 기록되지만, 성공으로 잘못 판정되어 추가 재시도 없이 남은 예산을 쓰지 않고 종료됩니다.

- **왜 조용한가**: 저장하지 않은 것이 실패 조건이 아니기 때문입니다. `save=False`는 정상적인 읽기 전용 경로이고, 원장도 이를 실패로 기록하지 않습니다. `_outcome_from_persisted_run`이 `status == success`만 봅니다.
- **모듈이 스스로 경고한 함정**: 디스패처 주석에 그대로 적혀 있습니다 — *"The crawl arguments are pinned explicitly. `save` defaults to False on most of these crawlers, so inheriting the default would mean a replay that fetched the page and stored nothing, and reported success."* **바로 그일이 이 핸들러에서 일어납니다.**
- **실측 증거**: 스텁 크롤러로 `crawl_schedule`의 실제 인자를 관찰 → `kwargs = {'run_spec': ..., 'record_dead_letters': False}`, `save` 없음.
- **영향**: 실패했던 스케줄 월(경기 일정)이 **재수집되지도 저장되지도 않은 채** 복구된 것으로 표시됩니다. 스케줄은 대부분의 후속 크롤의 입력이라(경기 ID, 날짜) 하위 영향이 큽니다.
- **mutation 증명**: `record_dead_letters=False`를 제거하는 mutation을 넣었을 때 **기존 40개 핸들러 계약 테스트는 전부 통과**했습니다(40 passed). 즉 `save`는 그 테스트도 지키지 않았고, **BUG-010은 진짜 방어 공백**이었습니다. (파일은 되돌린 뒤 확인)
- **심각도**: **P1 — 발견 당시 미저장 성공 보고 위험.**
- **수정**: 사용자가 승인해 `_execute_schedule_replay`가 `save=True`를 전달하도록 변경했다. 재수집된 월 데이터가 저장 경로에 들어가며, 기존 `run_spec` 및 `record_dead_letters=False` 인자는 유지한다.
- **회귀 테스트**: `tests/services/test_replay_handler_write_contracts.py` (**11 passed**, xfail 제거)
- **mutation 확인**: `save=True`를 다시 제거하면 실제 호출 인자를 확인하는 회귀 테스트가 실패한다.

### 부수 발견 — 파싱 불가 schedule target은 `missing` 실패로 귀결됨

| 핸들러 | 파싱 실패 시 | 계약 |
|---|---|---|
| `_replay_team_page` (food/parking) | `status="unaddressable"` | 거부 ✅ |
| `_replay_kbo_event` (URL 없음) | `status="unaddressable"` | 거부 ✅ |
| `_replay_player_movement` (연도 불가독) | `status="unaddressable"` | 거부 ✅ |
| **`_replay_schedule`** (`_month_of`) | `(None, 0)` → 크롤러 호출 없이 반환; 원장 행 없음 | `status="missing"`, `success=False` |

`_month_of`의 주석은 기본 날짜로 fallback한다고 적혀 있지만 실제 `_execute_schedule_replay`는 `year is None` 또는 월 범위 밖이면 크롤러를 부르지 않고 반환한다. 원장 행도 생성되지 않아 `_outcome_from_persisted_run`은 `status="missing"`, `success=False`를 반환한다. `finalize_retry`는 이 결과를 성공으로 처리하지 않으므로 레터가 잘못 `resolved`로 닫히지는 않는다.

실측 결과:

```
_month_of(None)      -> (None, 0)     'garbage'   -> (None, 0)
_month_of('2026-05') -> (2026, 5)     'may-2026'  -> (None, 0)
_month_of('2026-13') -> (2026, 13)    ← 13월을 "유효"로 반환 (검증은 호출부)
_year_range_of('2023-') -> None        ← 같은 축의 다른 함수는 거부
```

- **판정**: BUG-010과는 다른 동작이다. 잘못된 target은 재수집 없이 실패 처리되며, 오류 상태도 `unaddressable` 대신 `missing`으로 뭉뚱그려진다. 이로 인해 잘못된 DLQ target이 자동 재시도 예산을 쓸 수 있지만, 현재 증거만으로 운영 영향이나 심각도를 확정하지 않는다.
- **범위**: BUG-010 승인에는 포함하지 않았고, 별도 수정은 하지 않았다. `unaddressable`로 명시 거부하는 다른 핸들러와의 계약 일치 여부는 후속 검토 대상이다.

### BH11 산출물

| 파일 | 역할 |
|---|---|
| `tests/services/test_replay_handler_write_contracts.py` | BUG-010. schedule replay의 저장 인자와 run 기반 핸들러 계약 (**11 passed**) |
| `tests/services/test_replay_target_validation_contracts.py` | BH11 대상 검증. schedule 3건 + 거부 경로 6건 + DLQ 비재시도 1건 + kbo_event URL·slug 1건 + `target_type` 5건 + 미존재 경기 종결 2건, **18 passed** (BUG-011 수정 완료) |

### BH11-b. 현재 12개 replay handler의 target 계약

DLQ 생산자와 replay consumer를 대조했다. 현재 `REPLAY_HANDLERS`는 12개다. 생산자는 각 경로에서 target을 채워 넣지만 `CrawlDeadLetter.target_id` 자체는 nullable이고, dispatcher는 handler마다 검증 수준이 다르다. 프로덕션 PostgreSQL은 접근하지 않았으므로 잘못된 레터의 실제 발생 빈도는 알 수 없다.

| handler | 생산 계약 (`target_type` / `target_id` / `source_url`) | replay 소비·잘못된 입력 처리 | 판정 |
|---|---|---|---|
| `awards` | `award_history` / source key / 실패한 source URL | `target_id`를 source key로만 사용; source URL은 실행 선택에 쓰지 않음. 알 수 없는 key는 실행 전 `unaddressable`로 거부 | 거부 계약 ✅ (BUG-011) |
| `roster_transactions` | `roster_date` / `YYYY-MM-DD` / mobile URL; `game_id`에도 날짜 기록 | `target_id`만 `target_date`로 전달하고 source URL은 쓰지 않음. 결측·불가독 날짜는 오늘로 기본값 처리하지 않고 실행 전 `unaddressable`로 거부 | 거부 계약 ✅ (BUG-011) |
| `schedule` | `schedule_month` / `YYYY-MM` / crawler 기본 URL | `target_id` 파싱 실패 또는 월 범위 오류는 실행을 건너뜀; 원장 행이 없어 `missing`, 실패. `VALIDATION_SCHEMA`로 재시도 없이 종료 | `unaddressable`이 아니라 `missing`으로 남지만 false-success와 재시도 누적은 닫힘 |
| `game_detail` | `game` / game ID; `game_id`에도 같은 값 기록 / URL 없음 | `target_id` 우선, 없으면 `game_id`; 둘 다 비면 `unaddressable`. **형식 맞는 미존재 ID는 페치 후 `failed`로 종결** (below) | 레터는 닫히지 않음 — 예산 소진 후 `EXHAUSTED` |
| `relay` | `game` / game ID; `game_id`에도 같은 값 기록 / URL 없음 | `target_id` 우선, 없으면 `game_id`; 둘 다 비면 `unaddressable`. **형식 맞는 미존재 ID는 `EMPTY` → `success`로 종결** (below) | **의도된 부재 처리** ✅ |
| `food` / `parking` | 각 타입(`food` / `parking`) / 설정된 팀 코드 / 해당 팀 페이지 URL | `target_id`만 팀 필터로 사용; 저장된 URL은 재생 시 쓰지 않고 팀 설정에서 다시 선택. 미등록 코드는 빈 선택으로 진행하지 않고 `unaddressable`로 거부 | 거부 계약 ✅ (BUG-011) |
| `kbo_event` | `kbo_event` / URL에서 만든 페이지 slug / 실패한 페이지 URL | 실행 주소는 `source_url`; URL·slug 불일치 시 `unaddressable` + `VALIDATION_SCHEMA`로 거부 | 거부 계약 ✅ (부분 보강 완료) |
| `player_movement` | `player_movement` / 한 연도 또는 양끝 포함 연도 범위 / 기준 URL | `target_id`만 연도 범위로 파싱. 불가독·역순 범위는 실행 전 `unaddressable`로 거부 | 거부 계약 ✅ (BUG-011) |
| `team_history` | `team_history` / `kbo_team_history` / 고정 기준 URL | 항상 고정 페이지를 재생; `target_id`가 다르면 값이 RUN-B metadata에 남을 수 있음 | 실행 범위는 고정; metadata 일관성 공백 |
| `realtime_issue` | `realtime_issue_source` / 허용된 두 source ID / 해당 source URL | `target_id`를 허용 목록과 대조하고, 실제 URL은 ID에서 다시 선택. 그 외 값은 `unaddressable` | 거부 계약 ✅ |
| `preview` | `preview_date` / `YYYYMMDD`; `game_id`에도 같은 날짜 기록 / 게임 목록 URL | `target_id` 우선, 없으면 `game_id`; `YYYYMMDD`가 아닌 날짜는 실행 전 `unaddressable`로 거부 | 거부 계약 ✅ (BUG-011) |

### BUG-011 — 잘못된 replay target이 성공으로 닫히는 경로 (P1 후보, 수정 완료)

오프라인 SQLite 원장과 네트워크 없는 대역 테스트로 다음을 재현했다.

| 입력 | 실제 관찰 |
|---|---|
| `food` / `parking`: `NOT_A_TEAM` | `success`, `records_read=0`, `records_written=0` → `success=True` |
| `roster_transactions`: `target_id=None` | KST 오늘 날짜로 실제 fetch 경로 선택; 생산자는 `game_id`에도 날짜를 넣지만 handler가 대체값을 소비하지 않음 |
| `awards`: `UNKNOWN_SOURCE` | source 선택·fetch 없이 `success`, 0 write |
| `preview`: `not-a-date` + 빈 날짜 확인 응답 | `success`, 0 write |
| `player_movement`: `2026-2023` | 역순 범위를 그대로 전달; 읽은/저장한 domain row 없이 `success` |

이들 handler는 `_outcome_from_persisted_run`이 성공으로 읽는 RUN-B를 남기므로 `finalize_retry`가 레터를 `resolved`로 바꾼다. 따라서 잘못된 레터가 존재하면 원래 실패 단위를 처리하지 않고 닫을 수 있다. 다만 모든 현재 생산자는 정상 경로에서 source key·date·team·year·game ID를 채운다. nullable schema나 수동/오염 레터 외에 이 값들이 깨지는 운영 경로는 확인하지 못했고, 프로덕션 레터도 읽을 수 없어 발생 빈도는 미측정이다.

끝까지의 상태 전이도 잘못된 awards source로 실증했다. 로컬 SQLite에서 `retry_dead_letter()`를 실행한 결과 `retry_count=1`, RUN-B `success`(읽기 0 / 쓰기 0), DLQ `resolved`가 됐다. 네트워크와 운영 DB는 사용하지 않았다.

- **심각도**: 결과 영향은 P1급 잘못된 성공 종결이지만, 입력 도달성과 발생 빈도는 미확인이다. 지금은 P1 후보로 기록하며 P0로 올리거나 추정 임계값을 만들지 않는다.
- **수정 (2026-10-07 승인 후 적용)**: 5개 경로에 실행 전 거부 가드를 추가했다. `food`/`parking`은 `TEAM_*_SOURCES` 미등록 코드, `roster_transactions`는 결측·불가독 날짜(오늘 기본값 금지), `awards`는 미지 source key, `preview`는 `YYYYMMDD`가 아닌 날짜, `player_movement`는 역순 연도 범위를 각각 `unaddressable`로 반환한다. `schedule`은 기존 `missing` 실패를 유지한다(성공 오판이 아니며 기존 테스트가 그 계약을 고정). 운영 발생 빈도는 여전히 미측정이다.
- **회귀 테스트**: 6개 거부 경로의 `xfail(strict)` 마커를 제거해 **9 passed**가 됐고, 이후 DLQ 비재시도·kbo_event URL·slug·`target_type` 계약이 추가되어 현재 **18 passed**. `tests/services/test_replay_handler_contracts.py`의 placeholder target(`OB`, `wikipedia`)은 실제 유효 값(`LT`/`LG`, `WIKI_SOURCE_KEY`)으로 갱신했다 — 거부 가드가 켜지면 그 테스트들이 거부 경로를 타기 때문이다. mutation 증명: 팀 코드·preview 날짜·역순 범위 가드를 각각 제거하면 해당 계약이 `ReplayOutcome(success=True)`를 관찰하며 실패한다.
- **남은 것**: schedule의 `missing` 분류는 다른 handler의 `unaddressable`과 형태가 다르지만, 거부 outcome 전부 `error_code="VALIDATION_SCHEMA"`를 달아 `NON_RETRYABLE_CODES`로 분류되고 `finalize_retry`가 재시도 예산 소진 없이 즉시 `EXHAUSTED`로 종결한다(DLQ 수준 계약 `test_invalid_target_does_not_reschedule`가 고정). `target_type` 대조도 아래 절대로 보강했다.

### 실측 — 형식은 맞지만 존재하지 않는 game ID

빈 ID는 `unaddressable`로 거부된다. 그다음 질문은 **형식은 유효하지만 소스에 없는 경기**였다. `season_of`·`game_date_of`가 모두 `20991231ZZZZ0`을 정상 파싱하므로 target 검사 계층을 통과해 실제 fetch까지 도달한다. 그래서 판정은 크롤이 돌려준 결과에서 나온다.

두 handler가 **의도적으로 다르게** 종결한다.

| handler | 소스 응답 | RUN-B status | 레터 종결 |
|---|---|---|---|
| `game_detail` | payload 없음 → `no_detail_payload` | `failed` | 닫히지 않음. 예산 소진 후 `EXHAUSTED` |
| `relay` | `RelayStatus.EMPTY` | `success` (`records_written=0`) | `resolved` |

**이 차이는 버그가 아니다.** `build_attempt`의 docstring이 규칙을 명시한다 — "A terminal failure becomes `EMPTY` rather than staying `FAILED`, because 'the source has nothing' and 'we could not get it' must not share a status: the first is never retried and the second always is." `_collect_one_relay`도 같은 이유로 `EMPTY`를 success로 기록한다.

**부재로 오판되지 않는 이유**도 확인했다. `_resolve_naver_game_id`는 스케줄 조회가 **실제로 성공했을 때만** `relay_not_found`/`invalid_relay_match`를 남긴다(`relay_crawler.py` 712–721). 조회 자체가 실패하면 부재를 결론 내리지 않으므로, 전송 장애가 영구absence처럼 보이거나 아무도 재시도하지 않는 상황이 생기지 않는다. 404도 relay 엔드포인트 한정에서만 absence다.

**`game_detail`의 남는 특성**: 존재하지 않는 경기를 가리키는 레터는 예산 5회를 모두 소모한 뒤에야 `EXHAUSTED`로 닫힌다. 하지만 닫히는 시점과 코드가 정직하고(성공 아님), 실측으로 잃어버리는 데이터가 없다는 점을 확인했다. 따라서 **수정 대상이 아니라 관찰 기록**으로 남긴다. 존재 확인을 replay마다 하지 않는 이유도 분명하다 — 지금은 DLQ가 만들어진 경기만 재생하는데, 그 경기가 사라지는 일은 저장소 문제이지 수집 원본 문제가 아니다.

**회귀 테스트**: `TestAGameTheSourceDoesNotHave` 2건. relay 쪽은 collection 서비스 레벨(`test_relay_runs.py::test_a_genuine_absence_succeeds_and_is_never_queued`)에서 이미 규칙이 검증되므로, 이 테스트는 **replay → 레터 종결 경계**에서 그 결과를 본다 — 어느 계층에서 무엇을 보장하는지 분리해서 고정한다.

### `target_type` 감사 (P2 후보 → 수정 완료)

생산자들은 crawler별 상수로 `target_type`을 기록한다. dispatcher도 `crawler`로 dispatch하므로 잘못된 `target_type`이 다른 handler를 실행시키지는 않는다. 피해는 lineage metadata에 한정된다 — RUN-B 행이 크롤러가 생산하지 않는 타입으로 기록되고, 타입별로 묶는 모든 메트릭이 오염된다. replay 결과 자체는 정상이라 기존 재현 경로로는 드러나지 않는다.

**수정**: `_EXPECTED_TARGET_TYPES` 매핑(15개 크롤러)과 `_target_type_mismatch()`를 추가했다. `ReplayDispatcher.replay()`이 유일한 운영 진입점이므로 거기서 한 번만 검사한다 — handler마다 넣으면 검사 누락이 곧 회귀가 된다. 불일치는 `unaddressable` + `VALIDATION_SCHEMA`로 거부한다.

**빈 값은 불일치가 아니다.** 컬럼이 non-nullable이므로 빈 문자열은 legacy·수동 생성 행이지 틀린 주장이 아니다. 모든 handler가 이미 `target_type or <상수>`로 정규화하므로, 여기서 거부하면 회복 경로가 없는 레터를 그냥 방치하게 된다. 거부는 거짓 주장에만 적용한다.

**회귀 테스트**: `test_target_type_must_match_the_crawler_contract`(4개 크롤러) + `test_a_blank_target_type_is_not_treated_as_a_mismatch`.

**남은 것**: 운영에서 실제로 잘못된 `target_type`이 발생했는지는 프로덕션 DB 미접촉으로 확인하지 못했다. 검사 자체는 코드 계약이므로 운영 빈도와 무관하게 유효하다.

---

## 4-BH2 산출물 (실행 가능한 형태)

문서에만 두지 않고 회귀 테스트로 고정했다.

| 파일 | 역할 |
|---|---|
| `tests/monitoring/test_partial_run_invariants.py` | INV-METRIC-01/03와 liveness 유지 계약. **6건 통과** (현재 구현이 이미 지켜짐을 확인) |
| `tests/monitoring/test_partial_degradation_detection.py` | INV-METRIC-02. BUG-001의 실행 가능한 형태. **1 passed + 1 skip** (수정 완료) |
| `tests/crawlers/test_page_outcome_agreement.py` | BUG-005. 3개 outcome 모듈의 shape 일치. **6 passed + 1 xfailed** |
| `tests/crawlers/test_page_outcome_producers.py` | BUG-006. reason 행은 생산 가능한 상태만 분류해야 한다. **10 passed** |
| `tests/services/test_snapshot_persist_crash_window.py` | BUG-007. 커밋 경계 crash 창과 복구. **5 passed** |
| `tests/services/test_snapshot_drift_baseline.py` | BH2-c. 기준선 유무에 따른 게이트 판단 차이. **6 passed** |
| `tests/services/test_replay_handler_write_contracts.py` | BUG-010. schedule replay 저장 회귀 및 run 기반 저장 인자 계약. **11 passed** |
| `tests/services/test_replay_target_validation_contracts.py` | BH11. 잘못된 target 처리. **18 passed** (BUG-011 수정 완료) |
| `monitoring/prometheus/tests/crawler_alert_dlq_test.yml` | BUG-002. `KboDlqRecoveryStalled` 발동 + 조용 (promtool) |
| `monitoring/prometheus/tests/crawler_alert_dlq_backlog_test.yml` | BUG-002. `KboDlqBacklogAgeHigh` 발동 + 경계 + 조용 (promtool) |

xfail이 strict인 이유: 버그를 고치면 XPASS로 실패해 마커를 떼라고 강제한다. 임시로 degradation 규칙 하나를 Rules 파일에 넣어 실제로 확인했다 — 테스트가 FAIL로 바뀌었다. (파일은 되돌린 뒤 `git status`로 변경 없음 확인)

`test_partial_run_invariants.py`에 한 의도적인 명시가 있다. `TestLivenessSurvivesAPartialRun`은 **partial이 last_success를 갱신하는 현재 동작을 고정이 아니라 "보수적 운영 선택"으로 단언**한다. 사용자의 지시에 따라 BH9 실측 전까지 기존 critical의 의미를 바꾸지 않기 때문이다. 나중에 이 계약을 뒤집으면 그 테스트가 경고가 된다.

## 5. BH10에 넘길 관찰 사항

사용자 지적대로 BH10은 **기대값을 가정하지 않고 현재 계약을 먼저 관찰**해야 한다.

```
RUN-A PARTIAL
→ DLQ enqueue
→ [관찰] 현재 incident 생성 여부   ← BUG-002 때문에 없을 가능성이 높음
→ replay RUN-B
→ DLQ resolution
→ [관찰] incident state 변화
```

BUG-002가 확정적으로 수정되기 전까지는 `incident OPEN → RECOVERED`를 기대값으로 쓰면 안 된다. hunt가 기존 설계를 검사하는 게 아니라 새 설계를 테스트하게 된다.

## 5. BH0이 남긴 미해결 질문

- PARTIAL을 발생시키는 크롤러는 실제로 어떤 것인가 (BH2 입력) — **답**: `award`/`food`/`parking`/`kbo_event`/`player_movement` 5종 + `game_detail_runs`/`relay_runs` 2개 경로. 그중 4개는 error_code가 비어 있다(BUG-004).
- DLQ alert 규칙을 넣을 때 어느 임계값이 적절한가 — **답(2026-10-09)**: 24h 유지로 확정했으나 **적정성은 미검증**이다. 유지 근거는 재시도 스케줄에서 온 구조적 추론이지 관측이 아니다. 운영 7일 측정 후 재판정한다. BUG-002는 이 근거가 확보될 때까지 미종결.
- EXHAUSTED 누적은 누가 감시하는가 — **답(2026-10-09, c387f8b3로 해소)**: 처음 판단은 "아무도 감시하지 않는다"였다. `oldest_due_next_retry_at()`이 PENDING만 조회하고 지표는 있어도 규칙이 없었다. Prometheus 규칙 추가는 보류했고, 누적값이 아니라 **신규 발생률**로 설계해야 한다고 적었다.

  **이 설계 보류는 더 이상 필요하지 않다.** 같은 자리에 `dlq:exhausted` 인시던트를 넣으면서 정답이 다른 형태라는 게 드러났다: EXHAUSTED는 누적값을 감시하는 게 아니라 **조치 가능한 상태**를 감시해야 한다. 누적은 0이 될 수 없어서(New rate로 보정해도 절차 잡음이 되고) 무엇이 나빠졌는지 말해주지 않는다. 운영자가 할 일은 "새 letter가 소진되었다"가 아니라 "소진된 letter가 있다"이고, 인시던트가 정확히 그것을 표현한다 — 누적되어 **자가 해소되지 않으며** `kbo dlq retry`/`ignore`로만 닫힌다.

  **그래서 EXHAUSTED Prometheus 규칙은 추가하지 않는다.** 두 경로를 만들면 같은 상태에 중복 경보가 생기고, 인시던트 쪽이 더 정확하다. 운영 실측은 2026-10-09 기준 `ignored` 4건·`exhausted` 0건이라 인시던트가 실제 발동한 적은 아직 없다.
- `src/utils/metrics.py`의 다른 메트릭도 같은 사각지대에 있는가 (BUG-002 확장) — **답: 4종이었고 분류를 마쳤다. 사각지대 자체도 닫았다(BUG-012).**

## 5-BH10. DLQ → incident 사슬 종결 관찰 결과 (2026-10-10)

BH10은 사슬을 **기대값 없이 관찰**하라고 지정했다:

```
RUN-A PARTIAL → DLQ enqueue → [incident] → replay RUN-B → DLQ resolution → [incident 변화]
```

데이터 절반은 `tests/crawlers/test_award_dead_letter_e2e.py` 2건이 이미 끝까지 검증한다
(실제 원장·DLQ 서비스·dispatcher·canary, 네트워크만 fake). 관찰되지 않은 것은
`c387f8b3`이 `dlq:` 키를 도입한 **이후의 인시던트 절반**이었다.

### 발견: `dlq retry`가 인시던트를 닫지 않았다

`src/cli/dlq_operator.py`의 `_refresh_metrics()`는 `publish_dlq_state_metrics()`만
호출했다 — 게이지와 heartbeat는 갱신되지만 **인시던트 원장은 손대지 않는다.**

```
letter resolve (운영자가 dlq:exhausted가 물은 질문에 답함)
        ↓
게이지 갱신 ✓ / heartbeat 갱신 ✓
        ↓
dlq:exhausted 인시던트 = ERROR 그대로  ← 최대 30분(다음 recovery tick)까지
```

30분 뒤 스케줄러가 같은 상태를 재도출해 닫아주므로 데이터는 상하지 않는다. 그러나
**방금 손으로 해소한 경보가 계속 열려 있다고 주장**했고, 그 지연은 정확히 해소한
사람에게 떨어진다. `dlq:exhausted`는 자가 해소가 불가능한 유일한 조건이라 더 나쁘다.

### 수정 (`src/cli/dlq_operator.py`)

`_refresh_metrics()`가 읽기에서 인시던트까지 도출한다. 세 mutation 경로
(`retry`/`requeue`/`ignore`)가 **이미 이 함수를 호출**하므로 고치는 지점이 하나다.

- 읽기 결과로 도출하지, 직접 패치하지 않는다 — `apply_dlq_incidents`가 `dlq:`
  네임스페이스 전체를 reconcile하므로 이 mutation이 다른 키에 영향을 줬다면 함께 닫힌다.
- **읽을 수 없으면 도출하지 않는다.** `None`과 예외 둘 다 "읽기가 아님"이다.
  기본값으로 대체하면 평가되지 않은 조건을 reconcile해 **일어나지 않은 회복을 보고**한다
  (B1/B2가 스케줄러 경로에서 제거한 바로 그 혼동).
- **가드는 두지 않는다.** `apply_dlq_incidents`가 자기 실패를 삼키고 로깅하므로
  커밋된 mutation이 알림 실패로 실패 처리되지 않는다. 이중 가드는 그 보장이
  불신뢰라는 인상을 준다.

### 검증

회귀 `tests/cli/test_dlq_operator_incidents.py`(9건). 뮤테이션 3건 모두 검출:

| 뮤테이션 | 결과 |
|---|---|
| metrics-only로 회귀(발견한 버그 그대로) | 5건 실패 |
| 읽기 실패 시 빈 `DlqStats()`로 대체 | 2건 실패 |
| `None` 가드 제거 | `AttributeError` (커밋된 mutation 실패 경로) |

**뮤테이션 중 제 테스트의 허점을 하나 잡았다**: `published == []`는 "호출 안 됨"과
"빈 이벤트로 호출됨"을 구분하지 못한다. 후자는 더 위험한데 `apply_incidents`가
prefix로 reconcile하므로 **빈 배치가 모든 `dlq:` 인시던트를 닫기** 때문이다.
단언을 "호출되지 않음"으로 강화해 두 번째 뮤테이션이 검출된다.

### 남은 관찰

`retry_count`가 운영에서 **전부 0**이고 replay-linked run도 **0건**이다 — 사슬의
재시도 구간은 production에서 한 번도 밟히지 않았다. BH11이 이미 DLQ 수준 계약
(`test_invalid_target_does_not_reschedule` 등)을 고정했으므로 추가 조치는 없다.
다만 "재시도가 현장에서 돌았다"는 증거는 아직 없고, 그 사실은 숨기지 않는다.

## 5-BH0-가정. "크롤러는 정직하게 보고한다" 가정의 검증 결과 (2026-10-10)

BH0이 명시적으로 남긴 미검증 가정이 있었다 (§1, line 43):

> 레이어는 "크롤러가 자기 결과를 정직하게 보고한다"를 가정하고 있고, 실제 강제 지점은
> 각 크롤러의 `record_dead_letters` 경로다. 다만 **이 가정은 검증되지 않은 채 전 계층에
> 전파되어 있으므로** BH2에서 크롤러별 확인 대상으로 남긴다.

**BH2는 이 확인을 하지 않고 crash window(BUG-007)로 이동했다.** 그래서 검증했다.

### 결과: 보고는 정직하다. 그런데 `success`가 과대하다

원장을 read-only로 전수 조회해 찾은 위반은 **0건**이다:

| 검사 | 결과 |
|---|---|
| `success`인데 `records_failed > 0` (거짓 성공) | 0 |
| `failed`인데 `records_written > 0` (쓰고 실패 주장) | 0 |
| `failed` 89건의 read/write | 전부 0 — 일관 |

크롤러가 **거짓말하지는 않는다.** 문제는 다른 데 있었다.

### BUG-014 — 준법 차단이 `success`로 기록되어 liveness를 갱신한다

`success` 1088건 중 **1051건이 아무것도 쓰지 않았고**, 그중 14건은 **읽지도 않았다**
(`read=0 AND wrote=0`). 14건 전부 `checkpoint`가 답을 갖고 있었다:

| crawler | 건수 | `checkpoint.outcome` | `reason` |
|---|---|---|---|
| `roster_transactions` | 8 | `source_limited` | `compliance_blocked` |
| `player_movement` | 6 | `source_limited` | `kbo_robots_blocked` |

소스를 **아예 조회하지 않고** `success`를 반환한다. 크롤러 자신의 주석은 그 이유를
정확히 설명한다 — "정책적 건너뛰기는 데이터 결과가 아니므로 EMPTY도 실패도 아니다".
**그 판단 자체는 옳다.** 문제는 그 `success`가 `SUCCESS_STATUSES`에 속해
`kbo_crawl_last_success_timestamp`를 갱신한다는 것이다:

```python
# src/monitoring/crawler_metrics.py
if status in SUCCESS_STATUSES:                  # {"success", "partial"}
    CRAWL_LAST_SUCCESS_TIMESTAMP.labels(crawler=crawler).set(...)
```

따라서 **매 실행마다 건너뛰는 크롤러는 `KboCrawlerNoRecentSuccess`에게 영원히
건강하다.** "소스가 가진 게 없다"와 "우리가 묻지 않았다"가 구별되지 않는다.

### 실측 피해

`roster_transactions` 도메인 테이블의 **마지막 행이 2026-08-16**이다. 조회 시점은
2026-10-10 — **약 8주간 0건**이고, 그 기간 원장은 계속 성공을 기록했다.
`KNOWN_LIMITATIONS.md`에도 이 차단은 **기록돼 있지 않았다.**

`source_limited`는 또한 운영에서 기록된 **유일한 checkpoint outcome**이다 —
checkpoint 메커니즘 전체가 이 한 가지 경우만 담고 있었다.

### 수정 (10-10)

원장이 이미 갖고 있던 증거를 메트릭으로 투영한다. 새로운 판정 규칙을 만드는 게
아니라 **이미 기록된 것을 보이게** 하는 것이다.

- `kbo_crawl_last_source_consulted_timestamp` — **실제로 소스를 조회한 런만** 갱신한다.
  건너뛴 런이 이걸 갱신하면 결함이 그대로 재현되므로 의도적으로 비워 둔다.
- `kbo_crawl_source_limited_total{crawler, reason}` — 같은 상황을 사유별로 분해.
  reason은 폐쇄 집합(`compliance_blocked`/`kbo_robots_blocked`)이고 그 밖은 `other`로
  묶는다 — 크롤러 라벨과 같은 이유(자유 형식은 시리즈를 무한히 늘린다).
- `KboCrawlerSourceNeverConsulted`(warning) — 실행은 계속되는데 48시간 넘게 조회가
  없을 때. **critical이 아닌 이유**: 정책 차단은 고장이 아니며, 수용 여부는 운영자가
  결정한다. 그 결정을 심각도가 아니라 런북이 기록한다.

`checkpoint`는 크롤러 소유 JSON 컬럼이므로 형태를 가정하지 않는다 — 문자열·리스트·정수
모두 "차단 아님"으로 읽는다. 투영이 한 행에서 예외를 내면 **그 뒤의 모든 행을 잃는다.**

### 남은 결정 (미해결)

**두 크롤러의 차단이 수용 가능한지 결정되지 않았다.** 코드는 이제 그것을 말할 수
있게 됐지만, "그래서 괜찮은가"는 운영자 몫이다:

- **수용** — `KNOWN_LIMITATIONS.md`에 기록하고 경보가 계속 발동하는 것을 받아들인다
  (경보는 선택이 아직 유효하다는 기록이다).
- **불수용** — 데이터가 필요하므로 허가된 경로나 다른 소스를 찾아야 한다. 그때까지
  `roster_transactions`/`player_movements`는 8월 데이터다.

### 검증

회귀 `tests/monitoring/test_crawler_source_limited_metrics.py`(29건) + promtool
픽스처 5케이스(연속 차단 발동 / 계속 조회 조용 / 실행 중단 조용 / 1회 건너뜀 후 복구
조용 / 신규 크롤러 발동). 뮤테이션 4건 모두 검출:

| 뮤테이션 | 결과 |
|---|---|
| 건너뛴 런을 조회한 것으로 취급 (결함 그대로) | 8건 실패 |
| reason을 그대로 수용 (라벨 무한) | 3건 실패 |
| 건너뛰기를 세지 않음 | 3건 실패 |
| malformed checkpoint에서 예외 | `AttributeError` (이후 행 전부 유실) |

**작업 중 발견한 함정**: 카운터의 이름이 세 가지 형태로 나타난다 — `_name`은
`kbo_crawl_source_limited`, 노출 형식과 `get_sample_value`는 `..._total`,
`Counter.collect()`는 다시 축약형. 규칙이 읽는 것은 **노출 형식**이므로, 이 차이를
테스트로 고정했다. 두 형태가 같다고 가정하는 것이 동작하는 규칙을 조용한 규칙으로
바꾸는 경로다.

## 5-BH0-가정-b. KBO robots.txt 전면 금지와 그 귀결 (2026-10-10)

BUG-014를 조사하다 **데이터 원천 자체가 닫힌 사실**을 발견했다. 앞 절의 두 크롤러만의
문제가 아니었다.

### robots.txt 타임라인 (준법 모듈이 저장한 스냅샷 3,000여 건에서 복원)

| 기간 | `User-agent: *` 정책 | 스냅샷 수 |
|---|---|---|
| ~2026-06-26 ~ 07-13 | `Disallow: /Manager` | 974 |
| ~2026-06-26 ~ 08-02 | `Disallow: /Common/`, `/Help/`, `/Member/`, `/ws/` | 991 |
| **~2026-08-05 ~ 현재** | **`Disallow: /`** | **964** |

**KBO가 8월 초에 사이트 전체를 일반 에이전트에게 금지했다.** Googlebot·Yeti·Daumoa·
Bingbot만 예외이며 그것도 `/ws/`만 막힌다.

### 범위: 검사하는 26개 중 24개가 KBO 사이트 의존

준법 계층 자체는 철저하다 — 26개 크롤러가 `compliance.is_allowed`를 호출하고,
미호출은 인프라(`http_client.py`, `selectors.py`)뿐이다. 문제는 그 26개 중
**24개가 `koreabaseball.com`을 원천으로 쓴다**는 것이다. 살아남은 것은 다른 도메인을
쓰는 크롤러뿐이다:

| 크롤러 | 원천 | 상태 |
|---|---|---|
| `game_detail` | Naver API만 | 정상 |
| `schedule` | Naver 우선 + KBO fallback | Naver 경로로 수집 |
| `parking` | 구장·구단 사이트 | 정상 |

즉 경기 데이터는 신선하지만(`game_*`, `player_season_*` 최근 갱신), **KBO 자체 페이지만
가진 데이터는 8월 이후 갱신되지 않고 있다.**

### BUG-015 — 차단이 "HTTP 오류 + 재시도 가능"으로 둔갑한다

`preview_crawler`에서 실측했다. `_fetch_api_json`이 차단 시 **조용히 `None`을 반환**하고
(INFO 로그만), 그 결과가 "no preview data obtained" → `_date_is_confirmed_empty`가
`False` → 다음으로 분류된다:

```python
error_code=FailureCode.FETCH_HTTP_ERROR.value,   # HTTP 요청을 한 적이 없다
run.status = RUN_STATUS_FAILED
self._enqueue_dead_letter(...)                   # 재시도 불가능한데 레터를 만든다
```

**두 번 틀렸다.** HTTP 요청이 없었는데 HTTP 오류라고 하며, robots가 금지하는 동안
어떤 재시도도 성공할 수 없는데 재시도 큐에 넣는다.

실측 피해:

| 항목 | 값 |
|---|---|
| `preview` 런 | **459건 전부 failed** / `FETCH_HTTP_ERROR` |
| DLQ 레터 | **89건** (exhausted 77 + pending 12) |
| 대상 날짜 | 10/10·11·12 — **경기가 있는 날**(3·2·3경기) |
| 결과 | `dlq:exhausted` 인시던트(ERROR/31회)로 표면화 |

즉 **현재 열려 있는 `dlq:exhausted`의 출처가 이 오분류**다. 운영자가 런북대로
`dlq retry`를 해도 같은 차단을 다시 만난다.

### 수정

`roster_transaction_crawler`가 이미 쓰는 관례를 preview에 적용한다 — 차단은 데이터
결과도 실패도 아니므로 `source_limited`로 기록하고 DLQ에 넣지 않는다.

- `_source_limited_reason` 플래그를 **차단이 발견되는 두 지점 모두**에서 설정한다
  (크롤 자체의 fetch, 그리고 빈 결과일 때만 도는 확인 read).
- **런마다 초기화**한다. 크롤러 객체는 날짜보다 오래 살므로, 한 날짜의 차단이 다음
  날짜의 결과로 보고되면 그것은 반대 방향의 같은 결함이다.
- 결과 판정은 **두 지점을 모두 지난 뒤**에 한다. 처음에 플래그 검사를 확인 read 앞에
  뒀더니 주 경로(빈 크롤 → 확인 중 차단 발견)가 그대로 `FETCH_HTTP_ERROR`로 남았다 —
  기존 테스트 12건이 통과한 것이 이를 드러냈다.

### 부수 효과: BUG-014 수정의 구멍이 닫혔다

BUG-014의 `KboCrawlerSourceNeverConsulted`는 `checkpoint.outcome == "source_limited"`를
읽는다. **preview는 그 체크포인트를 쓰지 않았으므로 새 경보에도 잡히지 않았다.**
이 수정으로 preview가 그 어휘를 쓰게 되어 메트릭·경보가 함께 동작한다
(`tests/monitoring/test_crawler_source_limited_metrics.py`의 크롤러 스캔이 preview를
찾는 것으로 확인).

### 검증

회귀 `tests/crawlers/test_preview_reliability_canary.py`(16건, 신규 5건 포함).
기존 `test_a_compliance_block_is_not_confirmation`이 **잘못된 동작을 고정하고 있었다** —
이름은 "차단은 확인이 아니다"인데 본문은 `FAILED`와 DLQ 적재를 단언했다. 의도는
보존하고 분류를 바로잡았다.

뮤테이션 4건 모두 검출:

| 뮤테이션 | 결과 |
|---|---|
| `FETCH_HTTP_ERROR` 분류로 회귀 (결함 그대로) | 5건 실패 |
| 플래그 검사를 확인 read 앞으로 (무효했던 첫 수정) | 3건 실패 |
| 런별 초기화 제거 (차단이 다음 날짜로 누출) | 1건 실패 |
| 사유를 상수로 고정 (정책/네트워크 구분 불가) | 1건 실패 |

### 남은 것 (운영자 결정)

- **77건의 exhausted 레터**: 재시도로 풀리지 않는다. `dlq ignore`(사유 기록)가 맞지만
  이는 가드된 mutation이므로 운영자 판단이다. 12건의 pending은 다음 재시도에서
  `source_limited`로 종결된다.
- **24개 크롤러의 8월 이후 공백**: 차단이 정확하므로 수집할 수 없다. 허가된 경로나
  대체 원천이 필요하며, 그것이 `KNOWN_LIMITATIONS.md`의 열린 결정이다.

## 6. 누적 버그 목록 (BH0~BH11)

| ID | 심각도 | 영역 | 요약 | 상태 |
|---|---|---|---|---|
| BUG-001 | P1 | metrics | 지속 PARTIAL 탐지 불가 | **수정 완료; KboCrawlerSustainedPartial(warning) + 발동/조용 픽스처** |
| BUG-002 | P1 | metrics/incident | DLQ에 규칙·인시던트 없음; orphan 사각지기가 구조적 | **종결**(c387f8b3). 규칙 4종 + `dlq:` 인시던트 3키 + heartbeat 2종. `dlq:exhausted`가 자가 해소되지 않던 문제도 해결 |
| BUG-003 | — | DLQ | 동시 워커 이중 claim | **기각** |
| BUG-004 | — | ledger | PARTIAL error_code가 비어 있음 | **기각 — 의도된 계약**(8fa569a5). partial에 코드 없음은 거부된 설계이며 `test_food_persist_failure_contracts.py`가 고정한다 |
| BUG-005 | P2 | crawler | `kbo_event`만 4튜플, `absence` 필드 미소비 | **수정 완료**(8fa569a5). dead 컬럼 제거 → 3튜플. strict xfail이 XPASS로 표시 제거를 강제했다 |
| BUG-006 | P2 | crawler | `FETCH_FAILED`를 아무도 생산하지 않음 | **수정 완료**(8fa569a5). `_page_reads` producer 추가. 기존 테스트는 생성자 도달 가능성만 봐서 통과했었다 |
| BUG-007 | P2 | persist/snapshot | 마지막 커밋~상태 갱신 사이 crash 창 | 회귀 테스트 5건 + **런북 §4.2a에 문서화**(0472ac17). 데이터 결함 아님; `pending` 스냅샷이 기준선 사각지대를 만든다 |
| **BUG-012** | **P1** | **metrics** | **`kbo_crawl`/`kbo_notification` 밖의 메트릭은 두 계약 모두 검사하지 않음; 4종이 미참조** | **분류 완료 + 접두어 필터 제거 (18 passed)** |
| **BUG-010** | **P1** | **replay** | **schedule replay가 저장 없이 성공 보고 → 레터가 닫힘** | **수정 완료; 회귀 테스트 통과** |
| **BUG-011** | **P1 후보** | **replay target** | **허용되지 않은 target이 빈/default 작업을 성공으로 기록해 레터를 닫을 수 있음** | **수정 완료; 거부 가드 + `target_type` 대조 + `VALIDATION_SCHEMA` 종료, 18 passed (운영 빈도 미측정)** |
| **BUG-015** | **P1** | **crawler/ledger** | **준법 차단이 `FETCH_HTTP_ERROR` + 재시도 가능 DLQ 레터로 기록됨 — 재시도로 절대 성공할 수 없는 레터 77건이 `exhausted`** | **수정 완료(10-10); `source_limited` 기록, DLQ 미적재** |
| **BUG-014** | **P1** | **metrics/crawler** | **준법 차단 런이 `success`로 기록돼 liveness 경보가 영원히 조용함 — 8주간 데이터 0건을 아무도 몰랐다** | **수정 완료(10-10); `kbo_crawl_last_source_consulted_timestamp` + `KboCrawlerSourceNeverConsulted` + `kbo_crawl_source_limited_total`** |
| **BUG-013** | **P2** | **incident** | **`dlq retry`/`requeue`/`ignore` 후 `dlq:` 인시던트가 최대 30분 열린 채 남음** | **수정 완료(BH10); `_refresh_metrics`가 읽기에서 도출, 뮤테이션 3건 검출** |
| BH2-c | P2 | drift gate | 기준선 없는 스냅샷은 의도적으로 판정 불가 (문서화 공백) | 회귀 테스트 6건 + **런북 §4.2a**(0472ac17) |

**BUG-010은 수정 전 hunt에서 데이터 손실에 가장 가까운 발견이었습니다.** schedule replay가 이제 `save=True`를 명시하며, 재도입 시 회귀 테스트가 실패합니다. **BUG-011은 6개 handler의 성공 오판 경로를 5개 거부 가드로 닫았고, schedule은 기존 `missing` 실패 계약을 유지합니다.**

---

## 6-b. BUG-011 조사 보충 기록

schedule은 false-success 경로는 아니지만, 잘못된 target이 raw 재시도 예산을 소모할 수 있다. `player_movement`의 역순 범위, `preview`의 형식 없는 날짜, `awards`의 알 수 없는 source key, `food`/`parking`의 미등록 팀 코드, `roster_transactions`의 missing date가 모두 같은 형태의 성공 오판 후보로 기록됐다. 승인 후에는 `VALIDATION_SCHEMA` 분류와 target refusal/DLQ exhausted 경로로 정리했다.

### BH11 schedule 분류 참고 (별도 버그 아님)

`target_id` 파싱 실패 시 핸들러별 처리가 다릅니다. `_replay_schedule`은 파싱 불가 또는 잘못된 월에 크롤러 호출 없이 반환하고, 원장 run 부재로 실패 outcome을 돌려준다. 다른 핸들러의 `unaddressable`과 달리 `missing`이 되지만 성공으로 오판되지는 않는다. **현재는 `VALIDATION_SCHEMA`로 DLQ 재시도를 더 내지 않도록 보정했다.**

**의도적 계약:** schedule의 malformed month는 `missing` 상태와 `VALIDATION_SCHEMA`를 유지한다. 다른 핸들러의 `unaddressable`로 바꾸지 않는 이유는 schedule의 실패가 “주소가 없음”이 아니라 “요청한 월 형식 자체가 유효하지 않음”을 원장 수준에서 반영해야 하기 때문이다. 상태명보다 `error_code=VALIDATION_SCHEMA` 유무가 핵심 계약이다.

### BH11 검증

- replay 집중 검증: BUG-011 수정 승인 후 **106 passed** (대상 검증 파일은 18 passed).
- 잘못된 awards source의 DLQ 끝단 검증: 현재 동작은 `VALIDATION_SCHEMA` 분류와 `EXHAUSTED` 종료다. 수정 전에는 빈 성공 RUN-B가 기록되고 DLQ가 `resolved`로 전이됐다.
- 변경 파일 단독 Ruff 검사와 형식 검사는 통과했다.
- 전체 비통합 테스트: `12,983 passed, 2 failed, 5 skipped, 2 xfailed`. 실패는 병행 변경 중인 adoption matrix 2건이다. BUG-011 회귀는 현재 없고 대상 검증은 전부 통과했다.
- 전체 Ruff는 병행 변경 파일에서 9건을 보고했다. replay dispatcher의 import 정리는 이번 수정으로 정리했다.
- 테스트는 로컬 SQLite와 대역만 사용했다. 운영 DB와 외부 네트워크에는 접근하지 않았다.

**P0는 0건.** P0급 문제는 찾지 못했다. BUG-011은 재현된 P1 후보로 유지하며, 잘못된 DLQ의 운영 빈도는 확인하지 못했다. 동시 claim 가설 1건은 기각됐다.

---

## 7. BUG-011 조사 상태 업데이트 (2026-10-07)

- BUG-011은 확인됐고, 6개 handler 경로가 strict `xfail`로 고정됐다. (→ 같은 날 마커 제거, 수정 완료)
- schedule은 false-success 경로는 아니지만 `missing` 분류가 다른 handler와 달랐다. 현재는 `VALIDATION_SCHEMA`를 붙여 DLQ 재시도 누적을 막았다.
- `target_type` 불일치는 현재 P2 후보이며, replay dispatch 자체는 crawler 이름으로 결정되어 다른 handler를 실행시키지는 않는다.
- 수정 승인 전까지 source 변경은 하지 않는다. 현재 replay 집중 검증은 `79 passed, 6 xfailed`다. (→ 승인 후 수정 완료; replay 집중 검증은 `106 passed`다. 대상 검증 파일은 18 passed)
- `tests/services/test_replay_target_validation_contracts.py`의 strict `xfail` 6건은 BUG-011 미수정 상태를 증명한다. schedule 3건은 현재도 `missing` 실패로 통과한다. → **같은 날 승인 후 수정 완료. 마커 6건을 제거했고 파일은 18 passed다 (아래).**

### BUG-001 임계값 실측 완료 (2026-10-09) — **보정 대상이 아니라 전제가 성립하지 않음**

운영 DB를 read-only(`SET TRANSACTION READ ONLY`)로 조회했다. 원장 전체 구간
(2026-09-29 ~ 10-09, 1093런):

| status | runs |
|---|---|
| `success` | 1088 |
| `failed` | 5 |
| `partial` | **0** |

**`partial`이 한 번도 기록된 적이 없다.** 희소가 아니라 부재이므로 24h 창을
보정할 표본이 없다. 관측을 더해도 바뀌지 않고, 표본이 생기기 전까지 이 규칙의
정직한 기술은 "보정됨"이 아니라 **"미검증"**이다.

#### "부분 실패 분기가 도달 불가능해서 규칙이 아무것도 못 본다"는 추론은 틀렸다

기록에 앞서 확인했다. `partial`을 만들 수 있는 5개 크롤러 **모두** 혼합 실패를
구동해 `status == "partial"`을 단언하는 테스트가 있다 — `kbo_event`(7개 중 1개
페이지 실패), `award`(2개 소스 중 1개 실패), `food`/`parking`(팀 1개 실패),
`player_movement`(연도 1개 실패). CI에서 계속 실행된다.

즉 **0건은 코드의 사실이 아니라 현장의 사실**이다. 운영이 기록한 5건의 실패는
모두 전부 실패였고(`records_written=0`), 그게 `failed` 분기로 가는 경로다.
혼합 실패가 현장에서 한 번이라도 일어나기 전까지는 재측정으로 달라질 게 없다.

#### 같은 실측에서 확인된 것: C1은 프로덕션에서 이미 동작한다

`crawl_execution_runs.id=37`이 **이미 회수됐다.** 2026-10-09 12:00:00 UTC에
`status='failed'`, `error_code='RUN_INTERRUPTED'`, `finished_at`이 채워졌고
메시지에 스위퍼 문구가 남아 있다 — 이 조회를 시각(12:11 UTC)에 직접 확인했다.

AGENTS.md의 "배포 후 `id=37` 회수 여부로 배포 성공을 확인한다"는 항목은
**기다릴 항목이 아니라 이미 충족된 항목**이다. 배포 전 확인을 기다릴 이유가 없다.

### BUG-011 수정 완료 (2026-10-07)

승인 후 수정을 적용했다. 수정 파일:

| 파일 | 변경 |
|---|---|
| `src/services/crawl_replay_dispatcher.py` | 5개 거부 가드 + 날짜 형식 헬퍼(`_is_iso_date`, `_is_compact_date`) + `_TEAM_SOURCES` 매핑 + schedule malformed month 가드 + `kbo_event` URL·slug 불일치 거부 + 모든 거부 outcome에 `VALIDATION_SCHEMA` 분류 추가. **병행 작업자의 preview/realtime/pbp 핸들러와 `save=True` 변경은 그대로 보존** |
| `tests/services/test_replay_target_validation_contracts.py` | `xfail(strict)` 마커 6건 제거 + `VALIDATION_SCHEMA` 어서션·DLQ 비재시도 계약 + `kbo_event` URL·slug·`target_type` 검사 + 미존재 경기 종결 2건 → **18 passed** |
| `tests/services/test_replay_handler_contracts.py` | placeholder target을 실제 유효 값으로 갱신 (`OB`→`LT`/`LG`, `wikipedia`→`WIKI_SOURCE_KEY`) |
| `tests/services/test_replay_handler_write_contracts.py` | `target_type="unit"` placeholder 제거 → 크롤러별 실제 계약 값. `target_type` 대조가 켜지면 placeholder는 거부 가드에서 멈춰 테스트 대상 handler에 도달하지 못한다 |

검증:
- replay 집중: **106 passed** (대상 검증 18 + handler 계약 + write 계약 + run replay + realtime issue + snapshot replay + preview canary).
- `tests/services/` 전체: **1608 passed**.
- `tests/services/` + 관련 crawler 테스트(6종): **1765 passed**.
- mutation: 팀 코드·preview 날짜·역순 범위 가드를 각각 제거 → 해당 계약이 `ReplayOutcome(success=True)`를 관찰하며 실패. 복원 후 통과(이후 DLQ 비재시도 계약, kbo_event URL·slug 불일치, `target_type` 대조, 미존재 경기 종결 계약이 추가되어 현재 18 passed).
- 변경 파일 Ruff/format 통과.

**정직하게 남기는 것**: ① 운영 레터의 잘못된 target 발생 빈도는 여전히 미측정(프로덕션 DB 미접근). ② 거부 outcome 14곳 전부 `error_code="VALIDATION_SCHEMA"`를 달고, `NON_RETRYABLE_CODES` 분류로 `finalize_retry`가 재시도 예산 소진 없이 즉시 `EXHAUSTED`로 종결한다(DLQ 수준 계약 `test_invalid_target_does_not_reschedule`). ③ 커밋은 하지 않았다: dispatcher에 병행 작업자의 미커밋 변경이 함께 있어 커밋 경계는 파일 소유자와 조율이 필요하다.
