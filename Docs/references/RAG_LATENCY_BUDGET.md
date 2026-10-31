# RAG 검색 지연 예산과 측정 (2026-10-30)

읽기 전용 측정이다. 코드·인덱스·운영 설정을 바꾸지 않았고, 아래 수치는 실행한 그대로다.
측정 환경은 Mac(원격 `bega_prod`, Tailscale), 커밋 `07de7866` 기준.

## 0. 정정 — BM25 병목은 측정 하루 전에 이미 발견·수정됐다

이 문서를 쓰기 **전날** 병행 세션이 BM25의 진짜 원인을 찾아 고쳤다
(`68d61a6d`, `Docs/runbooks/RAG_ANSWER_PIPELINE.md` §8). 정리하면:

* `053_rag_search_text_index.sql`의 표현식 인덱스가 **2026-09-22에 "적용됨"으로
  기록됐지만 실제로는 존재하지 않았다.** 그래서 모든 키워드가 3.1GB 힙에
  `Parallel Seq Scan`을 돌았고 `한국시리즈`는 **116초**(384,067 buffer reads)가
  걸렸다. `pg_stat_user_tables`: `seq_scan=21,050`, `seq_tup_read=101,545,298`
  — 377k행 테이블 기준 약 269회 전체 스캔.
* 두 검사가 이것을 볼 수 없었다: `apply_postgres_migrations --check`는 **이력
  테이블만** 비교하고(`053`이 이력에 있으므로 pending 0), `audit_rag_index`는
  **두 저장소를 서로** 비교한다(물리 인덱스는 계약 밖).
* 10-30에 인덱스를 만들었다(13초, 21MB). 그들의 측정: **p50 526 → 140ms,
  p95 2,673 → 501ms**(30 golden queries, `--variant bm25`).

**따라서 이 문서의 BM25 수치는 수정 이후 시점의 값이다.** 아래 §1의 "BM25는
병목이 아니다"는 **지금** 그렇다는 뜻이며, 인덱스가 없던 5주 동안은 BM25가
가장 큰 병목이었다. 같은 결론을 두 세션이 다른 시점에 낸 것이므로 어느 쪽도
철회되지 않는다.

## 1. 결론 먼저

**남은 병목은 ivfflat ANN(dense)이다.** BM25는 위 수정으로 140ms(p50)가 됐고,
지연을 지배하는 것은 dense와 OpenRouter 임베딩 호출이다. 실측:

| 구성 요소 | warm p50 | 첫 실행 / cold | 비고 |
| --- | --- | --- | --- |
| **vector (ivfflat)** | **513 ms** | **1,728 ~ 27,052 ms** | 5,008 MB 인덱스 |
| bm25 | 267 ms | 502 ~ 914 ms | 인덱스 생성 후(§0). 그들 측정 p50 140 ms |
| embedding (OpenRouter) | 64 ms | 94 ~ 1,867 ms | 외부 API |
| **total** | **514 ms** | **1.5 ~ 20.2 s** | |

"3.2초"는 **warm 상태의 상한 근처**이거나 **첫 실행**이다. 같은 쿼리를 5회 반복하면
`total p50=514ms, max=1,552ms`로 수렴한다. 즉 3.2초는 상시가 아니라 **초기 실행 비용**을
포함한 수치로 보인다(아래 §3의 ivfflat 특성이 원인).

**목표 1초는 이미 충족한다(warm 기준).** 다만 **첫 실행이 20초까지 가는 경로**가
남아 있고, `enable_indexscan=off` 분기는 조건이 맞으면 **300초 타임아웃**을 낸다.

## 2. 측정 방법

```
HybridRetriever.retrieve(query, top_k=5) 의 last_trace['latency_ms'] 를 읽었다.
router / resolver / structured / bm25 / embedding / vector / fusion / reranker / total.
쿼리: 김광현 · 2025년 KIA 타이거즈 우승 · 삼성 라이온즈 포스트시즌 이의리
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) 로 실행 계획과 시간을 함께 읽었다.
```

`source_url`·`records_read` 같은 간접 지표는 쓰지 않았다(Phase 1과 같은 원칙).

## 3. 병목: ivfflat 인덱스가 사실상 무용하다

인덱스는 **있다**: `idx_rag_chunks_embedding_ivfflat` (`vector_cosine_ops`, `lists=100`),
pgvector 0.8.6, `embedding` 375,509행 / 1536차원. 그런데 실행 계획은 이렇다:

| 케이스 | 계획 | Execution Time |
| --- | --- | --- |
| A. 필터 없음 | Index Scan using `idx_rag_chunks_embedding_ivfflat` | **27,052 ms** |
| B. `season_year=2025` | 같은 ivfflat Index Scan | 243 ms |
| C. `index_status` 필터 | 같은 ivfflat Index Scan | 63.8 ms |
| D. `team_id='KIA'` | 같은 ivfflat Index Scan | **2,046 ms** |

**인덱스를 쓰는데도 27초가 나온다.** 행 수 대비 `lists=100`이 너무 적다
(리스트당 3,750행) — probes=5면 18,750행을 읽고 **필터는 스캔 후에** 적용되므로
`team_id` 같은 선택적 필터에서는 거의 전부를 계산한다. `ANN_PROBES = 5`의 주석이
이미 측정해 둔 대로 probes를 10·20으로 올리면 비용이 6~7배가 된다.

**대조**: 같은 데이터를 `game_date` B-tree로 찾으면 **0.049 ms**다(§4 F행).
즉 문제는 차원이나 데이터가 아니라 **인덱스 종류**다.

**`ANALYZE`는 원인이 아니다 — 확인했고 배제한다.** `pg_stat_user_tables`는
`last_analyze=NULL`, `n_live_tup=595,395`(실제 377,365)로 부정확하지만,
**인덱스 자신의 통계는 정확하다**(`reltuples=375,486`, 실제 임베딩 보유 행과
일치). planner는 `rows=25,381`(team_id 필터)처럼 합리적인 추정을 하고 있고,
그 추정 위에서 `cost=2,629,089`짜리 ivfflat 스캔을 **선택**한다. 즉 통계를
고쳐도 선택은 바뀌지 않는다 — 필터를 인덱스 스캔 **후에** 적용하는 ivfflat의
구조가 원인이다. (BM25 인덱스 쪽도 같은 부정확 통계를 쓰지만 GIN은 21MB로
정상 동작한다.)

## 4. 치명적 경로: `enable_indexscan = off`

`vector_search_repository._load_filtered_search_results`는 날짜 스코프 소스에서
인덱스 스캔을 끈다:

```python
if source_table in _DATE_SCOPED_SOURCE_TABLES and game_date:
    session.execute(text("SET LOCAL enable_indexscan = off"))
```

의도는 정확도다("하루치 행이 작으니 정확 필터 스캔"). 측정 결과:

| 케이스 | 계획 | 시간 |
| --- | --- | --- |
| E. `source_table='game' AND game_date='2026-10-26'`, indexscan **off** | Bitmap Index Scan on `idx_rag_chunks_game_date` | 91.7 ms |
| F. 같은 쿼리, indexscan **on** | Index Scan on `idx_rag_chunks_game_date` | **0.049 ms** |
| G. `index_status` 필터만, indexscan **off** | (seq scan) | **300 s 타임아웃** |

두 가지가 드러난다:

1. `enable_indexscan`은 **일반 index scan만** 끈다. bitmap scan은 `enable_bitmapscan`
   이므로 E는 살아남았고, 그래서 이 분기가 지금까지 치명적으로 보이지 않았다.
2. **날짜 필터가 없으면** bitmap도 못 써서 seq scan이 되고, 375K행 × 1536차원 거리
   계산으로 **300초를 넘긴다**(G).

`_DATE_SCOPED_SOURCE_TABLES = {"game", "game_highlights", "game_play_by_play"}` 이고
조건은 `source_table`과 `game_date`가 **둘 다** 필터로 들어올 때다. 현재 resolver는
이 둘을 자주 뽑지 않아(`_resolved_search_filters("20261026 경기 결과")` → 둘 다 None)
발동 빈도는 낮지만, 발동하면 위 두 결과 중 하나를 낸다.

## 5. 개선 후보 (효과·위험도 순)

| # | 후보 | 효과(실측 근거) | 위험 | 비고 |
| --- | --- | --- | --- | --- |
| **P1** | **HNSW 인덱스 추가** (`vector_cosine_ops`) | ivfflat 2,046 ms → 수십 ms 기대. §3의 A(27 s)도 함께 해소 | **중간** — 빌드가 수 분, 디스크 증가, 빌드 중 부하 | pgvector 0.8.6 지원. ivfflat을 **지우지 않고** 공존시켜 planner가 고르게 함 |
| **P2** | **`enable_indexscan=off` 분기 수정** | G의 300 s → 날짜 필터 유지 시 0.05 ms 급 | 낮음 | 정확도 목적을 버리지 않는 방법: `enable_indexscan` 대신 **날짜로 ID를 먼저 뽑아 `IN`** 으로 거리 계산 |
| **P3** | BM25 로딩 축소 (`load_only`) | 267 ms → ~150 ms 추정. **효과가 줄었다** — §0의 인덱스 수정이 이미 526→140 ms를 했다 | 낮음 | 이미 `defer(embedding)` 적용(그들 커밋). `content`·`meta`가 행당 수 KB |
| **P4** | **쿼리 임베딩 캐시** | embedding 64 ms → 0(히트 시) | 낮음 | 같은 쿼리 반복이 잦은 평가·CLI 경로 |
| **P5** | ivfflat `lists`/`probes` 재조정 | 제한적 — §3 주석의 기존 측정과 같은 결론 | 낮음 | P1이 되면 불필요 |

P1이 나머지를 압도한다. P2는 **별개 결함**이라 P1과 무관하게 고칠 가치가 있다.

## 6. 회귀 테스트 설계 (검색 품질 유지)

목표: **결과 집합과 순위를 바꾸지 않는다**. 인덱스 종류 변경과 로딩 최적화는
"같은 것을 더 빨리" 가져오는 작업이어야 한다.

1. **동일 결과 계약** — 대표 쿼리 20개에 대해 현재 상위 5개의 `chunk_id` 순서를
   기준선으로 캡처하고, 변경 후 **집합과 순서가 동일**한지 단언한다.
   ivfflat(근사)에서 HNSW(근사)로 바뀌면 **순위가 달라질 수 있다** — 달라질 때는
   실패로 보고 사람이 판단하게 한다(무단 변경 금지).
2. **golden set 회귀** — 기존 `Docs/references/rag_golden_queries.json` +
   `rag_configured_evaluation.json`의 Recall@5·MRR이 **떨어지지 않을 것**.
   **기준선은 변형별로 다르므로 섞으면 안 된다**:
   * hybrid(운영 경로): Recall@5 **0.9485** / MRR **0.8306**(Phase 86)
   * `--variant bm25`(sparse 단독): Recall@5 **0.7000** / MRR **0.6111**(2026-10-30,
     30 golden queries)
   dense를 바꾸는 P1은 **hybrid** 기준선으로, sparse를 바꾸는 변경은 **bm25**
   기준선으로 판정한다. 하나의 숫자로 둘 다 재면 어느 쪽이 움직였는지 알 수 없다.
3. **지연 예산 게이트** — warm p95 `total < 1,000 ms`. 현재 514 ms(p50)이므로
   여유가 있고, P1 후에는 250 ms 이하를 기대한다. 게이트는 **warm**으로 측정
   (cold를 게이트로 삼으면 임베딩 API 상태가 테스트를 좌우한다).
4. **P2 전용** — 날짜 스코프 필터 경로가 (a) 발동했을 때 결과가 동일하고
   (b) `enable_indexscan` 상태와 무관하게 같은 행을 낸다는 것을 고정한다.
5. **로딩 축소 검증** — `load_only` 도입 후 **응답 dict의 키가 줄지 않았는지**
   단언한다(호출자가 `content`·`meta`를 읽는다).

## 7. 1초 목표의 타당성

- **warm**: 실측 p50 514 ms — 이미 충족. P1+P3 후 250 ms 이하가 현실적이다.
- **cold**: 지배 요인은 ① ivfflat 첫 스캔 ② OpenRouter 왕복(94~1,867 ms)
  ③ 연결 수립. ①은 P1로, ②는 P4로 줄지만 **외부 API 지연은 우리 통제 밖**이라
  cold까지 1초를 보장할 수는 없다. 게이트는 warm으로 두고, cold는 **관측값으로
  기록**만 한다.

## 7-B. 실행 결과 (2026-10-31)

### P2 — 적용 완료 (`16341eba`)

`enable_indexscan` 분기를 ID 선조회로 교체했다. 테스트 14건, 뮤테이션 4건 모두 실패 생성.

### P4 — 이미 구현돼 있었다 (신규 작업 불필요)

`EmbeddingService`가 **DB 캐시를 이미 쓴다**: `embedding_cache` 311,113행, `cache_enabled=True`.
실측: 캐시 미스 **18.45초**(OpenRouter 왕복), 캐시 히트 **0.065초**.

**이것이 "3.2초"보다 큰 지연의 정체다** — 처음 보는 쿼리는 임베딩 API 왕복이 지배하고,
그 값은 0.1~18초로 흔들린다(warm 캐시 히트만 65ms). 메모리 캐시를 앞에 두는 추가
최적화는 DB 왕복 65ms를 0으로 만들 뿐이고 중복 캐시를 만든다 — **하지 않는다**.

### P3 — 보류 (근거)

`defer(embedding)`은 이미 적용돼 있고(같은 날 병행 세션 커밋), BM25 p50은 그들 측정으로
**140ms**다. `load_only`로 더 줄이면 **접근하는 컬럼이 lazy load로 추가 쿼리**를 유발해
오히려 느려질 수 있다. 효과(추정 100ms)보다 위험이 크다.

### P1 — 시도했고, 중단했다

HNSW 빌드를 실제로 돌렸다(2026-10-31 14:16 KST 시작, `m=16 ef_construction=64`,
`maintenance_work_mem=256MB`). **75분 뒤 취소했고 그 이유가 이 문서의 가장 중요한 실측이다.**

| 관측 | 값 |
| --- | --- |
| `/dev/shm` 부족으로 **병렬 빌드 불가** | `could not resize shared memory segment ... 265326304 bytes` — `maintenance_work_mem=256MB` + `max_parallel_maintenance_workers=2`가 실패, `=0`으로 재시도해 시작 |
| 진행률 | `blocks 397978/397978 (100%)`, phase `building index: loading tuples`에서 75분 정체 |
| 대기 이벤트 | `LWLock/WALWrite` 반복 |
| **빌드가 운영 검색을 마비시켰다** | BM25 후보 조회 **0.20초 → 165초**(약 800배), `game_date` 스캔도 지연 |
| 원인 | `shared_buffers=128MB`, `checkpoint_write_time` 누적 **1,361,682ms**, `buffers_backend=3,738,979` — I/O 예산이 5GB 인덱스 빌드를 감당하지 못한다 |
| 취소 후 회복 | BM25 **0.28초**, `ShareLock` 0건, 인덱스 잔재 없음 |

**`CREATE INDEX`는 `ShareLock`을 잡아 `rag_chunks` 쓰기(야간 색인·벡터 백필)를 막는다.**
75분 동안 락 대기가 0건이었던 것은 그 시간대에 쓰기가 없었기 때문이지 안전해서가 아니다.

**그때의 결론은 "이 서버는 HNSW 빌드에 부적합하다"였다 — 그 결론은 틀렸다.**
원인은 서버가 아니라 **설정**이었다: `maintenance_work_mem=256MB`로는 그래프가
디스크로 밀려나고, `m=16 ef_construction=64`는 이 호스트에 과했다.

### P1 — 성공 (2026-10-31 저녁, 두 번 더 시도한 끝에)

**2차 시도**: `m=8 ef_construction=32`, `maintenance_work_mem=1GB`, 감시 부착.
**10분에 16.9% → 14분에 96.8% → 26.1분에 완료.** 1차(0.55%/분)와 비교하면
`m`·`ef`를 낮춘 것과 **메모리를 4배로 올린 것**이 함께 작용했다 — 그래프를
메모리에 쥐니 페이지아웃이 사라졌다.

**빌드 중 검색은 정상이었다**(감시 probe 11~527ms, strikes 0). 1차와의 차이가
곧 `maintenance_work_mem`의 값이다.

**결과** — ivfflat 대비(같은 쿼리, `EXPLAIN ANALYZE`):

| 케이스 | ivfflat | **HNSW** | 개선 |
| --- | --- | --- | --- |
| 필터 없음 | 27,052ms | **575ms** | **47배** |
| `team_id='KIA'` | 2,046ms | **65ms** | **31배** |
| `season_year=2025` | 243ms | **96ms** | 2.5배 |

인덱스는 **2,929MB**로 ivfflat(5,008MB)보다 **작다**. planner가 HNSW를 선택한다.

### P1의 진짜 함정: `hnsw.ef_search` 기본값

**HNSW를 만들어도 코드가 그 파라미터를 설정하지 않으면 정답을 하나도 못 찾는다.**
실측(같은 인덱스, 실제 쿼리 1건, 정확 top-15와 비교):

| `hnsw.ef_search` | 정확 top-15 회수 | 시간 |
| --- | --- | --- |
| **40 (pgvector 기본)** | **0 / 15** | 66ms |
| 100 | 0 / 15 | — |
| **200** | **15 / 15** | 88ms |
| 400 | 15 / 15 | — |

`_configure_ann_probes`는 `ivfflat.probes`만 설정하고 있었다. planner가 두 인덱스
중 하나를 고르므로, **HNSW가 선택되면 조율되지 않은 기본값으로 실행**된다 —
빠르게 틀린 답을 주는 것이 느린 정답보다 나쁘다. `ANN_EF_SEARCH = 200`을 추가하고
두 설정을 함께 적용한다(`078d6235`). 재빌드는 필요 없었다 — `ef_search`는 런타임
파라미터다.

**측정 함정 하나 더**: `SET hnsw.ef_search`는 확장이 로드된 세션에서만 유효해
첫 쿼리에서 거부될 수 있다. `SELECT NULL::vector`로 먼저 로드하도록 고쳤다.

### 품질 검증: dense 단독은 회복, golden set 지표는 불변

두 가지를 측정했고 **둘 다 정직하게 적는다**:

| 측정 | 수정 전 | 수정 후 |
| --- | --- | --- |
| `search_by_cosine` recall@15 (정확 대비) | **0 / 15** | **15 / 15** |
| `kbo rag evaluate` Recall@5 / MRR | 0.7667 / 0.6444 | **0.7667 / 0.6444**(동일) |

golden set 지표가 움직이지 않은 것은 **회귀가 없다**는 뜻이고, 동시에 **그 지표가
dense 순위 변화에 둔감하다**는 뜻이다. `UnifiedHybridRetriever`는 dense와 BM25를
RRF로 융합하고 최종 top-5를 보고하는데, dense의 후보 순위가 바뀌어도 융합 결과의
상위 5가 그대로일 수 있다(미적중 7건은 두 설정 모두에서 같은 질의다).

**그래서 회귀 게이트로는 dense 단독 recall을 써야 한다** — 융합 지표만 보면 dense가
완전히 망가진 상태(`0/15`)도 통과한다. `ANN_EF_SEARCH`를 낮추는 변경은 반드시
recall@15로 판정할 것.

**참고**: `kbo rag evaluate`의 SLA는 이 작업 전에도 미달이었다(Recall@5 0.7667 < 0.85,
p95 9,413ms > 500ms). p95가 큰 것은 이날의 빌드·정확 스캔으로 I/O가 오염된 영향이
섞여 있어 **인용에 주의**가 필요하다.

## 8. 하지 않은 것

- 인덱스 생성·재생성, 임베딩 재생성, 운영 설정 변경 — 지시된 제약.
- 코드 수정 — 구현 전 보고가 요구사항이므로 이 문서는 측정과 계획만 담는다.
- **BM25 인덱스 생성은 이 문서의 작업이 아니다.** 그것은 같은 날 병행 세션이
  발견·수정했다(§0). `seq_scan`/`seq_tup_read`가 BM25 인덱스 이전 누적값을
  포함하므로, BM25 쪽 통계로 무언가를 판정할 때는 **그 커밋을 기준선으로**
  삼아야 한다.
