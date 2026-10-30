# RAG 검색 지연 예산과 측정 (2026-10-30)

읽기 전용 측정이다. 코드·인덱스·운영 설정을 바꾸지 않았고, 아래 수치는 실행한 그대로다.
측정 환경은 Mac(원격 `bega_prod`, Tailscale), 커밋 `07de7866` 기준.

## 1. 결론 먼저

**BM25는 병목이 아니다.** 지연을 지배하는 것은 **ivfflat ANN(dense)** 이고, 그 다음이
OpenRouter 임베딩 호출이다. 실측:

| 구성 요소 | warm p50 | 첫 실행 / cold |
| --- | --- | --- |
| **vector (ivfflat)** | **513 ms** | **1,728 ~ 27,052 ms** |
| bm25 | 267 ms | 502 ~ 914 ms |
| embedding (OpenRouter) | 64 ms | 94 ~ 1,867 ms |
| **total** | **514 ms** | **1.5 ~ 20.2 s** |

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
| **P3** | **BM25 로딩 축소** (`load_only`) | 267 ms → ~100 ms 추정 | 낮음 | 이미 `defer(embedding)`만 적용. `content`·`meta`가 행당 수 KB |
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
   현재 기준선: Recall@5 0.9485 / MRR 0.8306(Phase 86).
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

## 8. 하지 않은 것

- 인덱스 생성·재생성, 임베딩 재생성, 운영 설정 변경 — 지시된 제약.
- 코드 수정 — 구현 전 보고가 요구사항이므로 이 문서는 측정과 계획만 담는다.
