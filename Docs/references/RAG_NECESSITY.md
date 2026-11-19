# RAG가 필요한가 — 정형/서술 비율 측정 (2026-11-20)

읽기 전용 측정이다. 데이터베이스에 연결하지 않고, 인덱스도 쓰지 않는다.
입력은 체크인된 골든셋 두 개와 코드 자신이다.

측정 도구: `python3 -m src.cli.rag.audit_retrieval_necessity`
(`kbo rag necessity`), 커밋 `HEAD` 기준.

---

## 0. 이 문서가 답하는 질문

`evaluate_rag_retrieval`은 **"검색이 정확한가"** 를 답한다. 이 문서는 다른 것을 묻는다.

> **"그 질문은 SQL로 답할 수 있었는가?"**

`player_season_batting` 한 행이 relevant chunk인 질문은 **테이블 조회**이고,
그것이 벡터 검색으로 도착한 것은 우연이다. `kbo_regulations`의 문단이 relevant
chunk인 질문만이 텍스트 인덱스를 **요구**한다. RAG를 유지할 근거는 두 번째뿐이다.

---

## 1. 결론 먼저

**30건 중 3건(10.0%)만이 진짜 산문이다.** 나머지는 전부 관계형 테이블의 행이고,
그중 16건(53.3%)은 **이미 SQL 템플릿이 있다**(`StructuredRetriever`).

```
queries=30 classified=30 unknown=0 (0.0%)
sql_covered=16 (53.3%) relational_no_template=11 prose=3 (10.0%)
verdict=sql_expansion_warranted
```

판정은 `sql_expansion_warranted` — **RAG가 하는 일의 90%는 SQL 조회의 우회로다.**
따라서 다음 투자는 "검색기를 더 좋은 검색기로 교체"가 아니라 **`StructuredRetriever`
확장**이다. 다만 이것은 **RAG 폐기가 아니다**(§4.2, §5).

---

## 2. 방법 — 목록은 복사하지 않고 코드에서 유도한다

이 저장소는 목록을 손으로 적어둔 대가를 이미 여러 번 치렀다. `league_type_code=0`이
16곳에 하드코딩돼 **2018~2026 모든 연도의 포스트시즌이 4개월간 인덱싱되지 않았고**
감사·커버리지 게이트는 초록이었다. 그래서 이 도구는 경계를 **소유한 코드에서 읽는다**.

| 버킷 | 판정 근거 | 유도 지점 |
| --- | --- | --- |
| `prose` | Markdown 문서에서 만들어진 청크 | `build_rag_index._MARKDOWN_SOURCE_TABLES` |
| `sql_covered` | 이미 구조화 검색 분기가 있는 테이블 | `structured_retriever.py`의 `source_table="..."` 리터럴 (AST 스캔) |
| `relational_no_template` | 테이블 행이지만 분기가 없음 | `build_rag_index._SOURCE_TABLE_BY_SOURCE` − 위 두 집합 |
| `unknown` | 케이스가 테이블을 선언하지 않음 | 골든셋의 `filters.source_table` 부재 |

세 가지 규칙이 여기에 걸려 있다.

1. **`prose`를 먼저 검사한다.** 어떤 테이블이 두 집합에 동시에 속하게 되면(예: 표를
   파싱해 만든 행) 그 청크는 **SQL 조회로 세지 않는다.** 이 순서가 뒤집히면 산문이
   `sql_covered`로 새고, 그 오염은 "SQL로 다 된다"는 결론 쪽으로만 작동한다.
2. **`unknown`은 어느 쪽도 아니다.** 선언이 없는 것은 "산문이다"도 "SQL로 된다"도
   아니다. 비율은 **분류된 케이스**에 대해서만 계산하고, 미분류가 10%를 넘으면 판정을
   `undetermined`로 내린다.
3. **빈 유도는 오류다.** 집합이 비면 모든 케이스가 "템플릿 없음"이 되고, 그것은
   발견이 아니라 측정 실패다. `RuntimeError`로 멈춘다.

---

## 3. 실측

### 3.1 전체 버킷 (`Docs/references/rag_golden_queries.json`, 30건)

| 버킷 | 건수 | 비율 | 의미 |
| --- | ---: | ---: | --- |
| `sql_covered` | 16 | 53.3% | 오늘 이미 SQL로 답할 수 있다 |
| `relational_no_template` | 11 | 36.7% | SQL로 답할 수 있으나 분기가 없다 |
| `prose` | 3 | 10.0% | 텍스트 인덱스가 필요하다 |
| `unknown` | 0 | 0.0% | — |

### 3.2 source_table 분해

| source_table | 건수 | 버킷 |
| --- | ---: | --- |
| `player_season_batting` | 5 | `sql_covered` |
| `player_season_pitching` | 5 | `sql_covered` |
| `team_standings_daily` | 5 | `sql_covered` |
| `player_basic` | 1 | `sql_covered` |
| `game` | 2 | `relational_no_template` |
| `teams` | 2 | `relational_no_template` |
| `awards` | 2 | `relational_no_template` |
| `team_history` | 2 | `relational_no_template` |
| `game_highlights` | 1 | `relational_no_template` |
| `game_play_by_play` | 1 | `relational_no_template` |
| `player_movements` | 1 | `relational_no_template` |
| `kbo_regulations` | 2 | `prose` |
| `kbo_definitions` | 1 | `prose` |

템플릿이 있는 4개 테이블이 **골든셋의 16/30을 이미 덮는다.** 즉 이 30건에서
검색의 부담은 "선수 시즌 기록·팀 순위·프로필"에 집중돼 있고, 그 셋은 전부
구조화된 키로 접근 가능하다.

### 3.3 라우팅 100건 (`Docs/references/rag_routing_golden_queries.json`)

```
STRUCTURED=60 DOCUMENT=35 MIXED=5 → retrieved_share=0.40
```

라우터는 **40%의 질문에만 검색을 호출한다.** 그리고 이 라벨에서
`intent_accuracy=1.0`, `route_accuracy=1.0`, `entity_accuracy=1.0`다
(`rag_configured_evaluation.json`).

여기에 정직하게 붙일 단서: **그 1.0은 이 라벨로 규칙을 만들었기 때문에 1.0이다.**
표본 밖 성능이 아니라 상한 추정치로 읽어야 한다. 다만 `DOCUMENT` 35건 중 16건이
`GAME_QUERY` — 즉 **테이블 행 조회인데 문서 경로로 간 것** — 이므로, 라우터가
완벽해도 "검색으로 간 40%"의 상당 부분은 여전히 SQL로 대체 가능하다.

---

## 4. 해석 — 어디까지 말할 수 있고, 어디부터 말할 수 없는가

### 4.1 이 수치가 말하는 것

**골든셋 기준으로**, 이 시스템이 받는 질문 중 **약 90%는 관계형 행 조회**이고
**10%만이 산문**이다. 그 90% 중 절반 이상은 이미 SQL 경로가 있다.

이것은 "RAG 품질이 나쁘다"는 말이 아니다. 품질은 이미 천장이다
(`resolver_hybrid` recall@5 0.9818 / hit 1.0 / MRR 0.8889). **비싼 도구가
싼 도구로도 되는 일을 하고 있다**는 뜻이다.

### 4.2 이 수치가 말하지 않는 것

**L3 — 실사용 질의 분포 — 는 측정 불가다.** API와 `RagAnswerService` 어디에도
질의 로그나 질의 테이블이 없다(`grep` 0건). 그러므로 위 비율은 **골든셋의 성질**이지
**트래픽의 성질**이 아니다.

이 구분을 무시하면 두 방향으로 틀린다.

- 골든셋이 실제 사용자 질문보다 정형적이라면 → 10%는 **과소**추정이고, 산문이 더 많다.
- 반대로 골든셋이 우리가 잘 답하는 것 위주로 만들어졌다면 → **과대**추정이고, 실제로는
  더 정형적이다.

그래서 이 문서의 숫자는 **방향 지표**다. `rag_configured_evaluation.json`의
`promotion_approved=false`와 골든셋의
`status: staging_acceptance_pending_threshold_budget_approval`이 같은 이유로 붙어 있다.
**SLA가 아니다.**

또한 `dataset_sha256`이 고정된 30건이다. 골든셋이 바뀌면 §3의 표는 재측정 없이
유효하지 않다 — §6의 절차를 다시 돌려야 한다.

---

## 5. 결정 규칙 (측정 **전에** 정한 임계값)

사후에 임계값을 맞추면 어떤 수치든 결론을 만들 수 있다. 그래서 순서를 고정했다.

| 산문 비율 | 판정 | 조치 |
| --- | --- | --- |
| **≤ 15%** | `sql_expansion_warranted` | `StructuredRetriever` 확장 착수 |
| 15% ~ 50% | `decompose_by_source_table` | §3.2처럼 테이블별로 쪼개서 판단 |
| **≥ 50%** | `keep_hybrid` | 의미 검색이 값을 하고 있다 — 현 구성 유지 |

미분류가 10%를 넘으면 위 셋 중 어느 것도 주장하지 않고 `undetermined`를 낸다.

**현재 값 10.0% → `sql_expansion_warranted`.**

우선순위는 §3.2의 `relational_no_template` 건수 순이다. `game`(2)·`teams`(2)·
`awards`(2)·`team_history`(2)가 1순위이고, 넷을 합치면 8/30을 추가로 덮는다.
그렇게 되면 SQL 커버리지는 16 → 24/30(80%)이 되고, 남는 검색 수요는 산문 3건 +
`game_highlights`/`game_play_by_play`/`player_movements` 3건이다.

**단, 이 확장은 RAG 제거가 아니다.** §4.2의 10%는 골든셋이 과소평가했을 수 있고,
그 10%는 정확히 텍스트 인덱스 없이는 **0%**가 되는 질문이다. 산문 3건을 SQL로
바꾸려는 시도는 정의·규정 문서를 파싱해 규칙 엔진을 만드는 일이며, 그건 검색을
없애는 게 아니라 다른 종류의 취약성을 추가하는 것이다.

---

## 6. 재측정 절차

```bash
# 1) 비율 측정 (DB 접속 없음)
python3 -m src.cli.rag.audit_retrieval_necessity \
  --dataset Docs/references/rag_golden_queries.json \
  --routing-dataset Docs/references/rag_routing_golden_queries.json \
  --json

# 2) 회귀 게이트 — SQL 커버리지가 줄었거나 케이스가 분류 불가가 되면 exit 1
python3 -m src.cli.rag.audit_retrieval_necessity --require-sql-covered 0.50 --fail-on-unknown
```

| 옵션 | 의미 |
| --- | --- |
| `--dataset` | 분류할 골든셋. 기본 `Docs/references/rag_golden_queries.json` |
| `--routing-dataset` | 선택. `expectedRoute` 분포를 함께 보고 |
| `--require-sql-covered R` | `sql_covered` 비율이 R 미만이면 exit 1 |
| `--fail-on-unknown` | `source_table`을 선언하지 않은 케이스가 있으면 exit 1 |
| `--json` | 기계 판독용. `thresholds`가 수치와 함께 실린다 |

종료 코드: `0` 통과 / `1` 게이트 실패 / `2` 데이터셋 계약 위반(빈 파일, 없음,
`source_table`을 하나도 선언하지 않은 파일).

마지막 항목이 중요한 이유: 라우팅 골든셋에는 `source_table`이 없다. 그 파일을
`--dataset`으로 넘기면 30건이 아니라 **100건 전부 `unknown`** 이 되고, 그것을
"분류 불가"로 보고하면 도구가 고장난 것처럼 보인다. 대신 그 파일은 다른 질문에
답한다는 것을 이름으로 알려주고 exit 2로 멈춘다.

**골든셋을 바꿀 때**: 새 케이스는 `filters.source_table`을 **반드시** 선언해야 한다.
선언이 없으면 그 케이스는 어느 쪽으로도 세지지 않고, 그것이 쌓이면 판정 자체가
`undetermined`가 된다.

---

## 부록 A. AtlasKV / HiKVP 검토 기록 (2026-11-20)

6개월 뒤 반드시 다시 올라올 질문이라 근거를 남긴다.

### A.1 무엇인가

**AtlasKV** (ICLR 2026 poster, HKUST-KnowComp, arXiv 2510.17934, MIT, 코드 공개)는
지식그래프 트리플을 파인튜닝된 Llama-3.1-8B의 KV 캐시로 인코딩해 두고, 추론 시
어텐션 안에서 직접 읽는다. 외부 검색기·벡터DB·RAG 파이프라인을 제거하는 것이
주장이다. 계층 구조는 루트(128) → 중간(64) → 리프(16)의 3단이고 복잡도는
`O((C_t·M^(1/3) + N)·N·D)`다. 논문이 보고한 것은 **ACC@k와 peak VRAM**이며,
오프라인 KGKV 인코딩 비용은 측정에서 제외한다고 명시한다.

### A.2 채택하지 않는 다섯 가지 이유

1. **이 코퍼스 규모에서 서브리니어 이득이 이미 포화다.**
   지배항은 `M`이 아니라 `(k_R+k_I)·M^(1/3)`이다. `M=207,305`면 `M^(1/3)≈59`,
   프루닝이 훑는 키는 약 11,300개다. 논문의 네 자리수 이득은 `M≈10⁹`에서 성립한다.
   207k에서는 선형과 서브리니어의 차이가 수 밀리초이고, 이 저장소는 그 이득을
   이미 `idx_rag_chunks_embedding_hnsw`(2.9GB, 2026-10-31 빌드)로 확보했다.
   `resolver_hybrid` p95 **386.91ms**가 그 위의 값이다.

2. **"RAG를 버린다"가 아니라 "검색을 GPU로 옮긴다"다.**
   root→inter→leaf로 top-k씩 좁히는 것은 알고리즘적으로 **계층 ANN 인덱스**다.
   ivfflat이 1단계인 것을 3단계로 늘린 것, DiskANN/Vamana의 beam search,
   ScaNN의 partitioning과 같은 계열이다. 달라지는 것은 **어디서 도는가**와
   키가 트리플에서 유도된 QKV라는 것뿐이다.

3. **관측 불가능한 실패 모드가 이 저장소의 최대 반복 결함과 같은 구조다.**
   계층 프루닝은 상위 1회의 오판을 `k_R`개 서브트리 손실로 **증폭**시킨다.
   이 저장소는 그 구조를 4개월간 겪었다 — `league_type_code=0` 하드코딩이
   포스트시즌 전체를 인덱스에서 지웠고 감사는 초록이었다. 결정적 차이는
   **볼 수 있는가**다. ANN 오판은 `EXPLAIN`·`last_trace`·`pg_stat`으로 보인다
   (ivfflat의 27초가 그렇게 잡혔다). 어텐션 내부 프루닝 오판은 볼 수단이 없다 —
   게이트를 만들려면 원본 트리플과 모델 출력을 대조해야 하고, 그것은 "지식이
   파라메트릭하다"는 전제와 모순된다.

4. **측정 축이 다르다.** 논문은 ACC@k와 peak VRAM을 재고, 이 저장소는
   wall-clock p95와 동시성을 잰다. 논문은 동시성 하 처리량도, 프루닝이 요구하는
   단계별 host↔device 왕복 비용도 보고하지 않는다. 수치를 직접 비교할 수 없다.

5. **GPU는 프로젝트 범위 밖이다.** `Docs/references/SKILL_LICENSE_REVIEW.md`가
   GPU 스킬을 프로젝트 범위 밖으로 명시한다. HiKVP만 떼어 쓸 수도 없다 —
   프루닝할 대상(파인튜닝된 8B)이 없으면 계층은 의미가 없다.

### A.3 대신 가져온 것 — 데이터 계층의 계층 축소

GPU 없이 같은 아이디어의 절반은 이미 구현돼 있다. `StructuredRetriever`가
팀·선수·시즌으로 먼저 좁히고, 벡터 경로는 ID 사전 조회 후 `IN`으로 거리를
계산한다(`16341eba`, 2026-10-31). 남은 결함은 **필터 적용 순서**다 —
ivfflat/HNSW는 필터를 스캔 **후에** 적용해서 `team_id='KIA'`가 2,046ms였다.
pgvector 0.8의 `hnsw.iterative_scan`이나 (팀·시즌·경기일) → ID 집합 → 정확
거리 계산으로 **명시적 2단 축소**를 고정하면, HiKVP와 같은 형태를 감사 가능한
층에서 얻는다.

### A.4 재검토 트리거

다음 중 하나가 참이 되면 이 부록을 다시 읽고 판단을 갱신한다.

- 코퍼스가 **10⁷ 청크**를 넘어 HNSW 빌드 시간·메모리가 문제가 될 때
- 이 저장소가 GPU 추론을 정식으로 도입할 때 (그때는 순서가 바뀐다)
- 외부 임베딩 API 의존이 비용·가용성 문제로 유지 불가가 될 때
- "검색 결과를 감사할 수 없어도 된다"는 제품 결정이 날 때

---

## 부록 B. 미해결 잔여

| # | 항목 | 상태 |
| --- | --- | --- |
| 1 | **L3 실사용 질의 로그 부재** — 비율이 트래픽의 성질인지 말할 수 없다 | 미해결. 질의 로그 테이블이 필요 |
| 2 | `rag_configured_evaluation.json` 골든셋이 30건 고정 | 표본이 작다. 산문이 3건이면 1건 차이가 3.3%p |
| 3 | 라우팅 `route_accuracy=1.0`이 표본 내(in-sample) 값 | 표본 밖 측정 없음 |
| 4 | `relational_no_template` 11건의 SQL 비용 미산정 | `game`·`awards` 등 분기 추가 시 JOIN 깊이·인덱스 비용 측정 필요 |
| 5 | 필터 후 적용 순서(`team_id` 2,046ms) | §A.3. `hnsw.iterative_scan` 미적용 |

---

## 관련 문서

- `Docs/references/RAG_EVALUATION.md` — 검색 아키텍처와 측정 방법
- `Docs/references/RAG_LATENCY_BUDGET.md` — 지연 예산과 P1~P5 실행 결과
- `Docs/references/RAG_IDENTITY_CONTRACT.md` — 청크 신원 계약
- `Docs/runbooks/RAG_ANSWER_PIPELINE.md` — 질의 경로 (`QueryRouter → Structured/HybridRetriever → RagAnswerService`)
- `Docs/references/rag_configured_evaluation.json` — variant별 기준선
