# RAG Store Reconciliation Contract

서로 다른 시점에 빌드된 RAG 저장소(primary `rag_chunks` vs staging sparse/vector)를
비교할 때 발생하는 **시점 드리프트 오탐**을 제거하기 위한 재비교 계약.

**계약 본문은 "도구"와 "백엔드 해석" 두 절이다.** 그 아래 `2026-08-28` 항목들은
그 시점의 실측 기록이며 현재 구성을 서술하지 않는다 — 읽을 때 날짜를 먼저 볼 것.

**작성 배경(2026-08-23, 역사)**: `data/archive/workspace_cleanup_20260823/rag_reconciliation_20260823/`의
후속 조치다. 당시 결론은 staging sparse↔vector는 일관, adb↔staging은
"공유 불변 소스 스냅샷 없이는 비교 불가"(좌 유일 15,992 / 우 유일 13,760 / 해시 불일치 668).
그때는 Oracle이 저장소였으므로 "adb↔staging"이 곧 교차 비교였다 — 지금은 그렇지 않다.

## 도구

- `python3 -m src.cli.rag.reconcile_rag_stores export --side {primary,staging} --out <file.ndjson>`
- `python3 -m src.cli.rag.reconcile_rag_stores compare --left <a.ndjson> --right <b.ndjson> [--as-of ISO8601] --output-dir <dir>`

두 side는 **백엔드 이름이 아니라 세션 해석 결과**로 정의된다. 이 구분을 놓치면
설정이 바뀔 때 문서가 조용히 거짓이 된다 — 실제로 그렇게 됐다(아래 "백엔드 해석").

- `primary` = `get_rag_index_session()` — `RAG_INDEX_DB_URL`이 있으면 그 세션, 없거나
  `DATABASE_URL`과 같으면 **운영 DB로 폴백**한다.
- `staging` = `get_vector_session()` — `PGVECTOR_URL`이 있으면 pgvector, 없고 운영 DB
  dialect가 oracle이면 `primary`와 **같은 세션으로 폴백**한다(`is_oracle_vector_backend()`).

매니페스트는 NDJSON 한 줄 = 청크 1개:

```
{"source_table": "...", "source_row_id": "...", "content_hash": "...",
 "index_version": "...", "index_status": "ACTIVE", "embedding_present": true,
 "updated_at": "2026-08-22T17:40:39+09:00" | null}
```

## 백엔드 해석

side 이름이 백엔드를 함의하지 않는다. 무엇을 비교하게 되는지는 환경이 결정한다.

| 환경 | `primary` | `staging` | 비교의 의미 |
| --- | --- | --- | --- |
| `PGVECTOR_URL` 설정 (현재 운영) | 운영 DB (`RAG_INDEX_DB_URL` 없음) | pgvector | **독립된 두 스토어** — 실제 교차 검증 |
| `PGVECTOR_URL` 미설정 + 운영 DB가 Oracle | Oracle | Oracle (폴백) | **자기 비교** — 불일치가 나올 수 없다 |

두 번째 행이 위험하다. `staging`이 `primary`로 폴백하면 두 매니페스트가 같은
세션에서 나오므로 `unexplained == 0`이 **구성상 보장**되고, 그것을 독립 검증으로
읽으면 안 된다. `2026-08-28 Exporter Verification`의 "clean self-comparison"이
정확히 이 경우다.

**현재 운영 구성 (2026-10-10 실측)**: 두 저장소 모두 PostgreSQL이다 —
`DATABASE_URL`(운영, `100.81.73.13:5432`)과 `PGVECTOR_URL`(`100.81.73.13:55433`).
양쪽 `rag_chunks`가 동일하게 223,114행이므로 위 표의 첫 번째 행에 해당한다.
`RAG_INDEX_DB_URL`은 설정돼 있지 않다. **Oracle은 이 배포의 RAG 저장소가 아니다** —
`build_rag_index`가 "Oracle production builds must not use PGVECTOR_URL"로 두 구성을
상호 배타로 강제하므로, `PGVECTOR_URL`이 설정된 이 배포는 Oracle 빌드가 될 수 없다.

## 스냅샷 의미론 (as-of 분류)

양쪽 모두 실시간으로 변하므로, 키 단위 차이를 `updated_at` 기준으로 3분류한다.

| 분류 | 조건 | 해석 |
| --- | --- | --- |
| `UNEXPLAINED_*` | 양쪽 `updated_at <= as_of`인데 해시/버전/상태 불일치 또는 한쪽 부재 | 진짜 드리프트 — 수동 조사 대상 |
| `TIME_EXPLAINABLE` | 공통 키인데 한쪽이라도 `updated_at > as_of` | 비교 창 백그라운드 변경 — 정상 |
| `*_ONLY_AFTER_CUTOFF` | 한쪽에만 있고 그쪽 `updated_at > as_of` | 후행 동기화 예정분 — 정상 |

`updated_at`이 없는 매니페스트(null)는 as-of 분류를 적용하지 않고 기존 방식대로
불일치로 집계한다(보수적). `--as-of` 미지정 시 전부 UNEXPLAINED 규칙으로 계산.

## 절차

1. primary/staging 각각 `export` (타깃 DB 쓰기 잠금 없음, read-only)
2. `compare --left <a.ndjson> --right <b.ndjson>` — 아래 "as-of 사용 불가" 참고
3. `unexplained_count == 0` 이면 PASS. 남으면 `*_keys.txt`의 키로 소스 테이블별 원인 조사
4. 결과 요약 JSON은 `reports/rag_reconciliation/<실행시각>/comparison_summary.json`에 기록
   (`reports/`는 gitignore — 대용량 산출물 보관 규칙은 `Docs/runbooks/WORKSPACE_HYGIENE.md`)

### as-of 사용 불가 (2026-10-10 확인)

**이 절차의 이전 판본이 지시한 `--as-of <min(left.exported_at, right.exported_at)>`은 실행할 수 없다.**
**매니페스트에 `exported_at`이 없다.** 실측 필드는 `source_table`·`source_row_id`·
`content_hash`·`index_version`·`index_status`·`embedding_present`·`updated_at` 일곱 개뿐이고
(`ManifestEntry.to_manifest_dict`), 코드베이스 전체에 `exported_at`이라는 이름이 없다.

`--as-of`를 생략하면 전부 UNEXPLAINED 규칙으로 계산되어 **보수적 방향으로 실패**하므로
안전하지만, 그 상태로는 `TIME_EXPLAINABLE`이 영영 0이 되어 시점 드리프트를 걸러내려는
이 계약의 목적이 달성되지 않는다. as-of를 쓰려면 **먼저 매니페스트에 내보낸 시각을
기록해야 한다**(미구현).

## 2026-10-10 최초 교차 검증 실측

두 side가 실제로 분리된 뒤(위 "백엔드 해석") **처음 돌린** 비교다. `--as-of` 없이 실행했다.

| 항목 | 값 |
| --- | --- |
| left / right 행 수 | 228,681 / 228,681 |
| 공통 키 | 228,681 (한쪽만 있는 키 **0**) |
| `unexplained_count` | **221,656** |
| `unexplained_by_issue` | `EMBEDDING_MISSING` 단일 종류 |
| `CONTENT_HASH_MISMATCH` / `_MISSING` | 0 |
| `INDEX_STATUS_MISMATCH` / `INDEX_VERSION_MISMATCH` | 0 |

**식별자·해시·버전·상태는 네 종류 모두 일치**하고, 차이는 오직 한 가지 — 운영 DB의
벡터 컬럼이 채워지지 않은 것이다.

| 저장소 | 컬럼 | 타입 | 채워진 행 |
| --- | --- | --- | --- |
| `primary` (운영 DB, 5432) | `embedding_vector` | `json` | **7,025** / 228,681 |
| `staging` (pgvector, 55433) | `embedding` | `vector` | **228,681** / 228,681 |

양쪽 `index_status` 분포는 동일하다(ACTIVE 226,660 / DELETED 2,021).

**이것이 결함인지 설계상 비대칭인지는 확정되지 않았다.** 근거가 양쪽으로 갈린다.

- **결함 쪽 근거**: `_pair_findings`가 `left.embedding_present is False **or** right...`로
  판정한다 — 즉 **양쪽 모두에 벡터가 있기를 요구**한다. `_resolve_embedding_column`이
  "두 저장소가 컬럼명이 달라 dialect 추정이 한쪽을 잘못 골랐다"며 테이블에 물어보도록
  고친 것도 두 저장소의 벡터 컬럼을 **비교하려는** 의도로 읽힌다.
- **비대칭 쪽 근거**: `RAG_INDEX_DB_URL`이 미설정이라 sparse는 운영 DB로 폴백하고 dense는
  `PGVECTOR_URL`이 갖는다. 역할이 갈렸다면 운영 DB의 dense 컬럼이 비어 있는 것이 정상이고,
  그러면 이 게이트는 **한쪽만 소유한 컬럼을 비교**하는 셈이 된다.

`unexplained=0`을 독립 검증으로 인용하려던 계획은 이 결과로 **보류**된다. 위 둘 중
어느 쪽인지는 RAG 저장소 설계 소유자가 정할 문제이며, 이 문서는 판정하지 않는다.

## 주의

- 운영 DB의 `rag_chunks.created_at/updated_at` 컬럼은 추적 마이그레이션 밖에서 추가된 것일 수 있다.
  export는 해당 컬럼 조회를 시도하고 실패하면 timestamps 없이 재시도한다(fallback).
- reconciliation은 절대 양쪽 저장소를 수정하지 않는다(read-only). 수정은
  `propagate_rag_index.py` / `tombstone_rag_chunks.py` 등 전용 경로로만.

## 2026-08-23 갭 원인 규명 결과

보관 매니페스트(08-22 17:40 export) 재분석. staging 전용 13,760청크의 정체:

| 원인 | 건수 | 내용 |
| --- | --- | --- |
| 팀 코드 ID 드리프트 | 4,321 | player_season_batting 2,424 + pitching 1,897. staging은 정규화 코드(KIA/SSG/DB/HH/KH/LG), 프로덕션은 원본 코드(HT/SK/OB/BE/NX/MBC)로 `source_row_id` 구성 → 동일 데이터가 다른 키로 존재 |
| 역사 데이터 미인덱스 | 9,252 | game 8,242(1982~2000 + 2001×3 + 2018×2), team_standings_daily 584(1982), game_lineups 74, game_play_by_play 350, awards 2. **프로덕션 rag_chunks에는 1980·1990년대 game 청크가 0건** (staging은 각 2,725/4,980건) |
| 잔여 시즌 스탯 갭 | 187 | 코드 치환으로도 해소 안 되는 batting 104 + pitching 83 |

**근본 원인(추정)**: 프로덕션 리빌드(08-21)는 역사 백필 소스(1982~2001)를 인덱싱하지 않았고,
staging 빌드(08-20)는 역사 백필이 반영된 소스에서 전체 스코프로 실행됨.
또한 선수 시즌 ID에 팀 코드가 포함되어 코드 정규화 시점 차이가 identity 불일치를 만듦.

**후속 조치 권고**:
1. 프로덕션에 역사 소스(1982~2001) 재인덱스 — 실행 절차는
   `Docs/runbooks/OPERATIONAL_RUNBOOK.md` §3-3 참조
2. `source_row_id`의 팀 코드 자리를 연도·선수만 남기거나, 코드 정규화 규칙을 양쪽 동일 적용
   (identity 계약: `Docs/references/rag_source_contract.json` 갱신 필요)
3. `awards` 등 autoincrement 숫자 ID는 저장소 간 불안정 — 안정 키(year+award_type+player_name 등)로 전환 검토

정합성 게이트(주간 권장): 양쪽 매니페스트를 export 후
`python3 -m src.cli.rag.reconcile_rag_stores compare --as-of <공통 시점> --fail-on-unexplained`.
unexplained > 0이면 원인 조사, TIME_EXPLAINABLE만 증가하면 정상 증분.

증거: `data/archive/workspace_cleanup_20260823/rag_reconciliation_20260823/gap_resolution_summary.json`, `exhaustive_resolution.json`

## 2026-08-28 Oracle Tombstone Audit

The production single-store audit reported 2,021 deleted rows while keeping the
index consistent. A read-only identity audit confirmed that all 2,021 deleted
rows were historical team-code rekeys, not missing source records:

- `player_season_batting`: 1,175 deleted legacy identities, each with exactly
  one current `REGULAR/KBO1` row under a canonical team code.
- `player_season_pitching`: 846 deleted legacy identities, each with exactly
  one current `REGULAR/KBO1` row under a canonical team code.
- Legacy-to-canonical mappings were `BE→HH`, `HT→KIA`, `MBC→LG`, `OB→DB`,
  and `SK→SSG`.
- All deletions were updated in the same bounded batch at
  `2026-08-27T01:33:02` through `2026-08-27T01:33:25`.

This is classified as `EXPECTED_IDENTITY_REKEY`; no restore, purge, or full
reindex is indicated. Evidence is preserved in
`data/recovery/rag_tombstone_identity_rekey_audit_20260828.json`.

The classification is reproducible with the read-only audit command:

```bash
python3 -m src.cli.rag.audit_rag_tombstones --json --fail-on-unexplained
```

The default command only reports findings. `--fail-on-unexplained` is the
explicit gate for automation; it never restores, purges, or reindexes rows.

## 2026-08-28 Exporter Verification

The identity exporter now selects the backend-specific vector column:
`embedding_vector` for Oracle native VECTOR and `embedding` for PostgreSQL
pgvector. The previous generic `embedding` expression produced a false
`EMBEDDING_MISSING` result against Oracle even though the native vector audit
was healthy.

After the fix, primary and staging exports each contained `221,554` rows and
the reconciliation reported `unexplained=0`. This local environment has no
`PGVECTOR_URL`, so `staging` intentionally falls back to the canonical Oracle
session; the result is a clean self-comparison, not independent PostgreSQL
staging evidence. An independent staging gate remains pending until a
separate pgvector endpoint or preserved staging manifest is available.

> **이 절은 2026-08-28 시점 기록이다.** 그때는 `PGVECTOR_URL`이 없어 `staging`이
> `primary`로 폴백했고, 그래서 `unexplained=0`이 구성상 보장되는 자기 비교였다.
> **이후 `PGVECTOR_URL`이 설정되어 두 side가 분리됐고, 2026-10-10에 처음 재실행했다**
> — 결과는 위 "2026-10-10 최초 교차 검증 실측" 참고. **`unexplained=0`이 아니다**
> (221,656건, 전부 `EMBEDDING_MISSING`). 따라서 이 절의 `unexplained=0`은 여전히
> 독립 검증으로 인용할 수 없다 — 이유가 "안 돌려서"에서 "돌렸는데 깨끗하지 않아서"로
> 바뀌었을 뿐이다.

## Tombstone Gate Policy

`rag_audit_sentinel_job` currently runs the sparse/vector consistency and sparse
postings checks only. The tombstone classifier remains a separate read-only
command because a deleted game or document can be intentional and is not
automatically an identity rekey.

- Use `audit_rag_tombstones --json` for report-only monitoring.
- Use `--fail-on-unexplained` only for an explicit review gate; it does not
  restore, purge, or reindex rows.
- Do not add the fail flag to the daily sentinel until approved intentional
  deletion identities have a documented allowlist or source-level reason.
