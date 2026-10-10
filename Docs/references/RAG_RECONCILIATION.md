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

| 환경 | `primary` | `staging` | 역할 | 비교의 의미 |
| --- | --- | --- | --- | --- |
| `PGVECTOR_URL` 설정 (현재 운영) | 운영 DB (`RAG_INDEX_DB_URL` 없음) | pgvector | `sparse` / `dense` | **독립된 두 스토어** — 실제 교차 검증 |
| `PGVECTOR_URL` 미설정 + 운영 DB가 Oracle | Oracle | Oracle (폴백) | `dense` / `dense` | **자기 비교** — 불일치가 나올 수 없다 |

`role`은 side 이름이 아니라 `get_vector_session()`의 해석에서 파생된다(그 세션이 dense
검색이 읽는 저장소다). 그래서 Oracle 단일 저장소에서는 `primary`가 `sparse`가 아니라
**`dense`**이고, 그 상태에서 임베딩 검사를 면제하면 **검사해야 할 유일한 쪽을 면제**하게
된다. export 시점에 각 행에 `role`이 기록되므로, 나중에 읽는 쪽이 파일 이름으로 추측할
필요가 없다.

두 번째 행이 위험하다. `staging`이 `primary`로 폴백하면 두 매니페스트가 같은
세션에서 나오므로 `unexplained == 0`이 **구성상 보장**되고, 그것을 독립 검증으로
읽으면 안 된다. `2026-08-28 Exporter Verification`의 "clean self-comparison"이
정확히 이 경우다.

**현재 운영 구성 (2026-10-10 실측)**: 두 저장소 모두 PostgreSQL이다 —
`DATABASE_URL`(운영, `100.81.73.13:5432`)과 `PGVECTOR_URL`(`100.81.73.13:55433`).
`RAG_INDEX_DB_URL`은 설정돼 있지 않다. **Oracle은 이 배포의 RAG 저장소가 아니다** —
`build_rag_index`가 "Oracle production builds must not use PGVECTOR_URL"로 두 구성을
상호 배타로 강제하므로, `PGVECTOR_URL`이 설정된 이 배포는 Oracle 빌드가 될 수 없다.

`rag_chunks` 행 수는 **재색인이 진행 중이라 변동한다** — 223,114(10/10 직접 조회) →
228,681(10/10~10/11 export). 행 수를 인용할 때는 export 시각을 함께 볼 것.

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

**판정: 설계상 비대칭이며, 게이트가 뒤처진 것이다 (2026-10-11 결정).** 근거의 무게가
한쪽으로 기울었고, 처음 "결함일 수 있다"고 적었던 항목은 철회한다.

- **`_pair_findings`의 양쪽 요구가 잘못이다.** `left.embedding_present is False **or** right...`는
  sparse 저장소에도 dense 컬럼을 요구하는데, **sparse 검색은 그 컬럼을 읽지 않는다.**
  PostgreSQL sparse 검색은 `RagSearchEngine._postgresql_candidates()`가 `to_tsvector` +
  `plainto_tsquery`로 수행하고, 그 세션은 `get_rag_index_session()`(운영 DB)이다.
  dense는 `vector_search_repository`가 `get_vector_session()`(pgvector)**만** 쓴다.
  즉 저장소 역할이 실제로 갈려 있으므로 이 검사는 **한쪽만 소유한 컬럼을 비교**하고 있다.
- **`rag_chunk_terms`를 sparse 근거로 쓰지 말 것**: 운영 DB에 4,287,429행이 있지만,
  PostgreSQL 검색 경로가 그 테이블을 사용한다는 증거가 아니다. 그 테이블을 읽는 것은
  `OracleSparseSearchRepository`이며 Oracle 분기에서 쓰인다. 행 수는 데이터가 존재한다는
  근거일 뿐 검색 경로의 근거가 아니다 — 별도 성능·정합성 검증 대상으로 둔다.
- **Oracle 단일 저장소는 면제 대상이 아니다.** `is_oracle_vector_backend()`는
  `PGVECTOR_URL`이 없고 운영 DB가 Oracle일 때 native VECTOR를 고른다. 그 구성에서는
  두 side가 같은 세션에서 나오므로 **dense 검사가 그대로 적용되어야** 한다.

**운영 `bega_prod`의 `embedding_vector` 221,656건을 채우지 않는다.** 운영 검색이 읽지
않는 컬럼이고, 백필은 임베딩 비용과 쓰기 위험만 추가한다. 향후 dense 저장소를 통합하기로
결정하면 그때 별도 마이그레이션으로 다룬다 — 현재 결함 수정과 미래 아키텍처 전환을
섞지 않는다.

**따라서 이 게이트의 수정 방향은 역할별 검사 분리다**: sparse side는 임베딩 검사에서
제외하고, dense side는 활성 청크에 대해 필수로 요구한다. identity·content hash·
index version·index status 일치는 **양쪽 모두 그대로 필수**다. `embedding_present`가
`None`(측정 불가)인 경우는 dense에서 `False`와 구분해 실패 처리해야 한다 — 현재 구현이
`is False`만 보므로 측정 실패가 통과할 여지가 있다.

`unexplained=0`을 독립 검증으로 인용하려던 계획은 이 결과로 **보류**된다. 게이트를 고친 뒤
재측정해야 하며, **221,656건이 사라졌다는 이유만으로 `is_clean=True`를 확정하면 안 된다** —
두 저장소가 같은 228,681개 identity인지, dense 벡터의 차원·제로 벡터가 정상인지까지
함께 확인해야 한다.

## 2026-10-11 게이트 수정 후 재측정

역할별 검사로 고친 뒤 다시 쟀다. **`EMBEDDING_MISSING`은 0이 됐고, 대신 다른 차이가 드러났다.**

| 항목 | 수정 전 | 수정 후 |
| --- | --- | --- |
| 공통 키 | 228,681 | 214,681 |
| `unexplained_count` | 221,656 | **28,000** |
| 종류 | `EMBEDDING_MISSING` | `MISSING_IN_LEFT` 14,000 + `MISSING_IN_RIGHT` 14,000 |
| 해시·버전·상태 불일치 | 0 | 0 |

**수정이 오경보만 없앤 게 아니라 진짜 신호를 드러냈다.** 221,656건의 소음이 28,000건을
가리고 있었고, 그것이 이 게이트를 고친 실질적 이유다.

### 드러난 28,000건은 진행 중인 재색인이다 (결함 아님)

양쪽 모두 `game_play_by_play` 14,000건씩이고 **키 체계가 다르다**:

- `MISSING_IN_LEFT`(운영 DB에만) — 숫자 행 ID: `1991502`, `1991505`
- `MISSING_IN_RIGHT`(pgvector에만) — 복합 ID: `20210403SSWO0_107`, `20210403SSWO0_122`

같은 PBP 청크가 두 저장소에서 **다른 `source_row_id` 체계로 색인**되어 있다. 두 export를
대조하니 이 차이는 export 사이에 생겼고, 양쪽이 **같은 방향으로** 움직였다.

| export | PBP 키 수 | 숫자형 잔여 (primary) | 숫자형 잔여 (staging) |
| --- | --- | --- | --- |
| 1차 | 125,218 | 121,449 | 121,449 |
| 2차 | 125,218 | **54,100** | **40,100** |

즉 숫자 → 복합 ID 재색인이 **진행 중**이고, pgvector가 운영 DB보다 앞서 있다. 키 수는
양쪽 125,218로 동일하므로 유실이 아니다 — 아직 옮겨지지 않은 구간이 `MISSING_IN_*`로
보이는 것이다. **재색인이 끝나면 사라질 차이이며, 게이트에 반영할 대상이 아니다.**
`is_clean=False`를 결함으로 읽지 말 것. 다만 재색인이 멈춘 채 이 상태가 고정되면
그때는 조사 대상이다.

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
