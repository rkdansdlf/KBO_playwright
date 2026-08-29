# RAG Store Reconciliation Contract

서로 다른 시점에 빌드된 RAG 저장소(primary `rag_chunks` vs staging sparse/vector)를
비교할 때 발생하는 **시점 드리프트 오탐**을 제거하기 위한 재비교 계약.
2026-08-23 reconciliation(`data/archive/workspace_cleanup_20260823/rag_reconciliation_20260823/`)의
후속 조치다. 당시 결론: staging sparse↔vector는 일관, adb↔staging은
"공유 불변 소스 스냅샷 없이는 비교 불가"(좌 유일 15,992 / 우 유일 13,760 / 해시 불일치 668).

## 도구

- `python3 -m src.cli.rag.reconcile_rag_stores export --side {primary,staging} --out <file.ndjson>`
- `python3 -m src.cli.rag.reconcile_rag_stores compare --left <a.ndjson> --right <b.ndjson> [--as-of ISO8601] --output-dir <dir>`

`primary` = `RAG_INDEX_DB_URL` 세션(Oracle `rag_chunks` 통합 스토어),
`staging` = `PGVECTOR_URL` 세션(pgvector 스토어). 매니페스트는 NDJSON 한 줄 = 청크 1개:

```
{"source_table": "...", "source_row_id": "...", "content_hash": "...",
 "index_version": "...", "index_status": "ACTIVE", "embedding_present": true,
 "updated_at": "2026-08-22T17:40:39+09:00" | null}
```

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
2. `compare --as-of <min(left.exported_at, right.exported_at)>`
3. `unexplained_count == 0` 이면 PASS. 남으면 `*_keys.txt`의 키로 소스 테이블별 원인 조사
4. 결과 요약 JSON은 `reports/rag_reconciliation/<실행시각>/comparison_summary.json`에 기록
   (`reports/`는 gitignore — 대용량 산출물 보관 규칙은 `Docs/runbooks/WORKSPACE_HYGIENE.md`)

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
