# RAG Identity Contract (제안 초안)

2026-08-23 reconciliation(`RAG_RECONCILIATION.md`)에서 확정된 3류 identity 불안정 문제의
근본 해결 계약. **구현은 프로덕션 역사 재인덱스 사이클과 함께 일괄 적용**한다 —
id 체계 변경은 전면 재색인을 수반하므로 단독 선반영하지 않는다.

## 배경: 불안정 identity 3류

| 유형 | 대상 | 문제 | 관측 피해 |
| --- | --- | --- | --- |
| 저장소 의존 값 | `player_season_batting/pitching`, `team_standings_daily` | `source_row_id`에 DB의 **원본 팀 코드**가 그대로 들어감 — 코드 정규화 시점·저장소마다 다른 id 생성 | 4,321키 드리프트 |
| autoincrement PK | `awards`, `player_movements`, `game_play_by_play`, `game_highlights` | 삽입 순서 = id → 저장소마다 다른 행에 같은 번호 | awards 2건, pbp 350건 불일치 |
| 안정 (유지) | `game`(game_id), `game_lineups`(`{game_id}_{side}`), `stat_rankings` | — | — |

## 규칙

- **R1 (정규값 원칙)**: `source_row_id`는 **현재 주(primary) DB의 정규 저장값**에서만
  조립한다. 프로덕션 `player_season_*`는 현대 정규 코드(LG/KIA/DB…)를,
  `team_standings_daily`는 시대 코드(MB/HT…)를 저장하며 각 표 규약이 곧 기준이다.
  빌더가 임의로 코드를 재해석(`resolve_team_code` 등)하지 않는다 — 재색인이 곧
  정규화된 id를 생성하는 경로다. 원본 보존이 필요하면 청크 메타데이터(`team_id`)에만 둔다.
- **R2 (콘텐츠 유도 키 — 별도 사이클로 연기)**: autoincrement PK 의존
  (`awards`, `player_movements`, `game_play_by_play`, `game_highlights`)은 대량
  기존 청크(12만+)를 재색인해야 하는 변경이라 본 사이클에서 제외한다. 역사 재색인
  대상(game·standings·player_season)과 무관한 소스이며, 전환 시 별도 사이클을
  열어 아래 방향을 유지한다:
  - `awards`: `{year}_{award_type}_{player_name}`
  - `player_movements`: `{movement_date}_{player_id}_{before}->{after}`
  - `game_play_by_play`: `{game_id}_{inning}_{play_seq}`
  - `game_highlights`: `{game_id}_{document_type}_{seq}`
- **R3 (계약 버전)**: id 체계 변경 시 `rag_chunks.index_version`을 올리고
  `Docs/references/rag_source_contract.json`의 규격을 함께 갱신한다.
  구버전 id 청크는 `tombstone_rag_chunks`로 무효화한다.

## 적용 대상 매핑 (역사 재색인 사이클)

| source_table | 현행 | 변경 후 |
| --- | --- | --- |
| player_season_batting | `{pid}_{season}_{team}_{league}` | **변경 없음** — 현재 DB 정규값 사용(재색인으로 구버전 raw-id 청크 자동 대체) |
| player_season_pitching | 동일 | 동일 |
| team_standings_daily | `{standings_date}_{시대 team}` | **변경 없음** — 동일 |
| game | `{game_id}` | **변경 없음** — 안정 자연키 |
| awards / movements / pbp / highlights | `str(id)` | R2 (별도 사이클) |

## 마이그레이션 경로

1. 본 계약 구현 PR (빌더 수정 + `rag_source_contract.json` 갱신 + 테스트)
2. `OPERATIONAL_RUNBOOK.md` §3-3 역사 재인덱스와 **동일 사이클**에서 전체 재색인
3. 재색인 후 `reconcile_rag_stores compare --fail-on-unexplained` 게이트 통과 확인
4. 구버전 id 잔존분 tombstone 처리

## 참고

- 드리프트 실측: `data/archive/workspace_cleanup_20260823/rag_reconciliation_20260823/gap_resolution_summary.json`
- 검증 도구: `src.cli.rag.reconcile_rag_stores`
