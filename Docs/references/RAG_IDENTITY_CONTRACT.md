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

- **R1 (정규값 원칙)**: `source_row_id` 구성 요소에는 빌더 실행 시점의 저장소 상태에
  의존하는 값을 쓰지 않는다. 팀 코드 자리는 반드시
  `src.utils.team_codes.resolve_team_code()`의 정규 결과(현행 코드)를 사용하고,
  원본 보존이 필요하면 청크 메타데이터(`team_id`)에만 둔다.
- **R2 (콘텐츠 유도 키)**: autoincremnet PK를 identity로 쓰지 않는다. 저장소 공통으로
  재현 가능한 속성 조합으로 대체한다:
  - `awards`: `{year}_{award_type}_{player_name}` (동명 이인 분기 필요 시 `_` + team_code 정규값)
  - `player_movements`: `{movement_date}_{player_id}_{before_team}->{after_team}`
    (`player_id` 결손분은 이름 해시 접미사로 대체)
  - `game_play_by_play` / `game_events`: `{game_id}_{inning}_{play_seq}`
    (provider 이벤트 번호는 메타데이터로만 보존)
  - `game_highlights`: `{game_id}_{document_type}_{seq}`
- **R3 (계약 버전)**: id 체계 변경 시 `rag_chunks.index_version`을 올리고
  `Docs/references/rag_source_contract.json`의 `source_row_id_rules`를 함께 갱신한다.
  구버전 id 청크는 `tombstone_rag_chunks`로 무효화한다.

## 적용 대상 매핑 (build_rag_index)

| source_table | 현행 | 변경 후 |
| --- | --- | --- |
| player_season_batting | `{pid}_{season}_{RAW team}_{league}` | `{pid}_{season}_{정규 team}_{league}` |
| player_season_pitching | 동일 | 동일 |
| team_standings_daily | `{standings_date}_{RAW team}` | `{standings_date}_{정규 team}` |
| awards | `str(id)` | R2 |
| player_movements | `str(id)` | R2 |
| game_play_by_play / events | `str(event.id)` | R2 |
| game_highlights | `str(id)` | R2 |
| game / game_lineups / stat_rankings / team_profiles | — | 유지 |

## 마이그레이션 경로

1. 본 계약 구현 PR (빌더 수정 + `rag_source_contract.json` 갱신 + 테스트)
2. `OPERATIONAL_RUNBOOK.md` §3-3 역사 재인덱스와 **동일 사이클**에서 전체 재색인
3. 재색인 후 `reconcile_rag_stores compare --fail-on-unexplained` 게이트 통과 확인
4. 구버전 id 잔존분 tombstone 처리

## 참고

- 드리프트 실측: `data/archive/workspace_cleanup_20260823/rag_reconciliation_20260823/gap_resolution_summary.json`
- 검증 도구: `src.cli.rag.reconcile_rag_stores`
