# RAG Identity Contract

2026-08-23 reconciliation(`RAG_RECONCILIATION.md`)에서 확정된 3류 identity 불안정 문제의
근본 해결 계약. **생성 경로는 적용되었고, 기존 프로덕션 legacy row rekey는 별도
apply 게이트에서 수행한다.** id 체계 변경은 기존 chunk identity와의 충돌 검토를
수반하므로 단독으로 자동 적용하지 않는다.

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
- **R2 (콘텐츠 유도 자연키 원칙 — 2026-08-29 생성 경로 적용)**: autoincrement PK 의존을
  제거하고 저장소 간 100% 일관된 결정론적 자연키를 생성한다:
  - `awards`: `{year}_{award_type}_{category or 'NONE'}_{player_name}` (예: `2025_골든글러브_투수_원태인`)
  - `team_history`: `{season}_{team_code}` (예: `1990_LG`)
  - `milestones`: `{season}_{player_id}_{category}` (예: `2026_50001_홈런`)
  - `futures_schedules`: `{game_id}` (예: `20260401OBHT0`)
  - `player_splits`: `{season}_{player_id}_{split_type}_{split_key}`
  - `player_movements`: `{movement_date}_{team_code}_{player_name}_{section}_{fingerprint}`
  - `game_play_by_play`: `{game_id}_{source_row_index}`, or a content digest when the source index is absent
  - `game_highlights`: `{game_id}_{highlight_type}_{event_seq}`, or a description digest for summary rows
- **R3 (계약 버전)**: id 체계 변경 시 `rag_chunks.index_version`을 올리고
  `Docs/references/rag_source_contract.json`의 규격을 함께 갱신한다.
  구버전 id 청크는 `tombstone_rag_chunks`로 무효화한다.

## 적용 대상 매핑 (확정)

| source_table | 현행 규격 | 변경 후 (자연키 표준) | 비고 |
| --- | --- | --- | --- |
| player_season_batting | `{pid}_{season}_{team}_{league}` | 동일 | DB 정규 팀코드 기반 |
| player_season_pitching | 동일 | 동일 | DB 정규 팀코드 기반 |
| team_standings_daily | `{standings_date}_{시대 team}` | 동일 | 표준 날짜+팀코드 |
| game | `{game_id}` | 동일 | 13자리 표준 자연키 |
| awards | `str(id)` (PK) | `{year}_{award_type}_{category}_{name}` | 생성 경로 적용; legacy rekey pending |
| team_history | `str(id)` (PK) | `{season}_{team_code}` | 생성 경로 적용; legacy rekey pending |
| milestone | `str(id)` (PK) | `{season}_{pid}_{category}` | 생성 경로 적용; legacy rekey pending |
| futures_schedule | `str(id)` (PK) | `{game_id}` | 생성 경로 적용; legacy rekey pending |
| player_splits | `str(id)` (PK) | `{season}_{pid}_{type}_{key}` | 생성 경로 적용; legacy rekey pending |
| player_movements | `str(id)` (PK) | `{movement_date}_{team_code}_{player_name}_{section}_{fingerprint}` | 생성 경로 적용; legacy rekey pending |
| game_play_by_play | `str(id)` (PK) | `{game_id}_{source_row_index}` 또는 content digest | 생성 경로 적용; legacy rekey pending |
| game_highlights | `str(id)` (PK) | `{game_id}_{highlight_type}_{event_seq}` 또는 description digest | 생성 경로 적용; legacy rekey pending |

## 마이그레이션 경로

1. 본 계약 구현 (빌더 수정 + 테스트)
2. production source identity census 및 collision/orphan manifest 생성
3. 안전한 legacy row rekey 또는 새 identity 재색인을 apply 게이트로 수행
4. rekey 후 `audit_rag_index --require-postings`와 tombstone audit 통과 확인
5. 구버전 id 잔존분은 replacement가 확인된 경우에만 tombstone 처리

## Production Census (2026-08-29)

The Oracle single-store census was read-only. Existing numeric identities remain
for historical rows, while the incremental writer has already created a small
number of natural-key rows.

| source | legacy numeric rows | safe source matches | existing natural target | orphan rows | collisions |
| --- | ---: | ---: | ---: | ---: | ---: |
| awards | 495 | 493 | 0 | 2 | 0 |
| team_history | 385 | 385 | 10 | 0 | 0 |
| futures_schedule | 127 | 127 | 0 | 0 | 0 |
| player_movements | 6,802 | 6,401 | 0 | 401 | 0 |
| game_play_by_play | 121,449 | 113,314 | 0 | 8,135 | 2 |
| game_highlights | 2,120 | 2,094 | 0 | 26 | 0 |

The orphan and PBP collision rows are not automatically rewritten. They require
source-specific review before the production apply step.

## 참고

- 드리프트 실측: `data/archive/workspace_cleanup_20260823/rag_reconciliation_20260823/gap_resolution_summary.json`
- 검증 도구: `src.cli.rag.reconcile_rag_stores`
