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
  embedding 모델·차원·청킹 버전은 sparse/vector metadata의
  `embedding_fingerprint`으로 저장하며, 값이 다른 청크는 선택적으로 재임베딩한다.
  fingerprint가 없는 legacy row는 read-only 선택 과정에서 보고만 하며,
  metadata 보드는 별도 승인 gate로 수행한다.
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
| team_events | `str(id)` (PK) | `{scope}_{team}_{type}_{date}_{fingerprint}` | 2026-10-11 추가·적용 완료. `id`는 재수집 시 재번호되고, 제목만 쓰면 같은 프로모션의 날짜 변형이 충돌 |
| press_release | `str(id)` (PK) | `{notice_id}` | 2026-10-11 추가·적용 완료. KBO 공지 번호는 공지 자체에 인쇄돼 재수집에도 불변. 날짜+제목은 제목 정정 시 청크가 개명됨 |
| player_basic | `str(player_id)` (PK) | **변경 없음** | 숫자 ID가 곧 자연키. 복합키는 데이터가 거부한다(아래) |

### player_basic을 재키하지 않는 이유

`player_basic`의 청크 8,066건은 `player_id`와 이미 1:1이므로, 자연키를 `player_id`로 정하면 **같은 값을 쓰는 무의미한 이름 변경**이 된다. 읽기 쉬운 복합키 대안은 데이터가 허용하지 않는다:

- `team`·`position`이 **85% NULL**(6,886/8,066)이라 4분의 3이 같은 키로 붕괴
- 이름만 쓰면 동명이인이 충돌 — '이원석'이 서로 다른 id **125개**, '이상훈' 4행도 **SK·CB·SS·LG로 실제 다른 선수**
- 동명이인이 125명인 사실 자체가 "이 id는 변할 수 없다"는 증거다

따라서 이 테이블의 숫자 id는 드리프트가 아니라 자연키이며, 복합키를 만들어야 한다는 압박만 제거하는 것이 이 절의 목적이다. 후속 판단은 `R2_SOURCE_TABLES`를 확장해 이 판단을 명시적으로 고정하거나, 아예 제외 대상으로 두는 것이다.

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

## 2026-10-12: awards 청크 재구축과 중복 정리

### 원래 상태

`awards` 청크 497건 중 **493건이 내용과 자연키가 불일치**했다 — chunk 74552는 키가
`2025_MVP_NONE_폰세`인데 내용은 `2025 올스타전MVP 박동원`이었다. 재키 이전
preimage의 `legacy_content_hash`가 현재 해시와 동일하므로 **재키가 만든 문제가
아니라**, `build_rag_index.py`가 `order_by(Award.year.desc())`로 순회한 것과
Phase 77의 `DELETE` 후 재크롤에서 id가 재배정된 결과 원본 인덱서부터 내용과 id가
어긋나 있었던 것이다. 다른 테이블은 전수 확인 결과 정상이었다(PBP 800/800 일치,
game_highlights·player_movements 각 300/300).

### 조치

1. **재구축**: 소스 495행을 그대로 재생성 — 내용과 자연키를 같은 행에서 만들므로
   불일치가 구조적으로 불가능하다(검증 495/495).
2. **중복 제거(안 A, 5건)**: 같은 (년도, 종류, 선수)에서 한쪽 category가 NULL인 행
   5건을 삭제하고 해당 청크를 양쪽에서 tombstone했다. NULL 행은 정보가 엄밀히 적으므로
   객관적 중복이다. `player_id`/`team_code` 연결은 없어 링크 손상도 없었다.

| 제거 source id | 항목 | 살아남은 키 |
| --- | --- | --- |
| 492 | 2022 신인상 정철원 | `2022_신인상_투수_정철원` |
| 491 | 2022 MVP 이정후 | `2022_MVP_외야수_이정후` |
| 485 | 2023 신인상 문동주 | `2023_신인상_투수_문동주` |
| 478 | 2024 신인상 김택연 | `2024_신인상_투수_김택연` |
| 477 | 2024 MVP 김도영 | `2024_MVP_3루수_김도영` |

복원용 백업: `$TMPDIR/opencode/awards_redundant_backup.json` (5행 원문) 및
`$TMPDIR/opencode/awards_dh_backup.json` (아래 지명타자 4행 원문).
삭제 전에 "살아남을 값 있는 행이 존재한다"를 SQL로 검증한 뒤 지웠다.

결과: awards 소스 490행, 활성 청크 490개(**1:1 일치**), 자연키 내용 불일치 0.

### 남은 판단 2건 (미해결)

- **포지션 충돌 4건**: 2022~2025 골든글러브에서 같은 선수가 한 크롤은 `지명 타자`,
  다른 크롤은 `외야수`로 기록됐다(짧은 팀명/전체 팀명으로 구분된다). 어느 쪽이
  사실인지는 이 저장소에 근거가 없어 **판단하지 않았다**. 자연키를
  `{year}_{type}_{player}`(포지션 제외)로 바꾸면 두 행이 한 키로 합쳐져 9개 그룹이
  모두 해소되지만 키 495건이 변하고 골든쿼리 2건의 갱신이 필요하다.
- **이름 변형 1건**: 2025 MVP 코디 폰세가 id=46(`코디 폰세`, 투수)과
  id=470(`폰세`, NULL) 두 행이다. 어느 표기를 정규로 할지 미결정이다.

### 포지션 충돌 4건은 해소됐다 (2026-10-12) — 구조로 판정

"근거가 없다"고 적었던 판단은 **틀렸다.** KBO 골든글러브는 포지션이 정해진 상이라
**외야수 3명 · 지명타자 1명**이 불변식이다. 그 기준으로 세어보면 답이 하나뿐이다.

| 연도 | Wikipedia | yagoonara | 양쪽에 나온 선수 | 외야수를 3으로 두면 |
| --- | --- | --- | --- | --- |
| 2022 | 외야수 1 + 지명타자 나성범 | 외야수 나성범·피렐라 + DH 이대호 | 나성범 | 외야수 3 ✓ / DH 1 ✓ |
| 2023 | 외야수 1 + 지명타자 홍창기 | 외야수 구자욱·홍창기 + DH 손아섭 | 홍창기 | 외야수 3 ✓ / DH 1 ✓ |
| 2024 | 외야수 1 + 지명타자 로하스 | 외야수 구자욱·로하스 + DH 최형우 | 로하스 | 외야수 3 ✓ / DH 1 ✓ |
| 2025 | 외야수 1 + 지명타자 안현민 | 외야수 구자욱·안현민 + DH 최형우 | 안현민 | 외야수 3 ✓ / DH 1 ✓ |

두 가정을 비교하면 한쪽만 성립한다 — 충돌 선수를 **외야수**로 두면 4개 연도 모두
`외야수 3 · DH 1`로 맞고, **지명타자**로 두면 `외야수 2 · DH 2`가 되어 외야수 한 명이
사라진다. 4개 연도가 **동일한 패턴**이라는 사실이 우연을 배제한다. 즉 yagoonara가
맞고 Wikipedia가 외야수 한 명을 DH로 잘못 적었다.

조치: Wikipedia의 `지명 타자` 행 4건을 삭제하고 해당 청크를 양쪽에서 tombstone했다
(그 선수는 yagoonara의 `외야수` 행으로 남아 있어 손실이 없다 — 중복된 라벨만 제거).
결과는 2022~2025 각 **10행 = 외야수 3 + DH 1**, 소스 486행 = 활성 청크 486개.

### 왜 두 행이 공존할 수 있었나 — 유일 제약이 카테고리·팀명을 포함한다

`awards`의 유일 제약은 `uq_award_record` = **(year, award_type, category,
player_name, team_name)** 이다. `AGENTS.md`는 이를 "unique:
year+award_type+player_name"라고 적고 있으나 **부정확하다** — 카테고리와 팀명이
키에 들어 있어, 두 소스가 포지션이나 팀 표기를 달리하면 UPSERT되지 않고 별개 행이
된다. 이것이 9개 중복 그룹이 생긴 구조적 이유다.

### 부수 발견: BM25가 복합 수상종류를 구분하지 못한다

"2024년 KBO MVP는 누구?"는 `한국시리즈MVP`·`올스타전MVP` 청크를 1~2위로 올리고
정답 `2024_MVP_3루수_김도영`은 3위다(12.33 vs 12.72). "MVP"가 복합어의 부분
토큰이기 때문이며, query에 선수명이 들어가면 정답이 1위가 된다. 데이터 문제가
아니라 순위 문제이므로 자연키 조작으로 해결되지 않는다.

## 참고

- 드리프트 실측: `data/archive/workspace_cleanup_20260823/rag_reconciliation_20260823/gap_resolution_summary.json`
- 검증 도구: `src.cli.rag.reconcile_rag_stores`
