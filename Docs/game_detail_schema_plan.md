# KBO Game Detail Schema Expansion

목표: 기존 `game`/`box_score`/`game_summary`/`game_play_by_play`만으로는 상세 분석이 어렵기 때문에, 라인업과 선수별 박스스코어, 이닝별 득점, 정규화된 이벤트 로그를 추가해 아이템포턴트하게 저장한다. `Docs/Season_ Game_Data_ Crawling_ Plan.md` 권고사항과 Supabase 동기화를 고려해 아래 설계를 사용한다.

## 변경/추가 테이블 요약

| 테이블 | 목적 | 주요 Unique 키 |
| --- | --- | --- |
| `game_metadata` | 경기 시각/관중/구장 정보 정규화 | `game_id` |
| `game_inning_scores` | 이닝별 득점 + 팀 구분 | `game_id, team_side, inning` |
| `game_lineups` | 선발/교체 라인업 기록 | `game_id, team_side, batting_order, appearance_seq` |
| `game_batting_stats` | 타자 박스스코어 | `game_id, player_id, appearance_seq` |
| `game_pitching_stats` | 투수 박스스코어 | `game_id, player_id, appearance_seq` |
| `game_events` | 구조화된 플레이 바이 플레이 이벤트 | `game_id, event_seq` |

기존 테이블 변경:

* `game`: CSV 스키마(기본 정보 + 승리팀) 유지. 확장 메타 필드(관중/시간)는 `game_metadata`로 분리.
* `box_score`: 이닝별 득점과 R/H/E만 저장, 추가 메타는 `game_metadata`.
* `game_summary`: `summary_type`, `detail_text` 컬럼명 사용 + 인덱스(`idx_game_summary_game_category`) 추가.

## 세부 스키마

### game_metadata

| 컬럼 | 타입 | 설명 |
| --- | --- | --- |
| `game_id` (PK, FK) | `String(20)` | `game.game_id` 참조 |
| `stadium_code` | `String(16)` | `teams`/`stadium` 매핑 코드 |
| `stadium_name` | `String(64)` | 표기 이름 |
| `attendance` | `Integer` | 관중수, `NULL` 허용 |
| `start_time` | `Time` | 개시 시각 |
| `end_time` | `Time` | 종료 시각 |
| `game_time_minutes` | `Integer` | 경기 시간(분) |
| `weather` | `String(32)` | 옵션 |
| `source_payload` | `JSON` | 원본 텍스트 보관 |

### game_inning_scores

| 컬럼 | 타입 | 설명 |
| --- | --- | --- |
| `id` (PK) | `Integer` | |
| `game_id` (FK) | `String(20)` | |
| `team_side` | `String(5)` | `'away'|'home'` |
| `team_code` | `String(10)` | `teams.team_id` |
| `inning` | `Integer` | 1~15 |
| `runs` | `Integer` | 각 이닝 득점 |
| `is_extra` | `Boolean` | 10회 이후 |
| `created_at/updated_at` | `DateTime` | `TimestampMixin` |

Unique: (`game_id`, `team_side`, `inning`).

### game_lineups

| 컬럼 | 타입 |
| --- | --- |
| `id` (PK) | `Integer` |
| `game_id` (FK) | `String(20)` |
| `team_side` | `String(5)` |
| `team_code` | `String(10)` |
| `player_id` | `Integer`, nullable (`player_basic.player_id`) |
| `player_name` | `String(64)` |
| `batting_order` | `Integer` (1~9) |
| `position` | `String(8)` |
| `is_starter` | `Boolean` |
| `appearance_seq` | `Integer` | 출전 순서 |
| `notes` | `String(64)` |

Unique: (`game_id`, `team_side`, `appearance_seq`).

### game_batting_stats

| 컬럼 | 타입 |
| --- | --- |
| `id` (PK) | `Integer` |
| `game_id` (FK) | `String(20)` |
| `team_side` | `String(5)` |
| `team_code` | `String(10)` |
| `player_id` | `Integer`, nullable |
| `player_name` | `String(64)` |
| `appearance_seq` | `Integer` |
| `batting_order` | `Integer`, nullable |
| `is_starter` | `Boolean` |
| `position` | `String(8)` |
| `plate_appearances` 등 `HITTER_HEADER_MAP` 수치 | `Integer`/`Float` | NULL 대신 0 기본값 권장 |
| `extra_stats` | `JSON` | 파싱되지 않은 열 |

Unique: (`game_id`, `player_id`, `appearance_seq`). `player_id`가 없을 경우 `player_name`으로 보조 Unique 인덱스 추가(`idx_game_batting_name`).

### game_pitching_stats

| 컬럼 | 타입 |
| --- | --- |
| `id` (PK) | `Integer` |
| `game_id` (FK) | `String(20)` |
| `team_side` | `String(5)` |
| `team_code` | `String(10)` |
| `player_id` | `Integer`, nullable |
| `player_name` | `String(64)` |
| `appearance_seq` | `Integer` |
| `is_starting` | `Boolean` |
| `innings_outs` | `Integer` | 1이닝=3, 1⅔=5 |
| `innings_pitched` | `Numeric(5,3)` | 파생 값 |
| `decision` | `Enum('W','L','S','H')`, nullable |
| `batters_faced`, `pitches`, ... | `Integer` |
| `era`, `whip`, `k_per_nine`, ... | `Float` |
| `extra_stats` | `JSON` |

Unique: (`game_id`, `player_id`, `appearance_seq`).

### game_events

| 컬럼 | 타입 |
| --- | --- |
| `id` (PK) | `Integer` |
| `game_id` (FK) | `String(20)` |
| `event_seq` | `Integer` | 경기 전체 순번 |
| `inning` | `Integer` |
| `inning_half` | `String(6)` (`'top'|'bottom'`) |
| `outs` | `Integer` |
| `batter_id` | `Integer`, nullable |
| `batter_name` | `String(64)` |
| `pitcher_id` | `Integer`, nullable |
| `pitcher_name` | `String(64)` |
| `description` | `Text` |
| `event_type` | `String(32)` | 예: `plate_appearance`, `pitching_change` |
| `result_code` | `String(16)` | e.g., `1B`, `HR`, `BB`, `K` |
| `rbi` | `Integer` |
| `bases_before`/`bases_after` | `String(3)` (`"100"` 등) |
| `extra_json` | `JSON` |

Unique: (`game_id`, `event_seq`). `event_seq`는 Relay DOM 순서대로 증가시켜 재실행 시 덮어쓰기.

## ETL 메모

1. `GameDetailCrawler`는 `hitters`/`pitchers` 리스트를 그대로 repository에 넘긴 후 각 테이블에 bulk upsert한다.
2. PBP 파서는 `RelayCrawler` 출력 텍스트를 규칙/정규표현식으로 변환해 `event_type`/`result_code`를 생성한다.
3. 모든 테이블은 `game_id` 기준으로 `DELETE`→`INSERT`를 기본 전략으로 하되, MySQL/Supabase에서는 `INSERT ... ON CONFLICT DO UPDATE`를 사용해 아이템포턴시를 확보한다.
4. `game_inning_scores`는 `team_side`, `inning`을 기준으로 1~12+회차까지 삽입하며, 라인업/타자/투수 테이블은 `player_basic.player_id` 매핑 실패 시 `player_name`만 저장한다.
5. Supabase 전송 시 대용량 이벤트를 고려해 `game_events`를 별도 배치로 sync하거나 CSV copy 전략을 검토한다.

## 검증 및 백필 런북

1. **로컬 DB 마이그레이션**: `python3 init_db.py`로 SQLite 스키마를 갱신한다. 의존성(`sqlalchemy`, `playwright` 등)이 설치돼 있지 않으면 `pip install -r requirements.txt` 후 재실행한다.
2. **단일 월 테스트 런**: `python -m src.cli.crawl_game_details --year 2024 --month 10 --limit 2 --relay --delay 5`로 1~2경기를 수집해 신규 테이블(`game_batting_stats`, `game_events` 등)이 채워지는지 확인한다.
3. **Supabase 반영**: `migrations/supabase/011_expand_game_detail.sql`을 프로젝트 브랜치에 적용한 뒤 `src/cli/sync_supabase.py`를 확장해 신규 테이블을 포함한다.
4. **백필 전략**: 최근 시즌(올해/작년)부터 월 단위로 실행 → 검증이 끝나면 5년 단위 배치로 확장. 장기 백필은 `scripts/crawl_year_range.sh`를 수정해 `--relay` 옵션과 재시도 로직을 포함시키고, 실패한 `game_id`는 `crawl_runs` 테이블에 기록한다.
