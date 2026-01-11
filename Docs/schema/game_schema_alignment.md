# Game Table Schema Alignment

Supabase 에 이미 구축된 경기 관련 테이블(`Docs/*.csv`)과 현재 코드베이스의 ORM/로컬 SQLite 스키마가 다르기 때문에, 아래와 같이 스키마를 재정렬해야 한다. 이 문서는 CSV 정의를 기준으로 필요한 변경 사항을 요약한다.

## 1. game 테이블 (`Docs/game.csv`)

| CSV 컬럼 | 타입 | 비고 |
| --- | --- | --- |
| `id` | `serial` | PK |
| `game_id` | `varchar(20)` | Unique, 비즈니스 키 |
| `game_date` | `date` |  |
| `stadium` | `varchar(50)` |  |
| `home_team` | `varchar(20)` |  |
| `away_team` | `varchar(20)` |  |
| `away_score` | `integer` |  |
| `home_score` | `integer` |  |
| `away_pitcher` | `varchar(30)` | 원정 선발 투수 |
| `home_pitcher` | `varchar(30)` | 홈 선발 투수 |
| `winning_team` | `varchar(20)` | 승리 팀 |
| `winning_score` | `integer` | 승리 팀 점수 |
| `season_id` | `integer` | 시즌 FK |

**현재 ORM (`src/models/game.py`)**

* PK가 `game_id` 하나뿐이라 `id serial` 없음.
* `away_pitcher`, `home_pitcher`, `winning_team`, `winning_score`, `season_id` 컬럼이 누락됨.
* 최근 추가했던 `attendance`, `start_time` 등은 CSV에 없음 → 확장 컬럼은 별도 테이블/컬럼으로 보존하되 기본 스키마를 유지해야 함.

**조치**

1. `Game` 모델에 `id` 컬럼을 추가하고 `game_id`를 Unique Key로 설정.
2. CSV에 정의된 컬럼을 모두 포함하도록 모델/마이그레이션 수정.
3. 확장 메타데이터(`attendance`, `start_time` 등)는 `game_metadata` 혹은 별도 테이블로 유지.

## 2. box_score 테이블 (`Docs/box_score.csv`)

CSV 정의는 기본 이닝별 득점 + 총합만 포함한다:

* `game_id` FK
* `away_1` ~ `away_15`, `home_1` ~ `home_15`
* `away_r`, `away_h`, `away_e`
* `home_r`, `home_h`, `home_e`

**현재 ORM**

* `id` + `game_id` Unique는 동일.
* `away_b`, `home_b`, `stadium`, `crowd`, `start_time`, `end_time`, `game_time` 등 CSV에 없는 컬럼이 존재.

**조치**

* CSV 스펙을 우선으로 두고 필수 컬럼을 유지.
* 확장 필드는 신규 메타데이터 테이블 혹은 `game_metadata`로 이동하는 방안을 검토.

## 3. game_summary 테이블 (`Docs/game_summary.csv`)

CSV 컬럼:

| 컬럼 | 타입 | 설명 |
| --- | --- | --- |
| `id` | `serial` | PK |
| `game_id` | `varchar(20)` | FK |
| `summary_type` | `varchar(50)` | 예: Home Run |
| `player_name` | `varchar(50)` |  |
| `detail_text` | `text` | 설명 |

**현재 ORM**

* 컬럼명이 `category`, `content`로 되어 있음.

**조치**

* 컬럼명을 CSV와 동일하게(`summary_type`, `detail_text`) 수정하고 마이그레이션 추가.

## 4. 작업 순서 제안

1. **마이그레이션 재정렬**: Supabase에서 이미 존재하는 테이블 구조에 맞춰 `011_expand_game_detail.sql`을 조정하거나 별도 마이그레이션으로 CSV 스펙과 일치하도록 한다.
2. **ORM 수정**: `Game`, `BoxScore`, `GameSummary` 모델을 CSV 정의와 동기화하고, 확장 속성은 별도 모델(`GameMetadata`, `GameEvents` 등)에 담는다.
3. **데이터 이전**: `game_id` 단일 PK → `id` + Unique 로 바뀌는 만큼, SQLite/Supabase 모두에 대해 데이터 마이그레이션 스크립트를 준비한다.
4. **검증**: 기존 2021~2025.10 데이터 샘플을 조회해 컬럼 매핑이 맞는지 확인한 뒤, 새로운 상세 수집 데이터를 동일 구조로 적재한다.

이 문서를 기준으로 스키마를 손보고 나면, 이후 추가되는 상세 정보(라인업, 플레이 기록 등)는 기존 CSV 기반 테이블을 건드리지 않고 별도의 확장 테이블로 저장할 수 있다.
