# 🏗️ Team Database Schema Optimization Plan

## 1. Current Pain Points (분석)
현재 구조는 다음과 같은 비효율성이 존재합니다.

1.  **데이터 분산**: 팀 정보가 5개 테이블(`teams`, `team_franchises`, `team_history`, `team_profiles`, `team_name_mapping`)로 흩어져 있어 단순 조회 시에도 다수의 JOIN이 필요합니다.
2.  **변동 데이터의 정적 저장**: `teams` 테이블에 `city`, `stadium_name`, `color`가 저장되어 있는데, 이는 역사적으로 변경될 수 있는 정보입니다 (예: OB 베어스의 연고지 이동).
3.  **관리 복잡성**: 단순 태그(`profiles`)나 별칭(`name_mapping`)을 위해 별도 테이블을 유지하는 것은 관리 비용이 높습니다.

---

## 2. Optimization Strategy (최적화 전략)

PostgreSQL(Supabase)의 강력한 기능인 **JSONB**와 **Array** 타입을 활용하여 테이블을 통합하고, 데이터의 성격(불변/가변)에 따라 명확히 분리합니다.

### ✅ 목표 모델 (Target Schema)
5개 테이블 → **3개 테이블**로 축소

### 1) team_franchises (유지/강화)
프랜차이즈의 불변하는 정체성을 관리합니다.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INT (PK) | 1, 2, ... |
| `name` | VARCHAR | 현재 구단명 (삼성 라이온즈) |
| `code` | VARCHAR | 대표 코드 (SS) |
| `metadata` | **JSONB** | **통합 필드** (기존 team_profiles 대체) |

*   **JSONB 예시**:
    ```json
    {
      "profiles": ["대구", "삼성", "전통의명가", "이승엽"],
      "social_media": {"instagram": "...", "youtube": "..."}
    }
    ```

### 2) teams (코드 관리 중심)
**통계 데이터의 Foreign Key 역할**에 집중합니다. 역사적 팀 코드(OB, MBC 등)를 모두 포함하되, 가변 데이터는 제거합니다.

| Column | Type | Description |
|--------|------|-------------|
| `team_id` | VARCHAR (PK) | SS, OB, MBC, LG... |
| `franchise_id` | INT (FK) | `team_franchises.id` |
| `is_active` | BOOLEAN | 현재 사용 중인 코드인지 여부 |
| `aliases` | **TEXT[]** | **통합 필드** (기존 team_name_mapping 대체) |

*   **Aliases 예시**: `['두산', 'DO', '베어스', '서울 두산']`
*   **장점**: 별도 매핑 테이블 없이 `@>` 연산자로 검색 가능 (`WHERE aliases @> ARRAY['두산']`)

### 3) team_history (시계열 데이터 통합)
모든 **가변 정보**(이름, 연고지, 홈구장, 색상)를 시간 축으로 관리합니다.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INT (PK) | |
| `franchise_id` | INT (FK) | |
| `team_code` | VARCHAR (FK) | 당시 사용된 코드 (`teams.team_id`) |
| `start_year` | INT | 시작 연도 |
| `end_year` | INT | 종료 연도 (NULL = 현재) |
| `team_name` | VARCHAR | 당시 이름 (OB 베어스) |
| `city` | VARCHAR | 당시 연고지 (대전 → 서울) |
| `stadium` | VARCHAR | 당시 홈구장 |
| `color` | VARCHAR | 당시 팀 컬러 |
| `logo_url` | VARCHAR | 당시 로고 (옵션) |

---

## 3. Benefits (기대 효과)

| 항목 | 기존 (Current) | 제안 (Proposed) | 효과 |
|------|---------------|----------------|------|
| **테이블 수** | 5개 | 3개 | 관리 포인트 40% 감소 |
| **속성 검색** | JOIN `team_profiles` | JSONB Query | 인덱싱 활용 시 검색 속도 향상, JOIN 불필요 |
| **별칭 검색** | JOIN `team_name_mapping` | Array Check | 단순 텍스트 배열로 관리 용이 |
| **역사 정확성** | `teams`에 최신 정보만 존재 | `team_history`에 모든 스냅샷 저장 | 올드 유니폼 경기 등 시점별 정확한 정보 제공 |

## 4. Migration Steps (적용 절차)

1.  **Schema Modification**:
    *   `teams` 테이블에 `aliases` (TEXT Array) 컬럼 추가
    *   `team_franchises` 테이블에 `metadata` (JSONB) 컬럼 추가
    *   `team_history` 테이블에 `color`, `stadium` 컬럼 추가 및 데이터 이관
2.  **Data Migration**:
    *   `team_name_mapping` 데이터를 `teams.aliases`로 집계(Agg)하여 업데이트
    *   `team_profiles` 데이터를 `team_franchises.metadata`로 집계하여 업데이트
3.  **Drop Tables**:
    *   데이터 검증 후 `team_name_mapping`, `team_profiles` 테이블 삭제
4.  **Code Update**:
    *   KBO Platform 조회 로직을 JSONB/Array 쿼리로 변경

이 구조는 Supabase(PostgreSQL)의 **NoSQL-like 기능**을 최대한 활용하여 유연성과 성능을 동시에 잡는 현대적인 접근 방식입니다.
