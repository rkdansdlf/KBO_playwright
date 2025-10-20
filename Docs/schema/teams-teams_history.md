## 🔗 팀과 팀 역사 테이블의 제약조건 설계

이 설계에서는 `teams` 테이블(이전의 단순 팀 정보 테이블)을 더 이상 사용하지 않고, 그 역할을 **`team_history`** 테이블로 대체합니다. 따라서 주된 관계는 **`team_history`** 테이블 자체 내에서, 그리고 **선수 기록 테이블**과의 연결에서 발생합니다.

### 1. **프랜차이즈 그룹핑 제약조건**

KBO 역사에서 MBC 청룡 $\rightarrow$ LG 트윈스처럼 구단 명칭이나 코드가 변경되더라도 **프랜차이즈(구단 역사 주체)**는 동일하게 유지됩니다. 이를 위해 `team_history` 테이블 내에 다음 관계를 설정합니다.

| 필드 | 역할 및 제약조건 설명 |
| :--- | :--- |
| **`franchise_id`** | **프랜차이즈 식별자**로 사용됩니다. 동일한 `franchise_id`를 가진 모든 행은 하나의 구단 역사를 공유함을 의미합니다. (`team_history` 테이블 내에서 **프랜차이즈 그룹핑**을 위한 논리적 FK 역할을 합니다.) |
| **시간 중복 금지** | **(논리적 제약조건)** 동일한 `franchise_id` 내에서는 **`start_season`**과 **`end_season`** 기간이 **중복될 수 없습니다.** 이 제약은 특정 프랜차이즈가 같은 시기에 두 가지 명칭을 가질 수 없음을 보장합니다. (예: 1990년에 MBC 청룡과 LG 트윈스가 동시에 존재 불가) |
| **연속성** | **(논리적 제약조건)** 연속된 구단 명칭 기록은 이전 기록의 `end_season`이 다음 기록의 `start_season`과 일치하거나 1 차이 나야 합니다. (예: MBC 청룡 `end_season` 1990년 $\rightarrow$ LG 트윈스 `start_season` 1990년 또는 1991년) |

---

### 2. **선수 기록 테이블과의 외래 키 제약조건**

선수 기록 테이블(`kbo_player_hitting_stats`, `kbo_player_pitching_stats`)은 더 이상 현재 팀 코드(`teams` 테이블의 PK)를 참조하지 않고, **특정 시점의 유효한 구단 역사 기록**을 참조해야 합니다.

| 필드 | 역할 및 제약조건 설명 |
| :--- | :--- |
| **`kbo_player_stats.team_id`** | **Foreign Key (FK) 폐지 및 변경** 이 필드를 삭제하고, 대신 `team_history` 테이블의 **`team_code`**를 사용하거나, `team_history`의 **`id` (PK)**를 참조하도록 변경해야 합니다. |
| **✅ 권장 대안:** **`team_history_id`** | 선수 기록 테이블에 **`team_history_id`** 필드를 추가하고, 이것이 **`team_history` 테이블의 `id` (PK)를 참조**하도록 설정합니다. |

#### 💡 **왜 `team_history.id`를 참조해야 하는가?**

1.  **유일한 식별:** 선수 기록 시점의 팀 명칭, 코드, 유효 연도 정보는 `team_history` 테이블의 `id`를 통해서만 **고유하게 식별**될 수 있습니다. (예: 'KIA 타이거즈'라는 팀 명칭을 2001년부터 사용하는 **기록 ID**)
2.  **데이터 무결성:** 선수 기록 행의 `team_history_id`가 `team_history` 테이블에 존재하는 **유효한 역사 기록**임을 보장합니다.
3.  **시간적 연결:** 스탯 기록 테이블은 `season_meta_id`를 통해 **연도**를 알고, `team_history_id`를 통해 해당 연도에 유효했던 **팀 명칭/코드**를 정확히 알 수 있게 됩니다.

### 3. **업데이트된 선수 기록 테이블 스키마 (제약조건 반영)**

| 필드명 | 데이터 타입 | 제약 조건 | FK 관계/비고 |
| :--- | :--- | :--- | :--- |
| **`id`** | `INT` | `PRIMARY KEY` | |
| `season_meta_id` | `INT` | `FOREIGN KEY` | `kbo_seasons_meta` 참조 |
| `player_id` | `INT` | `FOREIGN KEY` | `player_basic` 참조 |
| **`team_history_id`** | `INT` | **FOREIGN KEY** | **`team_history` 테이블의 `id` (PK) 참조** |
| `level` | `VARCHAR` | `NOT NULL` | |
| ... | (기타 스탯 필드) | | | |