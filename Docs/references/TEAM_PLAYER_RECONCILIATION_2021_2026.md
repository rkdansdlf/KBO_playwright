# Team-Player Reconciliation Report

작성 범위: 2021/2026 정규시즌, local SQLite와 OCI PostgreSQL sync target의
`team_season_*` 대 `player_season_*` 비교.

## 결론

ER(`earned_runs`) 차이는 의도한 공식 source semantics 차이로 비차단 처리한다.
그 외 불일치는 두 종류로 분리된다.

1. OCI target의 `player_season_*`에 여러 source가 함께 남아 있어, quality gate가
   비권위 과거 row까지 합산한다.
2. 공식 player 페이지가 제공하지 않는 필드와 팀 페이지의 전체 팀 집계 범위가
   다르다. 특히 2026 타격의 SB/CS가 대표적이다.

## Quality Gate 결과

| Season | Team batting | Team pitching | ER 처리 |
|---|---:|---:|---|
| Local 2021 | 0/10 teams mismatched | 0/10 teams mismatched | non-blocking |
| Local 2026 | 0/10 teams mismatched | 0/10 teams mismatched | non-blocking |
| OCI 2021 read-only audit | 2/10 teams mismatched | 10/10 teams mismatched | non-blocking |
| OCI 2026 read-only audit | 1/10 teams mismatched | 0/10 teams mismatched | non-blocking |

`team_pitching` 결과에는 `semantics_exempt_fields=["earned_runs"]`가 포함된다.
나머지 필드 불일치는 계속 blocking 상태다.

## Source Decomposition

OCI target의 정규시즌 row 수:

| Season | Table | Total | CRAWLER | PROFILE | Other |
|---|---|---:|---:|---:|---:|
| 2021 | `player_season_pitching` | 624 | 308 | 296 | 20 |
| 2021 | `player_season_batting` | 404 | 394 | 0 | 10 |
| 2026 | `player_season_pitching` | 486 | 271 | 134 | 81 |
| 2026 | `player_season_batting` | 354 | 329 | 0 | 25 |

### 2021 투수 중복 합산

2021 `player_season_pitching` 전체 source를 합산하면 예를 들어 삼성은
team wins 76 대비 player wins 152가 된다. `source=CRAWLER`만 합산하면 wins 76으로
일치한다. 다른 팀도 runs/strikeouts가 거의 같은 배율로 증가한다.

따라서 주된 원인은 ER semantics가 아니라 `PROFILE`/`AGGREGATED` 등 과거 row와
현재 `CRAWLER` row를 quality gate가 동시에 합산하는 것이다.

Raw key 수와 canonical team key 수의 차이는 2021 투수 기준 624 대 622로 작다.
단순 canonical team collision만으로는 2배 합산을 설명할 수 없다.

### 2026 타격 source field 제한

현재 공식 player 타격 crawler의 정규시즌 Basic1 payload에는 SB/CS가 포함되지 않아
`source=CRAWLER` row의 SB/CS 합계가 0이다. 반면 팀 페이지는 SB/CS를 제공한다.
이 필드는 source contract상 unavailable로 표시하거나 해당 비교에서 제외해야 하며,
0으로 저장해 실제 0으로 해석하면 안 된다.

### 타격 PA 범위 차이

`source=CRAWLER`만 보더라도 팀/선수 PA가 완전히 일치하지 않는 팀이 남는다.
예를 들면 2021 LG는 team PA 5,533 대 player PA 5,266이고, 2026 DB는
team PA 3,256 대 player PA 3,619이다. 이는 선수 페이지의 자격/노출 범위와
팀 페이지의 전체 팀 집계 범위, 다중 팀 row 보존 방식의 차이를 별도로 분석해야 한다.

## 적용 상태 및 남은 작업

1. Quality gate에 `(player, season, league, level, team)` 키별 source precedence를
   적용했다. `CRAWLER` row가 있는 키는 과거 `PROFILE`/`AGGREGATED` row와 중복
   합산하지 않고, 없는 키는 다음 우선 source로 fallback한다.
2. Read-only staging report와 quality gate 모두 player payload에서 실제 값이 없는
   필드를 `unavailable_fields`로 보고 비교에서 제외한다. SB/CS도 이 계약을 따른다.
3. Local SQLite 2021/2026 gate는 source precedence와 2026 aggregate-key remediation
   이후 통과한다. OCI quality gate도 `data_source` schema alias를 지원하도록 보정되어
   OCI 2026 pitching mismatch가 10개에서 0개로 줄었다.
4. OCI 2021 pitching에는 `PROFILE`과 `MANUAL_RECALC`가 서로 다른 player ID 집합으로
   저장된 identity duplication이 남아 있다. 동일 선수 여부를 이름만으로 판정할 수
   없으므로 자동 삭제나 ID 병합은 보류한다. OCI 2021 batting 2개와 2026 batting
   1개는 소규모 PA/AB source-scope 차이로 별도 검토한다.
5. OCI 2021/2026 audit은 실제 OCI 연결 dialect가 Oracle임을 확인했으며, 변경 없이
   quality gate와 source-row 집계만 수행했다. OCI의 과거 `PROFILE`/`AGGREGATED` row는
   백업과 dry-run diff 확인 전까지 삭제하지 않는다.

이번 변경에서도 target data를 삭제하거나 과거 source row를 자동 정리하지 않았다.

## OCI Read-Only Audit (2026-07-25)

The OCI target connection reports the Oracle dialect. The audit performed no writes or
deletes.

| Season | Table | Rows | Source distribution | Quality result |
|---|---|---:|---|---|
| 2021 | `player_season_batting` | 396 | MANUAL_RECALC 394, AGGREGATED 1, CRAWLER 1 | 2 team mismatches |
| 2021 | `player_season_pitching` | 623 | MANUAL_RECALC 307, PROFILE 291, CRAWLER 18, AGGREGATED 7 | 10 team mismatches |
| 2026 | `player_season_batting` | 351 | CRAWLER 320, AGGREGATED 17, FINAL_VERIFICATION 12, other 2 | 1 team mismatch |
| 2026 | `player_season_pitching` | 341 | AGGREGATED 261, CRAWLER 27, PROFILE 28, other 25 | 0 team mismatches |

The OCI quality gate now applies source precedence through the physical
`data_source` column. This removed all 2026 pitching mismatches. The remaining 2021
pitching difference is not an exact duplicate logical key: PROFILE and MANUAL_RECALC
rows frequently use different player IDs for the same apparent player/team identity.
Automatically deleting one source or merging IDs would risk historical identity loss.

The OCI regression pack passed all 10 checks for 2026. For 2021, 9 of 10 checks passed;
the only failure was the existing `era_range` check for player IDs 73 and 1352, where
the stored ERA has no positive innings basis. No repair was applied.
