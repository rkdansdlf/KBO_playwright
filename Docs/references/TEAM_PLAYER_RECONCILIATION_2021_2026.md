# Team-Player Reconciliation Report

작성 범위: 2021/2026 정규시즌, local SQLite와 OCI PostgreSQL sync target의
`team_season_*` 대 `player_season_*` 비교.

## 결론

ER(`earned_runs`) 및 SB/CS(`stolen_bases`, `caught_stealing`) 차이는 의도한 공식
source semantics 차이로 비차단 처리한다.
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

`team_*` 결과에는 ER/SB/CS 비차단 필드가 `semantics_exempt_fields`와
`semantics_exempt_diffs`로 별도 보고된다. 나머지 필드 불일치는 계속 blocking 상태다.

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
이 필드는 source contract상 unavailable로 표시하거나 non-blocking semantics 차이로
보고하며, 0으로 저장해 실제 0으로 해석하면 안 된다.

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
   필드를 `unavailable_fields`로 보고 비교에서 제외한다. 값이 있더라도 SB/CS 차이는
   `semantics_exempt_diffs`로만 보고하며 blocking하지 않는다.
3. Local SQLite 2021/2026 gate는 source precedence와 2026 aggregate-key remediation
   이후 통과한다. OCI quality gate도 `data_source` schema alias를 지원하도록 보정되어
   OCI 2026 pitching mismatch가 10개에서 0개로 줄었다.
4. OCI 2021 pitching에는 `PROFILE`과 `MANUAL_RECALC`가 서로 다른 player ID 집합으로
   저장된 identity duplication이 남아 있다. 동일 선수 여부를 이름만으로 판정할 수
   없으므로 자동 삭제나 ID 병합은 보류한다. OCI 2021 batting 2개와 2026 batting
   1개는 소규모 PA/AB source-scope 차이로 별도 검토한다.
5. OCI 2021/2026 audit은 실제 OCI 연결 dialect가 Oracle임을 확인했다. 지정한
   정규시즌 legacy row의 broad cleanup은 quality regression 확인 후 백업에서 복원했고,
   higher-priority source가 같은 logical key에 있는 경우만 삭제하는 safe cleanup으로
   전환했다.

다른 source와 다른 league의 row는 삭제하지 않았다.

## OCI Legacy Source Cleanup (2026-07-26)

- Scope: `player_season_batting`/`player_season_pitching`, `league=REGULAR`,
  seasons 2021 and 2026.
- Cleanup scope: `PROFILE`, `AGGREGATED`, `ROLLUP` only.
- Broad cleanup backup: `data/archive/oci_legacy_player_season_sources_2021_2026.json`.
- Broad apply temporarily removed batting 18 and pitching 587 rows, then restored them
  after 2026 pitching regressed because some legacy rows were the only fallback source.
- Final safe cleanup dry-run found 0 rows with a higher-priority source for the same
  logical key, so the final OCI state retains those fallback rows.
- OCI gate remains blocked by 2021/2026 identity/scope differences; ER and SB/CS are
  non-blocking.

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

The OCI regression pack passed all 10 checks for 2026. The 2026-08-02 rerun passed
all 10 checks for 2021 after innings evidence was applied for players 73 and 1352.

## OCI 2021 Pitching Identity Audit

The read-only identity audit found 319 `(team_code, player_name)` groups in the OCI
regular-season pitching table; 302 groups contain more than one `player_id` across
`PROFILE` and `MANUAL_RECALC` rows. The target has no 2021 `game_pitching_stats` rows,
no 2021 `player_game_pitching` rows, and no `team_daily_roster` table, so OCI alone
cannot provide exact game or roster evidence for merging these IDs.

The two sources often contain matching games and decisions under different IDs, but
that pattern is not sufficient to prove identity for every historical homonym. No
bulk identity override, source-row deletion, or player-ID merge was applied. Any future
override must be backed by local game evidence, an official profile, or a dated roster
record and must be recorded as a row-level or group-level override.

## OCI 2021 Identity Pilot (2026-07-26)

The pilot used the top three teams by duplicate-group impact (`LT`, `HH`, `SSG`) and
produced `data/audit/oci_2021_identity_audit_20260726_pilot.json`. It covered 101
duplicate `(team_code, player_name)` groups:

| Classification | Count |
|---|---:|
| Exact local game evidence | 100 |
| Ambiguous | 0 |
| Unresolved | 1 |

The unresolved group is `HH/박성웅`, with candidate IDs `2614` and `68703`. ID `68703`
has local season-level evidence, but neither candidate has local 2021 game-level evidence,
so it was not promoted to an override. The 100 exact candidates were exported to a
separate review CSV and were not merged into `data/player_id_overrides.csv`.

## OCI 2021 Full Identity Expansion (2026-08-02)

The pilot logic was expanded to all 302 duplicate groups. Unique local season evidence
was added as a lower-priority exact signal after game evidence:

| Classification | Count |
|---|---:|
| Exact | 301 |
| Ambiguous | 1 (`KT/오윤석`) |
| Unresolved | 0 |

The review workflow marked 301 candidates as `eligible` and kept the ambiguous group as
`manual_review`. No candidate was automatically appended to the override CSV.

## OCI 2021 Innings Backfill (2026-07-26)

The migration ran first in dry-run mode and then with `--apply` in one transaction.
Both selected rows are now populated:

| Target | Evidence | Before | After | Status |
|---|---|---|---|---|
| `player_id=73`, target row `472738`, `KH` | local `player_id=50397`, exact `박관진/KH` game evidence | 0 outs / 0.0 IP | 2 outs / 0.6667 IP | applied |
| `player_id=1352`, target row `472807`, `KIA` | local `player_id=61700`, exact `강경학/KIA` season evidence | 0 outs / 0.0 IP | 2 outs / 0.6667 IP | applied |

The apply report records the original values for rollback review. The post-apply 2021
regression pack passes all 10 checks. No identity merge or cross-team override was
applied; the season rows were supplemented only with positive innings evidence.
