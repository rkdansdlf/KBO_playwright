# 잔여 테이블 적재 설계안 (bega_prod, 2026-09-25 기준)

빈 19종 중 파생·시드·외부통계·응원가·이동시간으로 11종 해소.
아래 8종이 잔여. 공통 원칙: 네트워크 크롤보다 **보유 데이터(3.8M행) 유도 우선**.

## 1. 로컬 유도로 해결 가능 (네트워크 불필요)

| 테이블 | 설계 |
|---|---|
| `matchup_batter_home_away` / `matchup_pitcher_home_away` | `MatchupEngine`에 홈/원정 스플릿 계산 추가. `player_game_batting` + `game`(홈/원정 식별) 조인으로 집계. 기존 `_calc_batter_team_splits` 패턴 재사용. 입력 데이터 존재 확인됨 |
| `player_milestones` | 커리어 누적 임계치(2000안타·300홈런·200승 등)는 `player_season_*` 합산으로 유도 가능. `MilestoneDerivationService` 신설 + 임계치 상수 테이블. 크롤러는 불필요 |
| `player_splits_stats` | PBP 기반 스플릿은 이미 `matchup_batter/pitcher_splits`에 존재. 스키마 매핑 ETL로 이관 후 `recalc_milestones`(AVG/OPS 갱신) 실행. KBO Basic1 크롤 불필요 |
| `cheer_chants` | `cheer_songs` 448행에서 `chant_text`(곡명 정규화) 유도 ETL, 또는 `fan_culture_crawler`에 chants 저장 분기 추가. YouTube 추가 할당량 없이 해결 |

## 2. 대체 소스 필요 (robots.txt 차단)

| 테이블 | 차단 경로 | 대체안 |
|---|---|---|
| `player_draft_histories` | koreabaseball.com Draft.aspx | 위키백과 KBO 신인드래프트 문서 파서 (연 1회, 저빈도). `crawl_player_drafts --source wiki` 옵션 추가 |
| `ticket_schedules` | koreabaseball.com Map.aspx | 구단별 티켓 예매처 페이지 또는 `ticket_open_rules`(보유분) 기반 스케줄 생성. 우선순위 낮음(경기일 당일 수집으로 충분) |

## 3. 신규 개발 또는 수동 운영

| 테이블 | 설계 |
|---|---|
| `fa_contracts` | 연 1회·20여건이라 크롤러보다 **수동 CSV ingest** 권장. `data/seed/fa_contracts_<year>.csv` + 검증 CLI. 작성 코드 전무 상태이므로 repository부터 신설 필요 |
| `stadium_foods` | `crawl_stadium_food` 0건 반환(셀렉터 또는 구조 변경). Playwright 실측 후 셀렉터 갱신. 당장은 `stadium_food_vendors`(21)·`menu_items`(51) 시드로 커버 중이라 우선순위 낮음 |

## 4. 승인 대기 (제외 중)

- `stadium_congestion`: 서울 실시간 도시데이터 활용신청 승인 후 `crawl_congestion --save` 재실행 (3개 구역)

## 권장 순서

1. home_away 유도 → 2. milestones 유도 → 3. splits/chants ETL → 4. fa CSV ingest →
5. drafts wiki 어댑터 → 6. ticket 대체안 → 7. foods 셀렉터 수정 → 8. congestion 승인 후 실행
