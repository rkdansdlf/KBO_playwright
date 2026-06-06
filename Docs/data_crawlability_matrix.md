# KBO 데이터 크롤링 가능성 분류표

작성 기준: 2026-06-01 KST. 이 문서는 앱에서 제공하려는 KBO 데이터 항목을 공개 웹 소스 기준으로 `가능`, `조건부 가능`, `불가/비권장`으로 나눈 1차 분류다.

## 판정 기준

| 등급 | 의미 | 운영 기준 |
| --- | --- | --- |
| 가능 | KBO 공식 페이지나 공개 구조화 응답에서 안정적으로 반복 수집 가능 | 정규 파이프라인에 포함하고 원천 URL/수집시각을 저장 |
| 조건부 가능 | 발표 시점, 동적 페이지, 제3자 API, 뉴스 텍스트, 권리 이슈 때문에 누락/오탐 가능 | `confidence`, `source_url`, raw snapshot, 재검증 큐가 필요 |
| 불가/비권장 | 공개 원천이 없거나 로그인/유료/권리 제한/추측 영역 | 수집 대상에서 제외하거나 수동 입력/제휴 API로만 처리 |

## 확인한 주요 원천

| 원천 | 대표 URL | 주요 데이터 |
| --- | --- | --- |
| KBO 경기일정/결과 | `https://www.koreabaseball.com/Schedule/Schedule.aspx` | 일정, 시작 시간, 구장, TV/라디오, 게임센터 링크, 취소/순연/종료 상태 |
| KBO 스코어보드 | `https://www.koreabaseball.com/Schedule/ScoreBoard.aspx` | 당일 스코어, 경기 상태, 승/패/세이브, 이닝별 득점 |
| KBO 게임센터 | `https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx` | 리뷰/문자중계/라인업/엔트리/하이라이트 탭 |
| KBO 내부 XHR | `GetKboGameList`, `GetLineUpAnalysis` | 선발투수, 라인업 발표 여부, 선발 라인업 |
| Naver Sports relay API | `api-gw.sports.naver.com/schedule/.../relay` | 실시간/완료 문자중계, 점수, 주자, 아웃, 이닝 상태 |
| KBO 선수 등록 현황 | `https://www.koreabaseball.com/Player/Register.aspx` | 1군 등록 현황, 당일 등록/말소 |
| KBO 선수 이동 현황 | `https://www.koreabaseball.com/Player/Trade.aspx` | 선수 이동, 트레이드/FA/웨이버 계열 |
| KBO 티켓 안내 | `https://www.koreabaseball.com/Kbo/League/Map.aspx` | 구단별 입장요금/예매처 링크 |
| 구단 공식 사이트 | 각 구단 공지/티켓/구장 페이지 | 팬 이벤트, 티켓, 좌석, 주차, 교통, 먹거리 |
| Naver/KBO/구단 뉴스 | 뉴스 API/공지 페이지 | 부상, 복귀, 외국인 교체, 감독/코치 변동, 이슈 |

## 1. 경기 일정/상태 데이터

| 항목 | 판정 | 근거/구현 위치 | 주의사항 |
| --- | --- | --- | --- |
| 오늘/내일/이번 주 경기 일정 | 가능 | `ScheduleCrawler`, `save_schedule_game` | KST 기준 날짜로 월간 일정을 수집한 뒤 날짜 범위 질의 |
| 경기 시작 시간 | 가능 | KBO 일정, `GameMetadata.start_time` | 우천/특별 편성으로 변경될 수 있어 당일 재수집 필요 |
| 취소/순연 여부 | 가능 | KBO 일정/스코어보드 상태, `game_status` | 취소 직후 반영 지연 가능 |
| 경기 상태 | 가능 | `normalize_game_status`, Naver lifecycle | `SCHEDULED`, `LIVE`, `COMPLETED`, `CANCELLED`, `POSTPONED`, `DELAYED`, `SUSPENDED`로 표준화 |
| 당일 결과 | 가능 | KBO 스코어보드/게임센터 리뷰 | 종료 직후 30~60분 후 재검증 권장 |
| 현재 스코어 | 조건부 가능 | `live_crawler`, `RelayCrawler`, KBO 스코어보드 | 실시간은 Naver API 의존도가 높음 |
| 진행 이닝 | 조건부 가능 | Naver relay `textRelays`, `GamePlayByPlay` | KBO 공식 스코어보드만으로는 실시간 세부 상태가 제한적 |

## 2. 경기 당일 라인업/상황 데이터

| 항목 | 판정 | 근거/구현 위치 | 주의사항 |
| --- | --- | --- | --- |
| 선발투수 | 가능 | `PreviewCrawler.GetKboGameList`, `away_pitcher/home_pitcher` | 발표 전에는 빈 값 |
| 선발 라인업 | 조건부 가능 | `PreviewCrawler.GetLineUpAnalysis`, 게임센터 `LINEUP` | 보통 경기 전 발표 후 제공, 일부 경기/시리즈 누락 가능 |
| 타순 | 조건부 가능 | `GameLineup.batting_order`, 박스스코어 | 경기 전은 발표 후, 경기 후는 박스스코어 기준 가능 |
| 다음 타자 | 조건부 가능 | 현재 타순 + PBP 진행 상태로 추론 | 대타/교체는 실제 발표 전 예측 불가, `inferred`로 표시 |
| 다음 투수 | 불가/비권장 | 공개 사전 원천 없음 | 불펜 교체는 공식 발표 전 알 수 없으므로 예측 모델 영역 |
| 결승타 | 조건부 가능 | 게임센터 기타 요약 `#tblEtc`, `GameSummary` | 제공되지 않는 경기 있음 |
| 승리투수/패전투수/세이브 | 가능 | KBO 스코어보드, 투수 박스스코어 | 홀드 포함 저장 가능 |
| 홀드 | 가능 | 투수 박스스코어 `홀드`, `GamePitchingStat.holds` | 스코어보드에는 없고 상세 투수표 필요 |
| 경기 MVP | 조건부 가능 | `GameMvpCrawler` 뉴스 기반 | 공식 경기별 MVP 테이블이 아니므로 오탐/누락 가능 |
| 득점권/주자/아웃카운트 라이브 상황 | 조건부 가능 | Naver relay `currentGameState`, `GameEvent.base_state/outs` | 실시간 API 스키마 변경 리스크, 과거 재구성은 PBP 품질에 의존 |

## 3. 엔트리/부상/뉴스성 데이터

| 항목 | 판정 | 근거/구현 위치 | 주의사항 |
| --- | --- | --- | --- |
| 부상자 | 조건부 가능 | `InjuryCrawler` 뉴스 키워드, KBO 등록/말소 보조 | 공식 통합 부상자 명단이 없어 기사 기반 신뢰도 관리 필요 |
| 복귀 예정 선수 | 조건부 가능 | 뉴스 본문/구단 공지 | 예상일은 자주 바뀌며 구조화 정확도가 낮음 |
| 콜업/말소 | 가능 | KBO 선수 등록 현황, `RosterTransactionCrawler` | 당일 1군 기준은 안정적 |
| 엔트리 변동 | 가능 | `DailyRosterCrawler`, `team_daily_roster` | 전체 등록 현황과 당일 등록/말소를 구분 저장 |
| 트레이드 | 가능 | KBO 선수 이동 현황, `PlayerMovementCrawler` | 세부 조건은 기사/구단 공지 보강 필요 |
| 계약/FA | 조건부 가능 | KBO 이동 현황, 구단/KBO 보도자료, 뉴스 | 계약금/연봉 등 금액은 원문 확인 필요 |
| 외국인 선수 교체 | 조건부 가능 | `ForeignPlayerCrawler` 뉴스 기반 | 공식 발표는 수집 가능하나 구조화는 NLP/검수 필요 |
| 감독/코치 변동 | 조건부 가능 | `ManagerChangeCrawler`, 등록 현황 staff snapshot | 현재 staff roster는 가능, 변동 사유는 뉴스 기반 |
| 은퇴 소식 | 조건부 가능 | 은퇴 크롤러/뉴스/프로필 상태 | 공식 은퇴 공시가 아닌 기사 기반이면 신뢰도 표시 |
| 최신 이슈 | 조건부 가능 | `RealtimeIssueCrawler`, 뉴스/공지 | 루머/커뮤니티 글은 사실 데이터로 저장하지 않음 |

## 4. 구장/티켓/직관 정보

| 항목 | 판정 | 근거/구현 위치 | 주의사항 |
| --- | --- | --- | --- |
| 티켓 예매처 | 가능 | KBO 티켓 안내, 구단 티켓 페이지, `TicketCrawler` | 예매 플랫폼 URL 변경 감시 필요 |
| 티켓 가격 | 가능 | KBO/구단 입장요금, `TicketPrice` | 좌석/요일/상대/이벤트별 예외를 원문 링크와 함께 저장 |
| 예매 오픈일 | 조건부 가능 | `TicketOpenRule` 규칙 + 구단 공지 | 정확한 경기별 오픈은 공지/플랫폼 확인 필요 |
| 좌석 추천 | 불가/비권장 | 직접 원천 없음 | 좌석 속성 기반 자체 추천으로 생성해야 함 |
| 가족석/테이블석/휠체어석 | 조건부 가능 | 구단 좌석도/티켓 페이지, `SeatCrawler` | 구단별 표기 차이가 커서 수동 보정 필요 |
| 주차 | 조건부 가능 | 구단 구장 안내, `ParkingCrawler` | 주차 가능 여부/요금은 경기일 이벤트에 따라 변동 |
| 대중교통 | 조건부 가능 | 구단 구장 안내, `StadiumInfo.public_transport` | 정적 안내는 가능, 실시간 교통은 별도 API 필요 |
| 먹거리 | 조건부 가능 | 구단 F&B 페이지, `FoodCrawler` | 메뉴/가격은 누락과 변경이 잦음 |
| 매진 여부 | 불가/조건부 | 예매 플랫폼 동적/로그인/대기열 | 공개 잔여석 API 또는 제휴 없이는 안정 수집 불가 |
| 홈구장 주소 | 가능 | KBO/구단 구장 안내, `StadiumInfo.address` | 정적 마스터 데이터로 관리 가능 |
| 입장 마감 시간 | 조건부 가능 | 구단 공지/티켓 정책 | 경기별/행사별로 달라 통합 원천 부족 |

## 5. 중계/미디어 정보

| 항목 | 판정 | 근거/구현 위치 | 주의사항 |
| --- | --- | --- | --- |
| TV 중계 채널 | 가능 | KBO 일정 TV 컬럼, `BroadcastCrawler` | 채널 약어 정규화 필요 |
| 라디오 | 가능 | KBO 일정 라디오 컬럼 | 모든 경기에 제공되지는 않음 |
| 문자중계 | 가능 | KBO 게임센터/네이버 relay | 링크와 텍스트 이벤트 수집 가능 |
| 다시보기 | 조건부 가능 | KBO/구단/플랫폼 링크 | 영상 파일 저장은 권리 이슈가 있어 링크 메타데이터만 권장 |
| 하이라이트 | 조건부 가능 | KBO 하이라이트 페이지/게임센터 링크 | 경기 직후 지연, 영상 다운로드/재배포 금지 |
| 해외 시청 가능 여부 | 조건부 가능 | SOOP/중계권 보도자료 및 공지 | 지역 제한/권리 변경이 있어 시즌 단위 공지로 관리 |

## 6. 팬 이벤트/응원 정보

| 항목 | 판정 | 근거/구현 위치 | 주의사항 |
| --- | --- | --- | --- |
| 응원가 | 조건부/비권장 | 구단/팬 페이지, `FanCultureCrawler` | 가사 전문 저장은 저작권 리스크, 제목/링크/용도 중심 권장 |
| 응원 구호 | 조건부 가능 | 구단/팬 페이지 | 출처와 갱신일 필요 |
| 직관 이벤트 | 조건부 가능 | 구단 공지, `TeamEventCrawler` | 공지 형식이 구단마다 달라 parser 유지보수 필요 |
| 굿즈 | 조건부 가능 | 구단 샵/공지 | 가격/재고는 상거래 페이지라 변동/차단 가능 |
| 팬서비스 | 조건부 가능 | 구단 공지 | 비정형 공지라 분류 모델/검수 필요 |
| 응원석 정보 | 조건부 가능 | 티켓/좌석 안내 | 좌석명은 가능, 응원 강도 추천은 자체 해석 |

## 8. 선수 기록/통계 데이터

| 항목 | 판정 | 근거/구현 위치 | 주의사항 |
|------|------|----------------|----------|
| 게임별 타격/투수 기록 | 조건부 가능 | `GameDetailCrawler` | 2020-2021: HITTER/PITCHER 탭 없음 → SH/SF 수집 불가<br>2022+: 구조화된 테이블 제공 (희타/희비 컬럼 존재) |
| 선수 일별 기록 | 조건부 가능 | `PlayerDailyStatsCrawler` | 모든 연도: SH/SF 컬럼 없음 (PA, AB, BB, HBP 등 제공)<br>SH/SF 추정 불가 (보정 불가) |
| 선수 시즌 누적 기록 | 가능 | `PlayerSeasonCrawler` | 모든 연도: SH/SF 제공 (단, 구성요소 신뢰도는 소스에 따름) |
| 투수 상세 기록 (피안타/볼넷 등) | 조건부 가능 | `GameDetailCrawler` 투수 테이블 | 2020-2021: 일부 세부 항목 누락 가능 |

## 7. 시즌 메타 데이터

| 항목 | 판정 | 근거/구현 위치 | 주의사항 |
|------|------|----------------|----------|
| 2026 KBO 개막일 | 가능 | KBO 보도자료: 2026 정규시즌은 2026-03-28 개막 | `kbo_seasons.start_date`에 저장 가능 |
| 2026 개막전 장소 | 가능 | KBO 보도자료: 잠실, 대전, 문학, 대구, 창원 | 대진: KT-LG, 키움-한화, KIA-SSG, 롯데-삼성, 두산-NC |
| 올스타전/휴식기 | 가능 | K보 보도자료 | 시즌 메타 또는 별도 이벤트 테이블 필요 |

## 9. 주관 평가/전망 데이터

| 항목 | 판정 | 근거/구현 위치 | 주의사항 |
|------|------|----------------|----------|
| 언론 프리뷰/전망 기사 | 조건부 가능 | 뉴스/보도자료 링크 수집 | 원문 전문 저장 대신 제목/요약/링크/출처 중심 |
| 전문가 평점/파워랭킹 | 조건부/비권장 | 매체별 기사/콘텐츠 | 라이선스와 주관성 문제, 사실 테이블과 분리 |
| 자체 승부 예측/전망 | 크롤링 대상 아님 | 구조화 데이터 기반 생성/모델 추론 | `source=derived`로 명확히 구분 |

## 현 저장소 기준 구현 매핑

| 데이터군 | 이미 있는 주요 구성요소 | 판정 |
| --- | --- | --- |
| 일정/상태/점수 | `ScheduleCrawler`, `GameDetailCrawler`, `live_crawler`, `game_status` utils | 즉시 확장 가능 |
| 선발/라인업 | `PreviewCrawler`, `GameLineup`, `GamePitchingStat` | 발표 타이밍 보완 필요 |
| 라이브/PBP | `RelayCrawler`, `GameEvent`, `GamePlayByPlay`, WPA | Naver API 스키마 감시 필요 |
| 등록/말소/로스터 | `RosterTransactionCrawler`, `DailyRosterCrawler`, `TeamDailyRoster` | 공식 원천 기반 가능 |
| 부상/외국인/감독/MVP | `InjuryCrawler`, `ForeignPlayerCrawler`, `ManagerChangeCrawler`, `GameMvpCrawler` | 뉴스 NLP 품질 관리 필요 |
| 티켓/구장/좌석/주차/먹거리 | `TicketCrawler`, `SeatCrawler`, `ParkingCrawler`, `FoodCrawler`, `StadiumInfo` | 구단별 parser coverage 확대 필요 |
| 팬 이벤트 | `TeamEventCrawler`, `TeamEvent` | 공지 parser 강화 필요 |
| 응원 문화 | `FanCultureCrawler`, `CheerSong`, `CheerChant` | 저작권/출처 정책 필요 |

## 1차 우선순위

1. P0: 경기 일정/상태/점수, 선발투수, 선발 라인업, 등록/말소, TV/라디오, 2026 시즌 메타.
2. P1: 티켓 가격/예매처/오픈 규칙, 구장 주소/교통/주차/먹거리, 직관 이벤트, 부상/복귀 뉴스.
3. P2: MVP, 해외 시청 가능 여부, 하이라이트/다시보기 링크, 응원석/좌석 추천.
4. 제외 또는 수동: 다음 투수, 매진 여부, 전문 응원가 가사, 루머성 최신 이슈, 주관 전망 원문 전문.

## 운영 원칙

- 모든 조건부 데이터는 `source_url`, `source_type`, `confidence`, `last_seen_at`을 저장한다.
- 뉴스 기반 구조화는 사실 DB와 분리하고, 원문 링크를 함께 노출한다.
- 실시간 데이터는 KBO 공식 상태와 Naver relay를 교차 검증한다.
- 로그인/유료/대기열이 있는 예매 플랫폼은 무리하게 우회하지 않는다.
- 저작권이 걸린 영상/가사/기사 전문은 저장하지 않고 링크와 짧은 메타데이터 중심으로 처리한다.
- Pre-2024 시즌의 SH/SF 값은 추정치이므로, 신뢰도 플래그와 함께 기록 및 사용 시 주의 요망.
