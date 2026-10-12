# Stadium Master Data (`stadiums` / `places`) — 운영 런북

## 개요

- `stadiums`: 구장 마스터 13행 (1군 9 + 임시 1 + 2군 3). 펜스·잔디·불펜·위경도·연락처·유효기간 포함.
- `places`: 구장 편의시설 151행 (음식점·매점·용품점·화장실·기타). 좌표 NOT NULL(구장 좌표 fallback), 운영시간은 원천 미공개로 대부분 NULL.
- `homerun_park_factor`는 배치 계산값. 득점 기반 대리값 기입 금지.

## 시드

```bash
.venv/Scripts/python.exe scripts/seed_stadiums_places.py --dry-run   # 미리보기
.venv/Scripts/python.exe scripts/seed_stadiums_places.py              # 기본 적재 (upsert, 재실행 안전)
.venv/Scripts/python.exe scripts/seed_stadiums_places.py --from-food  # 음식점 이관
.venv/Scripts/python.exe scripts/seed_stadiums_places.py --park-factor-year 2025  # HR 팩터 기입
```

- 로컬 SQLite 대상: `DATABASE_URL=sqlite:///data/kbo_dev.db` prefix.
- 운영 PG 대상: `.env`의 `DATABASE_URL` (별도 지정 불필요).

## 검증 (읽기전용)

```bash
.venv/Scripts/python.exe scripts/verify_stadiums_places.py  # exit 0 정상, exit 1 고아 행
```

- 고아 `places` → exit 1. food 미이관·전화/팩터 NULL·구 `stadium_info` 미커버는 경고만.

## 스케줄러

- `compute_park_factor_job` (일 05:30): 득점 팩터 로그 + HR 팩터 계산·저장.
- `verify_stadiums_places_job` (일 05:45): 검증 실패 시 warning 알림.

## 수렴 방향 (결정됨)

- 정본은 `stadiums`. 1단계 완료: 구 구장 9행 이관으로 `stadium_info` 21코드 전부 커버.
- 2단계 완료: 6개 자식 FK를 `stadium_info` → `stadiums`로 전환
  (PG `065`·SQLite `070`·Oracle `082`, 고아 0건 사전 확인).
  `stadium_info` 테이블·시더·구 API는 동결 유지 (삭제하지 않음).
  값(코드)이 그대로라 크롤러·리포지토리 코드 변경 없음.

## 알려진 한계

- 2025년 경기 HR이 `game_batting_stats`·PBP에 없어 HR 백필은 빈손 복귀가 정상.
  player-game HR 백필이 들어오면 일요 잡이 자동 기입.
- 화장실·운영시간은 위치 특정된 것만 적재 (잠실·창원·고척 일부, SSG 매표소).
  나머지는 원천 부재로 수집 불가 확정.
- 문학 외 전화번호 4곳 NULL (공식 원천 없음).
- Oracle `080`은 레거시 체인으로 미적용.
