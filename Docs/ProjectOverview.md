# KBO Playwright 프로젝트 개요

KBO (한국야구위원회) 공식 웹사이트 및 Naver Sports 데이터를 수집, 정제, 저장하는 데이터 파이프라인입니다.

## 주요 기능

- **경기 데이터 수집**: KBO 웹사이트에서 경기 일정, 박스스코어, PBP, 수비 기록 크롤링
- **선수 통계**: 타/투/수비 시즌 통계 및 고급 스탯(WPA, sabermetrics) 계산
- **데이터 동기화**: 로컬 SQLite → OCI (Oracle Cloud Infrastructure) DB 동기화
- **품질 관리**: Quality Gate, Freshness Gate, Gap Report, PA Formula Audit
- **자동화 파이프라인**: GitHub Actions 기반 일일/주간/월간 스케줄링

## Two-Track Pipeline

- **Track A (KBO 공식)**: koreabaseball.com 크롤링 → Parser → Repository → DB
- **Track B (Relay/Naver)**: Naver Sports 문자중계 → Relay Crawler → Normalized Events → DB

## 주요 패키지

| 패키지 | 역할 |
|--------|------|
| `src/crawlers/` | Playwright 기반 웹 크롤러 |
| `src/parsers/` | HTML/JSON 파싱 |
| `src/repositories/` | DB 저장/조회 |
| `src/models/` | SQLAlchemy ORM 모델 |
| `src/services/` | 비즈니스 로직 (WPA, PlayerID, 등) |
| `src/cli/` | CLI 진입점 |
| `src/sync/` | OCI DB 동기화 |
| `src/aggregators/` | 팀/시즌 통계 집계 |
| `src/utils/` | 공통 유틸리티 (retry, throttle, 등) |
| `scripts/` | 유지보수/검증 스크립트 |

자세한 내용은 `Docs/architecture.md` 참조.
