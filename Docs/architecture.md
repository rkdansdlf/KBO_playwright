# KBO Playwright Architecture

## 데이터 흐름

```
KBO Website / Naver Sports
        │
        ▼
   [Crawler Layer]  ─── Playwright 기반 HTTP/브라우저 자동화
        │
        ▼
   [Parser Layer]   ─── HTML/JSON → 구조화된 dict
        │
        ▼
   [Repository Layer] ─── DB CRUD, Validation, Upsert
        │
        ▼
   [Database]       ─── Oracle Autonomous DB (운영) / SQLite (초기 원본·개발·테스트)
```

## 모듈 의존성 그래프

```
crawlers ──→ parsers ──→ repositories ──→ models
    │                      │
    │                      ▼
    └────────────────→ services ──→ utils
                              │
                              ▼
                         aggregators
                              │
                              ▼
                         cli (entry points)
```

## 크롤러 분류

| 유형 | 설명 | 사용 데이터 소스 |
|------|------|-----------------|
| `ScheduleCrawler` | 경기 일정 | koreabaseball.com |
| `GameDetailCrawler` | 박스스코어, 이닝별 점수 | koreabaseball.com |
| `RelayCrawler` | 실시간 문자중계 | Naver Sports |
| `FieldingStatsCrawler` | 수비 기록 | koreabaseball.com |
| `PlayerProfileCrawler` | 선수 프로필 | koreabaseball.com |
| `FuturesCrawler` | 퓨처스리그 데이터 | koreabaseball.com |
| `DynamicDataCrawler` | 티켓 오픈 시간, 로스터 변경 | koreabaseball.com |
| `TeamEventCrawler` | 팀 이벤트/뉴스 | 각 구단 사이트 |

## 동시성 제어

3-Stage Locking:
- **LIVE_LOCK**: 실시간 크롤 (경기 중)
- **DAILY_LOCK**: 일일 파이프라인 (일정/상세/PBP)
- **MAINTENANCE_LOCK**: 유지보수 (퓨처스/통계 재계산)

모든 DB 저장은 **UPSERT** 방식으로 멱등성 보장.

## 데이터 저장소

- **운영 야구 데이터**: Oracle Autonomous Database (`DATABASE_URL`)
- **초기 적재 원본/개발·테스트**: SQLite (`data/kbo_*.db`)
- **RAG BM25 원문**: Oracle `rag_chunks`
- **RAG dense vector**: 별도 PostgreSQL/pgvector (`PGVECTOR_URL`)

## 설정

`.env` 파일로 관리되는 주요 설정:
- `DATABASE_URL`
- `KBO_REQUEST_DELAY_MIN/MAX` — 요청 간격
- `KBO_USER_ID/PWD` — 로그인
- `YOUTUBE_API_KEY`, `NAVER_CLIENT_ID/SECRET` — 외부 API
