# KBO_playwright 명령어 완전 가이드

이 문서는 KBO_playwright 프로젝트의 모든 크롤링 명령어와 사용법을 정리한 완전 가이드입니다.

## 📋 목차

1. [빠른 시작](#빠른-시작)
2. [환경 설정](#환경-설정)
3. [데이터베이스 관리](#데이터베이스-관리)
4. [크롤링 명령어](#크롤링-명령어)
5. [자동화 스크립트](#자동화-스크립트)
6. [Supabase 동기화](#supabase-동기화)
7. [문제 해결](#문제-해결)
8. [고급 사용법](#고급-사용법)

---

## 🚀 빠른 시작

### 최신 1년 데이터 수집
```bash
# 2024-2025년 정규시즌 + 포스트시즌 (SQLite 초기화 + 자동 동기화)
./crawl_clean_and_sync.sh 2024 2025

# 최근 3년 빠르게
./venv/bin/python3 crawl_all_historical.py --recent
```

### 전체 역사 데이터 수집
```bash
# 1982-2025년 전체 KBO 역사 (자동 전략 선택)
./venv/bin/python3 crawl_all_historical.py --full-history

# 수동 범위 지정
./crawl_year_range.sh 1982 2025 full
```

---

## ⚙️ 환경 설정

### 1. 초기 설정
```bash
# 가상환경 생성 및 활성화
python3 -m venv venv
source venv/bin/activate  # Linux/macOS
# venv\Scripts\activate   # Windows

# 의존성 설치
pip install -r requirements.txt

# Playwright 브라우저 설치
playwright install chromium

# 환경변수 설정
cp .env.example .env
# .env 파일 편집하여 DATABASE_URL 설정
```

### 2. 데이터베이스 초기화
```bash
# SQLite 데이터베이스 생성
./venv/bin/python3 init_db.py

# 기본 팀 데이터 시드
./venv/bin/python3 seed_teams.py
```

---

## 🗄️ 데이터베이스 관리

### SQLite 초기화
```bash
# 전체 플레이어 데이터 삭제
./venv/bin/python3 reset_sqlite.py --all

# 특정 년도 데이터만 삭제
./venv/bin/python3 reset_sqlite.py --year 2025

# 연도 범위 삭제
./venv/bin/python3 reset_sqlite.py --range 2020 2025

# 특정 테이블만 초기화
./venv/bin/python3 reset_sqlite.py --all --tables player_season_batting

# 확인 없이 강제 실행
./venv/bin/python3 reset_sqlite.py --all --force
```

### 데이터 검증
```bash
# 데이터 무결성 검증
./venv/bin/python3 verify_sqlite_data.py

# 타자/투수 데이터 분리 확인
./venv/bin/python3 verify_data_separation.py
```

---

## 🕷️ 크롤링 명령어

### 현대 크롤링 (2002년 이후)

#### 타자 데이터
```bash
# 기본 사용법
./venv/bin/python3 -m src.crawlers.player_batting_all_series_crawler \
    --year 2025 \
    --series regular \
    --save

# 전체 옵션
./venv/bin/python3 -m src.crawlers.player_batting_all_series_crawler \
    --year 2025 \
    --series regular \
    --save \
    --headless \
    --limit 100
```

#### 투수 데이터
```bash
# 기본 사용법
./venv/bin/python3 -m src.crawlers.player_pitching_all_series_crawler \
    --year 2025 \
    --series regular \
    --save

# 헤드리스 모드로 실행
./venv/bin/python3 -m src.crawlers.player_pitching_all_series_crawler \
    --year 2025 \
    --series regular \
    --save \
    --headless
```

#### 시리즈 옵션
- `regular`: 정규시즌
- `exhibition`: 시범경기
- `wildcard`: 와일드카드 결정전
- `semi_playoff`: 준플레이오프
- `playoff`: 플레이오프
- `korean_series`: 한국시리즈

### 레거시 크롤링 (2001년 이전)

#### 타자 데이터 (단순 구조)
```bash
# 2000년 정규시즌 타자
./venv/bin/python3 -m src.crawlers.legacy_batting_crawler \
    --year 2000 \
    --series regular \
    --save \
    --headless

# 1995년 한국시리즈 타자
./venv/bin/python3 -m src.crawlers.legacy_batting_crawler \
    --year 1995 \
    --series korean_series \
    --save
```

#### 투수 데이터 (단순 구조)
```bash
# 2001년 정규시즌 투수
./venv/bin/python3 -m src.crawlers.legacy_pitching_crawler \
    --year 2001 \
    --series regular \
    --save \
    --headless

# 1990년 시범경기 투수 (100명 제한)
./venv/bin/python3 -m src.crawlers.legacy_pitching_crawler \
    --year 1990 \
    --series exhibition \
    --save \
    --limit 100
```

### Futures 리그 크롤링
```bash
# 전체 Futures 선수 프로필
./venv/bin/python3 -m src.crawlers.futures.futures_batting \
    --save \
    --headless

# 개발/테스트용 (10명 제한)
./venv/bin/python3 -m src.crawlers.futures.futures_batting \
    --limit 10 \
    --save
```

### 은퇴선수 크롤링
```bash
# 특정 연도 범위 은퇴선수
./venv/bin/python3 -m src.cli.crawl_retire \
    --years 1982-2025 \
    --concurrency 3

# 최근 5년 은퇴선수만
./venv/bin/python3 -m src.cli.crawl_retire \
    --years 2020-2025 \
    --concurrency 5
```

### 운영 엔트리포인트 (신규 경기/선수 무결성)
```bash
# 운영 기준: 하루 단위 통합 업데이트
./venv/bin/python3 -m src.cli.run_daily_update --date 20251015

# 스케줄만 월 단위 반영
./venv/bin/python3 -m src.cli.crawl_schedule --year 2025 --months 10

# 수동 상세 수집(월 단위 대상 필터)
./venv/bin/python3 -m src.cli.collect_games --year 2025 --month 10
```

---

## 🤖 자동화 스크립트

### 1. 연도 범위 크롤링
```bash
# 기본 사용법 (대화형)
./crawl_year_range.sh

# 2020-2025년 빠른 모드
./crawl_year_range.sh 2020 2025

# 1982-2025년 완전 모드 (시범경기 포함)
./crawl_year_range.sh 1982 2025 full

# 도움말
./crawl_year_range.sh --help
```

### 2. 깨끗한 크롤링 + 동기화
```bash
# 2024-2025년 (기본값)
./crawl_clean_and_sync.sh

# 2022-2025년 지정
./crawl_clean_and_sync.sh 2022 2025

# 완전 모드
./crawl_clean_and_sync.sh 2020 2025 full
```

### 3. 자동 전략 선택 크롤링
```bash
# 최근 3년 자동 크롤링
./venv/bin/python3 crawl_all_historical.py --recent

# 전체 역사 자동 크롤링
./venv/bin/python3 crawl_all_historical.py --full-history

# 사용자 정의 범위
./venv/bin/python3 crawl_all_historical.py \
    --start 1990 \
    --end 2010 \
    --series regular korean_series

# DB 초기화 없이 실행
./venv/bin/python3 crawl_all_historical.py \
    --start 2024 \
    --end 2025 \
    --no-reset

# 브라우저 UI 표시
./venv/bin/python3 crawl_all_historical.py \
    --recent \
    --no-headless
```

### 4. 스케줄러 (자동화)
```bash
# 로컬 스케줄러 실행
./venv/bin/python3 -m scripts.scheduler

# Docker 스케줄러
docker-compose up -d scheduler

# 스케줄러 로그 확인
docker-compose logs -f scheduler
```

---

## ☁️ Supabase 동기화

### 환경변수 설정
```bash
# Supabase 연결 정보 설정
export SUPABASE_DB_URL='postgresql://postgres.xxx:[PASSWORD]@xxx.pooler.supabase.com:5432/postgres'
```

### 동기화 명령어
```bash
# 전체 데이터 동기화
./venv/bin/python3 -m src.sync.supabase_sync

# 특정 모델만 동기화
./venv/bin/python3 -m src.sync.supabase_sync --models PlayerSeasonBatting

# 건배치 동기화 (메모리 절약)
./venv/bin/python3 -m src.sync.supabase_sync --batch-size 500

# 동기화 상태 확인
./venv/bin/python3 -c "
from src.sync.supabase_sync import check_sync_status
check_sync_status()
"
```

---

## 🚨 문제 해결

### 크롤링 실패 시
```bash
# 에러 로그 확인
tail -f crawl_errors.log

# 특정 년도/시리즈 재시도
./venv/bin/python3 -m src.crawlers.player_batting_all_series_crawler \
    --year 2024 \
    --series regular \
    --save \
    --headless

# 브라우저 표시로 디버깅
./venv/bin/python3 -m src.crawlers.player_batting_all_series_crawler \
    --year 2024 \
    --series regular \
    --save
```

### 데이터베이스 문제
```bash
# SQLite 손상 시 재구축
rm data/kbo_dev.db*
./venv/bin/python3 init_db.py
./venv/bin/python3 seed_teams.py

# 팀/시즌 데이터만 빠졌을 경우
# CSV에 스키마 행만 있어도 `seed_data.py`는 자동으로 무시하고
# 22개 기본 팀 리스트를 병합해 FK 무결성을 유지합니다.
./venv/bin/python3 seed_data.py

# 외래키 제약조건 확인
./venv/bin/python3 -c "
from src.db.engine import SessionLocal
with SessionLocal() as session:
    session.execute('PRAGMA foreign_key_check')
"
```

### 팀 매핑 문제
```bash
# 팀 매핑 테스트
./venv/bin/python3 -c "
from src.utils.team_mapping import get_team_code
print(get_team_code('MBC청룡', 1985))  # LG 트윈스로 매핑
print(get_team_code('해태타이거즈', 1990))  # KIA 타이거즈로 매핑
"

# Supabase 팀 히스토리 확인
./venv/bin/python3 -c "
from src.utils.team_mapping import TeamMappingService
tms = TeamMappingService()
tms.load_supabase_mapping()
print(tms.year_specific_mapping)
"
```

---

## 🎓 고급 사용법

### 개발/테스트 크롤링
```bash
# 소수 데이터로 테스트
./venv/bin/python3 -m src.crawlers.player_batting_all_series_crawler \
    --year 2025 \
    --series regular \
    --limit 10

# 브라우저 표시로 디버깅
./venv/bin/python3 -m src.crawlers.player_batting_all_series_crawler \
    --year 2025 \
    --series regular \
    --save
```

### 성능 최적화
```bash
# 병렬 크롤링 (조심해서 사용)
./venv/bin/python3 -m src.cli.crawl_retire \
    --years 2020-2025 \
    --concurrency 5

# 메모리 최적화 동기화
./venv/bin/python3 -m src.sync.supabase_sync --batch-size 100
```

### 데이터 분석
```bash
# 데이터 통계 확인
./venv/bin/python3 -c "
from src.db.engine import SessionLocal
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching
from sqlalchemy import func

with SessionLocal() as session:
    # 연도별 데이터 수
    for year in range(2020, 2026):
        batting = session.query(PlayerSeasonBatting).filter_by(season=year).count()
        pitching = session.query(PlayerSeasonPitching).filter_by(season=year).count()
        print(f'{year}년: 타자 {batting}, 투수 {pitching}')
"

# 시리즈별 통계
./venv/bin/python3 -c "
from src.db.engine import SessionLocal
from src.models.player import PlayerSeasonBatting
from sqlalchemy import func

with SessionLocal() as session:
    stats = session.query(
        PlayerSeasonBatting.league,
        func.count(PlayerSeasonBatting.id)
    ).group_by(PlayerSeasonBatting.league).all()
    
    for league, count in stats:
        print(f'{league}: {count}건')
"
```

### 커스텀 크롤링 스크립트
```python
# custom_crawl.py
from src.crawlers.player_batting_all_series_crawler import crawl_batting_stats

# 특정 조건 크롤링
results = crawl_batting_stats(
    year=2025,
    series_key='regular',
    save_to_db=True,
    headless=True
)

print(f"수집된 선수 수: {len(results)}")
```

---

## 📊 크롤링 전략 가이드

### 연도별 권장 전략

#### 1982-2001년 (레거시 모드)
- **특징**: 단순 컬럼 구조
- **타자**: 순위, 선수명, 팀명, AVG, G, PA, AB, H, 2B, 3B, HR, RBI, SB, CS, BB, HBP, SO, GDP, E
- **투수**: 순위, 선수명, 팀명, ERA, G, GS, W, L, SV, HLD, IP, H, HR, BB, SO, R, ER
- **명령어**: `legacy_batting_crawler.py`, `legacy_pitching_crawler.py`

#### 2002년-현재 (현대 모드)
- **특징**: 복합 구조, 상세 통계
- **타자**: 기본 + OPS, wOBA, WAR 등 세이버메트릭스
- **투수**: 기본 + WHIP, FIP, K/9, BB/9 등 고급 통계
- **명령어**: `player_batting_all_series_crawler.py`, `player_pitching_all_series_crawler.py`

### 시리즈별 우선순위
1. **정규시즌** (`regular`): 가장 중요, 우선 크롤링
2. **한국시리즈** (`korean_series`): 포스트시즌 최고 단계
3. **플레이오프** (`playoff`): 준결승/결승
4. **와일드카드** (`wildcard`): 추가 진출전
5. **시범경기** (`exhibition`): 참고용 데이터

### 크롤링 빈도 권장사항
- **정규시즌 중**: 매일 03:00 KST
- **포스트시즌**: 경기 후 즉시
- **비시즌**: 주 1회 (일요일 05:00 KST)
- **역사 데이터**: 월 1회 검증

---

## 🔧 환경별 설정

### 개발 환경
```bash
# SQLite 전용
export DATABASE_URL="sqlite:///./data/kbo_dev.db"

# 빠른 테스트
./venv/bin/python3 -m src.crawlers.player_batting_all_series_crawler \
    --year 2025 --series regular --limit 5
```

### 프로덕션 환경
```bash
# PostgreSQL 연결
export DATABASE_URL="postgresql://user:pass@localhost:5432/kbo_prod"
export SUPABASE_DB_URL="postgresql://postgres.xxx:pass@xxx.pooler.supabase.com:5432/postgres"

# 안정적인 크롤링
./crawl_clean_and_sync.sh
```

### Docker 환경
```bash
# 컨테이너 빌드
docker-compose build

# 스케줄러 실행
docker-compose up -d scheduler

# 로그 모니터링
docker-compose logs -f scheduler
```

---

## 📊 크롤링 전략 가이드

### 연도별 권장 전략

#### 1982-2001년 (레거시 모드)
- **특징**: 단순 컬럼 구조
- **타자**: 순위, 선수명, 팀명, AVG, G, PA, AB, H, 2B, 3B, HR, RBI, SB, CS, BB, HBP, SO, GDP, E
- **투수**: 순위, 선수명, 팀명, ERA, G, GS, W, L, SV, HLD, IP, H, HR, BB, SO, R, ER
- **명령어**: `legacy_batting_crawler.py`, `legacy_pitching_crawler.py`
- **시리즈**: regular, korean_series, exhibition (2000-2001년에는 플레이오프 없음)

#### 2002년-현재 (현대 모드)
- **특징**: 복합 구조, 상세 통계
- **타자**: 기본 + OPS, wOBA, WAR 등 세이버메트릭스
- **투수**: 기본 + WHIP, FIP, K/9, BB/9 등 고급 통계
- **명령어**: `player_batting_all_series_crawler.py`, `player_pitching_all_series_crawler.py`
- **시리즈**: regular, korean_series, playoff, semi_playoff (2007+), wildcard (2015+), exhibition

### 연도별 시리즈 존재 여부
- **1982-1985**: regular, korean_series
- **1986-1999**: regular, korean_series, exhibition  
- **2000-2001**: regular, korean_series, exhibition (플레이오프 없음)
- **2002-2006**: regular, korean_series, playoff, exhibition
- **2007-2014**: regular, korean_series, playoff, semi_playoff, exhibition
- **2015-현재**: regular, korean_series, playoff, semi_playoff, wildcard, exhibition

### 시리즈별 우선순위
1. **정규시즌** (`regular`): 가장 중요, 우선 크롤링 (1982+)
2. **한국시리즈** (`korean_series`): 포스트시즌 최고 단계 (1982+)
3. **플레이오프** (`playoff`): 준결승/결승 (2002+)
4. **준플레이오프** (`semi_playoff`): 포스트시즌 1차전 (2007+)
5. **와일드카드** (`wildcard`): 추가 진출전 (2015+)
6. **시범경기** (`exhibition`): 참고용 데이터 (1986+)

### 시리즈 검증 유틸리티
```bash
# 특정 연도에서 사용 가능한 시리즈 확인
./venv/bin/python3 -c "
from src.utils.series_validation import get_available_series_by_year
print('2001년:', get_available_series_by_year(2001))
print('2015년:', get_available_series_by_year(2015))
"

# 연도-시리즈 조합 유효성 검증
./venv/bin/python3 -c "
from src.utils.series_validation import validate_year_series_combination
valid, msg = validate_year_series_combination(2001, 'playoff')
print(f'2001년 플레이오프: {msg}')
"
```

---

## 📝 마무리

이 가이드는 KBO_playwright의 모든 크롤링 기능을 다룹니다. 추가 질문이나 문제가 있으면 프로젝트의 다른 문서들을 참고하세요:

- **[프로젝트 개요](projectOverviewGuid.md)**: 전체 아키텍처
- **[스케줄러 가이드](SCHEDULER_README.md)**: 자동화 설정
- **[Supabase 설정](SUPABASE_SETUP.md)**: 클라우드 연동
- **[URL 레퍼런스](URL_REFERENCE.md)**: KBO 사이트 구조
- **[크롤링 제약사항](CRAWLING_LIMITATIONS.md)**: 알려진 이슈들

---

**⚠️ 중요 알림**: KBO 사이트 정책을 준수하고, 크롤링 간격을 충분히 두어 서버에 부하를 주지 않도록 주의하세요.
