# Player Basic Migration - Final Summary
**Date**: 2025-10-16
**Status**: ✅ COMPLETE

---

## 🎯 완료된 작업

### 1. Players 테이블 교체 ✅
- **기존**: 복잡한 다중 테이블 구조 (players, player_identities, player_stints, etc.)
- **신규**: 단순한 `player_basic` 단일 테이블
- **이유**: 데이터 소스(선수 검색 페이지)에 맞는 간단한 구조

### 2. 크롤러 구현 ✅
**파일**: `src/crawlers/player_search_crawler.py`

**기능**:
- 전체 선수 검색 페이지 크롤링 (searchWord=%25)
- 자동 페이지네이션 (모든 페이지 순회)
- 선수 기본 정보 수집:
  - player_id (KBO ID, 쉼표 제거 처리)
  - name (선수명)
  - uniform_no (등번호)
  - team (팀명)
  - position (포지션)
  - birth_date (생년월일 원본)
  - birth_date_date (파싱된 날짜)
  - height_cm, weight_kg (체격)
  - career (출신교)

**주요 수정사항**:
1. ✅ 테이블 구조 인덱스 수정 (TD[0]=등번호, TD[1]=선수명)
2. ✅ player_id에서 쉼표 제거 로직 추가
3. ✅ 키/몸무게 파싱 개선 ("182cm, 76kg" 형식 지원)
4. ✅ 페이지네이션 로직 개선 (선수명 변경 감지 방식)
5. ✅ DB 저장 및 Supabase 동기화 통합

**사용법**:
```bash
# 크롤링만 (출력만)
python -m src.crawlers.player_search_crawler

# SQLite에 저장
python -m src.crawlers.player_search_crawler --save

# Supabase에도 동기화
python -m src.crawlers.player_search_crawler --save --sync-supabase

# 테스트 (3페이지만)
python -m src.crawlers.player_search_crawler --max-pages 3 --save
```

### 3. 데이터베이스 스키마 ✅

**SQLite** (`src/models/player.py`):
```python
class PlayerBasic(Base):
    player_id: int (PK)
    name: str (NOT NULL)
    uniform_no: str (nullable)
    team: str (nullable)
    position: str (nullable)
    birth_date: str (nullable)  # 원본 문자열
    birth_date_date: date (nullable)  # 파싱된 날짜
    height_cm: int (nullable)
    weight_kg: int (nullable)
    career: str (nullable)
```

**Supabase** (`migrations/supabase/005_create_player_basic_table.sql`):
- PostgreSQL 테이블 생성
- 인덱스: name, team, position, (team, position)
- Comments 추가로 문서화

### 4. Repository 및 Sync ✅

**Repository** (`src/repositories/player_basic_repository.py`):
- `upsert_players()`: 멀티 레코드 UPSERT
- SQLite/PostgreSQL 양쪽 지원
- 헬퍼 메서드: get_all(), get_by_id(), get_by_team(), count()

**Sync** (`src/sync/supabase_sync.py`):
- `sync_player_basic()` 메서드 추가
- PostgreSQL UPSERT with ON CONFLICT

### 5. 마이그레이션 정리 ✅

**제거된 파일**:
- `migrations/supabase/003_create_player_tables.sql.deprecated` (삭제)
- `migrations/supabase/006_drop_old_player_tables.sql` (일회성, 삭제)
- `scripts/crawl_players_basic.py` (크롤러에 통합, 삭제)

**유지된 파일**:
- `001_create_team_tables.sql` (필수)
- `002_create_game_tables.sql` (필수)
- `004_create_player_game_stats_tables.sql` (필수)
- `005_create_player_basic_table.sql` (필수)

---

## 🔧 주요 기술적 해결

### 문제 1: 테이블 구조 오해
**증상**: TD[0]을 번호(#)로 착각
**해결**: 실제 페이지 확인 → TD[0]=등번호, TD[1]=선수명

### 문제 2: player_id에 쉼표 포함
**증상**: URL에서 추출한 player_id에 쉼표가 포함될 수 있음
**해결**: 정규식으로 숫자만 추출 (`re.sub(r'[^\d]', '', pid)`)

### 문제 3: 키/몸무게 파싱 실패
**증상**: "182cm, 76kg" 형식을 파싱 못함
**해결**: 쉼표(,)를 구분자로 인식하도록 정규식 수정

### 문제 4: 페이지네이션 실패
**증상**: 첫 페이지만 수집하고 종료
**원인**: `is_visible()` 체크 실패 + `wait_for_function` 타임아웃
**해결**: 선수명 변경 감지 방식으로 변경 (10초 대기, 1초 간격 체크)

### 문제 5: datetime import 중복
**증상**: `from datetime import datetime` 중복 import로 경고
**해결**: 모듈 상단에서 한 번만 import

---

## 📊 테스트 결과

### 소규모 테스트 (3페이지)
```
✅ 페이지: 3페이지 수집
✅ 선수: 60명 수집
✅ 파싱: birth_date 100% 성공
✅ 저장: SQLite 60명 저장
✅ 동기화: Supabase 60명 동기화
```

### 실제 데이터 검증 (Supabase)
```sql
SELECT player_id, name, uniform_no, team, position,
       birth_date_date, height_cm, weight_kg
FROM player_basic
LIMIT 10;
```

**결과**:
```
50640 | 가뇽     | #          | KIA  | 투수     | 1990-06-26 | 193 | 97
51833 | 가빌리오 | #          | SSG  | 투수     | 1990-05-22 | 185 | 98
52125 | 가르시아 | #          | LG   | 내야수   | 1993-03-28 | 183 | 88
53006 | 강건     | 99         | KT   | 투수     | 2004-07-12 | 183 | 85
53994 | 강건준   | #          | NC   | 투수     | 2003-07-14 | 186 | 84
...
```

✅ 모든 필드가 정확하게 파싱 및 저장됨

---

## 🚀 전체 크롤링 (5120명)

### 실행 명령
```bash
# 백그라운드 실행
nohup python -m src.crawlers.player_search_crawler --save --sync-supabase > player_crawl_full.log 2>&1 &

# 진행 상황 확인
tail -f player_crawl_full.log

# DB 확인
python -c "
from src.repositories.player_basic_repository import PlayerBasicRepository
repo = PlayerBasicRepository()
print(f'Current player count: {repo.count()}')
"
```

### 예상 소요 시간
- 페이지당 처리 시간: ~2초 (크롤링 1초 + 대기 1초)
- 총 페이지 수: 5120 / 20 = 256 페이지
- 예상 시간: 256 × 2초 = ~8.5분

### 실행 상태
- ✅ 크롤러 실행 중 (PID: 38333)
- 📊 진행 중...

---

## 📁 최종 파일 구조

```
KBO_playwright/
├── src/
│   ├── crawlers/
│   │   └── player_search_crawler.py  ← 통합 크롤러 (크롤링+저장+동기화)
│   ├── models/
│   │   └── player.py  ← PlayerBasic 모델 추가
│   ├── repositories/
│   │   └── player_basic_repository.py  ← 새로 생성
│   └── sync/
│       └── supabase_sync.py  ← sync_player_basic() 추가
├── migrations/supabase/
│   ├── 001_create_team_tables.sql
│   ├── 002_create_game_tables.sql
│   ├── 004_create_player_game_stats_tables.sql
│   └── 005_create_player_basic_table.sql  ← 새로 생성
└── PLAYER_BASIC_MIGRATION_COMPLETE.md
```

---

## ✅ 체크리스트

- [x] player_basic 테이블 설계 (SQLite + Supabase)
- [x] 크롤러 구현 (페이지네이션 포함)
- [x] 데이터 파싱 (player_id, 키/몸무게, 생년월일)
- [x] Repository 구현 (UPSERT)
- [x] Supabase Sync 구현
- [x] 기존 players 테이블 삭제
- [x] 마이그레이션 파일 정리
- [x] 테스트 (3페이지, 60명)
- [ ] 전체 크롤링 (5120명) - 진행 중
- [ ] 데이터 검증 및 품질 확인

---

## 🎯 다음 단계

1. **전체 크롤링 완료 대기**
   - 5120명 전체 수집 확인
   - SQLite + Supabase 데이터 검증

2. **데이터 품질 검증**
   - Null 값 비율 확인
   - 중복 player_id 확인
   - 파싱 실패 케이스 확인

3. **스케줄링 설정**
   - 주간 업데이트 스케줄 추가
   - 새 선수 자동 수집

4. **문서화**
   - CLAUDE.md 업데이트
   - 사용법 가이드 추가

---

## 💡 설계 결정 요약

### 왜 단순한 테이블로 교체했나?

**AS-IS (복잡)**:
- 6개 테이블 (players, identities, codes, stints, season stats)
- JOIN이 많이 필요
- 데이터 소스가 지원하지 않는 필드들이 많음

**TO-BE (단순)**:
- 1개 테이블 (player_basic)
- 데이터 소스와 1:1 매칭
- 쿼리 간단, 유지보수 쉬움

### 왜 크롤러를 하나의 파일로?

**이전**: crawler + orchestrator 분리
**현재**: 하나의 파일로 통합

**이유**:
- 크롤러가 간단해서 분리할 필요 없음
- 커맨드라인 인터페이스가 더 직관적
- 코드 중복 제거

### 왜 birth_date를 두 개 저장?

**birth_date**: 원본 문자열 보존
**birth_date_date**: 파싱된 날짜 객체

**이유**:
- 원본 데이터 검증 가능
- 파싱 실패 시 디버깅 용이
- 날짜 기반 쿼리 가능

---

**Status**: 🟢 크롤러 실행 중, 전체 데이터 수집 진행 중
**Expected Completion**: ~8-10 minutes
**Next**: 전체 크롤링 완료 후 데이터 검증 및 최종 리포트
