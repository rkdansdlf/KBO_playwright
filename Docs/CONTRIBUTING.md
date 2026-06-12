# Contributing Guide

## 개발 환경 셋업

```bash
# 1. Python 가상환경
python3 -m venv venv
source venv/bin/activate

# 2. 의존성 설치
pip3 install -r requirements.txt
pip3 install -r requirements-dev.txt  # 추가 개발 도구

# 3. Playwright 브라우저 설치
playwright install chromium

# 4. 환경변수 설정
cp .env.example .env  # 필요값 입력

# 5. 테스트 실행
pytest
```

## 코딩 컨벤션

- **Python 3.12+**, 4-space indentation
- **타입 힌트**: 모든 함수 시그니처에 param/return type 필수
- **로깅**: `print()` 대신 `logger.info()`
- **에러 처리**: bare `except:` 금지, 구체적 예외 명시
- **멱등성**: 모든 DB 저장은 UPSERT 사용

## 브랜치 전략

- `main`: 안정 브랜치, PR로만 병합
- `develop`: 개발 브랜치
- `feature/*`: 기능 브랜치
- `fix/*`: 버그 수정

## PR 가이드라인

1. PR 본문에 변경 요약 포함
2. 영향받는 모듈 명시
3. 실행한 테스트 명령어 포함
4. URL/선택자 변경 시 명시

## 코드 스타일

```bash
# 자동 포매팅
ruff check src/ --fix
ruff format src/

# 린트
ruff check src/
ruff format --check src/
```

## 테스트 요구사항

- 새로운 기능: 테스트 필수
- 버그 수정: 회귀 테스트 포함
- Playwright 크롤러: 모킹 기반 단위 테스트
- CLI: `pytest tests/cli/` 내에서 검증
