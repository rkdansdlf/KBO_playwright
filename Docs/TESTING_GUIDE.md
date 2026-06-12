# Testing Guide

## 테스트 실행

```bash
# 전체 테스트
pytest

# 특정 파일만
pytest tests/crawlers/test_schedule_crawler.py

# 특정 테스트 클래스/함수
pytest tests/test_game_status.py::test_update_game_status

# 상세 출력
pytest -v --tb=short --durations=10
```

## 테스트 구조

| 디렉토리 | 대상 | 갯수 |
|----------|------|------|
| `tests/cli/` | `src/cli/` CLI 진입점 | 77 |
| `tests/crawlers/` | `src/crawlers/` 크롤러 | 42 |
| `tests/repositories/` | `src/repositories/` DB 레이어 | 40 |
| `tests/services/` | `src/services/` 서비스 레이어 | 21 |
| `tests/utils/` | `src/utils/` 유틸리티 | 35 |
| `tests/parsers/` | `src/parsers/` 파서 | 14 |
| `tests/sync/` | `src/sync/` OCI 동기화 | 7 |
| `tests/scripts/` | `scripts/` 유지보수 스크립트 | 84 |
| `tests/aggregators/` | `src/aggregators/` 통계 집계 | - |
| `tests/sources/` | `src/sources/` 데이터 소스 | - |

## 모킹 전략

- **DB**: `MagicMock(spec=Session)` — SQLAlchemy 세션 모킹
- **Playwright**: `AsyncMock()` — 페이지/브라우저 모킹
- **HTTP**: `responses` / `pytest-httpx` (드물게)
- **파일**: `tmp_path` pytest fixture
- **환경변수**: `monkeypatch.setenv()` 또는 `pytest-env`

### 크롤러 테스트 패턴

Playwright 크롤러는 `AsyncMock(page)`으로 HTML 파싱 로직만 테스트하고,
실제 브라우저 호출은 모킹합니다.

```python
@pytest.mark.asyncio
async def test_parse_row():
    mock_page = AsyncMock()
    mock_page.evaluate = AsyncMock(return_value={...})
    result = await crawler._parse(mock_page)
    assert result["player_id"] == "12345"
```

## CI 테스트

GitHub Actions `test_suite.yml`에서 실행:
- Lint: `ruff check`, `ruff format --check`, `scripts/lint_bare_except.py`
- Test: `pytest --tb=short -v --durations=10`
- Matrix: Python 3.12

`.gitignore`-된 `inspect_*.py` 스크립트는 `pytest.importorskip()` 사용.
`pytest-asyncio` 필요 (requirements.txt에 포함).

## 주의사항

- Playwright 미설치 환경에서 크롤러 테스트는 자동 skip
- async 함수는 `@pytest.mark.asyncio` 필요
- 로컬과 CI 모두 `pytest.ini` 설정 사용
