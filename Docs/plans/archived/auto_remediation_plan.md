# 자가 치유(Auto-Remediation) 시스템 구축 계획

## 1. 개요
현재의 감사(Audit) 시스템은 오차 발견 시 경고만 발생시킵니다. 이를 확장하여 오차 발견 시 즉시 `SeasonStatAggregator`를 호출하고 DB를 자동으로 업데이트하는 '자가 치유' 기능을 추가합니다.

## 2. 세부 설계

### 2.1 Audit 모듈 확장 (`scripts/verification/audit_fallback_stats.py`)
- `--fix` 인자 추가: 이 인자가 활성화되면 오차 발견 시 즉시 수정 로직을 실행합니다.
- `StatAudit.audit_batting` 및 `audit_pitching` 메서드 내부에 수정 로직 통합:
    - 오차 발견 시 `calc` 데이터를 기반으로 해당 선수의 시즌 기록을 `UPSERT` 합니다.
    - 수정 성공 시 Slack 알림에 "수정 완료" 상태를 포함합니다.

### 2.2 파이프라인 연동 (`src/cli/run_daily_update.py`)
- Step 10.5의 호출 방식을 변경합니다:
    ```bash
    PYTHONPATH=. ./venv/bin/python3 scripts/verification/audit_fallback_stats.py --year {year} --type all --fix
    ```
- 이를 통해 매일 밤 수집 후 자동으로 정합성을 맞춥니다.

### 2.3 안정성 장치
- **오차 임계치 설정:** 비정상적으로 큰 오차(예: 경기 수가 10경기 이상 차이)가 발생할 경우 자동 수정을 중단하고 관리자 확인을 요청하는 세이프 가드를 마련합니다.
- **백업 로그:** 수정 전 데이터를 별도 로그에 남겨 필요 시 복구가 가능하게 합니다.

## 3. 실행 로드맵
1. `audit_fallback_stats.py`에 `--fix` 옵션 및 수정 로직 구현.
2. `run_daily_update.py`에서 해당 옵션을 활성화하여 파이프라인에 적용.
3. 테스트 시즌(예: 2024, 2025)에 대해 시뮬레이션 및 검증.
