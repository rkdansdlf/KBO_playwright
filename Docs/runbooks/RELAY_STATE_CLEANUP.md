# 릴레이 상태 정리 Runbook

> 작성일: 2026-09-04 | 스크립트: `scripts/maintenance/fix_relay_state.py`

## 개요

릴레이 소스 어댑터의 상태를 감사하고, 잘못된 상태의 소스를 정리합니다.

- `ALLOWED_SOURCE_TYPES`에 없는 소스 이름 감지
- 소스 이름 불일치 감지 (`provider_log_id` prefix vs `source_name`)
- 중복/redundant 소스 감지
- 미분류(`unknown`/`unclassified`/`other`) 이벤트 카운트 기준 정리

## 사전 준비

```bash
# 가상환경 활성화
source venv/bin/activate

# 환경 변수 확인 (DATABASE_URL 필수)
cat .env | grep DATABASE_URL
```

## 명령어

### 1. 전체 감사 (읽기 전용)

```bash
python3 scripts/maintenance/fix_relay_state.py --audit-only
```

출력 예시:
```
============================================================
릴레이 소스 상태 감사 요약
============================================================
총 게임 수:           13342
PBP 행 수:            2881812
이벤트 행 수:         2881812
------------------------------------------------------------
허용 소스 게임:       13200
알 수 없는 소스 게임:   50  (정리 필요)
미분류 이벤트 게임:    100  (참고용)
소스 불일치 게임:      25  (정리 필요)
중복/Redundant 게임:   10  (정리 필요)
------------------------------------------------------------
소스별 분포 (상위 15개):
  naver                  :  1200000
  jumper                 :   100000 !
  unknown_source         :    50000 !
  redirect               :    30000 !
```

### 2. 알 수 없는 소스 정리 (DRY-RUN)

```bash
python3 scripts/maintenance/fix_relay_state.py --fix-unknown --dry-run
```

### 3. 알 수 없는 소스 정리 (실제 적용)

```bash
python3 scripts/maintenance/fix_relay_state.py --fix-unknown --apply
```

- `ALLOWED_SOURCE_TYPES`에 없는 `source_name`을 `'none'`으로 변경
- `provider_log_id`는 보존

### 4. 소스 불일치 확인 (참고용)

```bash
python3 scripts/maintenance/fix_relay_state.py --fix-mismatch --dry-run
```

- `provider_log_id` prefix와 `source_name` 간 불일치 감지
- 실제 수정은 수동 검토 후 진행 권장

### 5. 중복 소스 감지 (참고용)

```bash
python3 scripts/maintenance/fix_relay_state.py --fix-redundant --dry-run
```

- `jumper`, `jump`, `jmp`, `redirect`, `r2`, `r3` 접두사 감지
- 소스 선택 로직 검토 필요

### 6. 미분류 이벤트 처리 (DRY-RUN)

```bash
python3 scripts/maintenance/fix_relay_state.py --fix-unclassified --dry-run
```

### 7. 미분류 이벤트 처리 (실제 적용)

```bash
python3 scripts/maintenance/fix_relay_state.py --fix-unclassified --apply
```

- `event_type`이 `unknown`/`unclassified`/`other`인 행을 `'noise'`로 변경

### 8. 특정 게임만 처리

```bash
python3 scripts/maintenance/fix_relay_state.py --fix-unclassified --apply --game-ids "20260101K001,20260102K002"
```

### 9. 검증 지표 갱신

```bash
python3 scripts/maintenance/fix_relay_state.py --fix-unclassified --apply --game-ids "20260101K001"
```

- `validation_status = SOURCE_UNAVAILABLE`인 게임 재감사

## 복구 절차

### 문제: 알 수 없는 소스가 너무 많음

1. `--audit-only`로 전체 현황 파악
2. `--fix-unknown --dry-run`으로 영향 범위 확인
3. `--fix-unknown --apply`로 정리
4. 재감사로 확인

### 문제: 소스 불일치

1. `--fix-mismatch --dry-run`으로 불일치 게임 식별
2. `provider_log_id`와 `source_name` 매핑 확인
3. 수동 수정 또는 어댑터 로직 수정

### 문제: 중복 소스

1. `--fix-redundant --dry-run`으로 중복 게임 식별
2. 어댑터 등록 로직에서 중복 방지 로직 추가

## 주의사항

1. **대용량 DB**: `data/kbo_dev.db` (1.3GB, 288만 PBP 행)에서는 N+1 쿼리 패턴으로 인해 실행 시간이 매우 길 수 있음
2. **백업**: `--apply` 전 반드시 DB 백업 권장
3. **권한**: `data/` 디렉토리 쓰기 권한 필요
4. **병렬 실행 금지**: 동시 실행 시 데이터 불일치 발생 가능

## CI 통합

`.github/workflows/test_suite.yml`의 `lint` job에 `Relay Contract Check` 단계가 추가되어 있습니다.

```yaml
- name: Relay Contract Check
  run: |
    python3 scripts/maintenance/fix_relay_state.py --audit-only
    python3 scripts/maintenance/fix_relay_state.py --fix-unknown --dry-run
    python3 scripts/maintenance/fix_relay_state.py --fix-mismatch --dry-run
    python3 scripts/maintenance/fix_relay_state.py --fix-redundant --dry-run
    python3 scripts/maintenance/fix_relay_state.py --fix-unclassified --dry-run
```

## 관련 문서

- `Docs/references/RELAY_CONTRACTS.md`: 릴레이 계약 매트릭스
- `Docs/references/SCHEDULER_DEPENDENCIES.md`: 스케줄러 의존성 다이어그램