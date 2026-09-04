# 릴레이 계약 매트릭스

릴레이 소스 어댑터의 허용된 소스 유형, 소스 순서, 버킷 분류, 계약 유형을 정의합니다.

---

## 1. 허용된 소스 유형 (ALLOWED_SOURCE_TYPES)

| 소스 유형 | 설명 | 사용 시점 |
|-----------|------|-----------|
| `naver` | 네이버 스포츠 API | 정규 시즌 기본 소스 (1순위) |
| `kbo` | KBO 공식 릴레이 | 올스타/포스트시즌/국제전 기본 소스 |
| `html_archive` | HTML 아카이브 | 2023 이하 레거시 시즌 |
| `json_archive` | JSON 아카이브 | 2023 이하 레거시 시즌 |
| `manual_text` | 수동 텍스트 입력 | 비즈니스/예외 처리 |

> 허용되지 않는 소스 이름은 `unknown_source` 이슈로 분류되어 `source_name='none'`으로 정리됩니다.

---

## 2. 허용된 매니페스트 형식 (ALLOWED_MANIFEST_FORMATS)

| 형식 | 설명 |
|------|------|
| `naver_json` | 네이버 JSON 릴레이 |
| `kbo_html` | KBO HTML 릴레이 |
| `relay_html` | 릴레이 HTML |
| `pbp_text` | 순차적 텍스트 PBP |
| `normalized_events_json` | 정규화된 이벤트 JSON |

---

## 3. 버킷 분류 (Bucket Classification)

`derive_bucket_id()` 함수에 의해 게임 ID의 연도와 이벤트 유형에 따라 자동 분류됩니다.

| 버킷 ID | 분류 기준 | 기본 소스 순서 |
|----------|-----------|----------------|
| `{year}_regular_kbo` | 2024~ 연도, 정규 시즌 | `["naver", "kbo", "import"]` |
| `{year}_all_star` | 올스타전 (EAWE 팀 또는 리그에 "올스타" 포함) | `["kbo", "naver", "import", "manual"]` |
| `{year}_postseason` | 한국시리즈/포스트시즌 (2024: 1002~1028) | `["kbo", "naver", "import", "manual"]` |
| `{year}_international` | 국제전/WBC (2024: 1110~1124) | `["kbo", "naver", "import", "manual"]` |
| `{year}_legacy` | 2023 이하 연도 | `["kbo", "naver", "import", "manual"]` |

---

## 4. 소스 순서 (Source Order)

| 버킷 유형 | 소스 순서 | 비고 |
|-----------|-----------|------|
| `*_regular_kbo` | `["naver", "kbo", "import"]` | 네이버 1순위 (공식) |
| `*_all_star` | `["kbo", "naver", "import", "manual"]` | KBO 1순위 |
| `*_postseason` | `["kbo", "naver", "import", "manual"]` | KBO 1순위 |
| `*_international` | `["kbo", "naver", "import", "manual"]` | KBO 1순위 |
| `*_legacy` | `["kbo", "naver", "import", "manual"]` | KBO 1순위 |

---

## 5. 계약 유형 (Contract Type)

| 계약 유형 | 설명 | 예시 |
|-----------|------|------|
| `IDENTICAL_CONTRACT` | 모든 이벤트가 동일 | 일반 정규 시즌 경기 |
| `FILTERING_CONTRACT` | 콘텐츠 필터링/검증 적용 (단축 경기 등) | 5이닝 축약 경기 |

---

## 6. Circuit Breaker 설정

| 매개변수 | 기본값 | 설명 |
|----------|--------|------|
| `threshold` | `3` | 연속 실패 횟수 |
| `cooldown_seconds` | `60.0` | 쿨다운 기간 (초) |
| `persist_path` | `data/recovery/circuit_breaker_state.json` | 상태 저장 경로 |

### Circuit Breaker 상태 전이

```
CLOSED (정상) ──실패 3회──▶ OPEN (차단)
  │                            │
  │                     쿨다운 60초
  │                            │
  └──────── HALF_OPEN (탐색) ◀──┘
```

---

## 7. Capability Record 구조

`data/recovery/source_capability.csv`에 저장됩니다.

| 컬럼 | 설명 |
|------|------|
| `bucket_id` | 타깃 버킷 ID |
| `source_name` | 소스 이름 |
| `sample_size` | 테스트된 샘플 게임 수 |
| `supported` | 해당 소스 지원 여부 |
| `last_checked_at` | 마지막探测 시간 (ISO) |
| `notes` | 비고 또는 실패 사유 |

---

## 8. 릴레이 유효성 상태 (VALIDATION_STATES)

| 상태 | 값 | 설명 |
|------|-----|------|
| `pending_live` | `pending_live` | 라이브 게임, 검증 대기 |
| `provisionally_valid` | `provisionally_valid` | 이벤트 존재, 교차 검증 미완료 |
| `unverified` | `unverified` | 구조적 경고 또는 페이로드 검증 실패 |
| `source_incomplete` | `source_incomplete` | 원시 PBP 행만 존재 |
| `source_unavailable` | `source_unavailable` | 모든 퍼블릭 릴레이 소스 소진 |
| `recovered` | `recovered` | 이전에 사용 불가했던 소스 복구 |
| `verified` | `verified` | 완전히 검증 및 교차 확인 |

---

## 9. 릴레이 계약 위반 감지 규칙

### 9.1 알 수 없는 소스 (unknown_source)
- `ALLOWED_SOURCE_TYPES`에 없는 `source_name` 감지
- 감지 시 `source_name='none'`으로 정리
- 감사: `fix_relay_state.py --fix-unknown`

### 9.2 소스 불일치 (source_mismatch)
- `provider_log_id`의 접두사와 `source_name` 간 불일치
- `provider_log_id` 형식: `{prefix}_{rest}`
- 접두사 매핑: `{prefix} → source_name`
- 감사: `fix_relay_state.py --fix-mismatch`

### 9.3 중복/Redundant 접두사
- `KNOWN_REDUNDANT_PREFIXES`와 일치하는 소스 이름
- 접두사: `{"jumper", "jump", "jmp", "redirect", "r2", "r3"}`
- 감사: `fix_relay_state.py --fix-redundant`

### 9.4 미분류 이벤트
- `event_type`이 `{"unknown", "unclassified", "other"}`인 행
- `event_type = "noise"`로 변환
- 감사: `fix_relay_state.py --fix-unclassified`

---

## 10. 릴레이 복구 순서

```
[게임 ID]
  └─ derive_bucket_id()
       └─ default_source_order_for_bucket()
            ├─ *_regular_kbo → [naver, kbo, import]
            └─ * (기타) → [kbo, naver, import, manual]
```

복구 흐름:
1. `RelayRecoveryOrchestrator.fetch_game()`
2. `source_order` 순서로 소스 시도
3. 성공 시 즉시 종료 (short-circuit)
4. 실패 시 Circuit Breaker 확인
5. 모든 소스 실패 시 `VALIDATION_SOURCE_UNAVAILABLE` 기록
