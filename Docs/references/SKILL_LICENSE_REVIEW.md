# Skill License Review

## 목적

외부 스킬 어댑터 활성화를 위한 라이선스 검토 결과를 기록한다.
AGENTS.md 계약: "Do not fetch, vendor, install, or claim execution of third-party skills without explicit approval and a passing license review."

## 검토 일자

2026-10-19 (선별 자산 검토 갱신)

## 활성화 가능 스킬 (7개)

| 스킬 | 저장소 | 라이선스 | redistribution | 활성화 |
|---|---|---|---|---|
| superpowers | obra/superpowers | MIT | allowed | ✅ |
| ponytail | DietrichGebert/ponytail | MIT | allowed | ✅ |
| graphify | Graphify-Labs/graphify | Apache-2.0 | allowed | ✅ |
| understand-anything | Egonex-AI/Understand-Anything | MIT | allowed | ✅ |
| last30days | mvanhorn/last30days-skill | MIT | allowed | ✅ |
| i-have-adhd | ayghri/i-have-adhd | MIT | allowed | ✅ |
| diagram-design | cathrynlavery/diagram-design | MIT | allowed | ✅ |

### 활성화 조건

- MIT/Apache-2.0 라이선스로 redistribution이 허용된 스킬만 활성화한다.
- pinned revision을 그대로 vendor한다 (수정 없이).
- 라이선스 파일(LICENSE)을 vendor 디렉터리에 함께 복사한다.
- fetch 시 네트워크 실패 시 vendor 캐시를 사용한다.

## 보류 스킬 (3개) — 선별 자산 검토 결과 (2026-10-19)

### caveman

- **저장소**: praxstack/JuliusBrussee-caveman
- **라이선스**: **이중 라이선스** — 저장소 루트 `LICENSE`는 MIT이나, `LICENSING.md`에 따라 `engine/`, `proxy/`, `cacheengine/`, `rewriter/`, `browse/`, `mcp/`, `shrink/`, `cavemem` Go core, `shared/platform/` 등 **Engine-linked 디렉터리는 BSL-1.1**이다.
- **redistribution**: prohibited_pending_review (유지)
- **검토 결과**: caveman 어댑터의 용도는 서브에이전트 출력 압축(subagent-compression)이며, 이 기능은 BSL-1.1로 보호되는 Engine-linked 런타임(`shrink/`, `mcp/`)에 의존한다. MIT 부분만 vendoring하면 압축 기능이 빠져 의미가 없고, BSL-1.1 부분을 vendoring하면 redistribution 금지 조항에 저촉된다. 따라서 **활성화 불가** — policy-only 모드 유지.
- **현재 상태**: policy-only (서브에이전트 출력 압축). `executable: false`, `apply: false` 유지.
- **활성화 조건**: 업스트림이 압축 엔진을 MIT/Apache-2.0으로 재라이선스하거나, BSL-1.1의 Change Date가 도래해 MIT로 전환될 때까지 보류. 그 전까지는 AGENTS.md의 "Caveman only for subagent output" 정책을 로컬 정책으로만 적용한다.

### aas (agentic-awesome-skills)

- **저장소**: sickn33/agentic-awesome-skills (MIT, v19.9.0, 2,712+ skills)
- **라이선스**: MIT (README 배지 + LICENSE 명시)
- **redistribution**: selected_asset_review_required → **선별 자산 검토 완료 (2026-10-19)**
- **검토 결과**: 레포 전체는 MIT이므로 선별 자산의 redistribution은 허용된다. 어댑터 role은 `skill_registry` (catalog, stack-validation, planning)이며, 이 용도에 부합하는 선별 자산은 다음 3종이다:
  1. `aas stack validate` / `aas stack plan` — 스택 매니페스트 검증·계획 미리보기 (control_plane 용도)
  2. `compose_stack` MCP 도구 — 에이전트가 선택한 스킬 ID의 구조·identity 검증 (in-memory, apply 없음)
  3. `skills_index.json` v1 스키마 — 스킬 카탈로그 메타데이터 (발견·검색용)
- **제외 자산**: `aas stack install-preview` / `--apply` 경로 (실행·설치는 host execution의 몫이며, 하네스는 실행하지 않는다), 외부 호스트 설치 타겟 (Cursor/Codex 등) 관련 코드, Workbench 브라우저 UI.
- **현재 상태**: `executable: false`, `apply: false` 유지 (라이선스 검토 통과, 실제 fetch/vendor는 명시적 승인 필요).
- **활성화 조건**: 명시적 승인 시 `apply: true` 전환 후 `python3 -m tools.agent_harness fetch --skill aas` 실행. fetch 실패 시 vendor 캐시 사용.

### scientific (scientific-agent-skills)

- **저장소**: K-Dense-AI/scientific-agent-skills (MIT, v2.72.0, 177 skills)
- **라이선스**: MIT (README 배지 + LICENSE.md 명시)
- **redistribution**: selected_asset_review_required → **선별 자산 검토 완료 (2026-10-19)**
- **검토 결과**: 레포 전체는 MIT이므로 선별 자산의 redistribution은 허용된다. 어댑터 role은 `analytics` (statistics, time-series, model-evaluation)이며, 이 프로젝트의 sabermetrics/통계/시계열 분석 경로에 부합하는 선별 자산은 다음 3종이다:
  1. 통계 분석 스킬 (statsmodels/scipy 기반 hypothesis testing, power analysis, experimental design) — `src/analytics/` 및 sabermetrics 엔진 보조
  2. 시계열 분석 스킬 (aeon, TimesFM) — 시즌/경기 시계열 분석 보조
  3. 데이터 시각화 스킬 (publication-quality figures) — 리포트/스카우팅 카드 보조
- **제외 자산**: 라이브 서비스 연동 스킬 (Benchling, LatchBio, Opentrons 등 — 외부 계정/API 키 필요), GPU/대규모 컴퓨팅 스킬 (PyTorch Lightning, Qiskit 등 — 이 프로젝트 범위 밖), 임상/규제 문서 스킬 (clinical-reports 등 — 도메인 부적합).
- **현재 상태**: `executable: false`, `apply: false` 유지 (라이선스 검토 통과, 실제 fetch/vendor는 명시적 승인 필요).
- **활성화 조건**: 명시적 승인 시 `apply: true` 전환 후 `python3 -m tools.agent_harness fetch --skill scientific` 실행. fetch 실패 시 vendor 캐시 사용.

## 검토 요약

| 스킬 | 라이선스 | 검토 결과 | apply 전환 가능 |
|---|---|---|---|
| caveman | MIT + BSL-1.1 (이중) | 압축 엔진이 BSL-1.1 → vendoring 불가 | 불가 (업스트림 재라이선스 대기) |
| aas | MIT | 선별 3종 자산 검토 통과 | 가능 (명시적 승인 시) |
| scientific | MIT | 선별 3종 자산 검토 통과 | 가능 (명시적 승인 시) |

## 활성화 정책

1. **fetch**: pinned revision을 vendor 디렉터리에 checkout한다.
2. **vendor**: `vendor/skills/<skill_id>/` 디렉터리에 저장한다 (gitignored).
3. **license**: 라이선스 파일을 vendor 디렉터리에 함께 복사한다.
4. **execute**: 활성화된 스킬은 host execution을 요청한다 (native/cli transport).
5. **policy**: policy transport 스킬은 로컬에서 적용한다.
6. **fallback**: fetch 실패 시 vendor 캐시를 사용한다.

## 실행 계약 (Phase 4~5)

### `external_execution: reference_only` 문자열은 유지한다

라우트 계획·artifact 계약·routing golden의 `external_execution` 값은 `"reference_only"`로
그대로 둔다. 이 값이 말하는 것은 "어댑터 활성화 여부"가 아니라 **"이 프로세스(하네스 CLI)가
외부 스킬을 직접 실행하지 않는다"**는 계약이다. activation은 라이선스·vendor 게이트를 해제할 뿐,
스킬 마크다운의 실제 실행은 호스트 에이전트의 몫이다. 따라서 문자열을 `activated` 등으로 바꾸면
값이 계약이 아니라 활성화 상태를 말하게 되어, artifact 검증(`artifact_contract.py`)과 routing
golden 검증(`routing_golden.py`)이 같은 사실을 두 기준으로 검사하게 된다.

### Honesty 계약: activated여도 `host_execution_required`

`ActivatedSkillExecutor`는 activated 스킬에도 `HOST_EXECUTION_REQUIRED`를 반환한다.
detail에 vendored 상태(`activated skill vendored and awaiting host execution`)를 남길 뿐,
`EXECUTED`를 주장하지 않는다. policy transport만 로컬 적용이라 `EXECUTED`이며, 이 순서는
`executor_for`에서 transport 우선으로 고정된다. golden task 계약
(`native/cli/mcp transport는 반드시 host_execution_required`)과 일치한다.

### Golden 호환성 확인

- `tests/agent_harness/test_golden_tasks.py` round-three: native/cli/mcp 전량
  `host_execution_required`, policy `executed` — 활성화 후에도 충족.
- `routing_golden.json` / `golden_tasks.yaml`: `external_execution: reference_only` 유지로
  검증 통과 (`routing_golden.py` 게이트 미변경).
- `tests/agent_harness/` 377건 전부 통과.

## vendor 디렉터리 구조

```
vendor/
  skills/
    superpowers/
      LICENSE
      (pinned revision files)
    ponytail/
      LICENSE
      (pinned revision files)
    ...
```

## CI/네트워크 정책

- CI에서는 vendor 캐시를 사용한다 (네트워크 fetch 금지).
- 로컬에서는 fetch를 수행할 수 있다.
- `doctor` 명령에서 vendor 상태를 검증한다.
- `verify` 명령에서 활성화된 스킬의 실행 가능성을 확인한다.
