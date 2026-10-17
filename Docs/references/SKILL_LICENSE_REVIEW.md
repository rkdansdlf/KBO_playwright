# Skill License Review

## 목적

외부 스킬 어댑터 활성화를 위한 라이선스 검토 결과를 기록한다.
AGENTS.md 계약: "Do not fetch, vendor, install, or claim execution of third-party skills without explicit approval and a passing license review."

## 검토 일자

2026-10-18

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

## 보류 스킬 (3개)

### caveman

- **저장소**: praxstack/JuliusBrussee-caveman
- **라이선스**: NOASSERTION
- **redistribution**: prohibited_pending_review
- **보류 사유**: 라이선스가 명시되지 않아 redistribution이 금지됨. 활성화 전 라이선스 확인 필요.
- **현재 상태**: policy-only 모드 (서브에이전트 출력 압축). `executable: false` 유지.
- **활성화 조건**: 라이선스 명시 + redistribution allowed 확인 후.

### aas (agentic-awesome-skills)

- **저장소**: sickn33/agentic-awesome-skills
- **라이선스**: MIT
- **redistribution**: selected_asset_review_required
- **보류 사유**: 선별 자산 검토가 필요함. 전체 fetch가 아닌 선별 자산만 검토 후 활성화.
- **현재 상태**: control_plane 모드 (skill_registry). `executable: false` 유지, `apply: false`.
- **활성화 조건**: 선별 자산 검토 완료 후 `apply: true` 전환.

### scientific (scientific-agent-skills)

- **저장소**: K-Dense-AI/scientific-agent-skills
- **라이선스**: MIT
- **redistribution**: selected_asset_review_required
- **보류 사유**: 선별 자산 검토가 필요함. analytics 라우트에서 부분 활성화 가능.
- **현재 상태**: selected_skills_only 모드. `executable: false` 유지.
- **활성화 조건**: 선별 자산 검토 완료 후 `apply: true` 전환.

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
