# Agent Harness 운영 런북

`tools/agent_harness/`는 production `src/`와 물리적으로 분리된 AI 개발 통제 플레인이다.
외부 스킬은 실행하지 않고(`reference_only`) 라우팅·권한·검증·증거만 담당한다.

## 빠른 시작

```bash
python3 -m tools.agent_harness doctor
python3 -m tools.agent_harness route "boxscore crawler timeout 수정"
python3 -m tools.agent_harness plan "boxscore crawler timeout 수정"
python3 -m tools.agent_harness run "boxscore crawler timeout 수정" --profile crawler-bug
python3 -m tools.agent_harness verify <run-id>
python3 -m tools.agent_harness report <run-id>
```

- `route`: 분류+스킬 선택만 표시 (plan과 달리 실행계획을 만들지 않음).
- `plan`: route + context + verification 실행계획 생성.
- `verify`: `plan.json`의 verification 프로파일로 기존 프로젝트 게이트 실행.

## 프로파일 선택 규칙

우선순위: `explicit_profile > changed_files > prompt 키워드 > default(feature)`.

| 신호 | 예 | 결과 |
|---|---|---|
| 변경 파일 `src/crawlers/**`, `src/parsers/**` | 박스스코어 타임아웃 | `crawler-bug` |
| 변경 파일 `src/rag/**`, `src/analytics/**` | 지표 분석 | `analytics` |
| 변경 파일 `src/orchestration/**`, `.github/workflows/**` | DAG 개편 | `architecture` |
| 프롬프트 키워드 | refactor, architecture | 해당 프로파일 |
| 해당 없음 | 문서 정리 | `feature` (default) |

판단 로직은 `router.py` + `project_adapter.py`에 있으며 LLM을 쓰지 않는다.

## 스킬 transport 차이

| transport | 스킬 | 의미 |
|---|---|---|
| `native` | superpowers, ponytail, last30days, scientific, understand-anything, diagram-design | 호스트 에이전트가 직접 읽어 실행. Harness는 `HOST_EXECUTION_REQUIRED`로만 기록 |
| `cli` | graphify, aas | 호스트가 CLI 호출. Harness는 직접 실행하지 않음 |
| `policy` | caveman, i-have-adhd | 출력 지침으로 로컬 적용 (`EXECUTED`) |

`registry.get_skill_definition()`이 transport의 source of truth다.
`superpowers`를 executable처럼 찾으면 안 된다.

## 권한 경계

- 읽기 거부: `.env`, `*.pem`, `*.key`, `*wallet*`, `cwallet.sso`, `ewallet.p12`.
- 쓰기 허용: `artifacts/agent-harness/**`, `.opencode/plans/**`, `Docs/plans/**`만.
- 네트워크: `last30days`만 허용, 나머지는 기본 거부.
- 명령: `permissions.yaml: commands` allowlist만. 모든 subprocess는
  `CommandRunner` 경유 + `shell=False` 강제. `python`은 `-m <allowlisted module>`만.
- 시크릿: `redact()`는 env 값 마스킹 후 `src.certification.context.redact_secrets` 체인.
  `src → tools` 의존은 금지, `tools → src` 단방향만 허용.

## lock 갱신법

`harness.lock.json`의 40자 SHA를 새 릴리스 커밋으로 교체 후:

```bash
python3 -m tools.agent_harness doctor --json
python3 -m pytest tests/agent_harness -q
```

`doctor`가 FAIL이면 상위 작업을 진행하지 않는다.

## 검증 레벨

`policies/verification.yaml: levels` 기준, `verifier.build_plan()`이 변경 파일로 구체화:

- `none`: 검사 없음 (research 전용).
- `quick`: 영향 pytest + 변경 범위 ruff.
- `standard`: 영향 pytest + 프로젝트 ruff + 조건부 crawler gate.
- `full`: 전체 pytest + ruff (+ 조건부 certification, 승인 필요 시).

## 아티팩트 읽는 법

`artifacts/agent-harness/<run-id>/`:

- `task.json`: 요청 원문 + 프로파일.
- `plan.json`: 라우팅 결정 + 단계별 스킬.
- `context.json`: 재현용 해시 + 선택 엔진.
- `skill-trace.jsonl`: 단계 이벤트 + executor 정직 기록
  (`host_execution_required`는 미실행 선언이지 실행이 아님).
- `commands.jsonl` / `verification.json`: 실행 명령과 통과 여부.
- `report.md`: 사람용 요약.

## 새 adapter 추가법

1. `.agent-harness/adapters/<id>.yaml` 작성 (`name/source/role/transport/capabilities`).
2. `harness.yaml: skills`에 `role/mode` 등록.
3. `harness.lock.json`에 repo+SHA+license 핀.
4. `aas-stack.json` skills 목록에 추가 (`apply: false` 유지).
5. `doctor` + `tests/agent_harness` 통과 확인.

## 장애 대응

- `doctor FAIL`: 출력의 `ERROR` 행이 가리키는 yaml/lock 불일치 수정.
- `verify` 실패: `verification.json`의 첫 실패 명령부터 확인, 증거는 `commands.jsonl`.
- `PermissionDeniedError`: allowlist에 없는 실행 시도. 정책을 우회하지 말고
  `permissions.yaml` 변경 + 보안 회귀 테스트(`test_command_runner.py`)로 승인.
