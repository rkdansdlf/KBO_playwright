# Agent Harness 운영 런북

`tools/agent_harness/`는 production `src/`와 물리적으로 분리된 AI 개발 통제 플레인이다.
외부 스킬은 실행하지 않고(`reference_only`) 라우팅·권한·검증·증거만 담당한다.

## 빠른 시작

```bash
python3 -m tools.agent_harness doctor
python3 -m tools.agent_harness route "boxscore crawler timeout 수정"
python3 -m tools.agent_harness replay --round 1
python3 -m tools.agent_harness replay --round 3 --metrics-out artifacts/agent-harness/manual/round-3-metrics.json
python3 -m tools.agent_harness plan "boxscore crawler timeout 수정"
python3 -m tools.agent_harness run "boxscore crawler timeout 수정" --profile crawler-bug
python3 -m tools.agent_harness run "정리해줘" --changed-files src/crawlers/x.py
python3 -m tools.agent_harness verify <run-id>
python3 -m tools.agent_harness validate <run-id> --require-verified
python3 -m tools.agent_harness report <run-id>
```

- `route`: 분류+스킬 선택만 표시 (plan과 달리 실행계획을 만들지 않음).
- `replay --round <1|2|3>`: 실제 KBO 작업 replay dataset을 read-only로 평가한다.
  외부 skill이나 프로젝트 명령은 실행하지 않으며, 라우팅·permission·verification plan·declared deviation만 반환한다.
- `replay --metrics-out <path>`: 위 평가의 집계 metrics를 파일로 남긴다. 경로가 없으면 아무것도 쓰지 않는다.
- `plan`: route + context + verification 실행계획 생성.
- `route/plan/run`은 `--changed-files <paths...>`를 받으며 파일 신호가 프롬프트 키워드보다 우선한다.
- `verify`: `plan.json`의 verification 프로파일로 기존 프로젝트 게이트 실행.
  모든 명령은 `CommandRunner` allowlist를 통과하며 거부 시 exit 2로 종료된다.
- `verify --level <none|quick|standard|full> --changed-files <paths...>`: 고정 프로파일 대신
  영향 기반 플랜으로 실행. 예: crawler 파일 변경 시 selector gate 자동 포함.
  주의: `quick` 이상은 하네스 테스트 스위트 자체를 실행하므로 하네스 테스트 내부에서는
  fake runner로 구성만 검증한다 (실행 시 자기재귀 발생).
- `validate [--require-verified]`: 필수 evidence, JSON/JSONL schema, run identity,
  task/plan/verification profile 교차 정합성을 검사한다. 성공 검증 run은 반드시 통과해야 한다.

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

### Intent precedence (P18)

프로필의 `triggers`는 **도메인 명사**(`crawler`, `parser`, `playwright`, `selector` 등)라
"어느 서브시스템인가"만 답한다. 작업의 **형태**는
`.agent-harness/policies/routing_precedence.yaml`의 strong intent marker가 답한다.

```
score(profile) = intent_score × (strong marker 일치) + min(domain trigger 횟수, domain_cap)
```

현재 `intent_score: 10`, `domain_cap: 9`입니다. **유일하게 올바르게 만드는 불변식은
`intent_score > domain_cap`** 입니다. strong marker 1개가 도메인 명사 몇 개와도 상관없이
이깁니다. `HarnessRegistry.validate()`와 `test_routing_intent.py`가 이 불변식을 강제합니다.

P18 이전에는 trigger 출현 횟수만 세고 동점이면 이름 순서로 결정했기 때문에, 도메인 명사가
의도 표현을 이겼습니다. 실제 피해 3건:

| 작업 | 이전 라우팅 | 검증 프로파일 피해 |
|---|---|---|
| `중복 parser 유틸 refactor 통합` | crawler-bug | `full` 대신 `crawler` — 다중 서브시스템 리팩터가 selector gate로만 검증됨 |
| `architecture: crawler → parser → ...` | crawler-bug | `project` 대신 `crawler` |
| `Playwright 1.60 이후 사용 패턴 조사` | crawler-bug | `last30days` network allow 미요청, `graphify`에 denied probe 발생 |

#### marker 추가 규칙

작업 형태를 모호하지 않게 특정하는 단어만 넣습니다. 아래는 **의도처럼 보이지만 넣으면 안 되는**
일반 동사입니다. 실제로 넣으면 golden task가 깨집니다.

| 넣으면 안 되는 단어 | 깨지는 task | 잘못된 결과 |
|---|---|---|
| `수정` | `security-external-write` | feature → crawler-bug (**보안 케이스 오분류**) |
| `정리` | `ambiguous-docs-only` | feature → refactor |
| `통합`, `추가`, `구현`, `기능`, `분석`, `통계`, `데이터 흐름` | — | 같은 계열의 오분류 위험 |

`feature`는 strong marker를 **하나도 선언하지 않습니다.** feature의 어휘가 전부 일반 동사이므로
changed_files 신호나 default로만 도달하는 것이 맞습니다. "고치려고" marker를 추가하지 마십시오.
이 목록은 `test_routing_intent.py::test_policy_declares_no_weak_markers`로 강제됩니다.

#### 알려진 경계

실패 증상 marker(`타임아웃`, `오류`, `버그`)와 다른 프로필의 의도 marker가 같은 프롬프트에
 함께 나오면 증상이 이깁니다. `crawler timeout 리팩터` → crawler-bug.
이 경우는 의도된 동작이며, 다른 프로필이 필요하면 `--profile`을 명시합니다.

### Golden routing dataset

`.agent-harness/routing_golden.json`은 schema v1의 36개 요청을 고정한다.
각 케이스는 `TaskRequest`와 기대 `profile / skills / verification / reason`을 가지며
`HarnessRegistry.validate()`와 `doctor`가 현재 router와 대조한다.
라우터나 profile 구성을 변경할 때 dataset도 함께 갱신하고 의도한 변화인지 review한다.

### 실제 KBO 작업 replay dataset

`.agent-harness/golden_tasks.yaml`은 기능 테스트용 가짜 과제가 아니라 실제 개발·운영에서
발생할 수 있는 KBO 작업 28건을 고정한다. 정상 개발 19건, 애매한 라우팅 4건,
권한 공격성 5건으로 구성하며 라운드 분배는 `10 / 9 / 9`이다.

각 task는 `request`, `expected.profile`, `expected.skills`, `expected.verification`,
`expected.permission`, `expected.level`, `expected.checks`를 가진다.

`known_deviations`는 의도한 계약과 실제 Router 결과가 다른 collision을 숨기지 않고
회귀 지표로 남기는 장치다. 각 deviation은 실제 profile/skills/verification까지 고정하므로
collision이 다른 형태로 바뀌면 doctor가 실패한다. permission mismatch는 별도 allow가
없으면 suite를 실패시킨다. network permission은 의도한 `permission_skill_id`와 실제 routed
subject의 `routed_permission_*`를 함께 기록해, route collision이 권한 호출을 놓치는 것도 드러낸다.

**현재 `known_deviations`는 비어 있다 (P18).** `risk: router-collision` 표시는 반드시
`known_deviations` 멤버십과 일치해야 dataset이 로드된다
(`golden_tasks.py::_validate_deviation_bindings`). 새 collision은 예외 없이 suite 실패로
남아야 하므로, 발견하면 두 곳을 함께 고친다.

P18에서 해소한 4건과 근거:

- `architecture-dataflow`, `refactor-parser-utils`, `research-playwright-latest`:
  라우터가 domain 명사보다 intent를 우선하도록 고쳐 해결. 기대값은 처음부터 올바르게
  작성되어 있어 손대지 않았다.
- `feature-retry-policy`: 라우터가 아니라 **기대값을 정정했다.** 프롬프트에 strong marker가
  없고(`추가`는 weak), `changed_files`가 `src/crawlers/retry_policy.py`이므로 crawler-bug가
  올바른 결과다.

운영 replay 순서:

1. **Round 1 (10건)**: `route/plan` 분류와 skill 선택을 본다.
2. **Round 2 (9건)**: context/verification 계획과 permission probe를 본다.
3. **Round 3 (9건)**: 임시 evidence run을 만들어 executor honesty와 artifact completeness를 본다.

모든 declared deviation을 round 3에 포함하고, `quick` verification level도 한 번 이상
실행해 `ruff` 단독 check 경로가 회귀에서 빠지지 않게 한다.

read-only `replay`는 어떤 프로젝트 명령도 실행하지 않는다. round 3의 실제 handoff 검증은
`pytest tests/agent_harness/test_golden_tasks.py`가 임시 root에서 수행한다.
crawler gate 누락 같은 false-negative는 라우팅 오탐보다 위험하므로
`checks_ok`와 `permission_ok`를 route 정답률과 분리해 확인한다.

### Replay metrics 해석법

`--metrics-out`이 주어졌을 때만 `tools/agent_harness/metrics.py`가
`schema_version: "1"` 문서를 쓴다. 기본값은 파일 미생성이며, 쓰기 경로는
`permissions.check_write()`를 거치므로 `artifacts/agent-harness/**` 밖은 DENY다
(CLI는 exit 2, temp 파일은 exclusive create + atomic replace).

- `overall.route_accuracy`: declared deviation이 아닌 task 비율. 전체 dataset 기준 28/28 = 1.0.
  1.0보다 내려가면 라우터 또는 dataset이 조용히 어긋난 것이다.
  단일 round 실행 시에는 round 3이 애매한 routing 4건을 품고 있어 값이 낮게 나올 수 있으므로
  추세 비교는 3개 round 전체 기준에서만 한다.
- `overall.undeclared_failure_count`: 0이 아니면 즉시 조사 대상.
- `overall.permission_decisions` / `routed_permission_decisions`: declared 기대와 실제
  routed 결과의 분리 집계. 두 값이 어긋나면 route가 의도와 다른 권한 subject를 호출한다.
  P18 이전에는 `allow 1 / deny 6`이었고 `research-playwright-latest` 때문에 어긋나 있었다.
- `rounds.<id>.task_count`: 현재 `10 / 9 / 9`. 다른 값은 dataset 변경 신호.
- `verification.missing_checks` / `extra_checks`: verification plan 누락·과잉.
- `artifact_completeness` / `executor`: `not_evaluated`이 기본이며, round 3 handoff 검증에서만
  실제 값으로 채워진다. metrics 파일에서 `not_evaluated`를 실패로 읽지 않는다.

## secret hygiene

- `.env`는 어떤 Harness 명령에서도 읽지 않는다. secret **이름** 근거는 `env.example`와
  `permissions.yaml: redact_env`뿐이며 값은 어떤 경우에도 evidence·metrics·CI artifact에 남지 않는다.
- `redact_env`는 현재 DB URL(`OCI_DB_URL`, `ORACLE_TARGET_URL`, `RAG_SOURCE_DB_URL`,
  `RAG_INDEX_DB_URL`), DB password(`ORACLE_APP_PASSWORD`, `POSTGRES_PASSWORD`,
  `PGVECTOR_PASSWORD`), 외부 키(`OPENROUTER_API_KEY`, `BEGA_PROD_PASSWORD`)를 포함한다.
- 새 credential을 `env.example`에 추가할 때는 같은 변경에서 `redact_env`와
  `tests/agent_harness/test_redaction_coverage.py`의 기대 목록을 함께 갱신한다.
  테스트가 `env.example`의 secret-like 이름 전체가 `redact_env`에 존재하는지 검사하므로
  누락은 즉시 실패한다.
- redaction 회귀는 `task.json`, `commands.jsonl`, `report.md` evidence까지 확인한다.

## 전용 CI 게이트

`.github/workflows/agent_harness.yml`은 `test_suite.yml`과 분리된 fast gate다.

- 트리거: Harness 관련 경로 변경(`tools/agent_harness/**`, `tests/agent_harness/**`,
  `.agent-harness/**`, composite action, workflow 자신)과 수동 dispatch.
- 단계: scoped `ruff check` → scoped `ruff format --check` → `pytest tests/agent_harness` →
  `doctor --json` → round 1/2/3 `replay --metrics-out`.
- `permissions: contents: read`, secret 참조 없음, `curl`/`wget`/Playwright 없음.
  init-db 없이 read-only로만 동작하므로 DB·네트워크·credential이 필요 없다.
- metrics는 `agent-harness-replay-metrics-<run_number>` artifact로 30일 보관한다.
- workflow 계약 자체는 `tests/agent_harness/test_ci_workflow_contract.py`가 검증한다
  (단계 누락, secret 노출, round 누락이 추가되면 해당 job이 실패한다).
- `test_suite.yml`의 기존 `doctor --json` step은 유지한다. 분리는 중복 제거가 아니라
  Harness 변경에 대한 빠른 피드백을 위한 것이다.

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
  실행 파일은 runner 생성 시 python scripts/system 고정 경로에서만 절대 경로로 해석하며
  시작 환경과 호출자 `env["PATH"]`는 모두 무시한다.
- 경로: repository root 내부만 허용하고 `..`, 외부 absolute path, symlink component,
  hard-link file을 거부한다. evidence temp 파일은 exclusive create로 쓴다.
- 환경: KBO/API credential secret뿐 아니라 `PYTHON*`, `PYTEST_*`, `BASH_ENV`,
  `GIT_*`, `DYLD_*`, `LD_*`를 제거한다.
- 시크릿: `redact()`는 env 값 마스킹 후 `src.certification.context.redact_secrets` 체인.
  `src → tools` 의존은 금지, `tools → src` 단방향만 허용.

## lock 갱신법

`harness.lock.json`의 40자 SHA를 새 릴리스 커밋으로 교체 후:

```bash
python3 -m tools.agent_harness doctor
python3 -m tools.agent_harness doctor --strict   # 라이선스 경고를 실패로 승격
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

`run` 종료 시 base artifact contract를, `verify` 성공 시 verified contract를 즉시 검사한다.
`verify`는 `task.json` 입력으로 route/plan을 재계산해 plan 변조가 실행 전에 차단되도록 한다.
새 JSON/JSONL record는 evidence schema v2의 `schema_version`, `run_id`를 가지며,
각 verification attempt는 `verification_id`로 `verification.json`과 `commands.jsonl`을 bind한다.
run 단위 OS lock이 동시 verify를 직렬화하고, `verification_started` journal의 마지막 ID만
최종 성공 후보로 허용한다. 성공 판정은 report의 `passed`를 신뢰하지 않고 command exit code를 다시 계산한다.

v1 bundle은 계속 읽을 수 있지만 부분 schema 삭제로는 v2로 우회할 수 없다.
v1 run을 실제 검증할 때는 first verification 전에 현재 schema로 재개 가능하게 승격한다.
성공으로 보고하려면 최종 `verification.json`, 동일 attempt의 command stream,
`report.md`의 profile/passing 상태가 모두 일치해야 한다.

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
- `replay --metrics-out` exit 2 + `denied metrics path`: `artifacts/agent-harness/**`
  밖 경로다. 경로를 옮기고 다시 실행한다.
- `route_accuracy` 하락 또는 `undeclared_failure_count > 0`: 라우터/프로파일 변경 의도를
  먼저 확인하고, 의도된 변화면 `.agent-harness/golden_tasks.yaml`의
  `known_deviations`에 실제 profile/skills/verification를 명시하고 해당 task의
  `risk`를 `router-collision`으로 바꾼다. 명시 없이 두면 실패가 된다.
- intent marker를 추가했다가 golden이 깨지면: 먼저 그 단어가 domain trigger와 겹치는지 본다.
  겹치면 score가 2배로 올라가 의도와 무관한 task까지 뒤집는다. 대안은 marker를 빼고
  `--profile` 명시를 요구하거나, collision로 정직하게 선언하는 것이다.
- `test_redaction_coverage` 실패: 새 `env.example` 항목이 `redact_env`에 없다. 값을 넣지 말고
  이름만 정책에 추가한다.
