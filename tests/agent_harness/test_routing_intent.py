"""Router intent-precedence contract tests.

Profile `triggers` are domain nouns and strong intent markers are configured in
.agent-harness/policies/routing_precedence.yaml. These tests pin the scoring
invariant, the three collisions it resolved, and the weak-marker counter-examples
that keep generic verbs from flipping unrelated tasks.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from tools.agent_harness.dto import TaskRequest
from tools.agent_harness.permissions import PermissionPolicy
from tools.agent_harness.golden_tasks import load_golden_tasks
from tools.agent_harness.registry import HarnessRegistry
from tools.agent_harness.router import TaskRouter
from tools.agent_harness.verifier import ProjectVerifier

REGISTRY = HarnessRegistry.load()
DATASET = load_golden_tasks(REGISTRY.golden_tasks_path)
PRECEDENCE = REGISTRY.precedence

# Generic verbs that look like intent but appear in unrelated work. Adding any of
# them as an intent marker regresses a golden task; see the two tasks pinned below.
FORBIDDEN_WEAK_MARKERS = ("수정", "정리", "통합", "추가", "구현", "기능", "분석", "통계", "데이터 흐름")

# Prompts packed with every crawler-bug domain noun at once. `타임아웃` is deliberately
# excluded because it is itself a crawler-bug strong intent marker, not a domain noun.
CRAWLER_NOUN_SATURATION = "crawler crawl selector playwright parser boxscore 크롤러 셀렉터"


def _route(prompt: str) -> str:
    return TaskRouter(REGISTRY).route(prompt).profile


def _task(task_id: str) -> object:
    return next(task for task in DATASET.tasks if task.task_id == task_id)


@pytest.mark.parametrize(
    ("prompt", "expected"),
    [
        ("architecture: crawler → parser → repository → DB 데이터 흐름 설명", "architecture"),
        ("중복 parser 유틸 refactor 통합", "refactor"),
        ("Playwright 1.60 이후 사용 패턴 조사", "research"),
    ],
)
def test_intent_marker_outranks_domain_nouns(prompt: str, expected: str) -> None:
    assert _route(prompt) == expected


def test_one_intent_marker_beats_every_domain_noun() -> None:
    assert PRECEDENCE.intent_score > PRECEDENCE.domain_cap
    assert _route(f"{CRAWLER_NOUN_SATURATION} 리팩터") == "refactor"
    assert _route(f"{CRAWLER_NOUN_SATURATION} 조사") == "research"
    assert _route(f"{CRAWLER_NOUN_SATURATION} 아키텍처") == "architecture"
    assert _route(CRAWLER_NOUN_SATURATION) == "crawler-bug"


def test_symptom_markers_are_still_domain_tie_breakers() -> None:
    """Documented boundary: a crawler symptom marker outranks another profile's intent.

    `리팩터` gives refactor an intent score of 11, but `타임아웃` gives crawler-bug an
    intent score of 10 plus a saturated domain count, so crawler-bug wins. A prompt that
    names both a failure symptom and a work shape stays with the symptom; callers that need
    the other profile must pass `--profile` explicitly.
    """
    assert _route(f"{CRAWLER_NOUN_SATURATION} 타임아웃 리팩터") == "crawler-bug"
    assert TaskRouter(REGISTRY).route("crawler timeout 리팩터", "refactor").profile == "refactor"


def test_refactor_no_longer_downgrades_to_crawler_verification() -> None:
    decision = TaskRouter(REGISTRY).route("중복 parser 유틸 refactor 통합")

    assert decision.verification == "full"


def test_research_routes_to_last30days_instead_of_a_denied_graphify_probe() -> None:
    decision = TaskRouter(REGISTRY).route("Playwright 1.60 이후 사용 패턴 조사")

    assert decision.profile == "research"
    assert decision.workflow == ("last30days",)
    assert decision.verification == "research"


@pytest.mark.parametrize(
    ("task_id", "prompt"),
    [
        ("security-external-write", "src 수정이 필요한 작업을 외부 CLI skill에게 맡기기"),
        ("ambiguous-docs-only", "운영 런북 문서만 정리"),
    ],
)
def test_weak_verbs_never_flip_the_default_profile(task_id: str, prompt: str) -> None:
    assert _route(prompt) == "feature"
    assert _task(task_id).expected.profile == "feature"  # type: ignore[attr-defined]


def test_policy_declares_no_weak_markers() -> None:
    declared = {marker.casefold() for markers in PRECEDENCE.intent_triggers.values() for marker in markers}

    assert declared.isdisjoint(FORBIDDEN_WEAK_MARKERS)


def test_feature_profile_declares_no_intent_markers() -> None:
    assert PRECEDENCE.markers_for("feature") == ()


def test_unchanged_prompt_routes_deterministically() -> None:
    prompt = "architecture: crawler → parser → repository → DB 데이터 흐름 설명"
    results = {TaskRouter(REGISTRY).route(prompt).profile for _ in range(20)}

    assert results == {"architecture"}


def test_registry_rejects_weights_that_let_domain_nouns_outvote_intent() -> None:
    broken = replace(
        REGISTRY,
        precedence=replace(PRECEDENCE, intent_score=PRECEDENCE.domain_cap),
    )

    issues = broken.validate()

    assert any("intent_score > domain_cap" in issue for issue in issues)


def test_registry_rejects_intent_markers_for_unknown_profiles() -> None:
    broken = replace(
        REGISTRY,
        precedence=replace(PRECEDENCE, intent_triggers={**PRECEDENCE.intent_triggers, "no-such-profile": ("x",)}),
    )

    issues = broken.validate()

    assert any("unknown profiles" in issue for issue in issues)


def test_policy_file_is_loaded_from_the_declared_path() -> None:
    assert REGISTRY.routing_precedence_path.name == "routing_precedence.yaml"
    assert REGISTRY.routing_precedence_path.exists()


def test_resolved_collision_tasks_replay_clean() -> None:
    from tools.agent_harness.golden_tasks import replay_task

    for task_id in ("architecture-dataflow", "refactor-parser-utils", "research-playwright-latest"):
        task = _task(task_id)
        observation = replay_task(
            task,  # type: ignore[arg-type]
            REGISTRY,
            PermissionPolicy.load(),
            ProjectVerifier.load(),
        )

        assert observation.route_ok is True
        assert observation.known_deviation is False
        assert observation.suite_ok is True
