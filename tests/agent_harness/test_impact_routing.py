"""PR3 tests: changed-files routing priority and impact-based verification."""

from __future__ import annotations

from dataclasses import replace

import pytest

from tools.agent_harness.dto import TaskRequest
from tools.agent_harness.project_adapter import KBOProjectAdapter
from tools.agent_harness.registry import HarnessRegistry
from tools.agent_harness.router import TaskRouter
from tools.agent_harness.verifier import GatePolicy, ProjectVerifier, _load_gate_policies


def test_explicit_profile_beats_changed_files() -> None:
    router = TaskRouter(HarnessRegistry.load())
    request = TaskRequest(
        prompt="정리해줘",
        changed_files=["src/crawlers/game_boxscore_crawler.py"],
        explicit_profile="analytics",
    )

    assert router.route_request(request).profile == "analytics"


def test_changed_files_beat_prompt_keywords() -> None:
    router = TaskRouter(HarnessRegistry.load())
    request = TaskRequest(
        prompt="이거 좀 정리해줘",
        changed_files=["src/crawlers/game_boxscore_crawler.py", "tests/test_game_boxscore_crawler.py"],
    )

    decision = router.route_request(request)

    assert decision.profile == "crawler-bug"
    assert decision.verification == "crawler"


def test_prompt_fallback_without_files() -> None:
    router = TaskRouter(HarnessRegistry.load())

    assert router.route_request(TaskRequest(prompt="boxscore crawler timeout")).profile == "crawler-bug"
    assert router.route_request(TaskRequest(prompt="hello world")).profile == "feature"


def test_adapter_file_inference_and_subsystems() -> None:
    adapter = KBOProjectAdapter()

    assert adapter.infer_profile_from_files(["src/crawlers/x.py"]) == "crawler-bug"
    assert adapter.infer_profile_from_files(["src/rag/engine.py"]) == "analytics"
    assert adapter.infer_profile_from_files(["Docs/guide.md"]) is None
    assert "crawler" in adapter.affected_subsystems(["src/parsers/y.py"])
    assert adapter.needs_crawler_gate(["src/crawlers/x.py"]) is True
    assert adapter.needs_crawler_gate(["src/rag/x.py"]) is False
    assert adapter.needs_certification([], "refactor") is True
    assert adapter.needs_certification(["src/rag/x.py"], "analytics") is False


def test_verifier_pytest_targets_and_gates() -> None:
    verifier = ProjectVerifier.load()

    assert "tests/monitoring/test_crawler_selector_gate.py" in verifier.determine_pytest_targets(["src/crawlers/x.py"])
    assert verifier.determine_pytest_targets([]) == ["tests/agent_harness"]
    assert verifier.needs_crawler_gate(["src/parsers/x.py"]) is True
    assert verifier.needs_certification(["migrations/001.sql"], "feature") is True


def test_verifier_build_plan_levels() -> None:
    verifier = ProjectVerifier.load()

    assert verifier.build_plan(level="none").checks == ()

    quick = verifier.build_plan(level="quick", changed_files=["tools/agent_harness/x.py"])
    assert [check.check_id for check in quick.checks] == ["pytest-affected", "ruff-changed", "doctor"]

    standard_crawler = verifier.build_plan(level="standard", changed_files=["src/crawlers/x.py"])
    assert [check.check_id for check in standard_crawler.checks] == [
        "pytest-affected",
        "ruff-project",
        "doctor",
        "crawler-gate",
    ]

    standard_plain = verifier.build_plan(level="standard", changed_files=["src/rag/x.py"])
    assert "crawler-gate" not in [check.check_id for check in standard_plain.checks]

    full = verifier.build_plan(level="full")
    assert [check.check_id for check in full.checks] == [
        "pytest-full",
        "ruff-project",
        "format-check",
        "mypy-scoped",
        "doctor",
    ]


def test_every_level_except_none_runs_doctor() -> None:
    """Check that doctor is mandatory, because it is the only gate validating the manifest."""
    verifier = ProjectVerifier.load()

    for level in ("quick", "standard", "full"):
        assert "doctor" in [check.check_id for check in verifier.build_plan(level=level).checks], level
    for profile in ("project", "crawler", "analytics", "full"):
        assert "doctor" in [check.check_id for check in verifier.build_profile_plan(profile).checks], profile


def test_unknown_level_and_gate_fail_loudly() -> None:
    verifier = ProjectVerifier.load()

    with pytest.raises(ValueError, match="Unknown verification level"):
        verifier.build_plan(level="turbo")

    broken = replace(verifier, levels={"standard": GatePolicy(gates=("no-such-gate",))})
    with pytest.raises(ValueError, match="Unknown verification gate"):
        broken.build_plan(level="standard")


def test_policy_referencing_an_unknown_gate_is_rejected_at_load() -> None:
    payload = {
        "levels": {"standard": {"gates": ["pytest-affected", "imaginary-gate"]}},
        "profiles": {"project": {"level": "standard"}},
    }
    with pytest.raises(ValueError, match="unknown gate"):
        _load_gate_policies(payload, "levels")


def test_policy_referencing_an_unknown_level_is_rejected_at_load() -> None:
    payload = {"levels": {}, "profiles": {"project": {"level": "nonexistent"}}}
    with pytest.raises(ValueError, match="unknown level"):
        _load_gate_policies(payload, "profiles", {})


def test_crawler_profile_does_not_duplicate_its_conditional_gate() -> None:
    """`crawler` declares crawler-gate unconditionally and inherits it conditionally."""
    verifier = ProjectVerifier.load()

    plan = verifier.build_profile_plan("crawler", changed_files=["src/crawlers/x.py"])
    gate_ids = [check.check_id for check in plan.checks]

    assert gate_ids.count("crawler-gate") == 1
    assert "pytest-crawler-gate" in gate_ids


def test_full_level_sizes_the_suite_timeout_above_the_documented_baseline() -> None:
    """The documented full-suite baseline is ~186s, so a 300s cap is a false-negative trap."""
    verifier = ProjectVerifier.load()

    checks = {check.check_id: check for check in verifier.build_plan(level="full").checks}

    assert checks["pytest-full"].timeout_seconds >= 600
    assert checks["mypy-scoped"].timeout_seconds >= 600
