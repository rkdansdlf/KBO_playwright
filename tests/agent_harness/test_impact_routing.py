"""PR3 tests: changed-files routing priority and impact-based verification."""

from __future__ import annotations

from tools.agent_harness.dto import TaskRequest
from tools.agent_harness.project_adapter import KBOProjectAdapter
from tools.agent_harness.registry import HarnessRegistry
from tools.agent_harness.router import TaskRouter
from tools.agent_harness.verifier import ProjectVerifier


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
    assert [check.check_id for check in quick.checks] == ["pytest-affected", "ruff"]

    standard_crawler = verifier.build_plan(level="standard", changed_files=["src/crawlers/x.py"])
    assert "crawler-gate" in [check.check_id for check in standard_crawler.checks]

    standard_plain = verifier.build_plan(level="standard", changed_files=["src/rag/x.py"])
    assert "crawler-gate" not in [check.check_id for check in standard_plain.checks]

    full = verifier.build_plan(level="full")
    assert [check.check_id for check in full.checks] == ["pytest-full", "ruff-project"]
