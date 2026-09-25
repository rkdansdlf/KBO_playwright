"""Golden routing regression coverage for representative Harness requests."""

from __future__ import annotations

import pytest

from tools.agent_harness.dto import GoldenRoutingCase
from tools.agent_harness.registry import HarnessRegistry
from tools.agent_harness.router import TaskRouter
from tools.agent_harness.routing_golden import load_golden_routing, validate_golden_routing

DATASET_PATH = HarnessRegistry.load().root / ".agent-harness" / "routing_golden.json"
DATASET = load_golden_routing(DATASET_PATH)


def test_golden_dataset_contract() -> None:
    assert DATASET.schema_version == "1"
    assert DATASET.external_execution == "reference_only"
    assert len(DATASET.cases) == 36
    assert len({case.case_id for case in DATASET.cases}) == len(DATASET.cases)
    assert validate_golden_routing(DATASET, HarnessRegistry.load()) == ()


@pytest.mark.parametrize("case", DATASET.cases, ids=lambda case: case.case_id)
def test_golden_routing_case(case: GoldenRoutingCase) -> None:
    decision = TaskRouter(HarnessRegistry.load()).route_request(case.request)
    expected = case.expected
    actual_skills = (*decision.context, *decision.workflow, *decision.guards, decision.output)

    assert decision.profile == expected.profile
    assert actual_skills == expected.skills
    assert decision.verification == expected.verification
    assert decision.reason == expected.reason
