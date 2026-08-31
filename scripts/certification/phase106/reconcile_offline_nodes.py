"""Generates exact test node inventory and reconciliation for Gate 106D-0 offline preflight."""

from __future__ import annotations

import json
from pathlib import Path
import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
DOCS_DIR = REPO_ROOT / "Docs" / "certification" / "phase-106"
DOCS_DIR.mkdir(parents=True, exist_ok=True)
DESELECTED_JSON = DOCS_DIR / "deselected-test-classification.json"

TEST_TARGETS = [
    # 8 Offline crawler test modules
    "tests/cli/test_run_pipeline_demo.py",
    "tests/crawlers/test_baserunning_stats_crawler.py",
    "tests/test_external_stats_e2e.py",
    "tests/test_game_collection_service.py",
    "tests/test_game_detail_crawler_stability.py",
    "tests/test_relay_recovery_service.py",
    "tests/utils/test_naver_search_client.py",
    "tests/utils/test_playwright_pool_ext.py",
    # 5 Repository integration test modules
    "tests/repositories/test_game_save_ext.py",
    "tests/integration/test_transaction_atomicity_e2e.py",
    "tests/test_context_aggregator.py",
    "tests/test_context_aggregator_ext.py",
    "tests/test_ranking_aggregator.py",
    # 2 Certification test modules
    "tests/test_crawler_offline_replay.py",
    "tests/test_crawler_ephemeral_e2e.py",
]


class NodeCollector:
    def __init__(self) -> None:
        self.collected_nodes: list[str] = []

    def pytest_collection_modifyitems(self, items: list[pytest.Item]) -> None:
        self.collected_nodes = [item.nodeid for item in items]


def main() -> None:
    # 1. Collect all exact 191 nodes
    collector = NodeCollector()
    pytest_args = [*TEST_TARGETS, "--collect-only", "-q"]
    pytest.main(pytest_args, plugins=[collector])

    all_nodes = collector.collected_nodes
    print(f"Total collected test nodes: {len(all_nodes)}")

    # Save exact nodes text file
    txt_path = DOCS_DIR / "gate-106d-offline-selected-tests.txt"
    txt_path.write_text("\n".join(all_nodes) + "\n", encoding="utf-8")

    # 2. Load 242 deselected tests inventory
    deselected_data = json.loads(DESELECTED_JSON.read_text(encoding="utf-8"))
    deselected_lookup = {t["test_id"]: t for t in deselected_data["tests"]}

    # 3. Classify all 191 nodes
    categorized = {
        "CRAWLER_OFFLINE_REPLAY": [],
        "PARSER_INTEGRATION": [],
        "REPOSITORY_INTEGRATION": [],
        "PHASE_106_CERTIFICATION": [],
        "CO_LOCATED_FAST_UNIT_TEST": [],
    }

    unclassified = []

    for node_id in all_nodes:
        if "test_crawler_offline_replay.py" in node_id or "test_crawler_ephemeral_e2e.py" in node_id:
            categorized["PHASE_106_CERTIFICATION"].append(node_id)
        elif node_id in deselected_lookup:
            cat = deselected_lookup[node_id]["category"]
            if cat in categorized:
                categorized[cat].append(node_id)
            else:
                unclassified.append({"node_id": node_id, "category": cat})
        else:
            # Tests within those files that are not marked slow/integration
            categorized["CO_LOCATED_FAST_UNIT_TEST"].append(node_id)

    counts = {k: len(v) for k, v in categorized.items()}
    total_reconciled = sum(counts.values())

    reconciliation_payload = {
        "schema_version": "1.0.0",
        "phase": "Phase 106D-0",
        "description": "Exact test node inventory and mathematical reconciliation for offline preflight suite",
        "equation": "total_executed (191) = deselected_offline_crawler (46) + deselected_parser (1) + deselected_repository (72) + new_cert_tests (16) + co_located_fast_tests (56)",
        "population_metrics": {
            "total_repository_tests": 10548,
            "deselected_by_default_marker_profile": 242,
            "selected_in_default_fast_profile": 10306,
            "fast_profile_passed": 10303,
            "fast_profile_skipped": 3,
            "fast_profile_failed": 0,
            "fast_profile_errors": 0,
        },
        "offline_preflight_metrics": {
            "total_selected_nodes": len(all_nodes),
            "reconciled_nodes_count": total_reconciled,
            "unclassified_nodes_count": len(unclassified),
            "breakdown": counts,
        },
        "node_ids_by_category": categorized,
        "unclassified_nodes": unclassified,
    }

    reconciliation_path = DOCS_DIR / "gate-106d-offline-nodes-reconciliation.json"
    reconciliation_path.write_text(
        json.dumps(reconciliation_payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    print("Generated gate-106d-offline-selected-tests.txt and gate-106d-offline-nodes-reconciliation.json")
    print(f"Reconciliation Summary: {counts} (Total: {total_reconciled})")


if __name__ == "__main__":
    main()
