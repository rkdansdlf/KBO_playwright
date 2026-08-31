"""Classifies all 242 deselected tests into 8 operational categories."""

from __future__ import annotations

import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
DOCS_DIR = REPO_ROOT / "Docs" / "certification" / "phase-106"
DOCS_DIR.mkdir(parents=True, exist_ok=True)
SRC_TXT = REPO_ROOT / "Docs" / "certification" / "phase-105" / "deselected-tests.txt"


def classify_test(test_id: str) -> tuple[str, str]:
    """Classifies a test ID into one of 8 operational categories."""
    if "test_page52.py" in test_id or "test_basic2_headers.py" in test_id:
        return "CRAWLER_LIVE_BROWSER", "Live browser automation against active KBO web pages"

    if "TestLiveAwardCrawler" in test_id:
        return "CRAWLER_LIVE_API", "Live HTTP API/endpoint query for awards data"

    if "test_baserunning_stats_crawler.py" in test_id and "test_safe_int_handles_dash" in test_id:
        return "PARSER_INTEGRATION", "Parser unit/integration parsing edge case"

    if any(
        kw in test_id
        for kw in [
            "test_game_collection_service.py",
            "test_relay_recovery_service.py",
            "test_run_pipeline_demo.py",
            "test_game_detail_crawler_stability.py",
            "test_external_stats_e2e.py",
            "test_naver_search_client.py",
            "test_playwright_pool_ext.py",
        ]
    ):
        return "CRAWLER_OFFLINE_REPLAY", "Offline crawler/service replay and stability test"

    if any(
        kw in test_id
        for kw in [
            "test_game_save_ext.py",
            "test_context_aggregator.py",
            "test_context_aggregator_ext.py",
            "test_ranking_aggregator.py",
            "test_transaction_atomicity_e2e.py",
        ]
    ):
        return "REPOSITORY_INTEGRATION", "Repository persistence, upsert, and transaction integration"

    if any(
        kw in test_id
        for kw in [
            "test_auto_healer",
            "test_daily_update",
            "test_dynamic_live_crawler",
            "test_scheduler",
            "test_smart_polling_gate",
            "test_lock_skip_monitor",
            "test_process_lock",
        ]
    ):
        return "SCHEDULER_INTEGRATION", "Scheduler orchestration, auto-healing, DAG, and multi-tier locks"

    return "UNRELATED_SLOW_TEST", "FastAPI HTTP server endpoints, embedding cache, or model registry"


def main() -> None:
    lines = [line.strip() for line in SRC_TXT.read_text().splitlines() if line.strip() and not line.startswith("#")]

    classified = []
    summary: dict[str, int] = {
        "SCHEDULER_INTEGRATION": 0,
        "REPOSITORY_INTEGRATION": 0,
        "CRAWLER_OFFLINE_REPLAY": 0,
        "UNRELATED_SLOW_TEST": 0,
        "CRAWLER_LIVE_BROWSER": 0,
        "CRAWLER_LIVE_API": 0,
        "PARSER_INTEGRATION": 0,
        "ORACLE_INTEGRATION": 0,
    }

    for test_id in lines:
        cat, reason = classify_test(test_id)
        summary[cat] += 1

        parts = test_id.split("::")
        file_path = parts[0]
        class_name = parts[1] if len(parts) == 3 else None
        func_name = parts[-1]

        classified.append(
            {
                "test_id": test_id,
                "file": file_path,
                "class_name": class_name,
                "func_name": func_name,
                "category": cat,
                "reason": reason,
            }
        )

    crawler_related = (
        summary["CRAWLER_OFFLINE_REPLAY"]
        + summary["CRAWLER_LIVE_BROWSER"]
        + summary["CRAWLER_LIVE_API"]
        + summary["PARSER_INTEGRATION"]
    )

    payload = {
        "total_deselected": len(lines),
        "summary": summary,
        "crawler_related_count": crawler_related,
        "tests": classified,
    }

    (DOCS_DIR / "deselected-test-classification.json").write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    print(f"Classified {len(lines)} deselected tests into deselected-test-classification.json successfully.")


if __name__ == "__main__":
    main()
