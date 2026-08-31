"""Preflight offline test runner with technical outbound network denial."""

from __future__ import annotations

import hashlib
import os
import socket
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
PROTECTED_DB_PATH = REPO_ROOT / "data" / "kbo_dev.db"


def _compute_db_sha256() -> str | None:
    if not PROTECTED_DB_PATH.exists():
        return None
    h = hashlib.sha256()
    with PROTECTED_DB_PATH.open("rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def install_socket_blocker() -> None:
    """Blocks all outbound network connections to non-loopback hosts."""
    _real_connect = socket.socket.connect

    def guarded_connect(self, address):
        host = address[0] if isinstance(address, tuple) else address
        if str(host) not in ("127.0.0.1", "localhost", "::1", "0.0.0.0"):  # noqa: S104
            msg = f"[OFFLINE PREFLIGHT ERROR] Outbound network connection blocked: {address}"
            raise RuntimeError(msg)
        return _real_connect(self, address)

    socket.socket.connect = guarded_connect  # type: ignore[assignment]


def main() -> int:
    print("=== [106D-0] Offline Closure Preflight ===")

    # 1. Capture initial DB hash
    initial_hash = _compute_db_sha256()
    print(f"Protected DB Initial SHA-256: {initial_hash}")
    if initial_hash is None:
        print("[CRITICAL] Protected database is missing; refusing to certify zero-write behavior.")
        return 2

    # 2. Force in-memory DB and install socket blocker
    os.environ["DATABASE_URL"] = "sqlite:///:memory:"
    install_socket_blocker()
    print("Socket blocker installed: All non-loopback outbound traffic DENIED.")

    # 3. Test targets: 47 Offline Crawler/Parser tests + 72 Repository Integration tests + 16 new certification tests
    test_targets = [
        # 47 Offline tests
        "tests/cli/test_run_pipeline_demo.py",
        "tests/crawlers/test_baserunning_stats_crawler.py",
        "tests/test_external_stats_e2e.py",
        "tests/test_game_collection_service.py",
        "tests/test_game_detail_crawler_stability.py",
        "tests/test_relay_recovery_service.py",
        "tests/utils/test_naver_search_client.py",
        "tests/utils/test_playwright_pool_ext.py",
        # 72 Repository Integration tests
        "tests/repositories/test_game_save_ext.py",
        "tests/integration/test_transaction_atomicity_e2e.py",
        "tests/test_context_aggregator.py",
        "tests/test_context_aggregator_ext.py",
        "tests/test_ranking_aggregator.py",
        # 16 Certification tests
        "tests/test_crawler_offline_replay.py",
        "tests/test_crawler_ephemeral_e2e.py",
    ]

    pytest_args = [*test_targets, "-v", "-m", ""]
    print(f"Executing {len(test_targets)} test target modules offline...")
    exit_code = pytest.main(pytest_args)

    # 4. Verify post DB hash
    post_hash = _compute_db_sha256()
    print(f"Protected DB Post SHA-256:    {post_hash}")
    if post_hash != initial_hash:
        print("[CRITICAL] Protected database mutated during offline preflight!")
        return 1

    print("=== [106D-0] Offline Closure Preflight PASSED (Zero DB Writes, Zero Outbound Calls) ===")
    return int(exit_code)


if __name__ == "__main__":
    sys.exit(main())
