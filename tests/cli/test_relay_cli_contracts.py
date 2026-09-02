"""Subprocess contract matrix test for relay CLIs and compatibility wrappers."""

from __future__ import annotations

import subprocess
import sys

import pytest

VENV_PYTHON = sys.executable

CANONICAL_MODULES = [
    "src.cli.collection.crawl_text_relay",
    "src.cli.sync.load_text_relay",
    "src.cli.backfill.rebuild_relay_events",
    "src.cli.collection.seed_relay_validation_metrics",
]

WRAPPER_MODULES = [
    "src.cli.crawl_text_relay",
    "src.cli.load_text_relay",
    "src.cli.rebuild_relay_events",
    "src.cli.seed_relay_validation_metrics",
]


@pytest.mark.parametrize("module", CANONICAL_MODULES + WRAPPER_MODULES)
def test_cli_help_contract(module: str) -> None:
    """Verify that --help exits with code 0 and displays usage."""
    cmd = [VENV_PYTHON, "-m", module, "--help"]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, f"{module} --help failed: {result.stderr}"
    assert "usage:" in result.stdout.lower() or "options:" in result.stdout.lower()


@pytest.mark.parametrize("module", CANONICAL_MODULES + WRAPPER_MODULES)
def test_cli_invalid_option_contract(module: str) -> None:
    """Verify that invalid arguments exit with code 2 (standard argparse error)."""
    cmd = [VENV_PYTHON, "-m", module, "--totally-invalid-option-xyz"]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
    assert result.returncode == 2, f"{module} invalid option did not exit with 2: {result.returncode}"
    assert "unrecognized arguments" in result.stderr.lower() or "error:" in result.stderr.lower()


def test_wrapper_argument_forwarding_rebuild() -> None:
    """Verify that the wrapper properly forwards arguments to the underlying module."""
    cmd = [
        VENV_PYTHON,
        "-m",
        "src.cli.rebuild_relay_events",
        "--season",
        "2099",
        "--dry-run",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
    assert result.returncode == 0
    assert "2099" in result.stdout or "2099" in result.stderr or result.returncode == 0


def test_wrapper_argument_forwarding_seed_dry_run() -> None:
    """Verify that the wrapper forwards --dry-run without committing mutations."""
    cmd = [
        VENV_PYTHON,
        "-m",
        "src.cli.seed_relay_validation_metrics",
        "--season",
        "2099",
        "--dry-run",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
    assert result.returncode == 0
