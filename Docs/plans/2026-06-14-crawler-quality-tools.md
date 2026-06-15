# Crawler Quality Tools Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the top three crawler reliability improvements: selector stability gate, failure diagnosis report, and data quality regression pack.

**Architecture:** Add small, testable pure-Python modules under `src/monitoring` and `src/validators`, with thin CLI wrappers under `src/cli`. Keep browser/network work optional so tests stay offline and deterministic.

**Tech Stack:** Python 3.12, pytest, BeautifulSoup, SQLAlchemy, optional Playwright for live URL selector capture.

---

### Task 1: Selector Stability Gate

**Files:**
- Create: `src/monitoring/crawler_selector_gate.py`
- Create: `src/cli/crawler_selector_gate.py`
- Create: `tests/test_crawler_selector_gate.py`

- [ ] Write failing tests for fixture-backed selector checks, missing selector failures, and CLI JSON output.
- [ ] Implement dataclasses for selector checks, issues, and target results.
- [ ] Implement fixture/file HTML evaluation with BeautifulSoup CSS selectors.
- [ ] Implement optional Playwright URL capture for live checks, saving HTML and screenshots under `output/playwright/selector-gate`.
- [ ] Implement CLI with `--config`, `--output-dir`, and `--json`.
- [ ] Verify with `pytest tests/test_crawler_selector_gate.py -q`.

### Task 2: Failure Diagnosis Report

**Files:**
- Create: `src/monitoring/failure_diagnosis.py`
- Create: `src/cli/diagnose_crawler_failure.py`
- Create: `tests/test_failure_diagnosis.py`

- [ ] Write failing tests for log classification across selector, network, auth, database, quality gate, scheduler lock, and Playwright failures.
- [ ] Implement rule-based diagnosis with category, severity, evidence, and suggested command fields.
- [ ] Implement text and JSON report rendering.
- [ ] Implement CLI that reads one or more log files or stdin and exits non-zero when high-severity findings exist.
- [ ] Verify with `pytest tests/test_failure_diagnosis.py -q`.

### Task 3: Data Quality Regression Pack

**Files:**
- Create: `src/validators/data_quality_regression_pack.py`
- Create: `src/cli/data_quality_regression_pack.py`
- Create: `tests/test_data_quality_regression_pack.py`

- [ ] Write failing tests using in-memory SQLite tables for PA formula, hits > AB, earned runs > runs, and null player_id invariants.
- [ ] Implement table/column-aware checks that skip missing optional tables with an explicit skipped result.
- [ ] Implement JSON/text summaries and CLI support for `--database-url` and `--json`.
- [ ] Verify with `pytest tests/test_data_quality_regression_pack.py -q`.

### Task 4: Final Verification

**Files:**
- Modify only if needed: `AGENTS.md` or existing verification scripts.

- [ ] Run focused pytest targets for all three tools.
- [ ] Run `ruff check` on the created modules and tests.
- [ ] Inspect `git diff` to ensure unrelated dirty worktree changes were not modified.
