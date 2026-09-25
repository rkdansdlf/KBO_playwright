"""The checked-in selector gate config must stay runnable and complete.

`Docs/references/crawler_selector_gate.json` is the contract that catches KBO
markup drift before a production crawl does. A target is only useful if its
fixture is committed and the target actually passes, so both are enforced here
rather than left to whoever next runs the CLI by hand.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.monitoring.crawler_selector_gate import load_selector_config, run_selector_gate

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "Docs" / "references" / "crawler_selector_gate.json"

#: Crawler/parser areas that must be represented in the gate. Adding a target
#: is cheap; forgetting to add one is how drift reaches production unnoticed.
REQUIRED_AREA_KEYWORDS = {
    "batting": "team_batting_fixture",
    "pitching": "team_pitching_fixture",
    "game_detail": "game_detail_fixture",
    "events": "hh_events_notice",
    "ticket": "lg_ticket_prices",
}


@pytest.fixture(scope="module")
def config() -> dict:
    return json.loads(CONFIG.read_text(encoding="utf-8"))


def test_every_target_has_a_committed_fixture(config: dict) -> None:
    """A target pointing at a missing file can never fail usefully."""
    config_dir = CONFIG.parent
    missing: list[str] = []
    for target in config["targets"]:
        source = Path(target["source"])
        resolved = source if source.is_absolute() else (config_dir / source).resolve()
        if not resolved.is_file():
            missing.append(f"{target['name']} -> {target['source']}")
    assert not missing, f"selector gate targets without a fixture: {missing}"


def test_target_names_are_unique(config: dict) -> None:
    names = [target["name"] for target in config["targets"]]
    assert len(names) == len(set(names)), "duplicate selector gate target name"


def test_every_target_declares_checks(config: dict) -> None:
    empty = [target["name"] for target in config["targets"] if not target.get("checks")]
    assert not empty, f"selector gate targets with no checks: {empty}"


@pytest.mark.parametrize(("area", "expected_name"), sorted(REQUIRED_AREA_KEYWORDS.items()))
def test_required_crawler_area_is_covered(config: dict, area: str, expected_name: str) -> None:
    names = {target["name"] for target in config["targets"]}
    assert expected_name in names, f"selector gate no longer covers the {area} area"


def test_checked_in_config_passes() -> None:
    """The gate must be green on a clean checkout, or it gets ignored."""
    targets = load_selector_config(CONFIG)

    summary = run_selector_gate(targets)

    assert summary.ok, render(summary)
    assert summary.target_count >= len(REQUIRED_AREA_KEYWORDS)


def render(summary: object) -> str:
    from src.monitoring.crawler_selector_gate import render_selector_summary

    return render_selector_summary(summary)
