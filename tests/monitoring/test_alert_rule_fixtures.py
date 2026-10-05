"""Cross-cutting guard for the alert-rule firing layer.

The four rule files between them declare eleven alerts, and every one now has a
promtool fixture proving it fires on the data it should fire on and stays quiet
on the data it should not. Individually those fixtures are covered by their
group's contract test. What no group test can see is the layer itself:

    * a **new rule file** added later, or a **new group** in an existing one,
      would arrive with no fixture and no failing test -- the per-group contracts
      glob a fixed filename prefix and a fixed group name, so an unverified rule
      is simply absent from their view; and
    * every one of those tests **skips** when promtool is not installed. That is
      the right behaviour for a developer without the binary, and the wrong
      behaviour on a runner, where a skip reads as a pass.

The second one is why CI has an explicit promtool install step: without it the
runner verified nothing here while reporting success. This file makes the
absence of promtool a *failure* under CI, so removing that step breaks the build
instead of quietly restoring the skip.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
PROM_DIR = ROOT / "monitoring" / "prometheus"
FIXTURE_DIR = PROM_DIR / "tests"
RULE_FILES = ("alert_rules.yml", "alert_rules_crawler.yml", "alert_rules_notifications.yml")

PROMTOOL = shutil.which("promtool")


def _declared_alerts() -> dict[str, str]:
    """Return every declared alert mapped to the rule file that declares it."""
    declared: dict[str, str] = {}
    for name in RULE_FILES:
        document = yaml.safe_load((PROM_DIR / name).read_text(encoding="utf-8"))
        for group in document.get("groups", []):
            for rule in group["rules"]:
                if "alert" in rule:
                    declared[rule["alert"]] = f"{name}:{group['name']}"
    return declared


def _fixture_alerts() -> set[str]:
    tested: set[str] = set()
    for fixture in sorted(FIXTURE_DIR.glob("*_test.yml")):
        document = yaml.safe_load(fixture.read_text(encoding="utf-8"))
        for test in document.get("tests", []):
            for case in test.get("alert_rule_test", []):
                tested.add(case["alertname"])
    return tested


def test_the_rule_files_are_there_to_guard():
    """A renamed rule file would empty every glob below and pass them all."""
    for name in RULE_FILES:
        assert (PROM_DIR / name).exists(), f"{name} is missing; the firing fixtures point at it"


class TestEveryDeclaredAlertIsVerified:
    def test_no_rule_ships_without_a_fixture(self):
        """The gap this file exists to close.

        A rule with no fixture is not a rule that is known to work -- it is a
        rule whose behaviour nobody has ever observed. The four group contracts
        each check their own alerts by name, so an alert none of them lists
        would sit in the tree unverified and no existing test would object.
        """
        declared = _declared_alerts()
        missing = sorted(set(declared) - _fixture_alerts())

        assert not missing, (
            f"alert rules with no firing fixture: {missing}. "
            "Add a promtool case proving each one fires and stays quiet; "
            "'promtool check rules' only proves the expression parses."
        )

    def test_no_fixture_invents_an_alert(self):
        """The reverse direction: a case for a rule that no longer exists.

        Left in place it keeps passing forever while verifying nothing, and it
        makes the fixture count look higher than the rule count.
        """
        orphans = sorted(_fixture_alerts() - set(_declared_alerts()))

        assert not orphans, f"fixtures assert on alerts no rule declares: {orphans}"


class TestPromtoolIsAvailableWhereItMatters:
    def test_promtool_is_available_under_ci(self):
        """Under CI a missing promtool is a build failure, not a skip.

        Every firing check in this directory is guarded by
        `skipif(PROMTOOL is None)`, which is right for a developer who has not
        installed it and wrong for a runner: there, the skip silently removes
        the entire firing layer while the job reports success. CI installs
        promtool in the test job, and this test is what makes that install
        load-bearing -- deleting the step breaks the build instead of quietly
        turning eleven verified rules into eleven unverified ones.
        """
        # `CI` is a *string* in the environment, and the non-empty string
        # "false" is truthy. Testing it directly would make a developer who set
        # `CI=false` to opt out of runner behaviour fail on their own machine,
        # which is a worse outcome than the skip it was avoiding.
        if os.environ.get("CI", "").strip().lower() in {"", "0", "false", "no", "off"}:
            pytest.skip("only meaningful on a CI runner")

        assert PROMTOOL is not None, (
            "promtool is not installed on the runner, so every alert-rule firing "
            "fixture skipped. Install it in the `test` job of .github/workflows/test_suite.yml."
        )
