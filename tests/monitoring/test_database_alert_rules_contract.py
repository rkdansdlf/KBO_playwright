"""Contract for the database alert rules in `alert_rules.yml`.

Same intent as the crawler and notification rule contracts, for the one group
that is currently only half-guarded. `test_crawler_alert_rules_contract.py`
already reads `alert_rules.yml` for metric existence in both directions, and
`promtool check rules` parses it -- so what is missing is the part that actually
distinguishes a working alert from a dead one:

    * a **firing fixture**, so `kbo_db_available == 0` is proven to page, and
    * **wiring assertions** for `alert_rules.yml` itself, which until now were
      asserted for `alert_rules_crawler.yml` only.

Why this group carries more weight than the other two: every database-bound
scheduler job skips itself, silently, while the database is unreachable. That
skip is the fix for the 2026-10-03 outage -- a job probing a dead database while
holding `MAINTENANCE_LOCK` starved every other job in that tier -- but it means
`KBODatabaseUnavailable` is the *only* signal that the maintenance window was
lost. A renamed metric or a dropped compose mount leaves the pipeline green and
the operator blind, which is precisely the failure this file exists to prevent.

It deliberately does not duplicate the metric-existence checks; those belong to
the crawler contract, which already spans `BASE_RULES`.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
PROM_DIR = ROOT / "monitoring" / "prometheus"
PROM_CONFIG = PROM_DIR / "prometheus.yml"
BASE_RULES = PROM_DIR / "alert_rules.yml"
FIXTURE = PROM_DIR / "tests" / "database_alert_availability_test.yml"
COMPOSE_FILES = ("docker-compose.dev.yml", "docker-compose.prod.yml")

PROMTOOL = shutil.which("promtool")

#: The group these tests defend, and the alerts it must contain.
DATABASE_GROUP = "kbo_database_alerts"
DATABASE_ALERTS = ("KBODatabaseUnavailable", "KBODatabaseLatencyHigh")


class TestTheGroupStillExists:
    def test_the_database_group_is_declared(self):
        """A renamed or dropped group would leave the group glob in this file vacuous."""
        document = yaml.safe_load(BASE_RULES.read_text(encoding="utf-8"))
        groups = {group["name"]: group for group in document.get("groups", [])}

        assert DATABASE_GROUP in groups, sorted(groups)

    @pytest.mark.parametrize("alert", DATABASE_ALERTS)
    def test_the_alert_is_still_named(self, alert: str):
        """The scheduler's silence is only covered if these names survive."""
        document = yaml.safe_load(BASE_RULES.read_text(encoding="utf-8"))
        alerts = {
            rule["alert"] for group in document.get("groups", []) for rule in group.get("rules", []) if "alert" in rule
        }

        assert alert in alerts, sorted(alerts)

    def test_unavailable_is_critical_and_latency_is_not(self):
        """Severity is the operator's routing, not decoration.

        An unreachable database silently skips every maintenance job; a slow one
        still runs them. Collapsing both to `warning` would let the silent case
        arrive without anyone being woken.
        """
        document = yaml.safe_load(BASE_RULES.read_text(encoding="utf-8"))
        severities = {
            rule["alert"]: rule.get("labels", {}).get("severity")
            for group in document["groups"]
            if group["name"] == DATABASE_GROUP
            for rule in group["rules"]
        }

        assert severities["KBODatabaseUnavailable"] == "critical", severities
        assert severities["KBODatabaseLatencyHigh"] == "warning", severities


class TestTheyAreWired:
    def test_prometheus_loads_the_base_rules(self):
        """A file that is not in `rule_files` is a file that never loads."""
        config = yaml.safe_load(PROM_CONFIG.read_text(encoding="utf-8"))
        rule_files = config.get("rule_files", [])

        assert any(path.endswith("alert_rules.yml") for path in rule_files), rule_files

    @pytest.mark.parametrize("compose_file", COMPOSE_FILES)
    def test_compose_mounts_the_base_rules(self, compose_file: str):
        text = (ROOT / compose_file).read_text(encoding="utf-8")

        assert "alert_rules.yml:/etc/prometheus/alert_rules.yml:ro" in text, compose_file

    def test_the_fixture_reads_the_base_rules(self):
        """A fixture pointing at another file proves nothing about this one."""
        document = yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))
        rule_files = [str(path) for path in document["rule_files"]]

        assert any(path.endswith("alert_rules.yml") for path in rule_files), rule_files

    def test_the_fixture_covers_both_alerts(self):
        document = yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))
        tested = {case["alertname"] for test in document["tests"] for case in test.get("alert_rule_test", [])}

        assert tested == set(DATABASE_ALERTS), sorted(tested)


class TestPromtoolProvesTheyFire:
    @pytest.mark.skipif(PROMTOOL is None, reason="promtool is not installed")
    def test_the_fixture_passes(self):
        """`check rules` cannot tell you that a rule never fires."""
        result = subprocess.run(
            [PROMTOOL, "test", "rules", str(FIXTURE)],
            cwd=FIXTURE.parent,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, f"{result.stdout}{result.stderr}"

    def test_each_alert_has_a_firing_and_a_quiet_case(self):
        """Firing alone would not catch a rule that fires unconditionally."""
        document = yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))
        cases = [case for test in document["tests"] for case in test.get("alert_rule_test", [])]

        for alert in DATABASE_ALERTS:
            alert_cases = [case for case in cases if case.get("alertname") == alert]
            assert alert_cases, f"{alert} has no case in the fixture"
            assert any(case.get("exp_alerts") for case in alert_cases), f"{alert} never fires in the fixture"
            assert any(not case.get("exp_alerts") for case in alert_cases), f"{alert} never stays quiet in the fixture"
