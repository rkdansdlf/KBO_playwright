"""Contract for the scheduler alert rules in `alert_rules.yml`.

The database and notification groups each got a firing fixture after it became
clear that `promtool check rules` proves an expression *parses* and nothing
about whether it ever fires. `kbo_scheduler_alerts` was the one group left in
that state, and it is the group where the question matters most, because both
its rules watch the pipeline's own silence:

    * a job that cannot take its tier lock returns quietly and logs a warning;
    * a job skipped because the database is unreachable logs nothing at all.

Both are deliberate -- the alternative is a job holding `MAINTENANCE_LOCK`
while it burns connect retries, which is what starved the pipeline during the
2026-10-03 outage. The cost of that fix is that **a scheduler whose every job
skips is indistinguishable from a scheduler with nothing scheduled**. These two
rules are the only thing telling the two apart.

What is deliberately *not* repeated here:

    * metric-name existence in both directions -- `test_crawler_alert_rules_contract.py`
      already spans `BASE_RULES`, which is the file this group lives in;
    * `promtool check rules` parsing, for the same reason;
    * `rule_files` and compose mounts, asserted by the database contract for the
      same file.

What is added, and is covered nowhere else, is the one hole a name check cannot
see: **the label values a rule selects on are values the code emits.**
`KBOSchedulerJobFailed` matches `status="failure"`. Rename that to
`status="failed"` in `src/scheduler/metrics.py` and the metric name still exists,
`check rules` still passes, the rule still parses -- and it never fires once. The
pipeline goes green, the failures go unrecorded, and nothing fails a test.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
BASE_RULES = ROOT / "monitoring" / "prometheus" / "alert_rules.yml"
FIXTURE = ROOT / "monitoring" / "prometheus" / "tests" / "scheduler_alert_lock_contention_test.yml"

PROMTOOL = shutil.which("promtool")

#: The group these tests defend, and the alerts it must contain.
SCHEDULER_GROUP = "kbo_scheduler_alerts"
SCHEDULER_ALERTS = ("KBOLockSkipSpike", "KBOSchedulerJobFailed")

#: Every label matcher the group's rules pin, and the value each must select.
#: Written out rather than derived from the rules, because the point of the
#: check below is to compare the two -- deriving the expectation from the thing
#: under test would make it agree with any value, including a wrong one.
#: `test_the_matcher_table_matches_the_rules` keeps the table honest, so adding a
#: matcher to a rule without adding it here fails instead of going unchecked.
EXPECTED_MATCHERS: dict[str, dict[str, str]] = {
    "KBOLockSkipSpike": {},
    "KBOSchedulerJobFailed": {"status": "failure"},
}


def _document() -> dict:
    return yaml.safe_load(BASE_RULES.read_text(encoding="utf-8"))


def _rules() -> dict[str, dict]:
    """Return the group's rules by alert name, failing if the group is gone."""
    for group in _document().get("groups", []):
        if group["name"] == SCHEDULER_GROUP:
            return {rule["alert"]: rule for rule in group["rules"] if "alert" in rule}

    raise AssertionError(f"{SCHEDULER_GROUP} is missing from {BASE_RULES.name}")  # noqa: EM102 - assertion text


def _matchers(rule: dict) -> dict[str, str]:
    """Return the label matchers an expression pins, e.g. ``status="failure"``."""
    return dict(re.findall(r'(\w+)\s*=\s*"([^"]*)"', rule.get("expr", "")))


def _cases(alert: str) -> list[dict]:
    document = yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))
    return sorted(
        (
            case
            for test in document["tests"]
            for case in test.get("alert_rule_test", [])
            if case.get("alertname") == alert
        ),
        key=lambda case: case["eval_time"],
    )


class TestTheGroupStillExists:
    def test_the_scheduler_group_is_declared(self):
        """Without it, every glob and table in this file is vacuously satisfied."""
        names = {group["name"] for group in _document().get("groups", [])}

        assert SCHEDULER_GROUP in names, sorted(names)

    @pytest.mark.parametrize("alert", SCHEDULER_ALERTS)
    def test_the_alert_is_still_named(self, alert: str):
        """The scheduler's silence is only covered while these names survive."""
        assert alert in _rules(), sorted(_rules())


class TestSeverityIsTheOperatorsRouting:
    def test_a_failed_job_is_critical_and_contention_is_not(self):
        """Severity is what decides who is woken, so it is pinned rather than noted.

        A failed job is not something the next tick fixes: it stopped doing work
        that was supposed to happen, and nothing downstream reports the gap.
        Demoted to `warning` it arrives while nobody is looking.

        Contention is the opposite case. The skipped job runs on its next
        scheduled attempt, so promoting it to `critical` trains operators to
        ignore the rule -- which costs the very signal it was added to provide.
        """
        severities = {name: rule.get("labels", {}).get("severity") for name, rule in _rules().items()}

        assert severities["KBOSchedulerJobFailed"] == "critical", severities
        assert severities["KBOLockSkipSpike"] == "warning", severities


class TestTheSelectedLabelValuesAreTheOnesTheCodeEmits:
    """The hole a metric-name check cannot see.

    A rule that selects on a label value the code never produces is
    indistinguishable from a working rule: the metric exists, the expression
    parses, `promtool check rules` passes, and the alert is permanently silent.
    """

    @pytest.mark.parametrize("alert", SCHEDULER_ALERTS)
    def test_every_matcher_selects_a_value_the_code_emits(self, alert: str):
        source = "\n".join(path.read_text(encoding="utf-8") for path in sorted((ROOT / "src").rglob("*.py")))

        for label, value in EXPECTED_MATCHERS[alert].items():
            assert re.search(rf"""{label}=["']{re.escape(value)}["']""", source), (
                f'{alert} matches {label}="{value}", but no code emits that value, so the rule '
                f'would never fire. Either emit {label}="{value}" or change the rule to a value '
                "the code actually produces."
            )

    def test_the_matcher_table_matches_the_rules(self):
        """The expected values above are hand-written, so they can rot too.

        A rule that gains a matcher the table does not know about would leave
        that matcher unchecked -- which is how the check above silently becomes
        partial, and partial is what it was written to avoid.
        """
        for alert, rule in _rules().items():
            assert _matchers(rule) == EXPECTED_MATCHERS[alert], (
                f"{alert} selects on {_matchers(rule)}, which this file checks as "
                f"{EXPECTED_MATCHERS[alert]}. Update both together."
            )


class TestTheyActuallyFire:
    def test_a_fixture_exists(self):
        """Named here so a deleted fixture fails here instead of vanishing quietly."""
        assert FIXTURE.exists(), f"{FIXTURE.name} is missing; the firing behaviour would go unverified"

    def test_the_fixture_reads_the_base_rules(self):
        """A fixture pointing at another file proves nothing about this group."""
        document = yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))
        rule_files = [str(path) for path in document["rule_files"]]

        assert any(path.endswith("alert_rules.yml") for path in rule_files), rule_files

    def test_the_fixture_covers_both_alerts(self):
        document = yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))
        tested = {case["alertname"] for test in document["tests"] for case in test.get("alert_rule_test", [])}

        assert tested == set(SCHEDULER_ALERTS), sorted(tested)

    @pytest.mark.parametrize("alert", SCHEDULER_ALERTS)
    def test_each_alert_has_a_firing_and_a_quiet_case(self, alert: str):
        """Firing alone would not catch a rule that fires unconditionally."""
        cases = _cases(alert)

        assert cases, f"{alert} has no case in the fixture"
        assert any(case.get("exp_alerts") for case in cases), f"{alert} never fires in the fixture"
        assert any(not case.get("exp_alerts") for case in cases), f"{alert} never stays quiet in the fixture"

    @pytest.mark.parametrize("alert", SCHEDULER_ALERTS)
    def test_each_alert_proves_its_for_clause(self, alert: str):
        """`for` is the difference between a blip and an incident.

        A rule whose `for` is dropped still passes every firing case in the
        fixture -- it just pages one sample earlier. What pins it is a quiet
        evaluation *before* the first firing one: the condition already held
        there, and the hold had simply not elapsed yet.
        """
        cases = _cases(alert)
        quiet = [case["eval_time"] for case in cases if not case.get("exp_alerts")]
        firing = [case["eval_time"] for case in cases if case.get("exp_alerts")]

        assert quiet and firing, f"{alert} needs both a firing and a quiet case"
        assert min(quiet) < min(firing), (
            f"{alert} has no quiet evaluation before its first firing one, so `for` is unproven"
        )

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
