"""Contract for the scheduler alert rules in `alert_rules.yml`.

The database and notification groups each got a firing fixture after it became
clear that `promtool check rules` proves an expression *parses* and nothing
about whether it ever fires. `kbo_scheduler_alerts` was the one group left in
that state, and it is the group where the question matters most, because both
its original rules watch the pipeline's own silence:

    * a job that cannot take its tier lock returns quietly and logs a warning;
    * a job skipped because the database is unreachable logs nothing at all.

Both are deliberate -- the alternative is a job holding `MAINTENANCE_LOCK`
while it burns connect retries, which is what starved the pipeline during the
2026-10-03 outage. The cost of that fix is that **a scheduler whose every job
skips is indistinguishable from a scheduler with nothing scheduled**.

**That paragraph used to end here, and it was wrong.** It claimed the two
`increase()` rules were "the only thing telling the two apart", but both are
conditioned on the activity a stopped job stops producing: the window ages the
last increment out and *resolves* the alert the job had raised. On 2026-10-13
collection stopped across most crawlers and nothing fired, which is precisely
the case those two rules cannot see. `KBOSchedulerDown` and
`KBOSchedulerJobNotRunning` are the rules that close it, and the tests below
now pin them alongside the originals.

What is deliberately *not* repeated here:

    * metric-name existence in both directions -- `test_crawler_alert_rules_contract.py`
      already spans `BASE_RULES`, which is the file this group lives in;
    * `promtool check rules` parsing, for the same reason;
    * `rule_files` and compose mounts, asserted by the database contract for the
      same file.

What is added, and is covered nowhere else, is the one hole a name check cannot
see: **the label values a rule selects on are values the deployment emits.**
`KBOSchedulerJobFailed` matches `status="failure"`. Rename that to
`status="failed"` in `src/scheduler/metrics.py` and the metric name still exists,
`check rules` still passes, the rule still parses -- and it never fires once. The
pipeline goes green, the failures go unrecorded, and nothing fails a test.

The staleness rule adds a second hole of the same shape one level up: it is
thresholded against a *per-job cadence* rather than a constant, so a rule that
quietly went back to a hardcoded window would still parse, still fire in
fixtures, and still be wrong for most of the schedule. `TestTheStalenessRuleIsCadenceAware`
pins the indirection itself.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from functools import lru_cache
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
BASE_RULES = ROOT / "monitoring" / "prometheus" / "alert_rules.yml"
FIXTURE_DIR = ROOT / "monitoring" / "prometheus" / "tests"
FIXTURES = (
    FIXTURE_DIR / "scheduler_alert_lock_contention_test.yml",
    FIXTURE_DIR / "scheduler_alert_cessation_test.yml",
    FIXTURE_DIR / "scheduler_alert_skip_test.yml",
)

PROMTOOL = shutil.which("promtool")

#: The group these tests defend, and the alerts it must contain.
SCHEDULER_GROUP = "kbo_scheduler_alerts"
SCHEDULER_ALERTS = (
    "KBOLockSkipSpike",
    "KBOSchedulerJobFailed",
    "KBOSchedulerDown",
    "KBOSchedulerJobNotRunning",
    "KBOSchedulerJobSkipped",
)

#: Every label matcher the group's rules pin, and the value each must select.
#: Written out rather than derived from the rules, because the point of the
#: check below is to compare the two -- deriving the expectation from the thing
#: under test would make it agree with any value, including a wrong one.
#: `test_the_matcher_table_matches_the_rules` keeps the table honest, so adding a
#: matcher to a rule without adding it here fails instead of going unchecked.
EXPECTED_MATCHERS: dict[str, dict[str, str]] = {
    "KBOLockSkipSpike": {},
    "KBOSchedulerJobFailed": {"status": "failure"},
    "KBOSchedulerDown": {"job": "kbo-scheduler"},
    "KBOSchedulerJobNotRunning": {},
    "KBOSchedulerJobSkipped": {},
}

#: The cadence gauge the staleness rule must read instead of a constant.
CADENCE_GAUGE = "kbo_scheduler_job_expected_interval_seconds"
LAST_RUN_GAUGE = "kbo_scheduler_job_last_run_timestamp"

#: The smallest literal that could be a window rather than a multiplier.
_MINUTE = 60

#: promtool's evaluation times: `30s`, `45m`, `1h10m`, `14m30s`.
_DURATION = re.compile(r"(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?")


def _seconds(value: str) -> int:
    """Read a fixture ``eval_time`` as whole seconds.

    Read as text, ``"1h10m"`` sorts before ``"45m"``, so a fixture that puts its
    quiet evaluation first in time can look like it put it last -- the ordering
    check below would fail on a correct fixture, or pass on one whose hold is
    unproven. Converting is what that assertion actually needs; the fixture
    format is promtool's, not a text field this file gets to constrain.
    """
    match = _DURATION.fullmatch(value.strip())
    if match is None or not any(match.groups()):
        raise AssertionError(f"unreadable fixture eval_time: {value!r}")  # noqa: EM102 - assertion text

    hours, minutes, seconds = (int(part) if part else 0 for part in match.groups())
    return hours * 3600 + minutes * 60 + seconds


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


def _fixture_cases(fixture: Path) -> list[dict]:
    document = yaml.safe_load(fixture.read_text(encoding="utf-8"))
    return [case for test in document["tests"] for case in test.get("alert_rule_test", [])]


def _cases(alert: str) -> list[dict]:
    return sorted(
        (case for fixture in FIXTURES for case in _fixture_cases(fixture) if case.get("alertname") == alert),
        key=lambda case: case["eval_time"],
    )


@lru_cache(maxsize=1)
def _deployment_text() -> str:
    """Return the text of everything that can emit a label value we match on.

    Two kinds of file do that, and they are not the same kind of file: Python
    emits ``status="failure"`` from the metrics module, and the scrape
    configuration emits the ``job`` label of every series in this group. Scanning
    only `src/` would fail `job="kbo-scheduler"` -- correctly, in the sense that
    the value is real, but for the wrong reason, and it would push the next
    person to delete the matcher rather than follow it to where the value comes
    from.

    The rules and the fixtures are excluded deliberately. They contain every one
    of these values by construction, so scanning them would make the check agree
    with itself: rename ``status="failure"`` in the code and the assertion would
    still pass against the rule file that reads it -- which is the exact failure
    the check exists to catch.
    """
    roots = (ROOT / "src", ROOT / "monitoring")
    suffixes = {".py", ".yml", ".yaml"}
    return "\n".join(
        path.read_text(encoding="utf-8", errors="ignore")
        for root in roots
        for path in sorted(root.rglob("*"))
        if path.is_file()
        and path.suffix in suffixes
        and not path.name.startswith("alert_rules")
        and FIXTURE_DIR not in path.parents
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


class TestThereIsARuleThatCanSeeCessation:
    """The gap that let a four-day outage pass unnoticed.

    Every rule in the group before this one required recent activity to fire, so
    a scheduler that had stopped was indistinguishable from one with nothing to
    do. A rule that reads an *age* is what makes the absence visible, and these
    two tests keep one in the group rather than trusting that it stays there.
    """

    def test_the_group_contains_an_age_based_rule(self):
        age_rules = [name for name, rule in _rules().items() if re.search(r"time\(\)\s*-", rule.get("expr", ""))]

        assert age_rules, (
            "no rule in the group reads an age, so every rule still needs the job to have "
            "run recently -- a stopped scheduler cannot be reported by any of them"
        )

    def test_the_scrape_target_itself_is_watched(self):
        """A dead scheduler exports nothing, including the age gauges.

        The age rule above needs the process to be alive to be evaluated at all,
        so the process itself is a separate series (`up`) and has to be watched
        separately. Neither rule subsumes the other.
        """
        watched = {name for name, rule in _rules().items() if re.search(r"\bup\s*\{", rule.get("expr", ""))}

        assert watched, "nothing watches `up`, so a dead scheduler is only reported by the absence of reports"


class TestTheStalenessRuleIsCadenceAware:
    """A hardcoded window cannot serve both an every-fifteen-minute and a monthly job."""

    def test_the_rule_divides_by_the_exported_cadence(self):
        rule = _rules()["KBOSchedulerJobNotRunning"]

        assert CADENCE_GAUGE in rule["expr"], rule["expr"]
        assert LAST_RUN_GAUGE in rule["expr"], rule["expr"]

    def test_the_rule_carries_no_hardcoded_seconds(self):
        """A constant window here would silently override the gauge for every job.

        `time() - <gauge> > 86400` matches the same shape as the real expression
        and would pass every fixture that happens to use a cadence near a day,
        which is why the check looks at the whole expression rather than at a
        substring.

        Only second-scale literals count. The multiplier (`1.5`, i.e. "one missed
        firing, plus half of a second") is a policy choice that belongs in the
        rule -- it is the *size* of the window that has to come from the
        schedule, and that is what the gauge supplies.
        """
        expression = _rules()["KBOSchedulerJobNotRunning"]["expr"]

        constants = [value for value in re.findall(r">\s*(\d[\d.]*)", expression) if float(value) >= _MINUTE]

        assert not constants, (
            f"the staleness rule compares against the second-scale constant(s) {constants}; the window "
            f"has to come from {CADENCE_GAUGE}, or it is wrong for most of the schedule"
        )

    def test_the_cadence_gauge_is_published_by_the_scheduler(self):
        """The rule is inert without the gauge, so the code that feeds it is pinned here.

        This is the join between the two halves of the fix: a rule that reads a
        gauge the scheduler never sets is exactly the failure this group was
        written to stop shipping. Declared in the exporter *and* written by the
        scheduler, because either half alone leaves the rule permanently silent.
        """
        exporter = (ROOT / "src" / "utils" / "metrics.py").read_text(encoding="utf-8")
        scheduler = "\n".join(
            path.read_text(encoding="utf-8") for path in sorted((ROOT / "src" / "scheduler").rglob("*.py"))
        )

        for gauge in (CADENCE_GAUGE, LAST_RUN_GAUGE):
            assert f'"{gauge}"' in exporter, f"{gauge} is not declared by the metrics exporter"

        for constant in ("KBO_SCHEDULER_JOB_EXPECTED_INTERVAL_SECONDS", "KBO_SCHEDULER_JOB_LAST_RUN_TIMESTAMP"):
            assert constant in scheduler, f"{constant} is declared but nothing in the scheduler writes it"


class TestSeverityIsTheOperatorsRouting:
    def test_failures_and_stoppage_are_critical_and_contention_is_not(self):
        """Severity is what decides who is woken, so it is pinned rather than noted.

        A failed job is not something the next tick fixes: it stopped doing work
        that was supposed to happen, and nothing downstream reports the gap.
        Demoted to `warning` it arrives while nobody is looking. A job that has
        stopped firing is the same case with a longer fuse, and the whole reason
        the cessation rules were added is that the four-day outage produced no
        page at all -- demoting them restores exactly that silence.

        Contention is the opposite case. The skipped job runs on its next
        scheduled attempt, so promoting it to `critical` trains operators to
        ignore the rule -- which costs the very signal it was added to provide.

        `KBOSchedulerJobSkipped` is contention one level down: it is raised while
        a job is not running, which reads like a stoppage, but what it reports is
        a gate that will be retried -- lock contention, or a database the
        fail-fast gate found unreachable. The unreachable case raises its own
        cause (`KBODatabaseUnavailable`, critical) beside this rule, so promoting
        this one would page twice for one outage and put the warning back to
        sleep.
        """
        severities = {name: rule.get("labels", {}).get("severity") for name, rule in _rules().items()}

        for name in ("KBOSchedulerJobFailed", "KBOSchedulerDown", "KBOSchedulerJobNotRunning"):
            assert severities[name] == "critical", severities
        for name in ("KBOLockSkipSpike", "KBOSchedulerJobSkipped"):
            assert severities[name] == "warning", severities


class TestTheSelectedLabelValuesAreTheOnesTheDeploymentEmits:
    """The hole a metric-name check cannot see.

    A rule that selects on a label value nothing produces is
    indistinguishable from a working rule: the metric exists, the expression
    parses, `promtool check rules` passes, and the alert is permanently silent.
    """

    @pytest.mark.parametrize("alert", SCHEDULER_ALERTS)
    def test_every_matcher_selects_a_value_the_deployment_emits(self, alert: str):
        text = _deployment_text()

        for label, value in EXPECTED_MATCHERS[alert].items():
            emitted = (
                re.search(rf"""{label}=["']{re.escape(value)}["']""", text),
                re.search(rf"""{label}_name:\s*["']{re.escape(value)}["']""", text),
            )
            assert any(emitted), (
                f'{alert} matches {label}="{value}", but nothing emits that value, so the rule '
                f'would never fire. Either emit {label}="{value}" or change the rule to a value '
                "the deployment actually produces."
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
    @pytest.mark.parametrize("fixture", FIXTURES, ids=lambda path: path.name)
    def test_the_fixture_exists(self, fixture: Path):
        """Named here so a deleted fixture fails here instead of vanishing quietly."""
        assert fixture.exists(), f"{fixture.name} is missing; the firing behaviour would go unverified"

    @pytest.mark.parametrize("fixture", FIXTURES, ids=lambda path: path.name)
    def test_the_fixture_reads_the_base_rules(self, fixture: Path):
        """A fixture pointing at another file proves nothing about this group."""
        document = yaml.safe_load(fixture.read_text(encoding="utf-8"))
        rule_files = [str(path) for path in document["rule_files"]]

        assert any(path.endswith("alert_rules.yml") for path in rule_files), rule_files

    def test_the_fixtures_cover_every_alert(self):
        covered = {case["alertname"] for fixture in FIXTURES for case in _fixture_cases(fixture)}

        assert covered == set(SCHEDULER_ALERTS), sorted(covered)

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
        assert min(map(_seconds, quiet)) < min(map(_seconds, firing)), (
            f"{alert} has no quiet evaluation before its first firing one, so `for` is unproven"
        )

    @pytest.mark.parametrize("fixture", FIXTURES, ids=lambda path: path.name)
    @pytest.mark.skipif(PROMTOOL is None, reason="promtool is not installed")
    def test_the_fixture_passes(self, fixture: Path):
        """`check rules` cannot tell you that a rule never fires."""
        result = subprocess.run(
            [PROMTOOL, "test", "rules", str(fixture)],
            cwd=fixture.parent,
            capture_output=True,
            text=True,
            check=False,
        )

        assert result.returncode == 0, f"{result.stdout}{result.stderr}"
