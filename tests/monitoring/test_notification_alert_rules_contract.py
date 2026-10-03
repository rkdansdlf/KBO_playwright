"""Contract between the notification alert rules and the metrics they read.

Same intent as the crawler rules contract, for a different path: `promtool check
rules` proves the expressions parse, but not that the metric names still exist,
that the file is actually loaded and mounted, or that the rules ever fire. A
renamed metric leaves a rule permanently silent — a green pipeline and a blind
operator.

These rules watch the notification path itself, so the exemptions below are
deliberate: every exported `kbo_notification_*` series is either read by a rule
or recorded here with a reason.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
PROM_DIR = ROOT / "monitoring" / "prometheus"
PROM_CONFIG = PROM_DIR / "prometheus.yml"
NOTIFICATION_RULES = PROM_DIR / "alert_rules_notifications.yml"
TESTS_DIR = PROM_DIR / "tests"
COMPOSE_FILES = ("docker-compose.dev.yml", "docker-compose.prod.yml")

PROMTOOL = shutil.which("promtool")

#: Series the notification rules are allowed to leave unreferenced, with a reason.
UNREFERENCED_METRIC_EXEMPTIONS = {
    "kbo_notification_dispatch_failures_total": (
        'the rate rule reads dispatch_total{status="FAILED"} so numerator and denominator share one series'
    ),
    "kbo_notification_dispatch_duration_seconds": "latency SLO work is not in this track",
    "kbo_notification_deliveries_persisted_total": "diagnostic; audit *failures* are the failure signal",
    "kbo_notification_delivery_audit_failures_total": (
        "diagnostic; a failed audit does not change the transport result, and the rate rule already pages"
    ),
    "kbo_notification_delivery_retries_total": "diagnostic; retries are absorbed by the transport before they matter",
}


def _exposed_notification_names() -> set[str]:
    """Return every `kbo_notification_*` series name the metrics module exports.

    `prometheus_client` strips the `_total` suffix from a Counter's internal
    name and re-adds it on export, so reading `_name` directly would report
    `kbo_notification_dispatch` where PromQL says `kbo_notification_dispatch_total`.
    """
    from src.utils import metrics

    names: set[str] = set()
    for value in vars(metrics).values():
        name = getattr(value, "_name", None)
        if not isinstance(name, str) or not name.startswith("kbo_notification"):
            continue
        if getattr(value, "_type", None) == "counter":
            names.add(f"{name}_total")
        else:
            names.add(name)
    return names


def _referenced_notification_names(text: str) -> set[str]:
    """Return `kbo_notification_*` names referenced by PromQL in the given text."""
    return set(re.findall(r"\bkbo_notification_[a-z0-9_]+", text))


def _expressions(path: Path = NOTIFICATION_RULES) -> list[str]:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    return [rule["expr"] for group in document.get("groups", []) for rule in group.get("rules", []) if "expr" in rule]


def _rules(path: Path = NOTIFICATION_RULES) -> list[dict]:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    return [rule for group in document.get("groups", []) for rule in group.get("rules", [])]


def _fixtures() -> list[Path]:
    return sorted(TESTS_DIR.glob("notification_alert_*_test.yml"))


class TestRulesReferenceRealMetrics:
    def test_every_referenced_metric_is_declared_in_code(self) -> None:
        missing = sorted(_referenced_notification_names("\n".join(_expressions())) - _exposed_notification_names())

        assert not missing, (
            f"notification rules reference metrics that no longer exist: {missing}. "
            "A renamed metric would leave the rule permanently silent."
        )

    def test_declared_metrics_are_referenced_or_explicitly_exempted(self) -> None:
        referenced = _referenced_notification_names("\n".join(_expressions()))

        orphans = sorted(_exposed_notification_names() - referenced - set(UNREFERENCED_METRIC_EXEMPTIONS))

        assert not orphans, (
            f"notification metrics are exported but nothing reads them: {orphans}. "
            "Either add a rule or record an exemption with a reason."
        )

    def test_every_exemption_still_refers_to_a_real_metric(self) -> None:
        stale = sorted(set(UNREFERENCED_METRIC_EXEMPTIONS) - _exposed_notification_names())

        assert not stale, f"exemptions name metrics that no longer exist: {stale}"

    def test_every_exemption_carries_a_reason(self) -> None:
        undocumented = sorted(name for name, reason in UNREFERENCED_METRIC_EXEMPTIONS.items() if not reason.strip())

        assert not undocumented, f"exemptions without a reason: {undocumented}"


class TestNotificationRulesContent:
    def test_the_operational_rules_exist(self) -> None:
        names = {rule["alert"] for rule in _rules() if "alert" in rule}

        assert names == {
            "NotificationDeliveryFailureRateHigh",
            "CriticalIncidentDeliveryFailed",
        }

    def test_every_rule_declares_severity_and_annotations(self) -> None:
        for rule in _rules():
            assert rule.get("labels", {}).get("severity"), rule["alert"]
            assert "summary" in rule.get("annotations", {}), rule["alert"]
            assert "description" in rule.get("annotations", {}), rule["alert"]

    def test_failure_rate_requires_a_minimum_volume(self) -> None:
        """A ratio alone pages on one failed send against one attempt.

        The volume guard is what keeps a quiet channel from looking like an
        outage, so its absence must fail here rather than in production.
        """
        failure_rate = next(expr for expr in _expressions() if "kbo_notification_dispatch_total" in expr)
        normalized = " ".join(failure_rate.split())

        assert 'status="FAILED"' in normalized
        assert "sum by (channel)" in normalized
        assert ">= 5" in normalized

    def test_critical_delivery_rule_requires_an_open_critical_incident(self) -> None:
        """Delivery failure alone is too noisy to page on; the open CRITICAL
        incident is what makes it actionable.
        """
        critical = next(expr for expr in _expressions() if "kbo_notification_open_incidents" in expr)
        normalized = " ".join(critical.split())

        assert 'severity="CRITICAL"' in normalized
        assert "increase(" in normalized


class TestRulesAreWired:
    def test_prometheus_loads_the_notification_rules(self) -> None:
        config = yaml.safe_load(PROM_CONFIG.read_text(encoding="utf-8"))
        rule_files = config.get("rule_files", [])

        assert any(str(path).endswith("alert_rules_notifications.yml") for path in rule_files), rule_files

    @pytest.mark.parametrize("compose_file", COMPOSE_FILES)
    def test_compose_mounts_the_notification_rules(self, compose_file: str) -> None:
        """A rule file that is not mounted is a rule file that never loads."""
        text = (ROOT / compose_file).read_text(encoding="utf-8")

        assert "alert_rules_notifications.yml:/etc/prometheus/alert_rules_notifications.yml:ro" in text, compose_file

    def test_rule_fixtures_reference_the_notification_rules(self) -> None:
        fixtures = _fixtures()
        assert fixtures, "no rule fixtures found; the firing behaviour would go unverified"

        for fixture in fixtures:
            document = yaml.safe_load(fixture.read_text(encoding="utf-8"))
            assert any(str(path).endswith("alert_rules_notifications.yml") for path in document["rule_files"]), fixture


class TestPromtoolValidatesThem:
    @pytest.mark.skipif(PROMTOOL is None, reason="promtool is not installed")
    def test_notification_rules_parse(self) -> None:
        result = subprocess.run(
            [PROMTOOL, "check", "rules", str(NOTIFICATION_RULES)],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

        assert result.returncode == 0, result.stdout + result.stderr

    @pytest.mark.skipif(PROMTOOL is None, reason="promtool is not installed")
    def test_every_rule_fixture_actually_fires(self) -> None:
        """`check rules` cannot tell you that a rule never fires."""
        for fixture in _fixtures():
            result = subprocess.run(
                [PROMTOOL, "test", "rules", str(fixture)],
                cwd=fixture.parent,
                capture_output=True,
                text=True,
                check=False,
            )

            assert result.returncode == 0, f"{fixture.name}:\n{result.stdout}{result.stderr}"

    @pytest.mark.skipif(PROMTOOL is None, reason="promtool is not installed")
    def test_rule_fixtures_cover_firing_and_quiet_for_each_rule(self) -> None:
        for fixture in _fixtures():
            document = yaml.safe_load(fixture.read_text(encoding="utf-8"))
            cases = [case for test in document["tests"] for case in test.get("alert_rule_test", [])]

            firing = sum(1 for case in cases if case.get("exp_alerts"))
            assert firing, f"{fixture.name} has no case where an alert fires"
            assert firing < len(cases), f"{fixture.name} has no case where an alert must stay quiet"

    def test_every_rule_has_a_firing_case(self) -> None:
        """Each declared alert must appear as a firing case somewhere."""
        declared = {rule["alert"] for rule in _rules() if "alert" in rule}
        covered: set[str] = set()

        for fixture in _fixtures():
            document = yaml.safe_load(fixture.read_text(encoding="utf-8"))
            for test in document["tests"]:
                for case in test.get("alert_rule_test", []):
                    if case.get("exp_alerts"):
                        covered.add(case["alertname"])

        assert declared <= covered, f"alerts with no firing fixture: {sorted(declared - covered)}"
