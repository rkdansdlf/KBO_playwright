"""Contract between the crawler alert rules and the metrics they read.

`promtool check rules` proves the expressions parse. It cannot prove the metric
names still exist, that a renamed metric left a rule permanently silent, or that
the rules file is actually loaded and mounted. Those are the failures worth
catching, because each one produces a green pipeline and a blind alert.
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
CRAWLER_RULES = PROM_DIR / "alert_rules_crawler.yml"
BASE_RULES = PROM_DIR / "alert_rules.yml"
TESTS_DIR = PROM_DIR / "tests"
COMPOSE_FILES = ("docker-compose.dev.yml", "docker-compose.prod.yml")

PROMTOOL = shutil.which("promtool")

#: Series the crawler rules are allowed to leave unreferenced, with a reason.
UNREFERENCED_METRIC_EXEMPTIONS = {
    # Exposed for ad-hoc investigation and throughput dashboards; the alert reads
    # the gauge instead, because a rate cannot distinguish "slow" from "stopped".
    "kbo_crawl_records_written_total": "cumulative throughput for dashboards; the alert reads the gauge",
    "kbo_crawl_records_read_total": "diagnostic only",
    "kbo_crawl_records_failed_total": "diagnostic only, compared against the DLQ",
    "kbo_crawl_duration_seconds": "latency SLO work is not in this track",
}


def _exposed_name(collector: object) -> str | None:
    """Return the series name PromQL sees for a collector.

    `prometheus_client` strips the `_total` suffix from a Counter's internal
    name and re-adds it on export, so reading `_name` directly reports
    `kbo_crawl_runs` where the rule and the scrape endpoint both say
    `kbo_crawl_runs_total`.
    """
    name = getattr(collector, "_name", None)
    if not isinstance(name, str) or not name.startswith("kbo_crawl"):
        return None
    if getattr(collector, "_type", None) == "counter":
        return f"{name}_total"
    return name


def _declared_metric_names() -> set[str]:
    """Return every metric name the crawler metrics module exports to PromQL."""
    from src.monitoring import crawler_metrics

    names: set[str] = set()
    for value in vars(crawler_metrics).values():
        exposed = _exposed_name(value)
        if exposed:
            names.add(exposed)
    return names


def _referenced_metric_names(text: str) -> set[str]:
    """Return `kbo_crawl_*` names referenced by PromQL in the given text."""
    return set(re.findall(r"\bkbo_crawl_[a-z0-9_]+", text))


def _rule_expressions(path: Path) -> list[str]:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    return [rule["expr"] for group in document.get("groups", []) for rule in group.get("rules", []) if "expr" in rule]


class TestRulesReferenceRealMetrics:
    def test_every_referenced_metric_is_declared_in_code(self):
        declared = _declared_metric_names()
        referenced = set()
        for path in (BASE_RULES, CRAWLER_RULES):
            referenced |= _referenced_metric_names("\n".join(_rule_expressions(path)))

        missing = sorted(referenced - declared)
        assert not missing, (
            f"alert rules reference metrics that no longer exist: {missing}. "
            "A renamed or deleted metric would leave the rule permanently silent."
        )

    def test_declared_metrics_are_referenced_or_explicitly_exempted(self):
        referenced = set()
        for path in (BASE_RULES, CRAWLER_RULES):
            referenced |= _referenced_metric_names("\n".join(_rule_expressions(path)))

        orphans = sorted(_declared_metric_names() - referenced - set(UNREFERENCED_METRIC_EXEMPTIONS))
        assert not orphans, (
            f"metrics are exported but nothing reads them: {orphans}. "
            "Either add a rule or record an exemption with a reason."
        )

    def test_every_exemption_still_refers_to_a_real_metric(self):
        declared = _declared_metric_names()

        stale = sorted(set(UNREFERENCED_METRIC_EXEMPTIONS) - declared)
        assert not stale, f"exemptions name metrics that no longer exist: {stale}"

    def test_every_exemption_carries_a_reason(self):
        undocumented = sorted(name for name, reason in UNREFERENCED_METRIC_EXEMPTIONS.items() if not reason.strip())
        assert not undocumented, f"exemptions without a reason: {undocumented}"


class TestCrawlerRulesContent:
    def test_the_three_operational_rules_exist(self):
        document = yaml.safe_load(CRAWLER_RULES.read_text(encoding="utf-8"))
        names = {
            rule["alert"] for group in document.get("groups", []) for rule in group.get("rules", []) if "alert" in rule
        }

        assert names == {
            "KboCrawlerFailureBurst",
            "KboCrawlerNoRecentSuccess",
            "KboCrawlerWriteDrop",
        }

    def test_write_drop_does_not_exclude_a_zero_write_count(self):
        """The scenario is "was writing, now writes nothing", so a `> 0` guard
        on the current value would discard exactly the case the rule exists for.
        """
        write_drop = next(e for e in _rule_expressions(CRAWLER_RULES) if "records_written_last" in e)

        assert "kbo_crawl_records_written_last > 0" not in write_drop
        assert "kbo_crawl_records_written_last\n              > 0" not in write_drop
        assert "avg_over_time(kbo_crawl_records_written_last[7d]) > 0" in write_drop.replace("\n", " ").replace(
            "  ", " "
        )

    def test_freshness_rule_aggregates_by_crawler(self):
        """Without `sum by (crawler)` the alert inherits `status`, so one crawler
        raises the same alert under two label sets and grouping breaks.
        """
        freshness = next(e for e in _rule_expressions(CRAWLER_RULES) if "last_success_timestamp" in e)

        assert "sum by (crawler)" in freshness.replace("\n", " ")

    def test_every_rule_declares_severity_and_annotations(self):
        document = yaml.safe_load(CRAWLER_RULES.read_text(encoding="utf-8"))

        for group in document.get("groups", []):
            for rule in group.get("rules", []):
                assert rule.get("labels", {}).get("severity"), rule["alert"]
                assert "summary" in rule.get("annotations", {}), rule["alert"]
                assert "description" in rule.get("annotations", {}), rule["alert"]


class TestRulesAreWired:
    def test_prometheus_loads_the_crawler_rules(self):
        config = yaml.safe_load(PROM_CONFIG.read_text(encoding="utf-8"))
        rule_files = config.get("rule_files", [])

        assert any(path.endswith("alert_rules_crawler.yml") for path in rule_files), rule_files

    @pytest.mark.parametrize("compose_file", COMPOSE_FILES)
    def test_compose_mounts_the_crawler_rules(self, compose_file: str):
        """A rule file that is not mounted is a rule file that never loads."""
        text = (ROOT / compose_file).read_text(encoding="utf-8")

        assert "alert_rules_crawler.yml:/etc/prometheus/alert_rules_crawler.yml:ro" in text, compose_file

    def test_rule_tests_reference_the_crawler_rules(self):
        for fixture in sorted(TESTS_DIR.glob("crawler_alert_*_test.yml")):
            document = yaml.safe_load(fixture.read_text(encoding="utf-8"))
            assert any(str(path).endswith("alert_rules_crawler.yml") for path in document["rule_files"]), fixture


class TestPromtoolValidatesThem:
    @pytest.mark.skipif(PROMTOOL is None, reason="promtool is not installed")
    def test_crawler_rules_parse(self):
        result = subprocess.run(
            [PROMTOOL, "check", "rules", str(CRAWLER_RULES)],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stdout + result.stderr

    @pytest.mark.skipif(PROMTOOL is None, reason="promtool is not installed")
    def test_base_rules_still_parse(self):
        result = subprocess.run(
            [PROMTOOL, "check", "rules", str(BASE_RULES)],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stdout + result.stderr

    @pytest.mark.skipif(PROMTOOL is None, reason="promtool is not installed")
    def test_every_rule_fixture_actually_fires(self):
        """`check rules` cannot tell you that a rule never fires."""
        fixtures = sorted(TESTS_DIR.glob("crawler_alert_*_test.yml"))
        assert fixtures, "no rule fixtures found; the firing behaviour would go unverified"

        for fixture in fixtures:
            result = subprocess.run(
                [PROMTOOL, "test", "rules", str(fixture)],
                cwd=fixture.parent,
                capture_output=True,
                text=True,
                check=False,
            )
            assert result.returncode == 0, f"{fixture.name}:\n{result.stdout}{result.stderr}"

    @pytest.mark.skipif(PROMTOOL is None, reason="promtool is not installed")
    def test_rule_fixtures_cover_firing_and_quiet_for_each_rule(self):
        for fixture in sorted(TESTS_DIR.glob("crawler_alert_*_test.yml")):
            document = yaml.safe_load(fixture.read_text(encoding="utf-8"))
            cases = [case for test in document["tests"] for case in test.get("alert_rule_test", [])]

            firing = sum(1 for case in cases if case.get("exp_alerts"))
            assert firing, f"{fixture.name} has no case where the alert fires"
            assert firing < len(cases), f"{fixture.name} has no case where the alert must stay quiet"
