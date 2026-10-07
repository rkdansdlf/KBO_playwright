"""Contract between the alert rules and every metric the codebase exports.

`promtool check rules` proves the expressions parse. It cannot prove the metric
names still exist, that a renamed metric left a rule permanently silent, or that
the rules file is actually loaded and mounted. Those are the failures worth
catching, because each one produces a green pipeline and a blind alert.

This file covers the series that no other contract owns. Once the scan was
limited to the ``kbo_crawl`` prefix, which left a structural gap: the scheduler
contract delegates metric-existence checks here ("`test_crawler_alert_rules_contract.py`
already spans `BASE_RULES`"), while these tests filtered on a prefix that
excludes exactly the series that delegation was about. `kbo_scheduler_*`,
`kbo_api_cache_*` and `kbo_auto_healer_*` fell between the two contracts and
were checked by neither -- the same shape as BUG-002, where the DLQ series fell
between two prefix filters.

The prefix filter is therefore gone. Ownership is now explicit instead: a series
is either referenced by a rule file, or exempted here with a reason, or owned by
the notification contract, which is named so a new notification metric cannot
fall between the two scans.
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

#: Every rule file a reference may come from. A series read by any of them is
#: read; which file the rule lives in is not this contract's concern.
RULE_FILES = (
    BASE_RULES,
    CRAWLER_RULES,
    PROM_DIR / "alert_rules_notifications.yml",
)

#: Prefixes owned by another contract, mapped to the test that must cover them.
#: Excluding them here is a hand-off, not an exemption: if that file's own scan
#: stops covering its prefix, its tests fail, and this entry stays honest.
OWNED_ELSEWHERE = {
    "kbo_notification_": "tests/monitoring/test_notification_alert_rules_contract.py",
}

#: Series a custom collector yields at scrape time rather than a module-level
#: instrument. `prometheus_client` keeps their state in the collector, so a scan
#: of module attributes cannot see them, and they are listed here instead.
#: `test_the_collector_series_are_still_yielded` checks the listing against the
#: source, because an entry naming a series nobody yields would mask a rename.
COLLECTOR_SERIES = frozenset({"kbo_db_available", "kbo_db_ping_latency_seconds"})
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
    # --- Dead letter queue (BUG-002) ------------------------------------
    # Surfaced when the metric scan was widened to `src.utils.metrics`, which is
    # where these live. The two conditions worth paging on are now alerted
    # (KboDlqRecoveryStalled, KboDlqBacklogAgeHigh); the rest are diagnostic.
    # No threshold is recorded for these because no production sample was
    # available to calibrate one -- see BUG-002 in
    # Docs/certification/bug-hunt/BH0_CONTRACTS.md.
    "kbo_crawl_dlq_due_letters": (
        "diagnostic; the backlog-age alert reads oldest_due_age_seconds, which measures the same "
        "condition without depending on queue depth"
    ),
    "kbo_crawl_dlq_letters": "diagnostic breakdown by status/crawler for `kbo dlq stats` output",
    "kbo_crawl_dlq_failures_total": "diagnostic; crawler failures are alerted through kbo_crawl_failures_total",
    "kbo_crawl_dlq_retry_attempts_total": "diagnostic; attempt volume without an outcome says nothing on its own",
    "kbo_crawl_dlq_retry_outcomes_total": (
        "diagnostic; the outcome mix (resolved vs exhausted) is what an exhausted backlog would show, "
        'and that is visible through kbo_crawl_dlq_letters{status="exhausted"}'
    ),
    "kbo_crawl_dlq_recovery_actions_total": (
        "diagnostic; recovery actions only run when something is already stuck, which KboDlqRecoveryStalled alerts"
    ),
    # --- Series outside the crawler namespace -----------------------------
    # Surfaced when the prefix filter was removed. Each of these was exported
    # and read by nothing, and no test could have noticed: the crawler contract
    # filtered on `kbo_crawl`, the notification contract on `kbo_notification`,
    # and the scheduler contract delegates metric existence to this file.
    "kbo_scheduler_job_duration_seconds": (
        "no single threshold fits: normal durations differ by orders of magnitude between jobs "
        "(crawl_daily_games runs for hours, most finish in seconds), and the per-job ceiling is "
        "already enforced by the registry's timeout and misfire_grace_time. Exposed for dashboards"
    ),
    "kbo_api_cache_requests_total": (
        "a hit/miss ratio is a performance signal, not a failure: a miss means the endpoint "
        "computed the answer instead of reusing one, which is the correct behaviour when the cache "
        "is cold. Paging on it would report a slow path as an outage"
    ),
    "kbo_auto_healer_recovered_total": "diagnostic progress counter; recovered games are not actionable",
    "kbo_auto_healer_unresolved_total": (
        "already reported through the incident ledger: auto_healer calls apply_incidents with the "
        "`auto_healer:unresolved` key and resolves it when the count is zero. Unlike the DLQ series "
        "in BUG-002, which had no incident at all, a rule here would page a second time for one "
        "condition -- see BH0_CONTRACTS.md BUG-001 section"
    ),
}


def _exposed_name(collector: object) -> str | None:
    """Return the series name PromQL sees for a collector.

    `prometheus_client` strips the `_total` suffix from a Counter's internal
    name and re-adds it on export, so reading `_name` directly reports
    `kbo_crawl_runs` where the rule and the scrape endpoint both say
    `kbo_crawl_runs_total`.

    ``None`` is returned for a series this contract does not own: one under a
    prefix handed to another contract, or a counter whose name is read before
    the suffix is restored.
    """
    name = getattr(collector, "_name", None)
    if not isinstance(name, str) or not name.startswith("kbo_"):
        return None
    if any(name.startswith(prefix) for prefix in OWNED_ELSEWHERE):
        return None
    if getattr(collector, "_type", None) == "counter":
        return f"{name}_total"
    return name


def _declared_metric_names() -> set[str]:
    """Return every `kbo_*` series name the codebase exports to PromQL.

    Two modules declare these, not one. ``src.monitoring.crawler_metrics`` owns
    the per-run series; ``src.utils.metrics`` owns the DLQ gauges, the scheduler
    and auto-healer counters, and the API cache series.

    Scanning only the first is what let the DLQ series sit unreferenced with a
    green CI -- a rule referencing them failed to resolve, and a metric with no
    rule was never looked for (BUG-002). Both modules are now read here, and the
    scan no longer filters on one prefix: a series that belongs to neither the
    crawler nor the notification namespace used to fall between the two
    contracts, which is the failure this widening closes.
    """
    from src.monitoring import crawler_metrics
    from src.utils import metrics

    names: set[str] = set(COLLECTOR_SERIES)
    for module in (crawler_metrics, metrics):
        for value in vars(module).values():
            exposed = _exposed_name(value)
            if exposed:
                names.add(exposed)
    return names


def _referenced_metric_names(text: str) -> set[str]:
    """Return `kbo_*` names referenced by PromQL in the given text.

    Names under a prefix owned by another contract are dropped, for the same
    reason ``_exposed_name`` drops them: this scan does not own them, and
    counting a reference here while declining to declare the series would make
    every notification rule look like it referenced a missing metric.
    """
    names = set(re.findall(r"\bkbo_[a-z0-9_]+", text))
    return {name for name in names if not any(name.startswith(prefix) for prefix in OWNED_ELSEWHERE)}


def _rule_expressions(path: Path) -> list[str]:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    return [rule["expr"] for group in document.get("groups", []) for rule in group.get("rules", []) if "expr" in rule]


def _all_referenced_names() -> set[str]:
    """Return every `kbo_*` name any rule file references."""
    referenced: set[str] = set()
    for path in RULE_FILES:
        referenced |= _referenced_metric_names("\n".join(_rule_expressions(path)))
    return referenced


class TestRulesReferenceRealMetrics:
    def test_every_referenced_metric_is_declared_in_code(self):
        missing = sorted(_all_referenced_names() - _declared_metric_names())

        assert not missing, (
            f"alert rules reference metrics that no longer exist: {missing}. "
            "A renamed or deleted metric would leave the rule permanently silent."
        )

    def test_declared_metrics_are_referenced_or_explicitly_exempted(self):
        orphans = sorted(_declared_metric_names() - _all_referenced_names() - set(UNREFERENCED_METRIC_EXEMPTIONS))

        assert not orphans, (
            f"metrics are exported but nothing reads them: {orphans}. "
            "Either add a rule or record an exemption with a reason."
        )

    def test_the_collector_series_are_still_yielded(self):
        """The listing has to match the source, or it hides the rename it exists for.

        A custom collector's series live in the collector rather than in module
        attributes, so they cannot be discovered by scanning and are named above
        instead. That makes the list a claim: if the collector stops yielding one
        of these names, the scan would keep treating the old name as covered.
        """
        source = Path("src/monitoring/db_availability.py").read_text(encoding="utf-8")

        for series in COLLECTOR_SERIES:
            assert f'"{series}"' in source, f"{series} is listed as collected but no longer yielded"

    def test_the_handed_off_prefix_is_really_covered_elsewhere(self):
        """A hand-off is only honest while the other contract still scans it.

        Excluding a prefix here means those series are covered somewhere else.
        If the notification contract narrows its own filter, both scans would
        stop looking and nothing would fail -- which is the exact shape of
        BUG-002, and the reason this check exists rather than a comment.
        """
        for prefix, covering_test in OWNED_ELSEWHERE.items():
            text = (ROOT / covering_test).read_text(encoding="utf-8")

            assert f'"{prefix}"' in text or prefix.rstrip("_") in text, (
                f"{prefix} is handed to {covering_test}, but that file no longer names the prefix; "
                "either it stopped covering these series or the hand-off moved"
            )

    def test_every_exemption_still_refers_to_a_real_metric(self):
        declared = _declared_metric_names()

        stale = sorted(set(UNREFERENCED_METRIC_EXEMPTIONS) - declared)
        assert not stale, f"exemptions name metrics that no longer exist: {stale}"

    def test_every_exemption_carries_a_reason(self):
        undocumented = sorted(name for name, reason in UNREFERENCED_METRIC_EXEMPTIONS.items() if not reason.strip())
        assert not undocumented, f"exemptions without a reason: {undocumented}"


class TestCrawlerRulesContent:
    def test_the_operational_rules_exist(self):
        document = yaml.safe_load(CRAWLER_RULES.read_text(encoding="utf-8"))
        names = {
            rule["alert"] for group in document.get("groups", []) for rule in group.get("rules", []) if "alert" in rule
        }

        assert names == {
            "KboCrawlerFailureBurst",
            "KboCrawlerNoRecentSuccess",
            "KboCrawlerWriteDrop",
            # BUG-001: partial advanced last_success and usually kept writes
            # normal, so neither the freshness nor the write-drop rule could see
            # a crawler that only ever partially succeeded. This rule reads the
            # outcome mix directly.
            "KboCrawlerSustainedPartial",
            "KboCrawlLedgerFailure",
            "KboCrawlLedgerFailureSustained",
            # BUG-002: the DLQ series existed and nothing read them. These two
            # cover the two conditions with no agreed threshold debate --
            # recovery stalled, and the drain not keeping up.
            "KboDlqRecoveryStalled",
            "KboDlqBacklogAgeHigh",
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
