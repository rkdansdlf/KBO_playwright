"""Sustained-partial detection: the contract that keeps BUG-001 closed.

BUG-001 (`Docs/certification/bug-hunt/BH0_CONTRACTS.md`): a crawler that recorded
``partial`` instead of ``success`` indefinitely was not detectable. The state was:

    PARTIAL PARTIAL PARTIAL ... (continues)
    AND KboCrawlerNoRecentSuccess = healthy   (partial advances last_success)
    AND KboCrawlerWriteDrop       = healthy   (records_written may be normal)
    AND degradation alert         = none      (no rule read status="partial")

The most dangerous shape is the one ``KboCrawlerWriteDrop`` cannot help with:
"one of three sources quietly fails" leaves ``records_written`` healthy, so the
write gauge never drops.

The fix is the ``KboCrawlerSustainedPartial`` rule in
``monitoring/prometheus/alert_rules_crawler.yml``: partial runs that keep landing
while no complete run does now raise a warning. Firing and quiet behaviour is
pinned by ``monitoring/prometheus/tests/crawler_alert_partial_test.yml``; this
module pins the property the whole fix rests on -- a rule that reads partial
exists and is not itself silent -- so removing the rule fails here instead of
quietly reopening the gap. Do not delete this test; if the detection is
re-scoped, re-scope the test with it.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
PROM_DIR = ROOT / "monitoring" / "prometheus"
CRAWLER_RULES = PROM_DIR / "alert_rules_crawler.yml"
BASE_RULES = PROM_DIR / "alert_rules.yml"
NOTIFICATION_RULES = PROM_DIR / "alert_rules_notifications.yml"

#: Series that would carry a dedicated partial-degradation signal, should one
#: be added. Checked because a series nothing reads is the same silence wearing
#: a different hat.
_PARTIAL_SIGNAL_PREFIXES = (
    "kbo_crawl_partial",
    "kbo_crawl_last_usable",
)


def _rule_expressions() -> list[str]:
    expressions: list[str] = []
    for path in (BASE_RULES, CRAWLER_RULES, NOTIFICATION_RULES):
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        expressions += [
            rule["expr"] for group in document.get("groups", []) for rule in group.get("rules", []) if "expr" in rule
        ]
    return expressions


def _all_expressions_text() -> str:
    return "\n".join(" ".join(expr.split()) for expr in _rule_expressions())


def _reads_partial_status() -> bool:
    return 'status="partial"' in _all_expressions_text()


def _declares_partial_signal() -> bool:
    """Return whether a partial-degradation series exists in the code at all."""
    from src.monitoring import crawler_metrics
    from src.utils import metrics

    for module in (crawler_metrics, metrics):
        for value in vars(module).values():
            name = getattr(value, "_name", None)
            if isinstance(name, str) and name.startswith(_PARTIAL_SIGNAL_PREFIXES):
                return True
    return False


class TestSustainedPartialMustBeDetectable:
    """INV-METRIC-02: repeated PARTIAL must not read as indefinitely healthy."""

    def test_something_distinguishes_a_partial_run_from_a_complete_one(self) -> None:
        reads_status = _reads_partial_status()
        has_signal = _declares_partial_signal()

        assert reads_status or has_signal, (
            'BUG-001: no alert rule reads kbo_crawl_runs_total{status="partial"}, and no '
            "dedicated partial/usable-run series exists. A crawler that keeps reporting "
            "'partial' advances kbo_crawl_last_success_timestamp (so KboCrawlerNoRecentSuccess "
            "stays healthy) and may keep a normal records_written (so KboCrawlerWriteDrop stays "
            "healthy). Sustained partial degradation is therefore undetectable. "
            'Fix by adding a degradation rule on status="partial" -- deliberately NOT by '
            "removing partial from SUCCESS_STATUSES, which would reclassify a working crawler "
            "as an outage."
        )

    def test_the_partial_signal_is_not_itself_silent(self) -> None:
        """A new partial series must come with a reader, or it changes nothing.

        The orphan-detection contract scans ``crawler_metrics`` and (separately,
        for notifications) ``utils.metrics``, each filtered to its own prefix. A
        partial series added outside those filters would satisfy the test above
        while still alerting nobody -- which is the exact failure mode this
        module exists to catch.
        """
        if not _declares_partial_signal():
            pytest.skip("no partial signal exists yet; see TestSustainedPartialMustBeDetectable")

        text = _all_expressions_text()
        assert any(prefix.replace("kbo_crawl_", "kbo_crawl_") in text for prefix in ("kbo_crawl_partial",)) or (
            "last_usable" in text
        ), (
            "a partial/usable series is declared but no rule reads it. Adding a metric without a "
            "reader reproduces BUG-002 (an exported series that nothing observes) in a new place."
        )
