"""The cadence table has to match the scheduler and the ledger.

`KboCrawlerNoRecentSuccess` thresholds on
`kbo_crawl_expected_interval_seconds`, and that gauge is only published when
`src.monitoring.crawler_cadence.CRAWLER_JOBS` names a job that is actually
registered. A declaration that drifts from the registry is therefore silent in
the worst way: the rule stops judging a crawler and nothing fails.

Two directions are checked here, because both have failed in this repository
before:

* completeness -- every crawler that can write a run-ledger row is either
  scheduled or exempted with a reason. A new crawler added without either would
  simply not be judged;
* existence -- every declared name and job id is real. A typo in `CRAWLER_JOBS`
  would publish nothing, and the rule would skip a working crawler.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

import pytest
from apscheduler.triggers.cron import CronTrigger
from prometheus_client import REGISTRY

from src.monitoring.crawler_cadence import (
    CRAWLER_JOBS,
    NOT_SCHEDULED,
    declared_crawlers,
    publish_crawler_cadence,
)
from src.scheduler.cadence import expected_interval_seconds
from src.scheduler.registry import job_specs

ROOT = Path(__file__).resolve().parents[2]
REGISTRY_SOURCE = ROOT / "src" / "scheduler" / "registry.py"
CADENCE_SOURCE = ROOT / "src" / "monitoring" / "crawler_cadence.py"


@dataclass(frozen=True)
class _Job:
    id: str
    trigger: object


class _Scheduler:
    """The only part of a scheduler this module reads: its registered jobs."""

    def __init__(self, jobs: list[_Job]) -> None:
        self._jobs = jobs

    def get_jobs(self) -> list[_Job]:
        return self._jobs


def _spec_triggers() -> dict[str, CronTrigger]:
    return {job_id: trigger for _fn, trigger, job_id, _name, _grace in job_specs(CronTrigger)}


def _module_string_constants(tree: ast.Module) -> dict[str, str]:
    constants: dict[str, str] = {}
    for node in tree.body:
        targets: list[ast.expr] = []
        value: ast.expr | None = None
        if isinstance(node, ast.Assign):
            targets, value = node.targets, node.value
        elif isinstance(node, ast.AnnAssign):
            targets, value = [node.target], node.value
        if value is None or not isinstance(value, ast.Constant) or not isinstance(value.value, str):
            continue
        for target in targets:
            if isinstance(target, ast.Name):
                constants[target.id] = value.value
    return constants


def _call_name(node: ast.expr) -> str | None:
    return getattr(node, "id", None) or getattr(node, "attr", None)


def _ledger_crawler_names() -> set[str]:
    """Return every crawler name a `CrawlRunSpec` can write to the ledger.

    Reading the constructor rather than every ``CRAWLER_NAME`` constant is what
    keeps the claim narrow: a constant that never reaches a run spec cannot
    appear in `kbo_crawl_*` series, and requiring it here would add exemptions
    for crawlers that were never in scope.
    """
    names: set[str] = set()
    for path in sorted((ROOT / "src").rglob("*.py")):
        source = path.read_text(encoding="utf-8")
        if "CrawlRunSpec(" not in source:
            continue
        tree = ast.parse(source)
        constants = _module_string_constants(tree)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or _call_name(node.func) != "CrawlRunSpec":
                continue
            for keyword in node.keywords:
                if keyword.arg != "crawler":
                    continue
                value = keyword.value
                if isinstance(value, ast.Constant) and isinstance(value.value, str):
                    names.add(value.value)
                elif isinstance(value, ast.Name) and value.id in constants:
                    names.add(constants[value.id])
    return names


class TestTheTableCoversTheLedger:
    def test_every_ledger_crawler_is_scheduled_or_exempted(self) -> None:
        unaccounted = sorted(_ledger_crawler_names() - set(declared_crawlers()))

        assert not unaccounted, (
            f"these crawlers write run-ledger rows but have no cadence and no exemption: {unaccounted}. "
            "Add them to CRAWLER_JOBS with the job that starts them, or to NOT_SCHEDULED with the reason "
            "their runs never reach the scheduler's metrics -- otherwise the freshness alert silently "
            "skips them."
        )

    def test_every_declared_name_is_real(self) -> None:
        source = "\n".join(path.read_text(encoding="utf-8") for path in (ROOT / "src").rglob("*.py"))

        for name in declared_crawlers():
            assert f'"{name}"' in source, f"{name} is not a crawler name anywhere in src/"

    def test_a_crawler_is_not_both_scheduled_and_exempt(self) -> None:
        both = sorted(set(CRAWLER_JOBS) & set(NOT_SCHEDULED))

        assert not both, f"declared twice, so the two claims contradict each other: {both}"

    def test_every_exemption_carries_a_reason(self) -> None:
        undocumented = sorted(name for name, reason in NOT_SCHEDULED.items() if not reason.strip())

        assert not undocumented, f"exemptions without a reason: {undocumented}"


class TestTheTableMatchesTheRegistry:
    def test_every_declared_job_is_registered(self) -> None:
        """Jobs live in `job_specs()` and in the bootstrap, and both count.

        `preview`, `team_history` and `pbp` are added directly in
        `_start_scheduler` rather than returned by `job_specs()`, so the check
        accepts a literal job id in the registry module. Reading the source is
        what makes the bootstrap visible without starting a scheduler.
        """
        registered = set(_spec_triggers())
        source = REGISTRY_SOURCE.read_text(encoding="utf-8")

        missing = sorted(
            f"{crawler}->{job_id}"
            for crawler, job_id in CRAWLER_JOBS.items()
            if job_id not in registered and f'"{job_id}"' not in source
        )

        assert not missing, f"cadence names jobs the scheduler never registers: {missing}"

    def test_the_interval_comes_from_the_trigger(self) -> None:
        triggers = _spec_triggers()
        jobs = [_Job(job_id, triggers[job_id]) for job_id in sorted(set(CRAWLER_JOBS.values())) if job_id in triggers]

        published = publish_crawler_cadence(_Scheduler(jobs))

        for crawler, job_id in CRAWLER_JOBS.items():
            if job_id not in triggers:
                continue
            expected = expected_interval_seconds(triggers[job_id])
            assert published[crawler] == expected
            assert REGISTRY.get_sample_value("kbo_crawl_expected_interval_seconds", {"crawler": crawler}) == float(
                expected
            )

    def test_a_job_registered_outside_the_specs_still_resolves(self) -> None:
        """`preview` is the crawler whose six-hour silence started this work.

        Its job is registered inline in `_start_scheduler`, so a table resolved
        only from `job_specs()` could not describe it -- and exempting it would
        leave the pregame preview unjudged.
        """
        trigger = CronTrigger(hour="10-23", minute="*/15")

        published = publish_crawler_cadence(_Scheduler([_Job("crawl_pregame_refresh", trigger)]))

        assert published == {"preview": expected_interval_seconds(trigger)}
        # The gap is the overnight one, not the 15-minute tick: a rule reading the
        # tick would page every night.
        assert published["preview"] > 6 * 3600

    def test_an_unregistered_job_publishes_nothing_and_says_so(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level("WARNING"):
            published = publish_crawler_cadence(_Scheduler([]))

        assert published == {}
        assert any("cadence" in record.message for record in caplog.records)

    def test_publishing_a_cadence_does_not_claim_a_run(self) -> None:
        """Unlike a job's last-run seed, a crawler's cannot be set to "now".

        `_publish_job_cadence_contract` seeds last-run at boot because its
        listener creates the series only when a job is submitted. Doing the same
        for crawlers would assert a collection that never happened and reset the
        staleness the rule is watching, so the publish path must not touch it.
        """
        body = CADENCE_SOURCE.read_text(encoding="utf-8").split("def publish_crawler_cadence", 1)[1]

        assert "CRAWL_LAST_RUN_TIMESTAMP" not in body
        assert "CRAWL_LAST_FULL_SUCCESS_TIMESTAMP" not in body
