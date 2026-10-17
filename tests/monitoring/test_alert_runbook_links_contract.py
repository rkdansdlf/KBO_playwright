"""Every ``runbook:`` annotation must land on a document that names its rule.

An alert's ``runbook`` annotation is a promise: whoever receives this page can
go read what to do about it. Nothing checked that the promise held. Both halves
were unverified -- the path did not have to exist, and the file it named did not
have to say anything about the rule that sent the reader there.

That gap is not hypothetical. ``KBOSchedulerDown`` and
``KBOSchedulerJobNotRunning`` shipped pointing at
``Docs/runbooks/DATA_RELIABILITY.md`` while the document did not mention either
one: a link that reads as documented, passes ``promtool check rules``, passes
every other contract in this directory, and lands the operator in the section
about DLQ retry policy (found 2026-10-18).

So the check is the stronger of the two possible ones -- the annotation must
resolve **and** the target document must name the alert. A file-existence check
alone would have passed on exactly the case that was wrong.

What this deliberately does not cover: rules that carry no ``runbook``
annotation. Absence is a separate contract (which rules are expected to have
one, and where a rule that never pages should send a reader left without one),
and mixing the two would make this file fail for a reason that is not about
links. When a rule declares a runbook, it has to work.

It also cannot see the *quality* of what the document says -- a single mention
satisfies it. That is the boundary of what a machine can check here; the failure
this file was written for is silence, not a thin section.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
PROM_DIR = ROOT / "monitoring" / "prometheus"

#: Every rule file the deployment loads, as literals rather than a glob: a
#: renamed file must fail here rather than quietly reduce the scan to nothing.
#: Same reasoning as `test_alert_rule_fixtures.py`, which lists them for the
#: same reason.
RULE_FILES = ("alert_rules.yml", "alert_rules_crawler.yml", "alert_rules_notifications.yml")


def _declared_links() -> list[tuple[str, str, str]]:
    """Return ``(rule_file, alert, runbook)`` for every declared link, sorted.

    Sorted so the parametrisation order does not depend on file iteration and a
    re-run reports the same ids.
    """
    links = []
    for name in RULE_FILES:
        document = yaml.safe_load((PROM_DIR / name).read_text(encoding="utf-8"))
        for group in document.get("groups") or ():
            for rule in group.get("rules") or ():
                alert = rule.get("alert")
                runbook = (rule.get("annotations") or {}).get("runbook")
                if alert and runbook:
                    links.append((name, str(alert), str(runbook)))
    return sorted(links)


LINKS = _declared_links()
IDS = [f"{name}:{alert}" for name, alert, _ in LINKS]


class TestTheScanStillSeesSomething:
    def test_every_rule_file_is_present(self):
        """A renamed rule file would empty the scan and pass every check below."""
        for name in RULE_FILES:
            assert (PROM_DIR / name).is_file(), f"rule file is missing: {name}"

    def test_links_were_found(self):
        """A scan that finds no links proves nothing about any link.

        No lower bound beyond "not empty" on purpose: the count is a property of
        the rules, and which rules exist is pinned by the group contracts. This
        file's subject is the links that *are* declared, so deleting a rule
        should not have to fail here as well.
        """
        assert LINKS, "no runbook annotations were found at all -- the scan is broken, not clean"


class TestEveryDeclaredLinkResolves:
    @pytest.mark.parametrize(("rule_file", "alert", "runbook"), LINKS, ids=IDS)
    def test_the_target_document_exists(self, rule_file: str, alert: str, runbook: str):
        assert (ROOT / runbook).is_file(), f"{rule_file}: {alert} points at a missing document: {runbook}"

    @pytest.mark.parametrize(("rule_file", "alert", "runbook"), LINKS, ids=IDS)
    def test_the_target_document_names_the_rule(self, rule_file: str, alert: str, runbook: str):
        """The half a file-existence check cannot see.

        An operator sent to a document that never names the alert has no way to
        tell "the runbook has nothing on this" from "I searched the wrong
        file", and the annotation keeps claiming otherwise.
        """
        text = (ROOT / runbook).read_text(encoding="utf-8")
        assert alert in text, (
            f"{rule_file}: {alert} names {runbook} as its runbook, but that document "
            f"never mentions {alert}. Either document the rule there or drop the annotation."
        )
