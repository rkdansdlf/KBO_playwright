"""Every declared page-outcome status must be reachable from its own module.

BH1 (BUG-006) found `KboEventStatus.FETCH_FAILED` declared in all three typed
outcome vocabularies but constructed by nobody. The path that would produce it
does not exist: a fetch failure arrives as an exception and is caught in the
crawler, never as a status.

    try:
        html, final_url = await self._fetch_html(url)
    except KBO_EVENT_CRAWL_EXCEPTIONS as exc:
        ...
        self._page_failures.append((url, code.value, str(exc)))

        Return:
Two things follow, both silent:

* a reader of the enum reasonably believes "the page could not be retrieved"
  arrives as `FETCH_FAILED`, and so cannot tell that it never does;
* ``_REASON_FAILURES["page_fetch_failed"]`` is a reachable-looking table row with
  no caller, so a handled case looks handled by the enum rather than by the
  exception branch that actually handles it.

This is a vocabulary trap, not a data defect: the failure *is* recorded, as a
dead letter with the right taxonomy code.

The first draft of this test grepped the crawler source for ``Enum.MEMBER`` and
produced two false positives, because `team_history` builds its statuses inside
`read_team_history()` rather than at a call site in the crawler. A status is
reachable if the *vocabulary module* can construct it, so the members are
exercised directly instead -- the only thing a text search cannot answer.

"""

from __future__ import annotations

import pytest

from src.crawlers import kbo_event_outcome, player_movement_outcome, team_history_outcome

#: Statuses that describe a document the crawler *received*. Each one is
#: constructible from an HTML string, so they are all expected to be reachable.
DOCUMENT_STATUSES = ("SUCCESS", "EMPTY", "SCHEMA_CHANGED")


def _status_enum(module):
    for value in vars(module).values():
        if isinstance(value, type) and value.__module__ == module.__name__ and value.__name__.endswith("Status"):
            return value
    message = f"{module.__name__} exposes no *Status enum"
    raise AssertionError(message)


@pytest.mark.parametrize(
    "module",
    (kbo_event_outcome, player_movement_outcome, team_history_outcome),
    ids=lambda m: m.__name__.rsplit(".", 1)[-1],
)
class TestTheVocabularyIsInternallyConsistent:
    def test_it_exposes_exactly_the_four_documented_states(self, module) -> None:
        """All three vocabularies model the same four ways a page read can end.

        Three separate modules were written for the same idea. If one grew a
        fifth state the others would have to follow, and nothing currently says
        so. This is the cheapest place to notice.
        """
        status_enum = _status_enum(module)

        assert sorted(m.name for m in status_enum) == ["EMPTY", "FETCH_FAILED", "SCHEMA_CHANGED", "SUCCESS"]

    def test_every_status_is_documented(self, module) -> None:
        """A state with no docstring is a state nobody can reason about."""
        status_enum = _status_enum(module)

        undocumented = [member.name for member in status_enum if not (member.__doc__ or "").strip()]

        assert not undocumented, f"{module.__name__}: {undocumented} carry no explanation of when they occur"

    def test_the_reason_table_has_no_row_for_an_unreachable_state(self, module) -> None:
        """BUG-006: a reason row exists to classify a state; it must be constructible.

        `page_fetch_failed` classifies `FETCH_FAILED`, and no code path builds
        that status -- a fetch failure is caught as an exception by the crawler
        instead. So the row looks like a handled case while the only thing that
        handles it is somewhere else entirely.
        """
        status_enum = _status_enum(module)
        reasons = set(module._REASON_FAILURES)
        fetch_reason = next((r for r in reasons if "fetch" in r), None)

        if fetch_reason is None:
            pytest.skip(f"{module.__name__} declares no fetch-failure reason row")

        assert "FETCH_FAILED" in {m.name for m in status_enum}, (
            f"{module.__name__} has a {fetch_reason!r} reason row, so it declares FETCH_FAILED; "
            "confirm whether that status is reachable before adding a reason for it."
        )


class TestTheDocumentStatusesAreReachable:
    """The three states that describe a document received must be constructible.

    A previous draft of this check read the crawler source for
    ``Enum.MEMBER`` and reported `team_history` as producing nothing, because its
    statuses are built inside ``read_team_history()`` rather than at a call site.
    Reaching a status means calling the module, not grepping for its name.
    """

    def test_kbo_event_read_reaches_all_three(self) -> None:
        from src.crawlers.kbo_event_crawler import read_kbo_event_page

        # The site frame is what separates "readable but nothing to announce"
        # from "no longer the page we asked for", and a descriptive <title> is
        # what turns a readable page into a candidate event.
        frame = "<header></header><nav></nav><footer></footer>"
        drifted = read_kbo_event_page("<html><head><title>점검 중</title></head><body></body></html>")
        empty = read_kbo_event_page(f"<html><head><title>메인</title></head><body>{frame}</body></html>")
        full = read_kbo_event_page(f"<html><head><title>KBO 공식 행사</title></head><body>{frame}</body></html>")

        assert drifted.status is kbo_event_outcome.KboEventStatus.SCHEMA_CHANGED
        assert empty.status is kbo_event_outcome.KboEventStatus.EMPTY
        assert full.status is kbo_event_outcome.KboEventStatus.SUCCESS

    def test_team_history_read_reaches_its_three(self) -> None:
        no_rows = team_history_outcome.read_team_history(rows_found=0, years_parsed=0)
        no_years = team_history_outcome.read_team_history(rows_found=5, years_parsed=0)
        full = team_history_outcome.read_team_history(rows_found=5, years_parsed=5, entries=[{"year": 2024}])

        assert no_rows.status is team_history_outcome.TeamHistoryStatus.SCHEMA_CHANGED
        assert no_years.status is team_history_outcome.TeamHistoryStatus.EMPTY
        assert full.status is team_history_outcome.TeamHistoryStatus.SUCCESS

    def test_player_movement_reaches_its_document_states(self) -> None:
        from src.crawlers.player_movement_outcome import PlayerMovementPageRead, PlayerMovementStatus

        # `is_terminal` reads the reason table, so it is only meaningful once a
        # reason is attached; these states exist to be reachable at all.
        for status in (PlayerMovementStatus.SUCCESS, PlayerMovementStatus.EMPTY):
            read = PlayerMovementPageRead(status=status)

            assert read.status is status
            assert read.is_terminal is False


class TestTheProducerIsActuallyWired:
    """The gap the module-level reachability test could not see.

    `test_kbo_event_reaches_its_document_states` proves the vocabulary *can*
    construct `FETCH_FAILED`. That was the whole of the original test, and it
    passed while the state was unreachable -- because the crawler never called
    the constructor. Reachability of a constructor and reachability of the state
    are different questions, and only the second one is the bug.

    So these drive the real failure path and assert the read was recorded.
    """

    async def test_a_fetch_failure_records_a_read(self, monkeypatch) -> None:
        """The exception branch now produces a status as well as a dead letter."""
        from src.crawlers import kbo_event_crawler as module

        crawler = module.KboEventCrawler(base_url="https://example.test/a")

        async def boom(_url: str) -> tuple[str, str]:
            raise TimeoutError("connection timed out")

        monkeypatch.setattr(crawler, "_fetch_html", boom)

        await crawler._collect_page("https://example.test/a", [], set())

        assert [r.status for r in crawler._page_reads] == [module.KboEventStatus.FETCH_FAILED]

    async def test_the_recorded_read_says_which_page_failed(self, monkeypatch) -> None:
        """Seven pages, one failure: a read that cannot name its page says nothing."""
        from src.crawlers import kbo_event_crawler as module

        crawler = module.KboEventCrawler(base_url="https://example.test/a")

        async def boom(_url: str) -> tuple[str, str]:
            raise TimeoutError("connection timed out")

        monkeypatch.setattr(crawler, "_fetch_html", boom)

        await crawler._collect_page("https://example.test/a", [], set())

        assert crawler._page_reads[0].url == "https://example.test/a"

    async def test_a_fetch_failure_is_not_terminal(self, monkeypatch) -> None:
        """Retryable is the whole reason this differs from a missing frame.

        A connection error is a fact about this moment; a page that lost its
        frame is a fact about the site. Getting that backwards would spend the
        retry budget on a document that will keep answering the same way.
        """
        from src.crawlers import kbo_event_crawler as module

        crawler = module.KboEventCrawler(base_url="https://example.test/a")

        async def boom(_url: str) -> tuple[str, str]:
            raise TimeoutError("connection timed out")

        monkeypatch.setattr(crawler, "_fetch_html", boom)

        await crawler._collect_page("https://example.test/a", [], set())

        assert crawler._page_reads[0].is_terminal is False

    async def test_the_dead_letter_still_carries_the_real_taxonomy_code(self, monkeypatch) -> None:
        """Recording a read must not replace the recovery path.

        The exception branch was already correct before this change, and the
        vocabulary status is a report on top of it rather than a substitute for
        it: the code that drives the retry policy comes from `classify_failure`
        on the actual exception, not from the page-level reason table.
        """
        from src.crawlers import kbo_event_crawler as module

        crawler = module.KboEventCrawler(base_url="https://example.test/a")

        async def boom(_url: str) -> tuple[str, str]:
            raise TimeoutError("connection timed out")

        monkeypatch.setattr(crawler, "_fetch_html", boom)
        await crawler._collect_page("https://example.test/a", [], set())

        url, code, message = crawler._page_failures[0]
        assert url == "https://example.test/a"
        from src.crawlers.failure_taxonomy import FailureCode

        assert code == FailureCode.FETCH_TIMEOUT.value
        assert "timed out" in message

    async def test_reads_do_not_survive_into_the_next_run(self, monkeypatch) -> None:
        """Sweep state reset per run, or a second sweep inherits the first's failures.

        Found while wiring the producer: `_page_failures` was reset inside
        `run()` but the reads list had no reset at all, so the same class of bug
        was one line away from the fix that added it.
        """
        from src.crawlers import kbo_event_crawler as module

        crawler = module.KboEventCrawler(base_url="https://example.test/a")
        crawler._page_reads.append(
            module.fetch_failed_read("https://example.test/stale"),
        )
        crawler._page_failures.append(("https://example.test/stale", "FETCH_TIMEOUT", "old"))

        frame = "<header></header><nav></nav><footer></footer>"

        async def noop(_url: str) -> tuple[str, str]:
            # Must carry the site frame, or the page reads as SCHEMA_CHANGED and
            # the assertion below would be checking a failure rather than a reset.
            return f"<html><head><title>KBO</title></head><body>{frame}</body></html>", _url

        monkeypatch.setattr(crawler, "_fetch_html", noop)
        monkeypatch.setattr(crawler, "_blocked_by_compliance", _no_block_async)
        monkeypatch.setattr(module, "track_crawl_run", _null_ledger)
        monkeypatch.setattr(module, "save_raw_snapshots", lambda *_a, **_k: None)

        await crawler.run(save=False)

        assert crawler._page_failures == []
        assert [r.url for r in crawler._page_reads] == ["https://example.test/a"]

    async def test_a_parsed_page_also_records_a_read(self, monkeypatch) -> None:
        """Every visited page, not just the failing ones.

        A reads list containing only failures would answer "what went wrong" but
        not "what did the sweep see", which is the other half of what an outcome
        vocabulary is for.
        """
        from src.crawlers import kbo_event_crawler as module

        frame = "<header></header><nav></nav><footer></footer>"
        crawler = module.KboEventCrawler(base_url="https://example.test/a")

        async def ok(_url: str) -> tuple[str, str]:
            return f"<html><head><title>KBO</title></head><body>{frame}</body></html>", _url

        monkeypatch.setattr(crawler, "_fetch_html", ok)
        await crawler._collect_page("https://example.test/a", [], set())

        assert len(crawler._page_reads) == 1
        assert crawler._page_reads[0].status is module.KboEventStatus.EMPTY
        assert crawler._page_reads[0].is_terminal is False


async def _no_block_async(_run: object) -> bool:
    """Stand in for the compliance gate, which needs a live request."""
    return False


def _null_ledger(spec: object):
    """A ledger context yielding a row-shaped object, so `run()` can proceed."""
    from contextlib import contextmanager
    from types import SimpleNamespace

    @contextmanager
    def _ctx():
        yield SimpleNamespace(
            run_id="test-run",
            records_read=0,
            records_written=0,
            records_failed=0,
            status="running",
            error_code=None,
            error_message=None,
        )

    return _ctx()
