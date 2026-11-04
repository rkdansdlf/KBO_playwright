"""Pin what the nightly RAG sync runs: PBP separately, with its presence filters."""

from __future__ import annotations


def test_pbp_runs_alone_with_the_identity_content_and_vector_filters() -> None:
    """Without these filters every stored PBP play would look missing and be embedded again."""
    from src.scheduler.jobs.maintenance import rag_incremental_invocations

    invocations = rag_incremental_invocations(2026)
    pbp = [argv for argv in invocations if "pbp" in argv]

    assert len(pbp) == 1
    assert pbp[0] == [
        "--source",
        "pbp",
        "--season",
        "2026",
        "--skip-existing",
        "--vector-presence",
        "--content-presence",
        "--play-presence",
    ]


def test_mapped_sources_get_the_presence_filters_and_unmapped_ones_keep_the_old_call() -> None:
    """Sources that map to a table skip identities already stored by content; the rest keep the old call."""
    from src.scheduler.jobs.maintenance import rag_incremental_invocations

    others = [argv for argv in rag_incremental_invocations(2026) if "pbp" not in argv]
    filtered = [argv for argv in others if "--content-presence" in argv]
    plain = [argv for argv in others if "--content-presence" not in argv]

    assert filtered
    assert all("--vector-presence" in argv and argv[-1] == "--content-presence" for argv in filtered)
    assert all(argv[-1] == "--skip-existing" for argv in plain)
    assert not any("all" in argv for argv in rag_incremental_invocations(2026))


def test_staging_source_is_never_part_of_the_nightly_run() -> None:
    """The staging source loads the whole canonical table into memory and aborted the sync mid-run."""
    from src.scheduler.jobs.maintenance import rag_incremental_invocations

    sources = [argv[argv.index("--source") + 1] for argv in rag_incremental_invocations(2026)]

    assert "staging_rag_chunks" not in sources


def test_pbp_is_the_last_invocation() -> None:
    """PBP is the slowest source, so a failure elsewhere must never be what leaves it unrun."""
    from src.scheduler.jobs.maintenance import rag_incremental_invocations

    invocations = rag_incremental_invocations(2026)

    assert invocations[-1][:2] == ["--source", "pbp"]


def test_sources_whose_past_is_still_corrected_run_without_a_season() -> None:
    """A season filter would re-read only this year and leave every earlier one alone.

    `awards` and `team_history` each describe the whole history in one cursor, and a
    1982 row can still be fixed today -- measured 2026-11-04, the crawler's `colspan`
    bug had dropped ~80 award rows across 1982-2021 while the nightly job reported
    success, because `--season 2026` only ever looked at 2026.
    """
    from src.scheduler.jobs.maintenance import rag_incremental_invocations

    by_source = {argv[argv.index("--source") + 1]: argv for argv in rag_incremental_invocations(2026)}

    assert "--season" not in by_source["awards"]
    assert "--season" not in by_source["team_history"]


def test_the_season_filter_stays_on_the_sources_that_need_it() -> None:
    """Games, plays and season aggregates are read a year at a time, and cheaply so."""
    from src.scheduler.jobs.maintenance import rag_incremental_invocations

    by_source = {argv[argv.index("--source") + 1]: argv for argv in rag_incremental_invocations(2026)}

    for source in ("games", "pbp", "lineups", "batting", "pitching", "standings"):
        argv = by_source[source]
        assert "--season" in argv, source
        assert argv[argv.index("--season") + 1] == "2026", source


def test_the_seasonless_sources_keep_their_presence_filters_and_skip_flag() -> None:
    """Dropping `--season` must not drop the filters that make the run incremental."""
    from src.scheduler.jobs.maintenance import rag_incremental_invocations

    by_source = {argv[argv.index("--source") + 1]: argv for argv in rag_incremental_invocations(2026)}

    for source in ("awards", "team_history"):
        argv = by_source[source]
        assert "--skip-existing" in argv
        assert "--vector-presence" in argv
        assert "--content-presence" in argv
