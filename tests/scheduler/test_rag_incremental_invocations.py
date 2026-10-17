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


def test_other_sources_keep_the_previous_skip_existing_run() -> None:
    """Only PBP changes; the other sources keep the run they had before."""
    from src.scheduler.jobs.maintenance import rag_incremental_invocations

    others = [argv for argv in rag_incremental_invocations(2026) if "pbp" not in argv]

    assert others
    assert all(argv[-1] == "--skip-existing" and "--season" in argv for argv in others)
    assert not any("all" in argv for argv in rag_incremental_invocations(2026))
