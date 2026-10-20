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
