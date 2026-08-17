#!/usr/bin/env python3
"""Report field-level provenance for generated historical archive payloads."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from typing import Any

from scripts.converters.convert_kbo_archive_records import (
    ARCHIVE_DATA_CLASS,
    ARCHIVE_FIELD_SOURCES,
    ARCHIVE_PROVENANCE,
    generate_season_dataset,
)


@dataclass(frozen=True, slots=True)
class ArchiveProvenanceAudit:
    """Summarize source classification for generated archive payloads."""

    start_year: int
    end_year: int
    data_class: str
    verified: bool
    safe_for_historical_ingest: bool
    season_game_counts: dict[str, int]
    field_sources: dict[str, str]
    findings: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible audit mapping."""
        return asdict(self)


def audit_archive_provenance(start_year: int = 1982, end_year: int = 2000) -> ArchiveProvenanceAudit:
    """Audit generated archive metadata and report whether it is ingestible as fact."""
    if start_year > end_year:
        raise ValueError("start_year must not exceed end_year")

    season_game_counts = {
        str(year): len(generate_season_dataset(year)["games"]) for year in range(start_year, end_year + 1)
    }
    findings: list[str] = []
    if ARCHIVE_DATA_CLASS == "synthetic_fixture":
        findings.append("All generated fields are deterministic fixtures, not official archive facts")
    if ARCHIVE_PROVENANCE.get("verified") is not True:
        findings.append("Provenance is explicitly unverified")
    if not all(source.startswith("synthetic:") for source in ARCHIVE_FIELD_SOURCES.values()):
        findings.append("At least one field source is not classified as synthetic")

    return ArchiveProvenanceAudit(
        start_year=start_year,
        end_year=end_year,
        data_class=ARCHIVE_DATA_CLASS,
        verified=bool(ARCHIVE_PROVENANCE.get("verified")),
        safe_for_historical_ingest=False,
        season_game_counts=season_game_counts,
        field_sources=dict(ARCHIVE_FIELD_SOURCES),
        findings=tuple(findings),
    )


def main(argv: list[str] | None = None) -> int:
    """Print the archive provenance audit and fail for non-factual fixtures."""
    parser = argparse.ArgumentParser(description="Audit historical archive fixture provenance")
    parser.add_argument("--start-year", type=int, default=1982)
    parser.add_argument("--end-year", type=int, default=2000)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    report = audit_archive_provenance(args.start_year, args.end_year)

    if args.json:
        sys.stdout.write(json.dumps(report.as_dict(), ensure_ascii=False, indent=2) + "\n")
    else:
        sys.stdout.write(
            f"data_class={report.data_class} verified={report.verified} "
            f"safe_for_historical_ingest={report.safe_for_historical_ingest}\n",
        )
        for finding in report.findings:
            sys.stdout.write(f"finding: {finding}\n")
        for field, source in report.field_sources.items():
            sys.stdout.write(f"{field}: {source}\n")
    return 0 if report.safe_for_historical_ingest else 1


if __name__ == "__main__":
    raise SystemExit(main())
