"""Show what the run ledger's columns mean, and what the recent runs say.

Exists because the ledger writes three dialects -- `status`, `error_code` and a
`checkpoint` JSON blob -- and none of them was written down. That is not a
cosmetic gap: `source_limited` was looked for in `status`, found zero times, and
read as "the fix is not deployed" when it was. Ninety rows carried it.

`--explain` answers the question an operator actually has when a report and a
ledger disagree: what does this value mean.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import TYPE_CHECKING

from src.crawlers.run_vocabulary import explain, latest, vocabulary

if TYPE_CHECKING:
    from collections.abc import Sequence

EXIT_OK = 0


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the argument parser.

    Returns:
        The parser for this command.

    """
    parser = argparse.ArgumentParser(
        prog="run_vocabulary",
        description="Explain the crawl run ledger's vocabulary and summarise recent runs.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of text.")
    parser.add_argument(
        "--explain",
        action="append",
        default=[],
        metavar="COLUMN=VALUE",
        help="Explain one value, e.g. status=success or checkpoint.outcome=source_limited. Repeatable.",
    )
    parser.add_argument(
        "--window-hours",
        type=int,
        default=72,
        help="How far back to summarise runs. Default 72.",
    )
    parser.add_argument(
        "--vocabulary-only",
        action="store_true",
        help="Print the vocabulary and skip the database entirely.",
    )
    return parser


def _render_explanations(pairs: Sequence[str]) -> tuple[list[str], bool]:
    """Render `--explain` answers, reporting whether every pair was understood.

    Args:
        pairs: The `COLUMN=VALUE` arguments.

    Returns:
        The lines, and whether any pair could not be explained.

    """
    lines: list[str] = []
    unknown = False
    for pair in pairs:
        column, _, value = pair.partition("=")
        answer = explain(column.strip(), value.strip())
        if answer is None:
            unknown = True
            lines.append(f"  {column}={value}: not a value this repository names")
        else:
            lines.append(f"  {column}={value}: {answer}")
    return lines, unknown


def _render(vocabulary_map: dict, summary: dict | None, explanations: list[str]) -> str:
    """Render the report for a human.

    Args:
        vocabulary_map: Column to value to meaning.
        summary: Recent run totals, or None when the database was not read.
        explanations: Rendered `--explain` lines.

    Returns:
        The rendered report.

    """
    lines = ["Run ledger vocabulary", ""]
    width = max((len(v) for values in vocabulary_map.values() for v in values), default=10)
    for column, values in vocabulary_map.items():
        lines.append(f"  {column}")
        for value, meaning in values.items():
            lines.append(f"    {value:{width}}  {meaning}")

    if summary is not None:
        totals = summary["totals"]
        lines.extend(
            [
                "",
                f"Runs in the last {summary['window_hours']}h",
                f"  runs        {totals['runs']}",
                f"  succeeded   {totals['succeeded']}",
                f"  collected   {totals['collected']}",
                f"  refused     {totals['refused']}",
                f"  failed      {totals['failed']}",
            ],
        )

    if explanations:
        lines.extend(["", "Explanations"])
        lines.extend(explanations)

    lines.extend(
        [
            "",
            "A `success` run that collected nothing is not a contradiction: the run finished and the",
            "answer was that the source was not consulted. Refusals are counted separately from",
            "outages because their remedies differ and no retry can clear either.",
        ],
    )
    return "\n".join(lines) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    """Run the command.

    Args:
        argv: Arguments to parse; defaults to ``sys.argv[1:]``.

    Returns:
        ``0``, or ``1`` when an `--explain` pair named something unknown.

    """
    args = build_arg_parser().parse_args(argv)
    explanations, unknown = _render_explanations(args.explain)
    vocabulary_map = vocabulary()
    summary = None if args.vocabulary_only else latest(args.window_hours)

    rendered = (
        json.dumps(
            {"vocabulary": vocabulary_map, "recent": summary, "unknown": unknown},
            ensure_ascii=False,
            indent=2,
            default=str,
        )
        + "\n"
        if args.json
        else _render(vocabulary_map, summary, explanations)
    )
    sys.stdout.write(rendered)
    return 1 if unknown else EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
