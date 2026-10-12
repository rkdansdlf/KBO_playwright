"""Render the crawl source inventory: what is stale, who reads it, and why.

The three questions the report answers are the ones an operator asks before
repairing anything, and none of them is available elsewhere: a crawler's run
status says nothing about whether its table is current, and the AI-visible
surface is only visible from `rag_chunks`.

Exit code is 0 whether or not drift is found. This is an observation, not a gate
-- `--fail-on-drift` exists for the CI gate, the same way `kbo snapshot validate`
separates reporting from enforcing.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import TYPE_CHECKING

from src.db.engine import SessionLocal
from src.reporting.source_inventory import (
    CollectionState,
    Freshness,
    PolicyStatus,
    SourceInventory,
    build_inventory,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

EXIT_OK = 0
EXIT_DRIFT = 3

#: Crawlers named per state line before the rest are counted.
NAMES_SHOWN = 4

_FRESHNESS_MARK = {
    Freshness.CURRENT: "ok",
    Freshness.STALE: "STALE",
    Freshness.EMPTY: "empty",
    Freshness.UNKNOWN: "?",
}


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the argument parser.

    Returns:
        The parser for this command.

    """
    parser = argparse.ArgumentParser(
        prog="crawler_source_inventory",
        description="Report each crawler's source, the tables it writes, and how current they are.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of text.")
    parser.add_argument("--output", help="Write to this path instead of stdout.")
    parser.add_argument(
        "--no-db",
        action="store_true",
        help="Skip the table measurements. Declarations and policy status only.",
    )
    parser.add_argument(
        "--fail-on-drift",
        action="store_true",
        help=f"Exit {EXIT_DRIFT} when drift is found, for use as a gate.",
    )
    return parser


def _render_states(inventory: SourceInventory) -> list[str]:
    """Render the ledger-and-table matrix.

    The two signals are shown side by side because neither answers alone. An
    age-only column reads "old" as "broken", which reported a crawler writing
    three records every day as a table that had not changed since August.

    Args:
        inventory: The inventory to render.

    Returns:
        The section's lines, heading included.

    """
    lines = ["", "Collection state"]
    for state in CollectionState:
        rows = inventory.by_state(state)
        if not rows:
            continue
        shown = rows[:NAMES_SHOWN]
        lines.append(f"  {state.value:14} {len(rows):3}  {', '.join(r.crawler for r in shown)}")
        if len(rows) > NAMES_SHOWN:
            lines.append(f"  {'':14}      {len(rows) - NAMES_SHOWN} more")
    disagreements = inventory.policy_disagreements()
    if disagreements:
        lines.extend(["", "Policy disagreements"])
        lines.extend(
            f"  {row.crawler}: sources read as permitted, but {row.runs.policy_blocked} run(s) were refused"
            for row in disagreements
            if row.runs is not None
        )
    return lines


def _render_text(inventory: SourceInventory) -> str:
    """Render the report for a human.

    Args:
        inventory: The inventory to render.

    Returns:
        The rendered report.

    """
    lines: list[str] = ["Crawl source inventory", ""]
    lines.append(f"  crawlers reported   {len(inventory.rows)}")
    lines.append(f"  declared            {sum(1 for r in inventory.rows if r.declared)}")
    lines.append(f"  blocked by policy   {sum(1 for r in inventory.rows if r.policy_status is PolicyStatus.BLOCKED)}")
    # How many rows the ledger actually joined. Most crawlers are weekly or run on
    # demand and simply did not run in the window, so this is a count rather than
    # a list of names -- but a drop in it is how a rename in either vocabulary
    # becomes visible instead of silent.
    lines.append(f"  with run summary    {sum(1 for r in inventory.rows if r.runs is not None)}")
    lines.append("")

    header = f"  {'crawler':34} {'policy':8} {'table':26} {'age':>7}  {'state':7} {'rag':>7}"
    lines.append(header)
    lines.append("  " + "-" * (len(header) - 2))
    for row in inventory.rows:
        if not row.declared:
            continue
        if not row.measurements:
            lines.append(f"  {row.crawler:34} {row.policy_status.value:8} {'(no table declared)':26}")
            continue
        for index, measurement in enumerate(row.measurements):
            name = row.crawler if index == 0 else ""
            policy = row.policy_status.value if index == 0 else ""
            lines.append(
                f"  {name:34} {policy:8} {measurement.table:26} {measurement.render_age():>7}  "
                f"{_FRESHNESS_MARK[measurement.freshness]:7} {row.rag_chunks if index == 0 else '':>7}",
            )

    lines.extend(_render_states(inventory))

    exposed = inventory.ai_exposure()
    lines.extend(["", f"AI-visible stale data ({len(exposed)})"])
    if not exposed:
        lines.append("  none")
    for row in exposed:
        stalest = row.stalest
        lines.append(
            f"  {row.crawler}: {row.rag_chunks} chunks from a table last current "
            f"{stalest.render_age() if stalest else 'never'}",
        )

    if inventory.advisories:
        lines.extend(["", "Advisories"])
        lines.extend(f"  - {line}" for line in inventory.advisories)
    if inventory.drift:
        lines.extend(["", "Drift"])
        lines.extend(f"  ! {line}" for line in inventory.drift)
    return "\n".join(lines) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    """Run the command.

    Args:
        argv: Arguments to parse; defaults to ``sys.argv[1:]``.

    Returns:
        ``0`` on success, ``EXIT_DRIFT`` when drift was found and the gate was asked for.

    """
    args = build_arg_parser().parse_args(argv)
    inventory = build_inventory(session_factory=None if args.no_db else SessionLocal)

    rendered = (
        json.dumps(
            {
                "rows": [row.to_dict() for row in inventory.rows],
                "drift": list(inventory.drift),
                "advisories": list(inventory.advisories),
            },
            ensure_ascii=False,
            indent=2,
            default=str,
        )
        + "\n"
        if args.json
        else _render_text(inventory)
    )

    if args.output:
        from pathlib import Path

        Path(args.output).write_text(rendered, encoding="utf-8")
    else:
        sys.stdout.write(rendered)

    if args.fail_on_drift and inventory.drift:
        return EXIT_DRIFT
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
