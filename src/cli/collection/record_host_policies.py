"""Record the robots policy of every host the crawlers read from.

Reads the host list from the crawl source inventory rather than a list typed out
here, so a new crawler brings its host with it instead of needing this file
edited too. The recording is the whole point: without a policy on file there is
no verdict to give, and the inventory reports `unknown` for every host nobody
asked about.

Read-only in what it measures, and deliberately separate from enforcement. The
compliance layer still governs koreabaseball.com alone; making a recorded policy
bind is a different decision and lives elsewhere.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from src.reporting.source_inventory import discover_crawler_modules, source_domains
from src.utils.host_policy import POLICY_DIR, HostPolicy, HostVerdict, read_policy, record_hosts

if TYPE_CHECKING:
    from collections.abc import Sequence

EXIT_OK = 0
EXIT_SOME_UNREACHABLE = 3


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the argument parser.

    Returns:
        The parser for this command.

    """
    parser = argparse.ArgumentParser(
        prog="record_host_policies",
        description="Fetch and archive robots.txt for every host the crawlers use.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of text.")
    parser.add_argument("--output", help="Write to this path instead of stdout.")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Read what is already recorded. Makes no network request and writes nothing.",
    )
    parser.add_argument(
        "--policy-dir",
        type=Path,
        default=POLICY_DIR,
        help="Where the snapshots live.",
    )
    parser.add_argument("--host", action="append", default=[], help="Record this host; repeatable.")
    return parser


def crawl_hosts() -> tuple[str, ...]:
    """Return every host the crawler sources mention.

    Returns:
        Sorted host names.

    """
    hosts: set[str] = set()
    for module in discover_crawler_modules():
        for domain in source_domains(module):
            # Keep the host as written; a bare hostname is already a hostname.
            hosts.add(domain.split("://")[-1].split("/")[0])
    return tuple(sorted(hosts))


def _render(policies: Sequence[HostPolicy], *, checked: bool) -> str:
    """Render the policies for a human.

    Args:
        policies: One policy per host.
        checked: Whether this was a read of the archive rather than a fetch.

    Returns:
        The rendered report.

    """
    heading = "Recorded host policies (from archive)" if checked else "Recorded host policies"
    lines = [f"{heading} -- {len(policies)} hosts", ""]
    width = max((len(p.host) for p in policies), default=10)
    for policy in policies:
        marker = {
            HostVerdict.ALLOWED: "ok",
            HostVerdict.DISALLOWED: "BLOCKED",
            HostVerdict.NO_POLICY: "no policy (default allow)",
            HostVerdict.UNREACHABLE: "not recorded",
        }[policy.verdict]
        when = policy.fetched_at or "-"
        lines.append(f"  {policy.host:{width}}  {marker:24}  {when}")
    unreachable = [p for p in policies if p.verdict is HostVerdict.UNREACHABLE]
    if unreachable:
        lines.extend(
            [
                "",
                f"{len(unreachable)} host(s) could not be read. That is neither permission",
                "nor refusal, and nothing was written down for them.",
            ]
        )
    return "\n".join(lines) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    """Run the command.

    Args:
        argv: Arguments to parse; defaults to ``sys.argv[1:]``.

    Returns:
        ``0`` on success, or ``EXIT_SOME_UNREACHABLE`` when a host could not be
        read and the caller asked for that to fail.

    """
    args = build_arg_parser().parse_args(argv)
    hosts = tuple(args.host) or crawl_hosts()

    policies = (
        read_policies(hosts, policy_dir=args.policy_dir)
        if args.check
        else asyncio.run(record_hosts(hosts, policy_dir=args.policy_dir))
    )

    rendered = (
        json.dumps([p.to_dict() for p in policies], ensure_ascii=False, indent=2) + "\n"
        if args.json
        else _render(policies, checked=args.check)
    )
    if args.output:
        Path(args.output).write_text(rendered, encoding="utf-8")
    else:
        sys.stdout.write(rendered)

    if any(p.verdict is HostVerdict.UNREACHABLE for p in policies):
        return EXIT_SOME_UNREACHABLE
    return EXIT_OK


def read_policies(hosts: Sequence[str], *, policy_dir: Path) -> list[HostPolicy]:
    """Return what the archive already says about each host.

    Args:
        hosts: Host names to look up.
        policy_dir: Where the snapshots live.

    Returns:
        One policy per host, in the order requested.

    """
    return [read_policy(host, policy_dir=policy_dir) for host in hosts]


if __name__ == "__main__":
    raise SystemExit(main())
