"""Record and read each crawled host's robots policy.

The compliance layer answers a question about one host. `ComplianceChecker` is
constructed with koreabaseball.com's robots.txt, its `_is_same_site` matches on
that host, and any other URL returns `True` -- so a club's site or a stadium's
page is treated as allowed because nothing ever looked. That is an assumption
reported as a fact, and it is why the inventory reported 27 crawlers as
`unknown` rather than as blocked: there was no policy on file to judge them by.

This module supplies that missing half. It records a robots.txt per host, in the
same format the KBO snapshots already use, and gives an honest verdict per host:

* a policy that was read and allows the path;
* a policy that was read and refuses it;
* no policy exists -- a host that returns 404 or an empty body is *publishing*
  permission, which is a different statement from one we failed to read;
* the file could not be retrieved, which is not a permission and not a refusal.

Recording is deliberately separate from enforcing. `src/utils/compliance.py`
still governs only koreabaseball.com, because making a recorded policy *bind*
changes what the crawlers do and deserves its own decision. Nothing here changes
crawler behaviour.
"""

from __future__ import annotations

import logging
import time
import urllib.robotparser
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlparse

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

from src.constants import KST

logger = logging.getLogger(__name__)

#: Where per-host policies are stored.
POLICY_DIR = Path("Docs/robots")

#: Snapshots carry their provenance in the first lines; the parser needs the rest.
SNAPSHOT_HEADER_LINES = 3

#: The agent name sent when fetching a policy. A crawler that presents no agent
#: is a crawler asking to be treated as a browser.
USER_AGENT = "KBODataCrawler/1.0 (+robots policy audit)"

#: Seconds between hosts. robots.txt is a small static file; the point of the
#: delay is not to be fast but to not arrive as a burst.
FETCH_DELAY_SECONDS = 1.0

FETCH_TIMEOUT_SECONDS = 10.0

ROBOTS_PATH = "/robots.txt"

#: Public suffixes under which the registrable label is the third from the right.
#: A short list rather than a full one: these are the suffixes in this
#: repository's host set, and a wrong entry would hand a club's policy to its
#: neighbour, which is worse than falling back to no record at all.
SECOND_LEVEL_SUFFIXES = frozenset({"co.kr", "com.br", "co.jp", "com.au", "co.nz"})

#: Labels needed before a suffix can be told from a registrable domain.
MIN_LABELS_FOR_REGISTRABLE = 3


class HostVerdict(StrEnum):
    """What is known about one host's crawling policy."""

    ALLOWED = "allowed"
    """A policy was read and it permits this path."""

    DISALLOWED = "disallowed"
    """A policy was read and it refuses this path."""

    NO_POLICY = "no_policy"
    """The host publishes no robots.txt, which is permission by default."""

    UNREACHABLE = "unreachable"
    """The file could not be retrieved. Neither permission nor refusal."""


@dataclass(frozen=True)
class HostPolicy:
    """One host's recorded policy and what it says about a path."""

    host: str
    verdict: HostVerdict
    snapshot: str | None = None
    fetched_at: str | None = None

    @property
    def recorded(self) -> bool:
        """Return whether a policy file exists for this host at all.

        Distinct from the verdict on purpose: a host we have never asked has no
        verdict, and saying so is more useful than reporting it as permissive.
        """
        return self.snapshot is not None

    def to_dict(self) -> dict[str, object]:
        """Return the policy as a plain mapping for JSON output."""
        return {
            "host": self.host,
            "verdict": self.verdict.value,
            "recorded": self.recorded,
            "snapshot": self.snapshot,
            "fetched_at": self.fetched_at,
        }


def _new_client() -> object:
    """Return a client that follows redirects and identifies itself.

    Redirects are the whole reason this function exists rather than a bare
    `httpx.get`: four of the five hosts that first came back `UNREACHABLE` serve
    their robots.txt behind a redirect, and recording those as unreadable
    reported an absence of knowledge about files that were one request away.
    """
    import httpx

    return httpx.AsyncClient(
        timeout=FETCH_TIMEOUT_SECONDS,
        headers={"User-Agent": USER_AGENT},
        follow_redirects=True,
    )


def snapshot_dir(host: str, *, policy_dir: Path = POLICY_DIR) -> Path:
    """Return where a host's snapshots live.

    Args:
        host: Host name, without a scheme.
        policy_dir: Root of the policy tree.

    Returns:
        The host's directory, which may not exist yet.

    """
    return policy_dir / host


def save_snapshot(host: str, content: str, *, policy_dir: Path = POLICY_DIR) -> Path:
    """Write one policy snapshot, in the format the KBO archive already uses.

    The two provenance header lines are kept byte-identical to
    `src/utils/compliance.py`'s writer so that a reader which already knows how
    to skip them keeps working, and so the two archives stay comparable.

    Args:
        host: Host the policy came from.
        content: The robots.txt body as retrieved.
        policy_dir: Root of the policy tree.

    Returns:
        The path written.

    """
    directory = snapshot_dir(host, policy_dir=policy_dir)
    directory.mkdir(parents=True, exist_ok=True)
    now = datetime.now(KST)
    path = directory / f"robots_{now.strftime('%Y%m%d_%H%M%S')}.txt"
    with path.open("w", encoding="utf-8") as handle:
        handle.write(f"# Source: https://{host}{ROBOTS_PATH}\n")
        handle.write(f"# Fetched at: {now.isoformat()}\n\n")
        handle.write(content)
    return path


def read_body(path: Path) -> str:
    """Return a snapshot's robots body, without its provenance header.

    Args:
        path: The snapshot file.

    Returns:
        The body text.

    """
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(lines[SNAPSHOT_HEADER_LINES:])


def source_host_of(path: Path) -> str | None:
    """Return the host a snapshot was fetched from, as recorded in its header.

    Args:
        path: The snapshot file.

    Returns:
        The host name, or None when the file carries no readable provenance.

    """
    try:
        header = path.read_text(encoding="utf-8", errors="replace").splitlines()[0]
    except OSError, IndexError:
        return None
    if not header.startswith("# Source:"):
        return None
    return urlparse(header.removeprefix("# Source:").strip()).hostname


def _snapshot_for_exact(host: str, *, policy_dir: Path) -> Path | None:
    """Return a snapshot recorded under exactly this host name."""
    own = sorted(snapshot_dir(host, policy_dir=policy_dir).glob("robots_*.txt"))
    if own:
        return own[-1]
    # Newest first, stop at the first match: the flat archive is one host's, so
    # this reads a couple of files rather than all 2,900 of them.
    for candidate in sorted(policy_dir.glob("robots_*.txt"), reverse=True):
        if candidate.is_file() and source_host_of(candidate) == host:
            return candidate
    return None


def candidate_names(host: str) -> tuple[str, ...]:
    """Return the host names a snapshot could legitimately be filed under.

    Mirrors the normalisation `ComplianceChecker._is_same_site` applies before it
    compares, so the report and the crawlers agree on which hosts are the same
    site. If they disagreed, the report would be describing a policy the crawlers
    do not enforce -- worse than reporting none.

    Args:
        host: Host name, without a scheme.

    Returns:
        Candidate names, most specific first.

    """
    names = [host]
    if not host.startswith("www."):
        names.append(f"www.{host}")
    registered = registered_domain(host)
    if registered and registered not in names:
        names.append(registered)
        if not registered.startswith("www."):
            names.append(f"www.{registered}")
    return tuple(names)


def newest_snapshot(host: str, *, policy_dir: Path = POLICY_DIR) -> Path | None:
    """Return the most recent snapshot belonging to a host.

    Looks in the host's own directory first, then in the flat layout where
    koreabaseball.com's archive lives. That fallback exists so the 2,900-odd
    snapshots recording the KBO policy timeline -- the evidence for the site-wide
    `Disallow: /` arriving on 8/5 -- keep being read rather than being quietly
    retired by a new directory layout.

    Matching is on the `# Source:` header and on the candidate names, never on
    "whatever file sorts last". Taking it blind handed koreabaseball.com's policy
    to every host without its own directory, reporting Lotte's site as
    `disallowed` for a reason that had nothing to do with Lotte.

    Args:
        host: Host name, without a scheme.
        policy_dir: Root of the policy tree.

    Returns:
        The newest snapshot for this host, or None when it has never been recorded.

    """
    for name in candidate_names(host):
        found = _snapshot_for_exact(name, policy_dir=policy_dir)
        if found is not None:
            return found
    return None


def registered_domain(host: str) -> str | None:
    """Return the registrable part of a host, for the same-site fallback.

    Only the common two-label public suffixes are handled, which is enough for
    the hosts in this repository and deliberately not a public-suffix-list
    implementation: guessing `.co.kr` handling would mean deciding which label is
    the suffix, and getting that wrong assigns a club's policy to a neighbour.

    Args:
        host: Host name, without a scheme.

    Returns:
        The registrable domain, or None when the host is too short to have one.

    """
    labels = host.split(".")
    if len(labels) < MIN_LABELS_FOR_REGISTRABLE:
        return host if len(labels) == MIN_LABELS_FOR_REGISTRABLE - 1 else None
    if ".".join(labels[-2:]) in SECOND_LEVEL_SUFFIXES:
        return ".".join(labels[-MIN_LABELS_FOR_REGISTRABLE:])
    return ".".join(labels[-2:])


def verdict_from_content(content: str, path: str = "/") -> HostVerdict:
    """Judge a robots body with the same parser the compliance layer uses.

    Sharing the parser matters more than sharing the code: a report that worked
    out the policy a second time could disagree with the crawlers about what the
    file says, and then the report would be wrong in the direction that matters
    least to notice.

    Args:
        content: The robots.txt body.
        path: The path being requested.

    Returns:
        The verdict for a generic agent.

    """
    parser = urllib.robotparser.RobotFileParser()
    parser.parse(content.splitlines())
    return HostVerdict.ALLOWED if parser.can_fetch("*", path) else HostVerdict.DISALLOWED


def read_policy(host: str, *, path: str = "/", policy_dir: Path = POLICY_DIR) -> HostPolicy:
    """Return what the recorded snapshots say about a host.

    Args:
        host: Host name, without a scheme.
        path: The path being requested.
        policy_dir: Root of the policy tree.

    Returns:
        The host's policy, including `UNREACHABLE` when nothing was ever recorded.

    """
    snapshot = newest_snapshot(host, policy_dir=policy_dir)
    if snapshot is None:
        return HostPolicy(host, HostVerdict.UNREACHABLE)
    body = read_body(snapshot)
    if not body.strip():
        # A host that serves an empty robots.txt is publishing permission, not
        # withholding a policy. Collapsing the two would report a recorded
        # permission as a failed read.
        return HostPolicy(host, HostVerdict.NO_POLICY, snapshot.name, _fetched_at(snapshot))
    return HostPolicy(host, verdict_from_content(body, path), snapshot.name, _fetched_at(snapshot))


def _fetched_at(snapshot: Path) -> str | None:
    """Return the timestamp a snapshot recorded, when it recorded one."""
    try:
        header = snapshot.read_text(encoding="utf-8", errors="replace").splitlines()[1]
    except OSError, IndexError:
        return None
    return header.removeprefix("# Fetched at:").strip() or None


def classify_response(status_code: int, body: str) -> HostVerdict:
    """Judge a fetched policy from the response alone.

    Split out so the rule is testable without a network: a 404 is the host
    publishing permission, a 200 with an empty body is the same statement, and
    anything else -- including 5xx and 403 -- is neither.

    Args:
        status_code: The HTTP status returned.
        body: The response body.

    Returns:
        The verdict the response supports.

    """
    if status_code == HTTPStatus.OK:
        return HostVerdict.NO_POLICY if not body.strip() else verdict_from_content(body)
    if status_code in (HTTPStatus.NOT_FOUND, HTTPStatus.GONE):
        return HostVerdict.NO_POLICY
    return HostVerdict.UNREACHABLE


async def record_host(
    host: str,
    *,
    client: object | None = None,
    save: bool = True,
    policy_dir: Path = POLICY_DIR,
) -> HostPolicy:
    """Fetch one host's robots.txt and record it.

    A failure is never written down as a policy. The point of the archive is to
    be able to say what a host said, and storing an empty file for a host that
    timed out would turn "we could not reach it" into "it permits everything".

    Args:
        host: Host name, without a scheme.
        client: An `httpx.AsyncClient` to use. One is created when omitted.
        save: Whether to write the snapshot. Off for a read-only check.
        policy_dir: Root of the policy tree.

    Returns:
        The host's policy as the fetch determined it.

    """
    import httpx

    owns_client = client is None
    active = client or _new_client()
    url = f"https://{host}{ROBOTS_PATH}"
    try:
        response = await active.get(url)  # type: ignore[attr-defined]
    except httpx.HTTPError, OSError:
        logger.warning("[HOST_POLICY] %s could not be reached; nothing recorded", host)
        return HostPolicy(host, HostVerdict.UNREACHABLE)
    finally:
        if owns_client:
            await active.aclose()  # type: ignore[attr-defined]

    body = response.text
    verdict = classify_response(response.status_code, body)
    snapshot_name: str | None = None
    if verdict is not HostVerdict.UNREACHABLE:
        if save:
            try:
                snapshot_name = save_snapshot(host, body, policy_dir=policy_dir).name
            except OSError:
                logger.exception("[HOST_POLICY] could not save the snapshot for %s", host)
        else:
            snapshot_name = "(not saved)"
        logger.info("[HOST_POLICY] %s -> %s", host, verdict.value)
    return HostPolicy(host, verdict, snapshot_name, datetime.now(KST).isoformat())


async def record_hosts(
    hosts: Iterable[str],
    *,
    save: bool = True,
    policy_dir: Path = POLICY_DIR,
) -> list[HostPolicy]:
    """Record every host's policy, one at a time.

    Sequential on purpose. The delay between hosts is the politeness, and a
    thread pool would turn 24 small requests into a burst that looks like the
    thing the delay exists to avoid.

    Args:
        hosts: Host names, without schemes. Duplicates are collapsed.
        save: Whether to write the snapshots.
        policy_dir: Root of the policy tree.

    Returns:
        One policy per host, in the order they were requested.

    """
    ordered: Sequence[str] = tuple(dict.fromkeys(hosts))
    results: list[HostPolicy] = []
    async with _new_client() as client:
        for index, host in enumerate(ordered):
            if index:
                await _sleep(FETCH_DELAY_SECONDS)
            results.append(await record_host(host, client=client, save=save, policy_dir=policy_dir))
    return results


async def _sleep(seconds: float) -> None:
    """Wait between hosts, off the event loop's critical path."""
    import asyncio

    await asyncio.sleep(seconds)


def hosts_of(crawler_source: str) -> tuple[str, ...]:
    """Return the hosts a crawler's source mentions, for scheduling a recording.

    Args:
        crawler_source: The crawler module's source text.

    Returns:
        Sorted host names.

    """
    import ast

    hosts: set[str] = set()
    for node in ast.walk(ast.parse(crawler_source)):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            for piece in node.value.split():
                if piece.startswith(("http://", "https://")):
                    hostname = urlparse(piece).hostname
                    if hostname:
                        hosts.add(hostname)
    return tuple(sorted(hosts))


def wait(seconds: float = FETCH_DELAY_SECONDS) -> None:
    """Pause, for callers that record synchronously.

    Args:
        seconds: How long to pause.

    """
    time.sleep(seconds)


__all__ = [
    "FETCH_DELAY_SECONDS",
    "POLICY_DIR",
    "USER_AGENT",
    "HostPolicy",
    "HostVerdict",
    "candidate_names",
    "classify_response",
    "hosts_of",
    "newest_snapshot",
    "read_body",
    "read_policy",
    "record_host",
    "record_hosts",
    "registered_domain",
    "save_snapshot",
    "snapshot_dir",
    "source_host_of",
    "verdict_from_content",
    "wait",
]
