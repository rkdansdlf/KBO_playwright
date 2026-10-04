"""A selector the crawler stopped using must not stay green in the gate.

The gate checks a captured page against a list of selectors written down in
``crawler_selector_gate.json``. That list is a copy, and a copy does not update
itself: rename ``span.nums`` to ``div.rank`` in the crawler, re-capture nothing,
and the gate still compares the fixture against ``span.nums`` -- which the
fixture still has -- and reports PASS. Every check is true and nothing is being
guarded, which is the shape of failure this gate exists to prevent.

So the copy is held against its source here. For each target that names a
crawler, every selector the crawler reads the DOM with has to be accounted for by
that target's checks, or be listed below with a reason. A new selector added to
a crawler fails this test the same day, which is the point at which a human
decides whether it is load-bearing enough to watch.

The two directions differ on purpose. A crawler selector is covered when a check
uses it or a *scoped* form of it -- the crawler asks for ``th`` inside a row, the
gate asks for ``table.tData.tbd02 tbody tr th``, and narrowing a selector cannot
turn a real match into a false one. Going the other way there is no such excuse:
a check wider than anything the crawler selects is watching a thing nobody reads.
"""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "Docs" / "references" / "crawler_selector_gate.json"
CRAWLER_DIR = ROOT / "src" / "crawlers"

#: Attribute and function names through which a crawler reaches into the DOM.
_DOM_CALLS = frozenset({"locator", "select", "select_one", "querySelector", "querySelectorAll"})

#: Which gate targets stand watch for which crawler module.
GUARDED_BY: dict[str, str] = {
    "kbo_team_history": "team_history_crawler",
    "kbo_business_event_guide": "kbo_event_crawler",
    "kbo_business_event_announcement": "kbo_event_crawler",
}

#: Selectors a crawler reads with that the gate is not asked to hold still, each
#: with the reason. An empty reason is not a reason -- that is what makes this
#: list worth reading.
UNWATCHED: dict[str, dict[str, str]] = {
    "kbo_business_event_guide": {
        "title": (
            "The element name is not a contract. The sweep accepts any descriptive"
            " <title>, so pinning it would make an ordinary copy edit look like"
            " drift -- which is the noise this gate is meant not to add."
        ),
    },
    "kbo_business_event_announcement": {
        "title": (
            "Same reasoning as kbo_business_event_guide: <title> carries prose,"
            " not structure, and the frame checks already say the page loaded."
        ),
    },
}


def _dom_selectors(module: str) -> set[str]:
    """Return every selector a crawler module reads the DOM with.

    Two forms count. A literal passed to ``locator``/``select``/``querySelector*``
    is the common case. A module-level ``*_SELECTORS`` tuple is the other: those
    exist precisely to be iterated over a set of pages, so the elements they name
    are exactly the ones a redesign would move, and an argument that only reaches
    the reader as a loop variable is otherwise invisible to any static check.
    """
    tree = ast.parse((CRAWLER_DIR / f"{module}.py").read_text(encoding="utf-8"))
    found: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            name = getattr(func, "attr", None) or getattr(func, "id", None)
            if name in _DOM_CALLS:
                found.update(
                    arg.value for arg in node.args if isinstance(arg, ast.Constant) and isinstance(arg.value, str)
                )

    for node in tree.body:
        if not isinstance(node, ast.Assign) or not node.targets:
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name) or not target.id.endswith("SELECTORS"):
            continue
        if isinstance(node.value, ast.Tuple | ast.List):
            found.update(
                element.value
                for element in node.value.elts
                if isinstance(element, ast.Constant) and isinstance(element.value, str)
            )

    return found


def _gate_selectors(target_name: str, config: dict) -> set[str]:
    """Return the selectors a gate target's checks actually look for."""
    for target in config["targets"]:
        if target["name"] == target_name:
            return {check["selector"] for check in target["checks"]}
    msg = f"no such selector gate target: {target_name}"
    raise AssertionError(msg)


def _narrows(check_selector: str, crawler_selector: str) -> bool:
    """Return whether a check watches the crawler's selector, possibly scoped.

    A descendant step cannot add matches that were not already reachable, so
    ``table.tData.tbd02 td span.nums`` genuinely witnesses the health of the bare
    ``span.nums`` the crawler asks for. The converse is not true -- ``td``
    matches everywhere -- which is why this is one-directional.
    """
    if check_selector == crawler_selector:
        return True
    steps = check_selector.split()
    return len(steps) > 1 and steps[-1] == crawler_selector


@pytest.fixture(scope="module")
def config() -> dict:
    return json.loads(CONFIG.read_text(encoding="utf-8"))


def test_every_guarded_target_still_exists(config: dict) -> None:
    """A guard naming a target that was renamed is not guarding anything."""
    names = {target["name"] for target in config["targets"]}

    missing = sorted(set(GUARDED_BY) - names)

    assert not missing, f"selector gate targets removed while still guarded: {missing}"


@pytest.mark.parametrize("target_name", sorted(GUARDED_BY))
def test_every_selector_the_crawler_uses_is_watched_or_explained(target_name: str, config: dict) -> None:
    """The copy in the config is checked against the source it was copied from."""
    module = GUARDED_BY[target_name]
    used = _dom_selectors(module)
    watched = _gate_selectors(target_name, config)
    unwatched = UNWATCHED.get(target_name, {})

    assert used, f"{module} reads the DOM but no selector was found"
    unaccounted = sorted(s for s in used if not any(_narrows(w, s) for w in watched) and s not in unwatched)

    assert not unaccounted, (
        f"{module} reads {unaccounted}, which {target_name} does not watch. Add a"
        " check, or add it to UNWATCHED with the reason it is not load-bearing."
    )


@pytest.mark.parametrize("target_name", sorted(GUARDED_BY))
def test_a_check_narrower_than_the_crawler_never_reads_is_left_behind(target_name: str, config: dict) -> None:
    """The other direction: a check nobody exercises is a check nobody reads."""
    module = GUARDED_BY[target_name]
    used = _dom_selectors(module)
    watched = _gate_selectors(target_name, config)

    unbacked = sorted(w for w in watched if not any(_narrows(w, s) for s in used))

    assert not unbacked, f"{target_name} watches {unbacked}, which {module} no longer reads"


def test_an_unwatched_selector_carries_a_reason() -> None:
    """A bare entry would just be a list of things nobody thought about."""
    for target, entries in UNWATCHED.items():
        for selector, reason in entries.items():
            assert reason.strip(), f"{target}/{selector} is unwatched with no reason"
            assert len(reason.split()) > 8, f"{target}/{selector} reason is a placeholder, not a reason"


def test_an_unwatched_selector_is_actually_unwatched(config: dict) -> None:
    """Otherwise the reason documents a selector that is in fact watched."""
    for target, entries in UNWATCHED.items():
        watched = _gate_selectors(target, config)
        for selector in entries:
            assert not any(_narrows(w, selector) for w in watched), f"{target}/{selector} is both watched and unwatched"


def test_no_entry_describes_a_target_that_does_not_exist() -> None:
    """An UNWATCHED key nothing reads is a place a real exception would hide."""
    stray = sorted(set(UNWATCHED) - set(GUARDED_BY))

    assert not stray, f"UNWATCHED names targets with no guard: {stray}"


def test_a_selector_built_at_runtime_is_not_claimed_to_be_covered() -> None:
    """The extraction reads literals, so a built selector is a known blind spot.

    ``player_movement_crawler`` builds its table selector inside a JavaScript
    string handed to ``page.evaluate``, which no AST walk can reach. Claiming the
    gate covers it would be the exact overclaim this file exists to prevent, so
    it is covered the other way round instead: the selector is read out of the
    JavaScript source and required to appear in the gate, rather than extracted
    from the Python AST and assumed present.

    This crawler was the reason the rule is stated instead of assumed. Its table
    selector is built inside a ``page.evaluate()`` string, so ``_dom_selectors``
    cannot see it -- it returned only ``a.pg_next`` and the naive version of this
    test would have passed while the one selector that decides whether the year
    reads at all went unwatched.
    """
    module = "player_movement_crawler"
    source = (CRAWLER_DIR / f"{module}.py").read_text(encoding="utf-8")

    assert "querySelector('.tbl-type02')" in source, (
        "the JS-side table selector moved; the gate and this test must be updated together"
    )
    assert _dom_selectors(module) == {"a.pg_next"}, (
        "player_movement_crawler now exposes selectors AST can see; _dom_selectors"
        " would cover them, so drop the JS assertion here rather than leave two paths"
    )


def test_a_selector_written_in_javascript_is_read_out_of_the_script() -> None:
    """The blind spot is closed for the one selector that carries the year read.

    ``player_movement_crawler`` finds its table with ``querySelector`` inside a
    string passed to ``page.evaluate``. If that class name is renamed, the
    returned page reads ``{'table_present': False, 'rows': []}`` -- indistinguishable
    from a year with no transactions -- so the gate has to hold the name still even
    though no Python AST walk can reach it.
    """
    source = (CRAWLER_DIR / "player_movement_crawler.py").read_text(encoding="utf-8")
    in_script = re.search(r"querySelector\(\s*['\"]([^'\"]+)['\"]\s*\)", source)
    assert in_script, "the page.evaluate script no longer uses a literal querySelector"

    config = json.loads(CONFIG.read_text(encoding="utf-8"))
    watched = _gate_selectors("kbo_player_trade", config)

    assert any(_narrows(w, in_script.group(1)) for w in watched), (
        f"the script selects {in_script.group(1)!r}, which kbo_player_trade does not watch"
    )
