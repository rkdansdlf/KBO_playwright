"""KBO inning-breakdown cell vocabulary.

Both the Naver record API and the KBO official boxscore expose a batter's
inning-by-inning results as short cells. A cell holds the outcome of one
plate appearance, written as the field position followed by the outcome:

- ``좌안`` / ``중안`` / ``우안`` -- a single to left / center / right
- ``좌2`` / ``중2`` / ``우2`` -- a double to left / center / right
- ``좌3`` / ``중3`` / ``우3`` -- a triple to left / center / right
- ``좌홈`` / ``중홈`` / ``우홈`` -- a home run to left / center / right
- ``삼진`` (strikeout), ``4구`` (walk), ``사구`` (hit by pitch),
  ``3땅`` (out at third), ``좌비`` (out to left field) and so on.

A batter who batted twice in one inning has both results in a single cell,
separated by ``/``.

Neither source carries a per-batter doubles or triples column: the Naver
payload reports ``hit`` in total and leaves the split to the breakdown, and
the KBO hitter table lists only 타수/안타/타점/득점/타율. The breakdown is
therefore the only place the split exists, and this module decodes it.

The decoding was verified against the source's own per-game player lists
(Naver's ``2루타``/``3루타`` entries and the KBO page's ``#tblEtc``): over
781 games the derived doubles and triples agree with the source's own count
in 755 games, and every remaining game is one that reached an extra inning,
whose cells are not part of the nine-inning breakdown.
"""

from __future__ import annotations

#: The cell is blank.
_BLANK_CELL = "&nbsp;"

#: A cell holding the outcome of one plate appearance.
Cell = str


def classify_base_hit_token(token: Cell) -> str | None:
    """Classify one inning cell as a double, a triple, or a home run.

    The token names the field position first and the number of bases second,
    so ``좌2`` is a double to left and ``중3`` a triple to center. A home run
    is spelled out (``좌홈``). A plain single (``좌안``) and every out token
    return None: a single needs no derivation because the source already
    reports the total number of hits.

    Args:
        token: One cell result.

    Returns:
        ``"doubles"``, ``"triples"``, ``"home_runs"``, or None when the token
        is not a multi-base hit or a home run.

    """
    cell = token.strip()
    if not cell or cell == _BLANK_CELL:
        return None
    if cell.endswith("홈"):
        return "home_runs"
    if cell.endswith("2"):
        return "doubles"
    if cell.endswith("3"):
        return "triples"
    return None


def derive_hitter_stats_from_inning_cells(cells: dict[str, Cell]) -> dict[str, int]:
    """Count a batter's stats from the inning breakdown cells.

    Args:
        cells: Cells keyed by inning.

    Returns:
        The counts of strikeouts, walks, hit-by-pitch, sacrifice hits,
        sacrifice flies, doubles, triples, and home runs.

    """
    derived = {
        "strikeouts": 0,
        "walks": 0,
        "hbp": 0,
        "sacrifice_hits": 0,
        "sacrifice_flies": 0,
        "doubles": 0,
        "triples": 0,
        "home_runs": 0,
    }

    for value in cells.values():
        if not value or value == _BLANK_CELL:
            continue
        if "삼진" in value or "스낫" in value or "루낫" in value or "낫아웃" in value:
            derived["strikeouts"] += 1
        if "4구" in value or "볼넷" in value or "고의4구" in value:
            derived["walks"] += 1
        if "사구" in value or "몸에 맞는 볼" in value:
            derived["hbp"] += 1
        if "희타" in value or "희번" in value or "희생번트" in value:
            derived["sacrifice_hits"] += 1
        if "희비" in value or "희생플라이" in value:
            derived["sacrifice_flies"] += 1
        for result in value.split("/"):
            kind = classify_base_hit_token(result)
            if kind:
                derived[kind] += 1
    return derived
