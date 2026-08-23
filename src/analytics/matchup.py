"""Matchup Analytics Engine for Batter vs. Pitcher (BvP) and contextual split aggregations."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from sqlalchemy import func

from src.analytics.dto import MatchupMatrix, SplitMetrics
from src.models.game import Game, GameEvent
from src.models.matchup import MatchupBvP

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

BASES_LEN = 3


def _apply_event_to_bvp_stat(st: dict[str, Any], desc: str) -> None:
    """Update accumulating BvP counting statistics from event description."""
    if any(k in desc for k in ["안타", "2루타", "3루타", "홈런"]):
        st["ab"] += 1
        st["h"] += 1
        if "2루타" in desc:
            st["d2"] += 1
        elif "3루타" in desc:
            st["d3"] += 1
        elif "홈런" in desc:
            st["hr"] += 1
    elif "볼넷" in desc:
        st["bb"] += 1
    elif "사구" in desc:
        st["hbp"] += 1
    elif "희생플라이" in desc:
        st["sf"] += 1
    elif "희생번트" not in desc:
        st["ab"] += 1
        if "삼진" in desc:
            st["so"] += 1


class MatchupAnalyticsEngine:
    """Computes Head-to-Head (BvP) matchups and multi-dimensional splits from Game & PBP events."""

    def __init__(self, session: Session) -> None:
        """Initialize MatchupAnalyticsEngine with a database session."""
        self.session = session

    def calculate_bvp_matchups(self, season_year: int) -> list[MatchupMatrix]:
        """Aggregate BvP (Batter vs Pitcher) matchup records from play-by-play events."""
        events = (
            self.session.query(GameEvent)
            .join(Game, Game.game_id == GameEvent.game_id)
            .filter(func.substr(Game.game_id, 1, 4) == str(season_year))
            .filter(GameEvent.batter_id.isnot(None))
            .filter(GameEvent.pitcher_id.isnot(None))
            .all()
        )

        bvp_map: dict[tuple[int, int], dict[str, Any]] = {}

        for ev in events:
            key = (ev.batter_id, ev.pitcher_id)
            if key not in bvp_map:
                bvp_map[key] = {
                    "batter_id": ev.batter_id,
                    "pitcher_id": ev.pitcher_id,
                    "pa": 0,
                    "ab": 0,
                    "h": 0,
                    "d2": 0,
                    "d3": 0,
                    "hr": 0,
                    "bb": 0,
                    "hbp": 0,
                    "so": 0,
                    "sf": 0,
                }
            st = bvp_map[key]
            st["pa"] += 1
            desc = ev.description or ""
            _apply_event_to_bvp_stat(st, desc)

        results: list[MatchupMatrix] = []
        for (b_id, p_id), st in bvp_map.items():
            ab = st["ab"]
            h = st["h"]
            d2 = st["d2"]
            d3 = st["d3"]
            hr = st["hr"]
            bb = st["bb"]
            hbp = st["hbp"]
            pa = st["pa"]

            avg = (h / ab) if ab > 0 else 0.0
            obp_den = ab + bb + hbp + st["sf"]
            obp = ((h + bb + hbp) / obp_den) if obp_den > 0 else 0.0
            slg = ((h - d2 - d3 - hr + 2 * d2 + 3 * d3 + 4 * hr) / ab) if ab > 0 else 0.0
            ops = obp + slg

            results.append(
                MatchupMatrix(
                    batter_id=b_id,
                    pitcher_id=p_id,
                    plate_appearances=pa,
                    at_bats=ab,
                    hits=h,
                    doubles=d2,
                    triples=d3,
                    home_runs=hr,
                    walks=bb,
                    strikeouts=st["so"],
                    hbp=hbp,
                    avg=avg,
                    obp=obp,
                    slg=slg,
                    ops=ops,
                )
            )

        return results

    def sync_bvp_to_db(self, season_year: int) -> int:
        """Calculate and persist MatchupBvP entities for a season."""
        matrix = self.calculate_bvp_matchups(season_year)

        for m in matrix:
            bvp_row = (
                self.session.query(MatchupBvP)
                .filter(
                    MatchupBvP.season == season_year,
                    MatchupBvP.batter_id == m.batter_id,
                    MatchupBvP.pitcher_id == m.pitcher_id,
                )
                .first()
            )
            if not bvp_row:
                bvp_row = MatchupBvP(
                    season=season_year,
                    batter_id=m.batter_id,
                    pitcher_id=m.pitcher_id,
                )
                self.session.add(bvp_row)

            bvp_row.plate_appearances = m.plate_appearances
            bvp_row.at_bats = m.at_bats
            bvp_row.hits = m.hits
            bvp_row.doubles = m.doubles
            bvp_row.triples = m.triples
            bvp_row.home_runs = m.home_runs
            bvp_row.walks = m.walks
            bvp_row.strikeouts = m.strikeouts
            bvp_row.avg = m.avg
            bvp_row.obp = m.obp
            bvp_row.slg = m.slg
            bvp_row.ops = m.ops

        self.session.flush()
        return len(matrix)

    def calculate_situational_splits(self, season_year: int) -> list[SplitMetrics]:
        """Aggregate situational split metrics (e.g. RISP) from play-by-play events."""
        events = (
            self.session.query(GameEvent)
            .join(Game, Game.game_id == GameEvent.game_id)
            .filter(func.substr(Game.game_id, 1, 4) == str(season_year))
            .filter(GameEvent.batter_id.isnot(None))
            .all()
        )

        risp_map: dict[int, dict[str, int]] = {}
        for ev in events:
            desc = ev.description or ""
            # Consider RISP if runner on 2B or 3B
            is_risp = False
            if ev.bases_before and len(ev.bases_before) == BASES_LEN:
                is_risp = ev.bases_before[1] == "1" or ev.bases_before[2] == "1"
            elif ev.base_state is not None:
                is_risp = bool(ev.base_state & 6)  # 2nd or 3rd base bit
            elif "득점권" in desc or "주자 2루" in desc or "주자 3루" in desc:
                is_risp = True

            if not is_risp:
                continue

            b_id = ev.batter_id
            if b_id not in risp_map:
                risp_map[b_id] = {"pa": 0, "ab": 0, "h": 0, "rbi": 0}

            st = risp_map[b_id]
            st["pa"] += 1
            if any(k in desc for k in ["안타", "2루타", "3루타", "홈런"]):
                st["ab"] += 1
                st["h"] += 1
            elif "볼넷" not in desc and "사구" not in desc and "희생" not in desc:
                st["ab"] += 1
            st["rbi"] += ev.rbi or 0

        splits: list[SplitMetrics] = []
        for b_id, st in risp_map.items():
            avg = (st["h"] / st["ab"]) if st["ab"] > 0 else 0.0
            splits.append(
                SplitMetrics(
                    category="risp",
                    entity_id=b_id,
                    season=season_year,
                    split_key="RISP",
                    sample_size=st["pa"],
                    stats={"ab": st["ab"], "h": st["h"], "avg": round(avg, 3), "rbi": st["rbi"]},
                )
            )
        return splits

    def execute_all(self, season_year: int) -> dict[str, int]:
        """Execute full suite of matchup and split calculations for a given season."""
        from src.services.matchup_engine import MatchupEngine

        legacy_engine = MatchupEngine(self.session)
        legacy_engine.execute_all(season_year)

        bvp_count = self.sync_bvp_to_db(season_year)
        self.session.flush()
        return {
            "bvp_records": bvp_count,
            "season": season_year,
        }
