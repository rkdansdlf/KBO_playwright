from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import case, func, select

from src.db.engine import SessionLocal
from src.models.game import Game, GameBattingStat, GameEvent, GamePitchingStat
from src.models.matchup import (
    BatterSplit,
    BatterStadiumSplit,
    BatterTeamSplit,
    BatterVsStarter,
    MatchupBvP,
    PitcherSplit,
    PitcherTeamSplit,
)
from src.models.player import PlayerBasic

logger = logging.getLogger(__name__)


class MatchupEngine:
    """Service to aggregate splits matrices natively from Box Scores and Play-by-Play."""

    def __init__(self, session=None) -> None:
        self.session = session

    def execute_all(self, season_year: int) -> None:
        """Runs the entire suite of split calculations."""
        sess = self.session or SessionLocal()
        try:
            logger.info("📊 Recalculating matchups for %s...", season_year)
            self._calc_batter_team_splits(sess, season_year)
            self._calc_pitcher_team_splits(sess, season_year)
            self._calc_batter_stadium_splits(sess, season_year)
            self._calc_batter_vs_starter(sess, season_year)

            # Precise PBP based analytics
            self._calc_precise_bvp(sess, season_year)
            self._calc_situational_splits(sess, season_year)

            sess.commit()
            logger.info("✅ Matchup metrics recalculated for season %s", season_year)
        except Exception:
            sess.rollback()
            logger.exception("❌ Matchup engine failed")
            raise
        finally:
            if not self.session:
                sess.close()

    def _calc_precise_bvp(self, session, season_year: int) -> None:
        """Aggregates precise batter vs pitcher stats from GameEvents."""
        logger.info("   🎯 Calculating precise BvP for %s...", season_year)

        # We fetch all plate appearance events
        events = (
            session.query(GameEvent)
            .join(Game, Game.game_id == GameEvent.game_id)
            .filter(func.substr(Game.game_id, 1, 4) == str(season_year))
            .filter(GameEvent.batter_id.isnot(None))
            .filter(GameEvent.pitcher_id.isnot(None))
            .all()
        )

        # Aggregation Map: (batter_id, pitcher_id) -> stats
        bvp_map = {}

        for ev in events:
            key = (ev.batter_id, ev.pitcher_id)
            if key not in bvp_map:
                bvp_map[key] = {
                    "batter_name": ev.batter_name,
                    "pitcher_name": ev.pitcher_name,
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
                    "rbi": 0,
                }

            stats = bvp_map[key]
            stats["pa"] += 1

            desc = ev.description or ""
            is_hit = any(k in desc for k in ["안타", "2루타", "3루타", "홈런"])
            is_bb = "볼넷" in desc or "사구" in desc
            is_sf = "희생플라이" in desc

            if is_hit:
                stats["ab"] += 1
                stats["h"] += 1
                if "2루타" in desc:
                    stats["d2"] += 1
                elif "3루타" in desc:
                    stats["d3"] += 1
                elif "홈런" in desc:
                    stats["hr"] += 1
            elif is_bb:
                if "볼넷" in desc:
                    stats["bb"] += 1
                else:
                    stats["hbp"] += 1
            elif is_sf:
                stats["sf"] += 1
            else:
                if "희생번트" not in desc:
                    stats["ab"] += 1
                    if "삼진" in desc:
                        stats["so"] += 1

            stats["rbi"] += ev.rbi or 0

        # Upsert Logic: Check for existing career record
        for (bid, pid), s in bvp_map.items():
            existing = session.query(MatchupBvP).filter_by(batter_id=bid, pitcher_id=pid).first()
            if existing:
                existing.plate_appearances += s["pa"]
                existing.at_bats += s["ab"]
                existing.hits += s["h"]
                existing.doubles += s["d2"]
                existing.triples += s["d3"]
                existing.home_runs += s["hr"]
                existing.rbi += s["rbi"]
                existing.walks += s["bb"]
                existing.hbp += s["hbp"]
                existing.strikeouts += s["so"]
                existing.sacrifice_flies += s["sf"]
                # Recalculate
                avg, obp, slg, ops = self._calc_rate_stats(
                    existing.hits,
                    existing.at_bats,
                    existing.plate_appearances,
                    existing.walks,
                    existing.hbp,
                    existing.doubles,
                    existing.triples,
                    existing.home_runs,
                )
                existing.avg, existing.obp, existing.slg, existing.ops = avg, obp, slg, ops
            else:
                avg, obp, slg, ops = self._calc_rate_stats(
                    s["h"],
                    s["ab"],
                    s["pa"],
                    s["bb"],
                    s["hbp"],
                    s["d2"],
                    s["d3"],
                    s["hr"],
                )
                session.add(
                    MatchupBvP(
                        batter_id=bid,
                        batter_name=s["batter_name"],
                        pitcher_id=pid,
                        pitcher_name=s["pitcher_name"],
                        plate_appearances=s["pa"],
                        at_bats=s["ab"],
                        hits=s["h"],
                        doubles=s["d2"],
                        triples=s["d3"],
                        home_runs=s["hr"],
                        rbi=s["rbi"],
                        walks=s["bb"],
                        hbp=s["hbp"],
                        strikeouts=s["so"],
                        sacrifice_flies=s["sf"],
                        avg=avg,
                        obp=obp,
                        slg=slg,
                        ops=ops,
                    ),
                )

    def _clear_situational_splits(self, session, season_year: int) -> None:
        session.query(BatterSplit).filter(BatterSplit.season_year == season_year).delete()
        session.query(PitcherSplit).filter(PitcherSplit.season_year == season_year).delete()

    def _fetch_situational_events(self, session, season_year: int) -> list[GameEvent]:
        return (
            session.query(GameEvent)
            .join(Game, Game.game_id == GameEvent.game_id)
            .filter(func.substr(Game.game_id, 1, 4) == str(season_year))
            .filter(GameEvent.batter_id.isnot(None))
            .filter(GameEvent.pitcher_id.isnot(None))
            .all()
        )

    def _load_player_map(self, session) -> dict[int, PlayerBasic]:
        return {player.player_id: player for player in session.query(PlayerBasic).all()}

    @staticmethod
    def _empty_batter_split() -> dict[str, int]:
        return {"pa": 0, "ab": 0, "h": 0, "bb": 0, "hbp": 0, "hr": 0, "sf": 0}

    @staticmethod
    def _empty_pitcher_split() -> dict[str, int]:
        return {"bf": 0, "h": 0, "hr": 0, "bb": 0, "so": 0, "outs": 0}

    @staticmethod
    def _classify_event(description: str) -> dict[str, Any]:
        return {
            "desc": description,
            "is_hit": any(keyword in description for keyword in ["안타", "2루타", "3루타", "홈런"]),
            "is_hr": "홈런" in description,
            "is_bb": "볼넷" in description or "사구" in description,
            "is_sf": "희생플라이" in description,
            "is_pa": True,
            "is_ab": not ("볼넷" in description or "사구" in description)
            and "희생플라이" not in description
            and "희생번트" not in description,
        }

    def _update_batter_split(
        self,
        bat_splits: dict[int, dict[str, dict[str, int]]],
        player_id: int,
        split_type: str,
        event_flags: dict[str, Any],
    ) -> None:
        player_splits = bat_splits.setdefault(player_id, {})
        split = player_splits.setdefault(split_type, self._empty_batter_split())
        if event_flags["is_pa"]:
            split["pa"] += 1
        if event_flags["is_ab"]:
            split["ab"] += 1
        if event_flags["is_hit"]:
            split["h"] += 1
        if event_flags["is_hr"]:
            split["hr"] += 1
        if event_flags["is_bb"]:
            split["bb"] += 1
        if event_flags["is_sf"]:
            split["sf"] += 1

    def _update_pitcher_split(
        self,
        pit_splits: dict[int, dict[str, dict[str, int]]],
        player_id: int,
        split_type: str,
        event_flags: dict[str, Any],
    ) -> None:
        player_splits = pit_splits.setdefault(player_id, {})
        split = player_splits.setdefault(split_type, self._empty_pitcher_split())
        split["bf"] += 1
        if event_flags["is_hit"]:
            split["h"] += 1
        if event_flags["is_hr"]:
            split["hr"] += 1
        if event_flags["is_bb"]:
            split["bb"] += 1
        if "삼진" in event_flags["desc"]:
            split["so"] += 1
        if not event_flags["is_hit"] and not event_flags["is_bb"] and "실책" not in event_flags["desc"]:
            split["outs"] += 1

    def _update_situational_maps(
        self,
        event: GameEvent,
        players: dict[int, PlayerBasic],
        bat_splits: dict[int, dict[str, dict[str, int]]],
        pit_splits: dict[int, dict[str, dict[str, int]]],
    ) -> None:
        batter = players.get(event.batter_id)
        pitcher = players.get(event.pitcher_id)
        event_flags = self._classify_event(event.description or "")
        is_risp = "2" in (event.bases_before or "") or "3" in (event.bases_before or "")

        if is_risp:
            self._update_batter_split(bat_splits, event.batter_id, "RISP", event_flags)
            self._update_pitcher_split(pit_splits, event.pitcher_id, "RISP", event_flags)
        if pitcher:
            self._update_batter_split(bat_splits, event.batter_id, f"vs{pitcher.throws or 'R'}", event_flags)
        if batter:
            self._update_pitcher_split(pit_splits, event.pitcher_id, f"vs{batter.bats or 'R'}", event_flags)

    def _insert_batter_situational_splits(
        self,
        session,
        season_year: int,
        bat_splits: dict[int, dict[str, dict[str, int]]],
    ) -> None:
        for player_id, splits in bat_splits.items():
            for split_type, stats in splits.items():
                avg, obp, slg, ops = self._calc_rate_stats(
                    stats["h"],
                    stats["ab"],
                    stats["pa"],
                    stats["bb"],
                    stats["hbp"],
                    0,
                    0,
                    stats["hr"],
                    is_full=False,
                )
                session.add(
                    BatterSplit(
                        player_id=player_id,
                        season_year=season_year,
                        split_type=split_type,
                        plate_appearances=stats["pa"],
                        at_bats=stats["ab"],
                        hits=stats["h"],
                        home_runs=stats["hr"],
                        walks=stats["bb"],
                        avg=avg,
                        obp=obp,
                        slg=slg,
                        ops=ops,
                    ),
                )

    def _insert_pitcher_situational_splits(
        self,
        session,
        season_year: int,
        pit_splits: dict[int, dict[str, dict[str, int]]],
    ) -> None:
        for player_id, splits in pit_splits.items():
            for split_type, stats in splits.items():
                avg_against = (
                    round(stats["h"] / (stats["bf"] - stats["bb"]), 3) if (stats["bf"] - stats["bb"]) > 0 else 0.0
                )
                ip = stats["outs"] / 3.0
                whip = round((stats["h"] + stats["bb"]) / ip, 2) if ip > 0 else 0.0
                session.add(
                    PitcherSplit(
                        player_id=player_id,
                        season_year=season_year,
                        split_type=split_type,
                        batters_faced=stats["bf"],
                        innings_outs=stats["outs"],
                        hits_allowed=stats["h"],
                        home_runs_allowed=stats["hr"],
                        walks_allowed=stats["bb"],
                        strikeouts=stats["so"],
                        avg_against=avg_against,
                        whip=whip,
                    ),
                )

    def _calc_situational_splits(self, session, season_year: int) -> None:
        """Calculates RISP and vs Handedness splits using GameEvents."""
        logger.info("   📉 Calculating situational splits for %s...", season_year)

        self._clear_situational_splits(session, season_year)
        players = self._load_player_map(session)
        events = self._fetch_situational_events(session, season_year)
        bat_splits: dict[int, dict[str, dict[str, int]]] = {}
        pit_splits: dict[int, dict[str, dict[str, int]]] = {}

        for event in events:
            self._update_situational_maps(event, players, bat_splits, pit_splits)

        self._insert_batter_situational_splits(session, season_year, bat_splits)
        self._insert_pitcher_situational_splits(session, season_year, pit_splits)

    def _calc_batter_team_splits(self, session, season_year: int) -> None:
        """Aggregates batter stats partitioned by the opposing team."""
        # Find which team is the opponent
        opponent_case = case((GameBattingStat.team_code == Game.home_team, Game.away_team), else_=Game.home_team).label(
            "opponent_team_code",
        )

        stmt = (
            select(
                GameBattingStat.player_id,
                func.max(GameBattingStat.player_name).label("player_name"),
                GameBattingStat.team_code,
                opponent_case,
                func.count(GameBattingStat.id).label("games"),
                func.sum(GameBattingStat.plate_appearances).label("plate_appearances"),
                func.sum(GameBattingStat.at_bats).label("at_bats"),
                func.sum(GameBattingStat.runs).label("runs"),
                func.sum(GameBattingStat.hits).label("hits"),
                func.sum(GameBattingStat.doubles).label("doubles"),
                func.sum(GameBattingStat.triples).label("triples"),
                func.sum(GameBattingStat.home_runs).label("home_runs"),
                func.sum(GameBattingStat.rbi).label("rbi"),
                func.sum(GameBattingStat.walks).label("walks"),
                func.sum(GameBattingStat.intentional_walks).label("intentional_walks"),
                func.sum(GameBattingStat.hbp).label("hbp"),
                func.sum(GameBattingStat.strikeouts).label("strikeouts"),
                func.sum(GameBattingStat.stolen_bases).label("stolen_bases"),
                func.sum(GameBattingStat.caught_stealing).label("caught_stealing"),
                func.sum(GameBattingStat.gdp).label("gdp"),
            )
            .join(Game, Game.game_id == GameBattingStat.game_id)
            .where(func.substr(Game.game_id, 1, 4) == str(season_year), GameBattingStat.player_id.isnot(None))
            .group_by(GameBattingStat.player_id, GameBattingStat.team_code, opponent_case)
        )

        rows = session.execute(stmt).all()
        # Delete existing for year
        session.query(BatterTeamSplit).filter(BatterTeamSplit.season_year == season_year).delete()

        splits = []
        for row in rows:
            avg, obp, slg, ops = self._calc_rate_stats(
                row.hits,
                row.at_bats,
                row.plate_appearances,
                row.walks,
                row.hbp,
                row.doubles,
                row.triples,
                row.home_runs,
            )
            splits.append(
                BatterTeamSplit(
                    season_year=season_year,
                    league_type_code=0,  # Assuming Regular Season primarily for box scores here
                    player_id=row.player_id,
                    player_name=row.player_name,
                    team_code=row.team_code,
                    opponent_team_code=row.opponent_team_code,
                    games=row.games,
                    plate_appearances=row.plate_appearances,
                    at_bats=row.at_bats,
                    runs=row.runs,
                    hits=row.hits,
                    doubles=row.doubles,
                    triples=row.triples,
                    home_runs=row.home_runs,
                    rbi=row.rbi,
                    walks=row.walks,
                    intentional_walks=row.intentional_walks,
                    hbp=row.hbp,
                    strikeouts=row.strikeouts,
                    stolen_bases=row.stolen_bases,
                    caught_stealing=row.caught_stealing,
                    gdp=row.gdp,
                    avg=avg,
                    obp=obp,
                    slg=slg,
                    ops=ops,
                ),
            )
        session.add_all(splits)

    def _calc_pitcher_team_splits(self, session, season_year: int) -> None:
        opponent_case = case(
            (GamePitchingStat.team_code == Game.home_team, Game.away_team),
            else_=Game.home_team,
        ).label("opponent_team_code")

        stmt = (
            select(
                GamePitchingStat.player_id,
                func.max(GamePitchingStat.player_name).label("player_name"),
                GamePitchingStat.team_code,
                opponent_case,
                func.count(GamePitchingStat.id).label("games"),
                func.sum(GamePitchingStat.innings_outs).label("innings_outs"),
                func.sum(GamePitchingStat.batters_faced).label("batters_faced"),
                func.sum(GamePitchingStat.pitches).label("pitches"),
                func.sum(GamePitchingStat.hits_allowed).label("hits_allowed"),
                func.sum(GamePitchingStat.runs_allowed).label("runs_allowed"),
                func.sum(GamePitchingStat.earned_runs).label("earned_runs"),
                func.sum(GamePitchingStat.home_runs_allowed).label("home_runs_allowed"),
                func.sum(GamePitchingStat.walks_allowed).label("walks_allowed"),
                func.sum(GamePitchingStat.strikeouts).label("strikeouts"),
            )
            .join(Game, Game.game_id == GamePitchingStat.game_id)
            .where(func.substr(Game.game_id, 1, 4) == str(season_year), GamePitchingStat.player_id.isnot(None))
            .group_by(GamePitchingStat.player_id, GamePitchingStat.team_code, opponent_case)
        )

        rows = session.execute(stmt).all()
        session.query(PitcherTeamSplit).filter(PitcherTeamSplit.season_year == season_year).delete()

        splits = []
        for row in rows:
            era = 0.0
            whip = 0.0
            ip = float(row.innings_outs or 0) / 3.0
            if ip > 0:
                era = round(((row.earned_runs or 0) * 9.0) / ip, 2)
                whip = round(((row.hits_allowed or 0) + (row.walks_allowed or 0)) / ip, 2)
            splits.append(
                PitcherTeamSplit(
                    season_year=season_year,
                    league_type_code=0,
                    player_id=row.player_id,
                    player_name=row.player_name,
                    team_code=row.team_code,
                    opponent_team_code=row.opponent_team_code,
                    games=row.games,
                    innings_outs=row.innings_outs,
                    innings_pitched=round(ip, 1),
                    batters_faced=row.batters_faced,
                    pitches=row.pitches,
                    hits_allowed=row.hits_allowed,
                    runs_allowed=row.runs_allowed,
                    earned_runs=row.earned_runs,
                    home_runs_allowed=row.home_runs_allowed,
                    walks_allowed=row.walks_allowed,
                    strikeouts=row.strikeouts,
                    era=era,
                    whip=whip,
                ),
            )
        session.add_all(splits)

    def _calc_batter_stadium_splits(self, session, season_year: int) -> None:
        """Aggregates batter stats by stadium."""
        stmt = (
            select(
                GameBattingStat.player_id,
                func.max(GameBattingStat.player_name).label("player_name"),
                GameBattingStat.team_code,
                Game.stadium.label("stadium_name"),
                func.count(GameBattingStat.id).label("games"),
                func.sum(GameBattingStat.plate_appearances).label("plate_appearances"),
                func.sum(GameBattingStat.at_bats).label("at_bats"),
                func.sum(GameBattingStat.runs).label("runs"),
                func.sum(GameBattingStat.hits).label("hits"),
                func.sum(GameBattingStat.doubles).label("doubles"),
                func.sum(GameBattingStat.triples).label("triples"),
                func.sum(GameBattingStat.home_runs).label("home_runs"),
                func.sum(GameBattingStat.rbi).label("rbi"),
                func.sum(GameBattingStat.walks).label("walks"),
                func.sum(GameBattingStat.strikeouts).label("strikeouts"),
            )
            .join(Game, Game.game_id == GameBattingStat.game_id)
            .where(
                func.substr(Game.game_id, 1, 4) == str(season_year),
                Game.stadium.isnot(None),
                GameBattingStat.player_id.isnot(None),
            )
            .group_by(GameBattingStat.player_id, GameBattingStat.team_code, Game.stadium)
        )

        rows = session.execute(stmt).all()
        session.query(BatterStadiumSplit).filter(BatterStadiumSplit.season_year == season_year).delete()

        splits = []
        for row in rows:
            avg, obp, slg, ops = self._calc_rate_stats(
                row.hits,
                row.at_bats,
                row.plate_appearances,
                row.walks,
                0,
                row.doubles,
                row.triples,
                row.home_runs,
            )
            splits.append(
                BatterStadiumSplit(
                    season_year=season_year,
                    league_type_code=0,
                    player_id=row.player_id,
                    player_name=row.player_name,
                    team_code=row.team_code,
                    stadium_name=row.stadium_name,
                    games=row.games,
                    plate_appearances=row.plate_appearances,
                    at_bats=row.at_bats,
                    runs=row.runs,
                    hits=row.hits,
                    doubles=row.doubles,
                    triples=row.triples,
                    home_runs=row.home_runs,
                    rbi=row.rbi,
                    walks=row.walks,
                    strikeouts=row.strikeouts,
                    avg=avg,
                    obp=obp,
                    slg=slg,
                    ops=ops,
                ),
            )
        session.add_all(splits)

    def _calc_batter_vs_starter(self, session, season_year: int) -> None:
        """Determines the opposing starting pitcher heuristically and aggregates batter stats against them."""
        # Find which team is opposing, and get their starting pitcher from `Game`
        opposing_pitcher = case(
            (GameBattingStat.team_code == Game.home_team, Game.away_pitcher),
            else_=Game.home_pitcher,
        ).label("pitcher_name")

        stmt = (
            select(
                GameBattingStat.player_id,
                func.max(GameBattingStat.player_name).label("player_name"),
                opposing_pitcher,
                func.count(GameBattingStat.id).label("games"),
                func.sum(GameBattingStat.plate_appearances).label("plate_appearances"),
                func.sum(GameBattingStat.at_bats).label("at_bats"),
                func.sum(GameBattingStat.hits).label("hits"),
                func.sum(GameBattingStat.home_runs).label("home_runs"),
                func.sum(GameBattingStat.walks).label("walks"),
                func.sum(GameBattingStat.strikeouts).label("strikeouts"),
            )
            .join(Game, Game.game_id == GameBattingStat.game_id)
            .where(
                func.substr(Game.game_id, 1, 4) == str(season_year),
                opposing_pitcher.isnot(None),
                opposing_pitcher != "",
                GameBattingStat.player_id.isnot(None),
            )
            .group_by(GameBattingStat.player_id, opposing_pitcher)
        )

        rows = session.execute(stmt).all()
        session.query(BatterVsStarter).filter(BatterVsStarter.season_year == season_year).delete()

        splits = []
        for row in rows:
            avg, obp, slg, ops = self._calc_rate_stats(
                row.hits,
                row.at_bats,
                row.plate_appearances,
                row.walks,
                0,
                0,
                0,
                row.home_runs,
                is_full=False,
            )
            splits.append(
                BatterVsStarter(
                    season_year=season_year,
                    league_type_code=0,
                    player_id=row.player_id,
                    player_name=row.player_name,
                    pitcher_name=row.pitcher_name,
                    games=row.games,
                    plate_appearances=row.plate_appearances,
                    at_bats=row.at_bats,
                    hits=row.hits,
                    home_runs=row.home_runs,
                    walks=row.walks,
                    strikeouts=row.strikeouts,
                    avg=avg,
                    obp=obp,
                    slg=slg,
                    ops=ops,
                ),
            )
        session.add_all(splits)

    def _calc_rate_stats(
        self,
        hits,
        ab,
        pa,
        walks,
        hbp,
        double,
        triple,
        hr,
        is_full=True,
    ) -> tuple[float, float, float, float]:
        """Helper to calculate composite rates safely."""
        h = hits or 0
        ab = ab or 0
        pa = pa or 0
        bb = walks or 0
        hb = hbp or 0

        avg = round(h / ab, 3) if ab > 0 else 0.0

        obp = 0.0
        if pa > 0:
            obp = round((h + bb + hb) / pa, 3)

        slg = 0.0
        if is_full:
            single = h - (double or 0) - (triple or 0) - (hr or 0)
            tb = single + ((double or 0) * 2) + ((triple or 0) * 3) + ((hr or 0) * 4)
            slg = round(tb / ab, 3) if ab > 0 else 0.0

        ops = round(obp + slg, 3)
        return avg, obp, slg, ops
