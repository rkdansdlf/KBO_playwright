from typing import List, Dict, Any
from sqlalchemy import select, func, and_, case
from src.db.engine import SessionLocal
from src.models.game import Game, GameBattingStat, GamePitchingStat
from src.models.matchup import BatterTeamSplit, PitcherTeamSplit, BatterStadiumSplit, BatterVsStarter

class MatchupEngine:
    """Service to aggregate splits matrices natively from Box Scores."""
    
    def __init__(self, session=None):
        self.session = session

    def execute_all(self, season_year: int) -> None:
        """Runs the entire suite of split calculations."""
        sess = self.session or SessionLocal()
        try:
            self._calc_batter_team_splits(sess, season_year)
            self._calc_pitcher_team_splits(sess, season_year)
            self._calc_batter_stadium_splits(sess, season_year)
            self._calc_batter_vs_starter(sess, season_year)
            sess.commit()
            print(f"✅ Matchup metrics recalculated for season {season_year}")
        except Exception as e:
            sess.rollback()
            print(f"❌ Matchup engine failed: {e}")
            raise
        finally:
            if not self.session:
                sess.close()

    def _calc_batter_team_splits(self, session, season_year: int) -> None:
        """Aggregates batter stats partitioned by the opposing team."""
        # Find which team is the opponent
        opponent_case = case(
            (GameBattingStat.team_code == Game.home_team, Game.away_team),
            else_=Game.home_team
        ).label("opponent_team_code")

        stmt = select(
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
            func.sum(GameBattingStat.gdp).label("gdp")
        ).join(
            Game, Game.game_id == GameBattingStat.game_id
        ).where(
            func.substr(Game.game_id, 1, 4) == str(season_year),
            GameBattingStat.player_id.isnot(None)
        ).group_by(
            GameBattingStat.player_id,
            GameBattingStat.team_code,
            opponent_case
        )
        
        rows = session.execute(stmt).all()
        # Delete existing for year
        session.query(BatterTeamSplit).filter(BatterTeamSplit.season_year == season_year).delete()
        
        splits = []
        for row in rows:
            avg, obp, slg, ops = self._calc_rate_stats(row.hits, row.at_bats, row.plate_appearances, row.walks, row.hbp, row.doubles, row.triples, row.home_runs)
            splits.append(
                BatterTeamSplit(
                    season_year=season_year,
                    league_type_code=0, # Assuming Regular Season primarily for box scores here
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
                    ops=ops
                )
            )
        session.add_all(splits)

    def _calc_pitcher_team_splits(self, session, season_year: int) -> None:
        opponent_case = case(
            (GamePitchingStat.team_code == Game.home_team, Game.away_team),
            else_=Game.home_team
        ).label("opponent_team_code")

        stmt = select(
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
            func.sum(GamePitchingStat.strikeouts).label("strikeouts")
        ).join(
            Game, Game.game_id == GamePitchingStat.game_id
        ).where(
            func.substr(Game.game_id, 1, 4) == str(season_year),
            GamePitchingStat.player_id.isnot(None)
        ).group_by(
            GamePitchingStat.player_id,
            GamePitchingStat.team_code,
            opponent_case
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
                    whip=whip
                )
            )
        session.add_all(splits)

    def _calc_batter_stadium_splits(self, session, season_year: int) -> None:
        """Aggregates batter stats by stadium."""
        stmt = select(
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
            func.sum(GameBattingStat.strikeouts).label("strikeouts")
        ).join(
            Game, Game.game_id == GameBattingStat.game_id
        ).where(
            func.substr(Game.game_id, 1, 4) == str(season_year),
            Game.stadium.isnot(None),
            GameBattingStat.player_id.isnot(None)
        ).group_by(
            GameBattingStat.player_id,
            GameBattingStat.team_code,
            Game.stadium
        )

        rows = session.execute(stmt).all()
        session.query(BatterStadiumSplit).filter(BatterStadiumSplit.season_year == season_year).delete()
        
        splits = []
        for row in rows:
            avg, obp, slg, ops = self._calc_rate_stats(row.hits, row.at_bats, row.plate_appearances, row.walks, 0, row.doubles, row.triples, row.home_runs)
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
                    ops=ops
                )
            )
        session.add_all(splits)

    def _calc_batter_vs_starter(self, session, season_year: int) -> None:
        """Determines the opposing starting pitcher heuristically and aggregates batter stats against them."""
        # Find which team is opposing, and get their starting pitcher from `Game`
        opposing_pitcher = case(
            (GameBattingStat.team_code == Game.home_team, Game.away_pitcher),
            else_=Game.home_pitcher
        ).label("pitcher_name")

        stmt = select(
            GameBattingStat.player_id,
            func.max(GameBattingStat.player_name).label("player_name"),
            opposing_pitcher,
            func.count(GameBattingStat.id).label("games"),
            func.sum(GameBattingStat.plate_appearances).label("plate_appearances"),
            func.sum(GameBattingStat.at_bats).label("at_bats"),
            func.sum(GameBattingStat.hits).label("hits"),
            func.sum(GameBattingStat.home_runs).label("home_runs"),
            func.sum(GameBattingStat.walks).label("walks"),
            func.sum(GameBattingStat.strikeouts).label("strikeouts")
        ).join(
            Game, Game.game_id == GameBattingStat.game_id
        ).where(
            func.substr(Game.game_id, 1, 4) == str(season_year),
            opposing_pitcher.isnot(None),
            opposing_pitcher != "",
            GameBattingStat.player_id.isnot(None)
        ).group_by(
            GameBattingStat.player_id,
            opposing_pitcher
        )

        rows = session.execute(stmt).all()
        session.query(BatterVsStarter).filter(BatterVsStarter.season_year == season_year).delete()

        splits = []
        for row in rows:
            avg, obp, slg, ops = self._calc_rate_stats(row.hits, row.at_bats, row.plate_appearances, row.walks, 0, 0, 0, row.home_runs, is_full=False)
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
                    ops=ops
                )
            )
        session.add_all(splits)

    def _calc_rate_stats(self, hits, ab, pa, walks, hbp, double, triple, hr, is_full=True):
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
