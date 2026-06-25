"""
Context Aggregator Service
Calculates derived metrics (Head-to-head, Streaks, Trends, WPA moments)
to provide rich context for LLM analysis.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any

from sqlalchemy import and_, desc, func, or_

from src.constants import KST
from src.models.game import (
    Game,
    GameBattingStat,
    GameEvent,
    GameLineup,
    GamePitchingStat,
    GameSummary,
)
from src.models.player import PlayerBasic, PlayerMovement, PlayerSeasonBatting, PlayerSeasonPitching
from src.models.team import TeamDailyRoster
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES
from src.utils.relay_text import is_relay_noise_text

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class ContextAggregator:
    def __init__(self, session: Session) -> None:
        self.session = session

    @staticmethod
    def _pitching_outs_from_value(innings_pitched: object) -> int | None:
        if innings_pitched is None:
            return None
        try:
            value = float(innings_pitched)
        except (TypeError, ValueError):
            return None

        whole = int(value)
        fractional = round(value - whole, 3)
        if abs(fractional - 0.1) < 0.02:
            return whole * 3 + 1
        if abs(fractional - 0.2) < 0.02:
            return whole * 3 + 2
        return round(value * 3)

    @classmethod
    def _pitching_outs(cls, row: GamePitchingStat) -> int:
        if row.innings_outs is not None:
            return int(row.innings_outs or 0)
        return cls._pitching_outs_from_value(row.innings_pitched) or 0

    @staticmethod
    def _innings_display_from_outs(outs: int) -> str:
        whole = int(outs or 0) // 3
        remainder = int(outs or 0) % 3
        return f"{whole}.{remainder}"

    @classmethod
    def _pitching_game_line(cls, row: GamePitchingStat) -> dict[str, Any]:
        outs = cls._pitching_outs(row)
        return {
            "innings_outs": outs,
            "innings_pitched": cls._innings_display_from_outs(outs),
            "batters_faced": row.batters_faced,
            "pitches": row.pitches,
            "hits_allowed": row.hits_allowed,
            "runs_allowed": row.runs_allowed,
            "earned_runs": row.earned_runs,
            "home_runs_allowed": row.home_runs_allowed,
            "walks_allowed": row.walks_allowed,
            "strikeouts": row.strikeouts,
            "decision": row.decision,
            "wins": row.wins,
            "losses": row.losses,
            "saves": row.saves,
            "holds": row.holds,
            "era": row.era,
            "whip": row.whip,
        }

    @staticmethod
    def _pitching_season_line(
        row: PlayerSeasonPitching | None,
    ) -> dict[str, Any] | None:
        if row is None:
            return None
        return {
            "season": row.season,
            "league": row.league,
            "team_code": row.team_code,
            "games": row.games,
            "games_started": row.games_started,
            "wins": row.wins,
            "losses": row.losses,
            "saves": row.saves,
            "holds": row.holds,
            "innings_pitched": row.innings_pitched,
            "innings_outs": row.innings_outs,
            "quality_starts": row.quality_starts,
            "era": row.era,
            "whip": row.whip,
            "fip": row.fip,
            "kbb": row.kbb,
        }

    @classmethod
    def _pitching_payload_row(
        cls,
        row: GamePitchingStat,
        season_row: PlayerSeasonPitching | None,
    ) -> dict[str, Any]:
        return {
            "team_side": row.team_side,
            "team_code": row.team_code,
            "player_id": row.player_id,
            "player_name": row.player_name,
            "appearance_seq": row.appearance_seq,
            "role": "starter" if bool(row.is_starting) else "bullpen",
            "is_starting": bool(row.is_starting),
            "game_line": cls._pitching_game_line(row),
            "season_line": cls._pitching_season_line(season_row),
            "season_stats_found": season_row is not None,
        }

    @staticmethod
    def _empty_bullpen_payload() -> dict[str, Any]:
        return {
            "pitchers": [],
            "totals": {
                "pitchers": 0,
                "innings_outs": 0,
                "innings_pitched": "0.0",
                "pitches": 0,
                "hits_allowed": 0,
                "runs_allowed": 0,
                "earned_runs": 0,
                "walks_allowed": 0,
                "strikeouts": 0,
            },
        }

    def get_completed_game_pitching_breakdown(
        self,
        game_id: str,
        season_year: int | None = None,
    ) -> dict[str, Any]:
        """Return starter and bullpen lines for a completed game's Coach payload.

        The game box-score table is authoritative for who appeared in the game.
        Season pitching rows are joined only as optional enrichment, so Coach
        sections still render when player/game stats exist but season aggregates
        or player master joins are incomplete.
        """
        game = self.session.query(Game).filter(Game.game_id == game_id).first()
        if season_year is None:
            if game and game.game_date:
                season_year = game.game_date.year
            else:
                try:
                    season_year = int(str(game_id)[:4])
                except (TypeError, ValueError):
                    season_year = None

        rows = (
            self.session.query(GamePitchingStat)
            .filter(GamePitchingStat.game_id == game_id)
            .order_by(
                GamePitchingStat.team_side.asc(),
                GamePitchingStat.appearance_seq.asc(),
                GamePitchingStat.id.asc(),
            )
            .all()
        )

        season_rows = self._season_pitching_rows(rows, season_year)

        starters: dict[str, dict[str, Any] | None] = {"away": None, "home": None}
        bullpen: dict[str, dict[str, Any]] = {
            "away": self._empty_bullpen_payload(),
            "home": self._empty_bullpen_payload(),
        }
        unmatched_season_stats: list[dict[str, Any]] = []

        for row in rows:
            season_row = season_rows.get(int(row.player_id)) if row.player_id is not None else None
            payload_row = self._pitching_payload_row(row, season_row)
            side = row.team_side if row.team_side in {"away", "home"} else "unknown"
            if not season_row:
                unmatched_season_stats.append(
                    {
                        "team_side": row.team_side,
                        "team_code": row.team_code,
                        "player_id": row.player_id,
                        "player_name": row.player_name,
                        "role": payload_row["role"],
                    },
                )

            if self._assign_pitching_role(row, payload_row, side, starters, bullpen):
                continue

        for side_payload in bullpen.values():
            totals = side_payload["totals"]
            totals["innings_pitched"] = self._innings_display_from_outs(totals["innings_outs"])

        raw_counts = {
            "game_pitching_rows": len(rows),
            "starter_rows": sum(1 for row in rows if bool(row.is_starting)),
            "bullpen_rows": sum(1 for row in rows if not bool(row.is_starting)),
            "player_id_missing_rows": sum(1 for row in rows if row.player_id is None),
            "season_pitching_matches": sum(
                1 for row in rows if row.player_id is not None and int(row.player_id) in season_rows
            ),
        }

        return {
            "game_id": game_id,
            "season_year": season_year,
            "raw_counts": raw_counts,
            "starters": starters,
            "bullpen": bullpen,
            "unmatched_season_stats": unmatched_season_stats,
        }

    def _season_pitching_rows(
        self,
        rows: list[GamePitchingStat],
        season_year: int | None,
    ) -> dict[int, PlayerSeasonPitching]:
        player_ids = sorted({row.player_id for row in rows if row.player_id is not None})
        if not player_ids or not season_year:
            return {}
        return {
            int(row.player_id): row
            for row in self.session.query(PlayerSeasonPitching)
            .filter(
                PlayerSeasonPitching.player_id.in_(player_ids),
                PlayerSeasonPitching.season == season_year,
                PlayerSeasonPitching.league == "REGULAR",
            )
            .all()
        }

    def _assign_pitching_role(
        self,
        row: GamePitchingStat,
        payload_row: dict[str, Any],
        side: str,
        starters: dict[str, dict[str, Any] | None],
        bullpen: dict[str, dict[str, Any]],
    ) -> bool:
        if bool(row.is_starting):
            if side in starters and starters[side] is None:
                starters[side] = payload_row
            return True
        if side not in bullpen:
            return True
        self._append_bullpen_pitcher(bullpen[side], payload_row)
        return False

    def _append_bullpen_pitcher(self, side_payload: dict[str, Any], payload_row: dict[str, Any]) -> None:
        side_payload["pitchers"].append(payload_row)
        totals = side_payload["totals"]
        game_line = payload_row["game_line"]
        totals["pitchers"] += 1
        totals["innings_outs"] += int(game_line.get("innings_outs") or 0)
        for key in ("pitches", "hits_allowed", "runs_allowed", "earned_runs", "walks_allowed", "strikeouts"):
            totals[key] += int(game_line.get(key) or 0)

    def diagnose_completed_game_coach_pitching(self, game_id: str) -> dict[str, Any]:
        """Trace pitcher data from raw tables through repository output to review JSON."""
        breakdown = self.get_completed_game_pitching_breakdown(game_id)
        raw = breakdown["raw_counts"]

        summaries = (
            self.session.query(GameSummary)
            .filter(
                GameSummary.game_id == game_id,
                GameSummary.summary_type == "리뷰_WPA",
            )
            .all()
        )

        final_payload = self._diagnose_final_pitching_payload(summaries)

        repository_starter_rows = sum(1 for row in breakdown["starters"].values() if row)
        repository_bullpen_rows = sum(len(side.get("pitchers") or []) for side in breakdown["bullpen"].values())
        expected_player_rows = raw["game_pitching_rows"] - raw["player_id_missing_rows"]
        warnings: list[str] = []
        if raw["season_pitching_matches"] < expected_player_rows:
            warnings.append("season_pitching_join_incomplete")

        drop_stage = self._coach_pitching_drop_stage(
            raw, repository_starter_rows, repository_bullpen_rows, final_payload
        )

        return {
            "game_id": game_id,
            "drop_stage": drop_stage,
            "warnings": warnings,
            "raw_tables": raw,
            "repository": {
                "starter_rows": repository_starter_rows,
                "bullpen_rows": repository_bullpen_rows,
                "season_pitching_matches": raw["season_pitching_matches"],
                "unmatched_season_stats": breakdown["unmatched_season_stats"],
            },
            "final_payload": {
                "review_summary_found": final_payload["found"],
                "review_summary_rows": len(summaries),
                "pitching_breakdown_found": final_payload["has_pitching"],
                "starter_rows": final_payload["starter_rows"],
                "bullpen_rows": final_payload["bullpen_rows"],
            },
        }

    def _diagnose_final_pitching_payload(self, summaries: list[GameSummary]) -> dict[str, Any]:
        result = {"found": False, "has_pitching": False, "starter_rows": 0, "bullpen_rows": 0}
        for summary in summaries:
            payload = self._summary_payload(summary)
            if not isinstance(payload, dict):
                continue
            result["found"] = True
            final_pitching = payload.get("pitching_breakdown") or payload.get("pitching")
            if not isinstance(final_pitching, dict):
                continue

            starter_rows, bullpen_rows = self._count_final_pitching_rows(final_pitching)
            if starter_rows or bullpen_rows:
                result["has_pitching"] = True
                result["starter_rows"] = max(result["starter_rows"], starter_rows)
                result["bullpen_rows"] = max(result["bullpen_rows"], bullpen_rows)
        return result

    def _summary_payload(self, summary: GameSummary) -> dict[str, Any] | None:
        if not summary.detail_text:
            return None
        try:
            payload = json.loads(summary.detail_text)
        except (TypeError, ValueError):
            return {}
        return payload if isinstance(payload, dict) else None

    def _count_final_pitching_rows(self, final_pitching: dict[str, Any]) -> tuple[int, int]:
        starters = final_pitching.get("starters") or {}
        bullpen = final_pitching.get("bullpen") or {}
        starter_rows = sum(1 for value in starters.values() if isinstance(value, dict))
        bullpen_rows = sum(
            len((value or {}).get("pitchers") or []) for value in bullpen.values() if isinstance(value, dict)
        )
        return starter_rows, bullpen_rows

    def _coach_pitching_drop_stage(
        self,
        raw: dict[str, Any],
        repository_starter_rows: int,
        repository_bullpen_rows: int,
        final_payload: dict[str, Any],
    ) -> str:
        if raw["game_pitching_rows"] == 0:
            return "raw_game_pitching_stats_missing"
        if raw["starter_rows"] == 0:
            return "raw_starter_flags_missing"
        if repository_starter_rows == 0 or (raw["bullpen_rows"] > 0 and repository_bullpen_rows == 0):
            return "repository_pitching_rows_missing"
        if not final_payload["found"]:
            return "final_review_payload_missing"
        if not final_payload["has_pitching"]:
            return "final_review_payload_missing_pitching"
        if final_payload["starter_rows"] == 0 or (raw["bullpen_rows"] > 0 and final_payload["bullpen_rows"] == 0):
            return "final_review_payload_pitching_empty"
        return "ok"

    def get_team_l10_summary(self, team_code: str, target_date: date) -> dict[str, Any]:
        """최근 10경기 승패 및 연승/연패 흐름 계산"""
        games = (
            self.session.query(Game)
            .filter(
                or_(Game.home_team == team_code, Game.away_team == team_code),
                Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
                Game.game_date < target_date,
            )
            .order_by(desc(Game.game_date))
            .limit(10)
            .all()
        )

        wins, losses, draws = 0, 0, 0
        streak_type = None
        streak_count = 0

        results = []
        for g in games:
            if g.home_score is None or g.away_score is None:
                continue
            is_home = g.home_team == team_code
            my_score = g.home_score if is_home else g.away_score
            opp_score = g.away_score if is_home else g.home_score

            if my_score > opp_score:
                wins += 1
                results.append("W")
            elif my_score < opp_score:
                losses += 1
                results.append("L")
            else:
                draws += 1
                results.append("D")

        # 연승/연패 계산 (가장 최근 경기부터 역순)
        if results:
            streak_type = results[0]
            for r in results:
                if r == streak_type:
                    streak_count += 1
                else:
                    break

        return {
            "team_code": team_code,
            "wins": wins,
            "losses": losses,
            "draws": draws,
            "l10_text": f"{wins}승 {losses}패 {draws}무",
            "streak": f"{streak_count}{'연승' if streak_type == 'W' else '연패' if streak_type == 'L' else '무'}"
            if streak_type
            else "-",
        }

    def get_head_to_head_summary(self, team_a: str, team_b: str, season_year: int, target_date: date) -> dict[str, Any]:
        """올 시즌 두 팀간의 맞대결 전적 계산"""
        games = (
            self.session.query(Game)
            .filter(
                or_(
                    and_(Game.home_team == team_a, Game.away_team == team_b),
                    and_(Game.home_team == team_b, Game.away_team == team_a),
                ),
                Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
                Game.game_date < target_date,
                func.substr(Game.game_id, 1, 4) == str(season_year),
            )
            .all()
        )

        a_wins, b_wins, draws = 0, 0, 0
        for g in games:
            if g.home_score is None or g.away_score is None:
                continue
            # team_a 기준 승패
            if g.home_team == team_a:
                if g.home_score > g.away_score:
                    a_wins += 1
                elif g.home_score < g.away_score:
                    b_wins += 1
                else:
                    draws += 1
            elif g.away_score > g.home_score:
                a_wins += 1
            elif g.away_score < g.home_score:
                b_wins += 1
            else:
                draws += 1

        superior = team_a if a_wins > b_wins else team_b if b_wins > a_wins else "동률"

        return {
            "matchup": f"{team_a} vs {team_b}",
            "a_wins": a_wins,
            "b_wins": b_wins,
            "draws": draws,
            "summary_text": f"{a_wins}승 {b_wins}패 {draws}무 ({superior} 우세)",
        }

    def get_crucial_moments(self, game_id: str, limit: int = 3) -> list[dict[str, Any]]:
        """WPA 기반 승부처(하이라이트) 추출"""
        candidate_limit = max(limit * 10, 25)
        events = (
            self.session.query(GameEvent)
            .filter(
                GameEvent.game_id == game_id,
                GameEvent.wpa.isnot(None),
                GameEvent.event_type.isnot(None),
                ~func.lower(GameEvent.event_type).in_(("unknown", "other", "substitution")),
            )
            .order_by(desc(func.abs(GameEvent.wpa)))
            .limit(candidate_limit)
            .all()
        )

        moments = []
        for e in events:
            if is_relay_noise_text(e.description):
                continue
            moments.append(
                {
                    "inning": f"{e.inning}회{'초' if e.inning_half == 'top' else '말'}",
                    "description": e.description,
                    "wpa": e.wpa,
                    "score": f"{e.away_score}:{e.home_score}",
                    "batter": e.batter_name,
                    "pitcher": e.pitcher_name,
                },
            )
            if len(moments) >= limit:
                break
        return moments

    def get_team_recent_metrics(self, team_code: str, target_date: date, limit_games: int = 10) -> dict[str, Any]:
        """최근 N경기 동안의 팀 타격/투구 지표 요약"""
        # 최근 경기들 ID 확보
        game_ids = [
            r[0]
            for r in self.session.query(Game.game_id)
            .filter(
                or_(Game.home_team == team_code, Game.away_team == team_code),
                Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
                Game.game_date < target_date,
            )
            .order_by(desc(Game.game_date))
            .limit(limit_games)
            .all()
        ]

        if not game_ids:
            return {}

        # 팀 타율 계산
        batting = (
            self.session.query(
                func.sum(GameBattingStat.hits).label("hits"),
                func.sum(GameBattingStat.at_bats).label("ab"),
            )
            .filter(GameBattingStat.game_id.in_(game_ids), GameBattingStat.team_code == team_code)
            .first()
        )

        avg = round(batting.hits / batting.ab, 3) if batting and batting.ab else 0

        # 팀 평균자책점(ERA) 계산
        pitching = (
            self.session.query(
                func.sum(GamePitchingStat.earned_runs).label("er"),
                func.sum(GamePitchingStat.innings_outs).label("outs"),
            )
            .filter(GamePitchingStat.game_id.in_(game_ids), GamePitchingStat.team_code == team_code)
            .first()
        )

        era = round((pitching.er * 27) / pitching.outs, 2) if pitching and pitching.outs else 0

        # 불펜 평균자책점(ERA) 및 이닝당 출루허용률(WHIP) 계산
        bullpen = (
            self.session.query(
                func.sum(GamePitchingStat.earned_runs).label("er"),
                func.sum(GamePitchingStat.hits_allowed).label("hits"),
                func.sum(GamePitchingStat.walks_allowed).label("walks"),
                func.sum(GamePitchingStat.innings_outs).label("outs"),
            )
            .filter(
                GamePitchingStat.game_id.in_(game_ids),
                GamePitchingStat.team_code == team_code,
                not GamePitchingStat.is_starting,
            )
            .first()
        )

        bp_era = round((bullpen.er * 27) / bullpen.outs, 2) if bullpen and bullpen.outs and bullpen.outs > 0 else None
        bp_whip = (
            round(((bullpen.hits or 0) + (bullpen.walks or 0)) * 3 / bullpen.outs, 2)
            if bullpen and bullpen.outs and bullpen.outs > 0
            else None
        )
        bp_ip = round(bullpen.outs / 3.0, 1) if bullpen and bullpen.outs else 0

        return {
            "avg": avg,
            "era": era,
            "bullpen_era": bp_era,
            "bullpen_whip": bp_whip,
            "bullpen_ip": bp_ip,
            "sample_games": len(game_ids),
        }

    def get_postseason_series_summary(
        self,
        team_a: str,
        team_b: str,
        season_year: int,
        target_date: date,
    ) -> dict[str, Any] | None:
        """포스트시즌 시리즈 전적(예: 준플레이오프 1승 2패) 계산"""
        # 현재 경기의 시리즈 유형 파악을 위해 1경기 조회
        sample_game = (
            self.session.query(Game)
            .filter(
                or_(
                    and_(Game.home_team == team_a, Game.away_team == team_b),
                    and_(Game.home_team == team_b, Game.away_team == team_a),
                ),
                Game.game_date <= target_date,
                func.substr(Game.game_id, 1, 4) == str(season_year),
            )
            .first()
        )

        if not sample_game or not sample_game.season_id:
            return None

        # season_id가 정규시즌(보통 코드 0)이 아닌 경우만 처리
        # kbo_seasons 테이블에서 league_type_code 확인 필요 (보통 2:와일드카드, 3:준PO, 4:PO, 5:한국시리즈)
        # 여기서는 단순하게 정규시즌 ID가 아닌 경우를 포스트시즌으로 간주하거나
        # season_id 범위를 통해 필터링 가능 (프로젝트 규칙에 따라)

        # OCI DB 기준: 정규시즌은 대개 season_year와 동일하거나 별도 매핑됨.
        # 여기서는 해당 season_id의 모든 맞대결을 합산.
        games = (
            self.session.query(Game)
            .filter(
                Game.season_id == sample_game.season_id,
                or_(
                    and_(Game.home_team == team_a, Game.away_team == team_b),
                    and_(Game.home_team == team_b, Game.away_team == team_a),
                ),
                Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
                Game.game_date < target_date,
            )
            .all()
        )

        if not games:
            return None

        a_wins, b_wins, draws = 0, 0, 0
        for g in games:
            if g.home_score is None or g.away_score is None:
                continue
            if g.home_team == team_a:
                if g.home_score > g.away_score:
                    a_wins += 1
                elif g.home_score < g.away_score:
                    b_wins += 1
                else:
                    draws += 1
            elif g.away_score > g.home_score:
                a_wins += 1
            elif g.away_score < g.home_score:
                b_wins += 1
            else:
                draws += 1

        return {
            "season_id": sample_game.season_id,
            "team_a": team_a,
            "team_b": team_b,
            "a_wins": a_wins,
            "b_wins": b_wins,
            "draws": draws,
            "series_text": f"시리즈 성적: {a_wins}승 {b_wins}패 {draws}무",
        }

    def get_pitcher_season_stats(self, player_id: int, season_year: int) -> dict[str, Any] | None:
        """선발 투수의 해당 시즌 성적 조회"""
        if not player_id:
            return None

        stats = (
            self.session.query(PlayerSeasonPitching)
            .filter(
                PlayerSeasonPitching.player_id == player_id,
                PlayerSeasonPitching.season == season_year,
                PlayerSeasonPitching.league == "REGULAR",
            )
            .first()
        )

        if not stats:
            return None

        return {
            "player_id": player_id,
            "season": season_year,
            "era": stats.era,
            "wins": stats.wins,
            "losses": stats.losses,
            "saves": stats.saves,
            "holds": stats.holds,
            "games": stats.games,
            "innings": stats.innings_pitched,
            "summary_text": f"{stats.wins}승 {stats.losses}패 {stats.era}ERA",
        }

    def get_recent_player_movements(
        self, team_code: str, target_date: str | date | datetime, days: int = 7
    ) -> list[dict[str, Any]]:
        """최근 N일간 해당 팀의 선수 이동 현황(부상, 트레이드 등) 조회"""
        if isinstance(target_date, str):
            target_date = datetime.strptime(target_date.replace("-", ""), "%Y%m%d").replace(tzinfo=KST).date()

        # HT -> KIA, LT -> 롯데 등 한국어 이름으로도 검색 가능하도록 확장
        # (테이블에 한국어 이름과 영문 코드가 섞여 있을 수 있음)
        possible_names = [team_code]
        # KIA, 롯데 등 한국어 이름을 포함시키기 위해 resolve_team_code 활용 (역방향 필요할 수도 있음)
        # 우선 수동 매핑 추가 (가장 확실한 방법)
        team_name_map = {
            "HT": "KIA",
            "LT": "롯데",
            "SS": "삼성",
            "OB": "두산",
            "HH": "한화",
            "KT": "KT",
            "NC": "NC",
            "SK": "SSG",
            "WO": "키움",
            "KH": "키움",
            "KIA": "KIA",
            "롯데": "롯데",
            "삼성": "삼성",
            "두산": "두산",
            "한화": "한화",
            "SSG": "SSG",
            "키움": "키움",
        }
        canonical_team_map = {
            "HT": "KIA",
            "KIA": "KIA",
            "롯데": "LT",
            "LT": "LT",
            "삼성": "SS",
            "SS": "SS",
            "두산": "DB",
            "OB": "OB",
            "DB": "DB",
            "한화": "HH",
            "HH": "HH",
            "SSG": "SSG",
            "SK": "SK",
            "키움": "KH",
            "KH": "KH",
            "WO": "WO",
            "NX": "NX",
            "KT": "KT",
            "NC": "NC",
            "LG": "LG",
        }
        if team_code in team_name_map:
            possible_names.append(team_name_map[team_code])

        # 롯데/NC/KT 등은 그대로 쓰거나 영문 코드를 추가
        reverse_map = {v: k for k, v in team_name_map.items()}
        if team_code in reverse_map:
            possible_names.append(reverse_map[team_code])
        canonical_codes = list(dict.fromkeys([team_code, canonical_team_map.get(team_code, team_code)]))

        start_date = target_date - timedelta(days=days)
        movements = (
            self.session.query(PlayerMovement)
            .filter(
                or_(
                    PlayerMovement.canonical_team_id.in_(canonical_codes),
                    PlayerMovement.team_code.in_(possible_names),
                ),
                PlayerMovement.movement_date >= start_date,
                PlayerMovement.movement_date <= target_date,
            )
            .order_by(desc(PlayerMovement.movement_date))
            .all()
        )

        return [
            {
                "date": m.movement_date.isoformat(),
                "section": m.section,
                "player": m.player_name,
                "remarks": m.remarks,
            }
            for m in movements
        ]

    def get_daily_roster_changes(self, team_code: str, target_date: str | date | datetime) -> dict[str, list[str]]:
        """해당 날짜의 1군 등록/말소 현황 비교 (어제와 비교)"""
        if isinstance(target_date, str):
            target_date = datetime.strptime(target_date.replace("-", ""), "%Y%m%d").replace(tzinfo=KST).date()

        prev_date = target_date - timedelta(days=1)

        curr_roster = (
            self.session.query(TeamDailyRoster)
            .filter(TeamDailyRoster.team_code == team_code, TeamDailyRoster.roster_date == target_date)
            .all()
        )

        prev_roster = (
            self.session.query(TeamDailyRoster)
            .filter(TeamDailyRoster.team_code == team_code, TeamDailyRoster.roster_date == prev_date)
            .all()
        )

        # 만약 전날 데이터가 없으면 '추가/삭제'를 판단할 수 없음
        if not curr_roster or not prev_roster:
            return {"added": [], "removed": []}

        curr_ids = {r.player_id: r.player_name for r in curr_roster}
        prev_ids = {r.player_id: r.player_name for r in prev_roster}

        added = [name for pid, name in curr_ids.items() if pid not in prev_ids]
        removed = [name for pid, name in prev_ids.items() if pid not in curr_ids]

        return {"added": added, "removed": removed}

    def get_team_error_games(
        self,
        team_code: str,
        season_year: int,
        target_date: str | date | datetime | None = None,
    ) -> list[dict[str, Any]]:
        """팀이 특정 경기에서 실책을 범한 경기 목록 및 실책 상세 정보 반환"""
        if isinstance(target_date, str):
            target_date = datetime.strptime(target_date.replace("-", ""), "%Y%m%d").replace(tzinfo=KST).date()

        # Resolve team code to canonical/aliases
        canonical_map = {
            "HT": "KIA",
            "KIA": "KIA",
            "롯데": "LT",
            "LT": "LT",
            "삼성": "SS",
            "SS": "SS",
            "두산": "DB",
            "OB": "OB",
            "DB": "DB",
            "한화": "HH",
            "HH": "HH",
            "SSG": "SSG",
            "SK": "SK",
            "키움": "KH",
            "KH": "KH",
            "WO": "WO",
            "NX": "NX",
            "KT": "KT",
            "NC": "NC",
            "LG": "LG",
        }
        possible_codes = [team_code, canonical_map.get(team_code, team_code)]

        query = (
            self.session.query(
                Game.game_id,
                Game.game_date,
                Game.home_team,
                Game.away_team,
                Game.home_score,
                Game.away_score,
                GameSummary.player_name,
                GameSummary.detail_text,
            )
            .join(GameSummary, Game.game_id == GameSummary.game_id)
            .join(
                GameLineup,
                and_(
                    Game.game_id == GameLineup.game_id,
                    or_(
                        GameSummary.player_id == GameLineup.player_id,
                        and_(GameSummary.player_id.is_(None), GameSummary.player_name == GameLineup.player_name),
                    ),
                ),
            )
            .filter(
                GameSummary.summary_type == "실책",
                or_(GameLineup.team_code.in_(possible_codes), GameLineup.canonical_team_code.in_(possible_codes)),
                func.substr(Game.game_id, 1, 4) == str(season_year),
            )
        )

        if target_date:
            query = query.filter(Game.game_date <= target_date)

        rows = query.order_by(desc(Game.game_date)).all()

        games_dict = {}
        for r in rows:
            g_id = r.game_id
            if g_id not in games_dict:
                games_dict[g_id] = {
                    "game_id": g_id,
                    "game_date": r.game_date.isoformat() if r.game_date else None,
                    "matchup": f"{r.away_team} vs {r.home_team}",
                    "score": f"{r.away_score}:{r.home_score}",
                    "errors": [],
                }
            games_dict[g_id]["errors"].append({"player": r.player_name, "detail": r.detail_text})

        return list(games_dict.values())

    def get_toughest_opponents(
        self,
        team_code: str,
        season_year: int,
        target_date: str | date | datetime | None = None,
    ) -> list[dict[str, Any]]:
        """상대팀별 승률을 계산하여 가장 까다로운(우리팀 승률이 낮은) 순으로 정렬하여 반환"""
        if isinstance(target_date, str):
            target_date = datetime.strptime(target_date.replace("-", ""), "%Y%m%d").replace(tzinfo=KST).date()

        query = self.session.query(Game).filter(
            or_(Game.home_team == team_code, Game.away_team == team_code),
            Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
            func.substr(Game.game_id, 1, 4) == str(season_year),
        )
        if target_date:
            query = query.filter(Game.game_date <= target_date)

        games = query.all()

        opponents = {}
        for g in games:
            if g.home_score is None or g.away_score is None:
                continue
            is_home = g.home_team == team_code
            opp = g.away_team if is_home else g.home_team

            my_score = g.home_score if is_home else g.away_score
            opp_score = g.away_score if is_home else g.home_score

            if opp not in opponents:
                opponents[opp] = {"wins": 0, "losses": 0, "draws": 0}

            if my_score > opp_score:
                opponents[opp]["wins"] += 1
            elif my_score < opp_score:
                opponents[opp]["losses"] += 1
            else:
                opponents[opp]["draws"] += 1

        results = []
        for opp, stats in opponents.items():
            w = stats["wins"]
            losses = stats["losses"]
            d = stats["draws"]
            total = w + losses
            win_rate = w / total if total > 0 else 0.0
            results.append(
                {
                    "opponent": opp,
                    "wins": w,
                    "losses": losses,
                    "draws": d,
                    "win_rate": round(win_rate, 3),
                    "summary_text": f"{w}승 {losses}패 {d}무",
                },
            )

        results.sort(key=lambda x: (x["win_rate"], -x["losses"]))
        return results

    def get_position_avg_comparison(self, player_id: int, position: str, season_year: int) -> dict[str, Any]:
        """특정 선수의 성적을 동종 포지션의 리그 평균 성적과 비교"""
        player_stat = (
            self.session.query(
                PlayerSeasonBatting.avg,
                PlayerSeasonBatting.obp,
                PlayerSeasonBatting.slg,
                PlayerSeasonBatting.ops,
                PlayerSeasonBatting.home_runs,
                PlayerSeasonBatting.rbi,
                PlayerSeasonBatting.hits,
                PlayerSeasonBatting.games,
                PlayerBasic.name,
            )
            .join(PlayerBasic, PlayerSeasonBatting.player_id == PlayerBasic.player_id)
            .filter(
                PlayerSeasonBatting.player_id == player_id,
                PlayerSeasonBatting.season == season_year,
                PlayerSeasonBatting.league == "REGULAR",
            )
            .first()
        )

        if not player_stat:
            return {}

        pos_stats = (
            self.session.query(
                func.avg(PlayerSeasonBatting.avg).label("avg"),
                func.avg(PlayerSeasonBatting.obp).label("obp"),
                func.avg(PlayerSeasonBatting.slg).label("slg"),
                func.avg(PlayerSeasonBatting.ops).label("ops"),
                func.avg(PlayerSeasonBatting.home_runs).label("home_runs"),
                func.avg(PlayerSeasonBatting.rbi).label("rbi"),
                func.avg(PlayerSeasonBatting.hits).label("hits"),
            )
            .join(PlayerBasic, PlayerSeasonBatting.player_id == PlayerBasic.player_id)
            .filter(
                PlayerSeasonBatting.season == season_year,
                PlayerSeasonBatting.league == "REGULAR",
                PlayerBasic.position == position,
                PlayerSeasonBatting.plate_appearances >= 10,
            )
            .first()
        )

        player_data = {
            "name": player_stat.name,
            "avg": round(player_stat.avg or 0.0, 3),
            "obp": round(player_stat.obp or 0.0, 3),
            "slg": round(player_stat.slg or 0.0, 3),
            "ops": round(player_stat.ops or 0.0, 3),
            "home_runs": player_stat.home_runs or 0,
            "rbi": player_stat.rbi or 0,
            "hits": player_stat.hits or 0,
            "games": player_stat.games or 0,
        }

        avg_data = {
            "avg": round(pos_stats.avg or 0.0, 3) if pos_stats and pos_stats.avg is not None else 0.0,
            "obp": round(pos_stats.obp or 0.0, 3) if pos_stats and pos_stats.obp is not None else 0.0,
            "slg": round(pos_stats.slg or 0.0, 3) if pos_stats and pos_stats.slg is not None else 0.0,
            "ops": round(pos_stats.ops or 0.0, 3) if pos_stats and pos_stats.ops is not None else 0.0,
            "home_runs": round(pos_stats.home_runs or 0.0, 1) if pos_stats and pos_stats.home_runs is not None else 0.0,
            "rbi": round(pos_stats.rbi or 0.0, 1) if pos_stats and pos_stats.rbi is not None else 0.0,
            "hits": round(pos_stats.hits or 0.0, 1) if pos_stats and pos_stats.hits is not None else 0.0,
        }

        comparison = {
            "avg": round(player_data["avg"] - avg_data["avg"], 3),
            "obp": round(player_data["obp"] - avg_data["obp"], 3),
            "slg": round(player_data["slg"] - avg_data["slg"], 3),
            "ops": round(player_data["ops"] - avg_data["ops"], 3),
            "home_runs": round(player_data["home_runs"] - avg_data["home_runs"], 1),
            "rbi": round(player_data["rbi"] - avg_data["rbi"], 1),
            "hits": round(player_data["hits"] - avg_data["hits"], 1),
        }

        return {
            "player_id": player_id,
            "position": position,
            "season": season_year,
            "player_stats": player_data,
            "position_averages": avg_data,
            "comparison": comparison,
        }
