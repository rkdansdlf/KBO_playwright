import csv
import logging
import os

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

from src.models.player import Player, PlayerBasic, PlayerSeasonBatting, PlayerSeasonPitching

ALIAS_CSV_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "data", "player_name_aliases.csv")


class PlayerIdResolver:
    """
    Resolver ensuring player IDs are found even if missing in game crawl data.
    """

    def __init__(
        self,
        session: Session,
        allow_unknown_registration: bool | None = None,
        *,
        strict_game_resolution: bool = False,
        allow_auto_register: bool | None = None,
    ):
        self.session = session
        if allow_auto_register is not None:
            allow_unknown_registration = allow_auto_register
        self.allow_unknown_registration = (
            bool(allow_unknown_registration) if allow_unknown_registration is not None else False
        )
        self.allow_auto_register = self.allow_unknown_registration
        self.strict_game_resolution = strict_game_resolution
        self._cache = {}

        # Load name aliases from CSV
        self.NAME_ALIASES: dict[str, str] = self._load_aliases_from_csv()

        # All-Star and International team mappings
        self.ALL_STAR_TEAMS = {
            "EA": "East",
            "WE": "West",
            "DRE": "드림",
            "NAN": "나눔",
            "드림": "드림",
            "나눔": "나눔",
            "KR": "Korea",
            "JP": "Japan",
            "TW": "Taiwan",
            "NL": "Nanum",
            "DL": "Dream",
        }

        # Comprehensive historical team mapping for disambiguation
        self.TEAM_NAME_MAP = {
            # Active
            "LG": "LG",
            "SS": "삼성",
            "SAMSUNG": "삼성",
            "KT": "KT",
            "NC": "NC",
            "LT": "롯데",
            "LOT": "롯데",
            "LOTTE": "롯데",
            "HH": "한화",
            "HANWHA": "한화",
            "KIA": "KIA",
            "HT": "KIA",
            "해태": "KIA",
            "DB": "두산",
            "OB": "두산",
            "DOOSAN": "두산",
            "BEARS": "두산",
            "SSG": "SSG",
            "SK": "SK",
            "KH": "키움",
            "WO": "히어로즈",
            "NX": "넥센",
            "KIWOOM": "키움",
            "HEROES": "히어로즈",
            "HD": "현대",
            "HYUNDAI": "현대",
            "현대": "현대",
            "HU": "현대",
            "SL": "쌍방울",
            "쌍방울": "쌍방울",
            "TP": "태평양",
            "태평양": "태평양",
            "CB": "청보",
            "청보": "청보",
            "SM": "삼미",
            "삼미": "삼미",
            "BE": "빙그레",
            "빙그레": "빙그레",
            "MBC": "MBC",
            "청룡": "MBC",
        }
        self.TEAM_NAME_MAP.update(self.ALL_STAR_TEAMS)

    @staticmethod
    def _load_aliases_from_csv() -> dict[str, str]:
        aliases: dict[str, str] = {}
        csv_path = os.path.normpath(ALIAS_CSV_PATH)
        if not os.path.exists(csv_path):
            return aliases
        try:
            with open(csv_path, encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    old = row.get("old_name", "").strip()
                    new = row.get("new_name", "").strip()
                    if old and new and old != new:
                        aliases[old] = new
        except Exception:
            logger.warning("Failed to load aliases from CSV")
        return aliases

    def _return_ambiguous(
        self, cache_key: str, player_name: str, team_code: str, season: int, candidate_ids
    ) -> int | None:
        candidates = sorted({int(pid) for pid in candidate_ids if pid is not None})
        print(
            f"   [AMBIGUOUS PLAYER] {player_name} ({team_code}, {season}) "
            f"matches multiple official candidates: {candidates}. Leaving player_id NULL."
        )
        self._cache[cache_key] = None
        return None

    def _filter_surrogate_ids(self, candidate_ids: set[int]) -> set[int]:
        if len(candidate_ids) <= 1:
            return candidate_ids

        stmt = select(Player.id, Player.kbo_person_id).where(
            Player.id.in_(list(candidate_ids)), Player.kbo_person_id.isnot(None)
        )
        rows = self.session.execute(stmt).fetchall()

        surrogates = {}
        for pid, kbo_id in rows:
            try:
                kbo_pid = int(kbo_id)
                surrogates[pid] = kbo_pid
            except (ValueError, TypeError):
                pass

        if not surrogates:
            return candidate_ids

        filtered = set()
        for pid in candidate_ids:
            if pid in surrogates:
                filtered.add(surrogates[pid])
            else:
                filtered.add(pid)
        return filtered

    def _return_existing_unknown_or_ambiguous(
        self,
        cache_key: str,
        player_name: str,
        team_code: str,
        season: int,
        uniform_no: str | None,
        candidate_ids,
    ) -> int | None:
        candidates = sorted({int(pid) for pid in candidate_ids if pid is not None})
        if team_code and candidates and all(pid >= 900000 for pid in candidates):
            if self.strict_game_resolution:
                return self._return_ambiguous(cache_key, player_name, team_code, season, candidates)
            existing_unknown_id = self._find_existing_unknown_player(player_name, team_code, uniform_no)
            if existing_unknown_id:
                self._cache[cache_key] = existing_unknown_id
                return existing_unknown_id
        return self._return_ambiguous(cache_key, player_name, team_code, season, candidates)

    def preload_season_index(self, season: int) -> None:
        print(f"🔄 Preloading player index for season {season}...")
        season_index: dict[str, set[int]] = {}

        # Batters
        stmt = (
            select(PlayerBasic.name, PlayerSeasonBatting.team_code, PlayerSeasonBatting.player_id)
            .join(PlayerBasic, PlayerSeasonBatting.player_id == PlayerBasic.player_id)
            .where(PlayerSeasonBatting.season == season)
        )
        for name, team, pid in self.session.execute(stmt).fetchall():
            if pid is not None:
                # Match the format used in resolve_id: {name}_{team}_{season}_{uniform_no or ''}
                season_index.setdefault(f"{name}_{team}_{season}_", set()).add(int(pid))

        # Pitchers
        stmt = (
            select(PlayerBasic.name, PlayerSeasonPitching.team_code, PlayerSeasonPitching.player_id)
            .join(PlayerBasic, PlayerSeasonPitching.player_id == PlayerBasic.player_id)
            .where(PlayerSeasonPitching.season == season)
        )
        for name, team, pid in self.session.execute(stmt).fetchall():
            if pid is not None:
                season_index.setdefault(f"{name}_{team}_{season}_", set()).add(int(pid))

        for cache_key, candidate_ids in season_index.items():
            filtered_ids = self._filter_surrogate_ids(candidate_ids)
            if len(filtered_ids) == 1:
                self._cache[cache_key] = next(iter(filtered_ids))
            else:
                self._cache[cache_key] = None

    def resolve_id(
        self,
        player_name: str,
        team_code: str,
        season: int,
        uniform_no: str | None = None,
        is_pitcher: bool | None = None,
    ) -> int | None:
        if not player_name:
            return None

        # Static overrides to resolve ambiguity/discrepancies in historical data
        overrides = {
            ("전준호", "HU", 2001, False): 91511,
            ("전준호", "HU", 2001, True): 94364,
            ("전준호", "HU", 2002, False): 91511,
            ("전준호", "HU", 2002, True): 94364,
            ("전준호", "HU", 2003, False): 91511,
            ("전준호", "HU", 2003, True): 94364,
            ("전준호", "HU", 2004, False): 91511,
            ("전준호", "HU", 2004, True): 94364,
            ("전준호", "HU", 2005, False): 91511,
            ("전준호", "HU", 2005, True): 94364,
            ("전준호", "HU", 2006, False): 91511,
            ("전준호", "HU", 2006, True): 94364,
            ("전준호", "HU", 2007, False): 91511,
            ("전준호", "HU", 2007, True): 94364,
            ("마일영", "HU", 2001, True): 70329,
            ("마일영", "HU", 2001, False): 70329,
            ("김민재", "LT", 2001, False): 91523,
            ("양현석", "KIA", 2001, False): 70608,
            ("임선동", "HU", 2001, True): 97133,
            ("김수경", "HU", 2001, True): 98330,
            ("위재영", "HU", 2001, True): 95318,
            ("테일러", "HU", 2001, True): 2943,
            # 2026 Season unresolved active players
            ("김민수", "KT", 2026, True): 65048,
            ("김민수", "KT", 2026, False): 52303,
            ("최재영", "KH", 2026, False): 56338,
            ("최재영", "KH", 2026, True): 56338,
            ("최원준", "KT", 2026, False): 66606,
            ("최원준", "KT", 2026, True): 66606,
            ("김민혁", "KT", 2026, False): 64004,
            ("박지훈", "DB", 2026, False): 50204,
            ("박지훈", "DB", 2026, True): 50204,
            ("김민석", "DB", 2026, False): 53554,
            ("김민석", "DB", 2026, True): 53554,
            ("오재원", "HH", 2026, False): 56754,
            ("오재원", "HH", 2026, True): 56754,
            ("박시원", "NC", 2026, False): 50996,
            ("박시원", "NC", 2026, True): 50996,
            ("박시원", "NC", 2026, None): 50996,
            ("신재인", "NC", 2026, False): 56909,
            ("신재인", "NC", 2026, True): 56909,
            ("신재인", "NC", 2026, None): 56909,
            ("안우진", "KH", 2026, True): 68341,
            ("안우진", "KH", 2026, False): 68341,
            ("보쉴리", "KT", 2026, True): 56036,
            ("박준영", "HH", 2026, True): 52731,
            ("이형범", "KIA", 2026, True): 62951,
            ("박세진", "LT", 2026, True): 66047,
            ("김민", "SSG", 2026, True): 68043,
            ("최용준", "SSG", 2026, True): 50650,
            ("왕옌청", "HH", 2026, True): 56719,
            ("왕옌청", "HH", 2026, False): 56719,
            ("왕옌청", "HH", 2026, None): 56719,
            ("박채울", "KH", 2026, False): 54303,
            ("박채울", "KH", 2026, True): 54303,
            ("박채울", "KH", 2026, None): 54303,
            # 2022 Season KIA unresolved players
            ("류지혁", "KIA", 2022, False): 62234,
            ("류지혁", "KIA", 2022, True): 62234,
            ("김선빈", "KIA", 2022, False): 78603,
            ("김선빈", "KIA", 2022, True): 78603,
            ("최형우", "KIA", 2022, False): 72443,
            ("최형우", "KIA", 2022, True): 72443,
            ("장현식", "KIA", 2022, True): 63950,
            ("장현식", "KIA", 2022, False): 63950,
            ("한승혁", "KIA", 2022, True): 61666,
            ("한승혁", "KIA", 2022, False): 61666,
            ("정해영", "KIA", 2022, True): 50662,
            ("정해영", "KIA", 2022, False): 50662,
            # 2018 Season Nexen unresolved player
            ("김태혁", "NX", 2018, True): 76430,
            ("김태혁", "NX", 2018, False): 76430,
        }

        override_key = (player_name, team_code, season, is_pitcher)
        if override_key in overrides:
            resolved_id = overrides[override_key]
            print(
                f"   [OVERRIDE RESOLVED] {player_name} ({team_code}, {season}, is_pitcher={is_pitcher}) -> {resolved_id}"
            )
            return resolved_id

        # Fallback key check without is_pitcher just in case
        override_key_no_pitcher = (player_name, team_code, season, None)
        if override_key_no_pitcher in overrides:
            resolved_id = overrides[override_key_no_pitcher]
            print(f"   [OVERRIDE RESOLVED (relaxed)] {player_name} ({team_code}, {season}) -> {resolved_id}")
            return resolved_id

        if player_name in self.NAME_ALIASES:
            player_name = self.NAME_ALIASES[player_name]

        cache_key = f"{player_name}_{team_code}_{season}_{uniform_no or ''}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        # 1. Try Seasonal Data (Most accurate)
        is_allstar = team_code in self.ALL_STAR_TEAMS
        season_candidate_ids = set()

        models = []
        if is_pitcher is True:
            models = [PlayerSeasonPitching]
        elif is_pitcher is False:
            models = [PlayerSeasonBatting]
        else:
            models = [PlayerSeasonBatting, PlayerSeasonPitching]

        for model in models:
            stmt = (
                select(PlayerBasic.player_id)
                .select_from(model)
                .join(PlayerBasic, model.player_id == PlayerBasic.player_id)
                .where(PlayerBasic.name == player_name, model.season == season)
            )
            if not is_allstar and team_code:
                stmt = stmt.where(model.team_code == team_code)
            if uniform_no:
                stmt = stmt.where(PlayerBasic.uniform_no == str(uniform_no))

            results = self.session.execute(stmt).fetchall()
            season_candidate_ids.update(row[0] for row in results)
        season_candidate_ids = self._filter_surrogate_ids(season_candidate_ids)
        if len(season_candidate_ids) == 1:
            pid = next(iter(season_candidate_ids))
            self._cache[cache_key] = pid
            return pid
        if len(season_candidate_ids) > 1:
            return self._return_existing_unknown_or_ambiguous(
                cache_key,
                player_name,
                team_code,
                season,
                uniform_no,
                season_candidate_ids,
            )

        # 2. Try PlayerBasic with Team/Career context
        kor_team_name = self.TEAM_NAME_MAP.get(team_code, "")
        if kor_team_name:
            stmt = select(PlayerBasic.player_id).where(
                PlayerBasic.name == player_name,
                or_(PlayerBasic.team.contains(kor_team_name), PlayerBasic.career.contains(kor_team_name)),
            )
            if uniform_no:
                stmt = stmt.where(PlayerBasic.uniform_no == str(uniform_no))

            results = self.session.execute(stmt).fetchall()
            candidate_ids = self._filter_surrogate_ids({row[0] for row in results})
            if len(candidate_ids) == 1:
                pid = next(iter(candidate_ids))
                self._cache[cache_key] = pid
                return pid
            if len(candidate_ids) > 1:
                return self._return_existing_unknown_or_ambiguous(
                    cache_key,
                    player_name,
                    team_code,
                    season,
                    uniform_no,
                    candidate_ids,
                )

        # 3. Fallback: Relaxed Uniqueness Check
        # If we have uniform_no, try unique by (name, uniform_no) global
        if uniform_no:
            stmt = select(PlayerBasic.player_id).where(
                PlayerBasic.name == player_name, PlayerBasic.uniform_no == str(uniform_no)
            )
            results = self.session.execute(stmt).fetchall()
            candidate_ids = self._filter_surrogate_ids({row[0] for row in results})
            if len(candidate_ids) == 1:
                pid = next(iter(candidate_ids))
                self._cache[cache_key] = pid
                return pid
            if len(candidate_ids) > 1:
                return self._return_existing_unknown_or_ambiguous(
                    cache_key,
                    player_name,
                    team_code,
                    season,
                    uniform_no,
                    candidate_ids,
                )

        if self.strict_game_resolution:
            print(
                f"   [UNRESOLVED PLAYER] {player_name} ({team_code}, {season}) lacked "
                "strict team/season/uniform evidence. Leaving player_id NULL."
            )
            self._cache[cache_key] = None
            return None

        # 4. Ultimate Fallback: Is the name unique in the entire KBO history?
        stmt = select(PlayerBasic.player_id).where(PlayerBasic.name == player_name)
        results = self.session.execute(stmt).fetchall()
        candidate_ids = self._filter_surrogate_ids({row[0] for row in results})
        if len(candidate_ids) == 1:
            pid = next(iter(candidate_ids))
            self._cache[cache_key] = pid
            return pid
        if len(candidate_ids) > 1:
            return self._return_existing_unknown_or_ambiguous(
                cache_key,
                player_name,
                team_code,
                season,
                uniform_no,
                candidate_ids,
            )

        # 5. Last resort: Try relaxed season resolution without uniform_no or strict team
        relaxed_id = self._resolve_relaxed(player_name, team_code, season)
        if relaxed_id:
            self._cache[cache_key] = relaxed_id
            return relaxed_id

        # 6. Optional legacy fallback: auto-register unknown player as a local profile.
        if not self.allow_unknown_registration:
            print(
                f"   [UNKNOWN PLAYER] {player_name} ({team_code}, {season}) was not resolved. "
                "Leaving player_id NULL; automatic local profile registration is disabled."
            )
            self._cache[cache_key] = None
            return None

        if not team_code:
            print(
                f"   [UNKNOWN PLAYER] {player_name} ({season}) has no team context. "
                "Leaving player_id NULL instead of auto-registering."
            )
            self._cache[cache_key] = None
            return None

        new_id = self.register_unknown_player(player_name, team_code, uniform_no)
        if new_id:
            self._cache[cache_key] = new_id
            return new_id

        return None

    def _unknown_profile_team(self, team_code: str) -> str:
        return self.TEAM_NAME_MAP.get(team_code, team_code)

    def _find_existing_unknown_player(self, name: str, team_code: str, uniform_no: str | None) -> int | None:
        team_name = self._unknown_profile_team(team_code)
        stmt = select(PlayerBasic.player_id).where(
            PlayerBasic.player_id >= 900000,
            PlayerBasic.name == name,
        )
        if team_name:
            stmt = stmt.where(PlayerBasic.team == team_name)
        else:
            stmt = stmt.where(or_(PlayerBasic.team.is_(None), PlayerBasic.team == ""))

        if uniform_no:
            stmt = stmt.where(PlayerBasic.uniform_no == str(uniform_no))
        else:
            stmt = stmt.where(or_(PlayerBasic.uniform_no.is_(None), PlayerBasic.uniform_no == ""))

        existing_ids = sorted(int(row[0]) for row in self.session.execute(stmt).fetchall() if row[0] is not None)
        return existing_ids[0] if existing_ids else None

    def register_unknown_player(self, name: str, team_code: str, uniform_no: str | None) -> int | None:
        existing_id = self._find_existing_unknown_player(name, team_code, uniform_no)
        if existing_id:
            print(f"   [UNKNOWN PLAYER REUSED] {name} ({team_code}) -> {existing_id}")
            return existing_id

        print(f"   [NEW PLAYER ADDED] Auto-registering local profile for {name} ({team_code})")
        # Generate a large fake ID (>900000)
        stmt = (
            select(PlayerBasic.player_id)
            .where(PlayerBasic.player_id >= 900000)
            .order_by(PlayerBasic.player_id.desc())
            .limit(1)
        )
        max_id = self.session.execute(stmt).scalar()
        if max_id is None or max_id < 900000:
            new_id = 900000
        else:
            new_id = max_id + 1

        try:
            kor_team_name = self._unknown_profile_team(team_code)
            new_player = PlayerBasic(
                player_id=new_id,
                name=name,
                team=kor_team_name,
                uniform_no=str(uniform_no) if uniform_no else None,
                status="Unknown/Local",
            )
            self.session.add(new_player)
            self.session.commit()
            return new_id
        except Exception:
            self.session.rollback()
            logger.exception("   ❌ Error auto-registering player")
            return None

    def _resolve_relaxed(self, player_name: str, team_code: str, season: int) -> int | None:
        """Relaxed matching: Name + Season match, ensuring exactly one candidate."""
        candidates = set()
        for model in [PlayerSeasonBatting, PlayerSeasonPitching]:
            stmt = (
                select(PlayerBasic.player_id)
                .select_from(model)
                .join(PlayerBasic, model.player_id == PlayerBasic.player_id)
                .where(PlayerBasic.name == player_name, model.season == season)
            )
            if team_code and team_code not in self.ALL_STAR_TEAMS:
                # Still try to filter by team if possible, but don't fail if team_code is weird
                pass

            for row in self.session.execute(stmt).fetchall():
                candidates.add(row[0])

        if len(candidates) == 1:
            return list(candidates)[0]

        # Try PlayerBasic with team/career again but even more relaxed
        kor_team_name = self.TEAM_NAME_MAP.get(team_code, "")
        if kor_team_name:
            stmt = select(PlayerBasic.player_id).where(
                PlayerBasic.name == player_name,
                or_(PlayerBasic.team.contains(kor_team_name), PlayerBasic.career.contains(kor_team_name)),
            )
            results = self.session.execute(stmt).fetchall()
            if len(results) == 1:
                return results[0][0]

        return None
