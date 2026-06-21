from __future__ import annotations

import csv
import logging
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from sqlalchemy import inspect, or_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

from src.constants import SURROGATE_PLAYER_ID_BOUNDARY
from src.models.player import Player, PlayerBasic, PlayerSeasonBatting, PlayerSeasonPitching

ALIAS_CSV_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "player_name_aliases.csv"

CANONICAL_TEAM_CODES = {
    "OB": "DB",
    "SK": "SSG",
    "WO": "KH",
    "NX": "KH",
    "HT": "KIA",
}

SAMSUNG_LEE_SEUNGHYUN_SEASON = 2026


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
    ) -> None:
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
        csv_path = ALIAS_CSV_PATH
        if not csv_path.exists():
            return aliases
        try:
            with csv_path.open(encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    old = row.get("old_name", "").strip()
                    new = row.get("new_name", "").strip()
                    if old and new and old != new:
                        aliases[old] = new
        except (csv.Error, OSError, TypeError, ValueError):
            logger.exception("Failed to load aliases from CSV")
        return aliases

    def _return_ambiguous(
        self,
        cache_key: str,
        player_name: str,
        team_code: str,
        season: int,
        candidate_ids: Iterable[int | str | None],
    ) -> int | None:
        candidates = sorted({int(pid) for pid in candidate_ids if pid is not None})
        logger.warning(
            "   [AMBIGUOUS PLAYER] %s (%s, %s) matches multiple official candidates: %s. Leaving player_id NULL.",
            player_name,
            team_code,
            season,
            candidates,
        )
        self._cache[cache_key] = None
        return None

    def _filter_surrogate_ids(self, candidate_ids: set[int], player_name: str | None = None) -> set[int]:
        if len(candidate_ids) <= 1:
            return candidate_ids

        stmt = select(Player.id, Player.kbo_person_id).where(
            Player.id.in_(list(candidate_ids)),
            Player.kbo_person_id.isnot(None),
        )
        rows = self.session.execute(stmt).fetchall()

        surrogates = {}
        for pid, kbo_id in rows:
            try:
                kbo_pid = int(kbo_id)
                surrogates[pid] = kbo_pid
            except (ValueError, TypeError):
                logger.debug("Invalid KBO ID for player %s: %s", pid, kbo_id)

        if not surrogates:
            return candidate_ids

        target_ids = sorted({pid for pid in surrogates.values() if pid is not None})
        existing_targets = {
            int(row[0]): str(row[1])
            for row in self.session.execute(
                select(PlayerBasic.player_id, PlayerBasic.name).where(PlayerBasic.player_id.in_(target_ids))
            ).fetchall()
            if row[0] is not None
        }

        filtered = set()
        for pid in candidate_ids:
            mapped_id = surrogates.get(pid)
            if mapped_id is not None and mapped_id in existing_targets:
                if player_name is None or existing_targets[mapped_id] == player_name:
                    filtered.add(mapped_id)
                    continue
                logger.debug(
                    "Skipping surrogate player_id mapping %s -> %s because target name %s does not match %s",
                    pid,
                    mapped_id,
                    existing_targets[mapped_id],
                    player_name,
                )
            if pid in surrogates and mapped_id not in existing_targets:
                logger.debug(
                    "Skipping surrogate player_id mapping %s -> %s because target profile is missing", pid, mapped_id
                )
            filtered.add(pid)
        return filtered

    def _return_existing_unknown_or_ambiguous(
        self,
        cache_key: str,
        player_name: str,
        team_code: str,
        season: int,
        uniform_no: str | None,
        candidate_ids: Iterable[int | str | None],
    ) -> int | None:
        candidates = sorted({int(pid) for pid in candidate_ids if pid is not None})
        if team_code and candidates and all(pid >= SURROGATE_PLAYER_ID_BOUNDARY for pid in candidates):
            if self.strict_game_resolution:
                return self._return_ambiguous(cache_key, player_name, team_code, season, candidates)
            existing_unknown_id = self._find_existing_unknown_player(player_name, team_code, uniform_no)
            if existing_unknown_id:
                self._cache[cache_key] = existing_unknown_id
                return existing_unknown_id
        return self._return_ambiguous(cache_key, player_name, team_code, season, candidates)

    def preload_season_index(self, season: int) -> None:
        logger.info("🔄 Preloading player index for season %s...", season)
        season_index: dict[str, dict[str, object]] = {}

        def add_index_entry(name: str, team: str, pid: int, is_pitcher: bool | None) -> None:
            cache_key = self._cache_key(name, team, season, None, is_pitcher)
            entry = season_index.setdefault(cache_key, {"name": name, "ids": set()})
            entry_ids = entry["ids"]
            if isinstance(entry_ids, set):
                entry_ids.add(int(pid))

        # Batters
        stmt = (
            select(PlayerBasic.name, PlayerSeasonBatting.team_code, PlayerSeasonBatting.player_id)
            .join(PlayerBasic, PlayerSeasonBatting.player_id == PlayerBasic.player_id)
            .where(PlayerSeasonBatting.season == season)
        )
        for name, team, pid in self.session.execute(stmt).fetchall():
            if pid is not None:
                add_index_entry(name, team, int(pid), False)
                add_index_entry(name, team, int(pid), None)

        # Pitchers
        stmt = (
            select(PlayerBasic.name, PlayerSeasonPitching.team_code, PlayerSeasonPitching.player_id)
            .join(PlayerBasic, PlayerSeasonPitching.player_id == PlayerBasic.player_id)
            .where(PlayerSeasonPitching.season == season)
        )
        for name, team, pid in self.session.execute(stmt).fetchall():
            if pid is not None:
                add_index_entry(name, team, int(pid), True)
                add_index_entry(name, team, int(pid), None)

        for cache_key, entry in season_index.items():
            candidate_ids = entry["ids"]
            if not isinstance(candidate_ids, set):
                continue
            player_name = entry.get("name")
            filtered_ids = self._filter_surrogate_ids(
                candidate_ids,
                str(player_name) if player_name is not None else None,
            )
            if len(filtered_ids) == 1:
                self._cache[cache_key] = next(iter(filtered_ids))
            else:
                self._cache[cache_key] = None

    def _cache_key(
        self,
        player_name: str,
        team_code: str | None,
        season: int,
        uniform_no: str | None,
        is_pitcher: bool | None,
    ) -> str:
        if is_pitcher is True:
            role = "P"
        elif is_pitcher is False:
            role = "B"
        else:
            role = "A"
        return f"{player_name}_{team_code}_{season}_{uniform_no or ''}_{role}"

    def _canonical_team_code(self, team_code: str | None) -> str | None:
        return CANONICAL_TEAM_CODES.get(team_code, team_code)

    def _resolve_static_override(
        self,
        player_name: str,
        team_code: str,
        season: int,
        is_pitcher: bool | None,
    ) -> int | None:
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
            ("임기영", "SS", 2026, True): 62754,
            ("임기영", "SS", 2026, False): 62754,
            ("임기영", "SS", 2026, None): 62754,
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
            ("히우라", "KH", 2026, False): 56305,
            ("히우라", "KH", 2026, None): 56305,
            ("유민", "HH", 2026, False): 52765,
            ("유민", "HH", 2026, None): 52765,
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
            ("김태혁", "NX", 2018, True): 76430,
            ("김태혁", "NX", 2018, False): 76430,
            ("이주형", "KH", 2026, False): 50167,
            ("이주형", "KH", 2026, True): 50167,
            ("이주형", "KH", 2026, None): 50167,
            ("양현종", "KH", 2026, False): 55370,
            ("양현종", "KH", 2026, True): 55370,
            ("양현종", "KH", 2026, None): 55370,
            ("브룩스", "KH", 2026, False): 56322,
            ("브룩스", "KH", 2026, True): 56322,
            ("브룩스", "KH", 2026, None): 56322,
            ("정다훈", "KH", 2026, True): 56345,
            ("정다훈", "KH", 2026, False): 56345,
            "정다훈": 56345,
            ("타케다", "SSG", 2026, True): 56823,
            ("타케다", "SSG", 2026, False): 56823,
            ("타케다", "SSG", 2026, None): 56823,
        }

        override_key = (player_name, team_code, season, is_pitcher)
        if override_key in overrides:
            resolved_id = overrides[override_key]
            logger.info(
                "   [OVERRIDE RESOLVED] %s (%s, %s, is_pitcher=%s) -> %s",
                player_name,
                team_code,
                season,
                is_pitcher,
                resolved_id,
            )
            return resolved_id

        override_key_no_pitcher = (player_name, team_code, season, None)
        if override_key_no_pitcher in overrides:
            resolved_id = overrides[override_key_no_pitcher]
            logger.info(
                "   [OVERRIDE RESOLVED (relaxed)] %s (%s, %s) -> %s",
                player_name,
                team_code,
                season,
                resolved_id,
            )
            return resolved_id
        return None

    def _resolve_samsung_lee_seunghyun(
        self,
        player_name: str,
        team_code: str,
        season: int,
        uniform_no: str | None,
        is_pitcher: bool | None,
    ) -> int | None:
        if not (
            player_name == "이승현"
            and team_code == "SS"
            and season == SAMSUNG_LEE_SEUNGHYUN_SEASON
            and is_pitcher is True
        ):
            return None
        if uniform_no == "57":
            return 51454
        if uniform_no in ("20", "26"):
            return 60146
        if not uniform_no:
            return 51454
        return None

    def _resolve_hanwha_park_junyoung(
        self,
        player_name: str,
        team_code: str,
        season: int,
        uniform_no: str | None,
        is_pitcher: bool | None,
    ) -> int | None:
        if not (player_name == "박준영" and team_code == "HH" and season == 2026 and is_pitcher is True):
            return None
        if uniform_no == "68":
            return 56709  # 68번 박준영 (2002년생)
        if uniform_no == "96":
            return 52731  # 96번 박준영 (2003년생)
        return None

    def _candidate_models(self, is_pitcher: bool | None) -> list[Any]:
        if is_pitcher is True:
            return [PlayerSeasonPitching]
        if is_pitcher is False:
            return [PlayerSeasonBatting]
        return [PlayerSeasonBatting, PlayerSeasonPitching]

    def _cache_single_or_ambiguous(
        self,
        cache_key: str,
        player_name: str,
        team_code: str,
        season: int,
        uniform_no: str | None,
        candidate_ids: set[int],
    ) -> int | None:
        candidate_ids = self._filter_surrogate_ids(candidate_ids, player_name)
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
        return None

    def _resolve_from_season_stats(
        self,
        player_name: str,
        team_code: str,
        season: int,
        uniform_no: str | None,
        is_pitcher: bool | None,
        cache_key: str,
    ) -> int | None:
        is_allstar = team_code in self.ALL_STAR_TEAMS
        candidate_ids = set()
        for model in self._candidate_models(is_pitcher):
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
            candidate_ids.update(row[0] for row in self.session.execute(stmt).fetchall())
        return self._cache_single_or_ambiguous(cache_key, player_name, team_code, season, uniform_no, candidate_ids)

    def _resolve_from_player_basic_context(
        self,
        player_name: str,
        team_code: str,
        season: int,
        uniform_no: str | None,
        cache_key: str,
    ) -> int | None:
        kor_team_name = self.TEAM_NAME_MAP.get(team_code, "")
        if not kor_team_name:
            return None
        stmt = select(PlayerBasic.player_id).where(
            PlayerBasic.name == player_name,
            or_(PlayerBasic.team.contains(kor_team_name), PlayerBasic.career.contains(kor_team_name)),
        )
        if uniform_no:
            stmt = stmt.where(PlayerBasic.uniform_no == str(uniform_no))
        candidate_ids = {row[0] for row in self.session.execute(stmt).fetchall()}
        return self._cache_single_or_ambiguous(cache_key, player_name, team_code, season, uniform_no, candidate_ids)

    def _resolve_by_uniform_no(
        self,
        player_name: str,
        team_code: str,
        season: int,
        uniform_no: str | None,
        cache_key: str,
    ) -> int | None:
        if not uniform_no:
            return None
        stmt = select(PlayerBasic.player_id).where(
            PlayerBasic.name == player_name,
            PlayerBasic.uniform_no == str(uniform_no),
        )
        candidate_ids = {row[0] for row in self.session.execute(stmt).fetchall()}
        return self._cache_single_or_ambiguous(cache_key, player_name, team_code, season, uniform_no, candidate_ids)

    def _resolve_strict_game_facts_or_none(
        self,
        player_name: str,
        team_code: str,
        season: int,
        uniform_no: str | None,
        is_pitcher: bool | None,
        cache_key: str,
    ) -> int | None:
        fact_id = self._resolve_from_same_season_game_facts(
            player_name,
            team_code,
            season,
            uniform_no=uniform_no,
            is_pitcher=is_pitcher,
        )
        if fact_id:
            self._cache[cache_key] = fact_id
            return fact_id
        logger.warning(
            "   [UNRESOLVED PLAYER] %s (%s, %s) lacked strict team/season/uniform evidence. Leaving player_id NULL.",
            player_name,
            team_code,
            season,
        )
        self._cache[cache_key] = None
        return None

    def _resolve_unique_historical_name(
        self,
        player_name: str,
        team_code: str,
        season: int,
        uniform_no: str | None,
        cache_key: str,
    ) -> int | None:
        stmt = select(PlayerBasic.player_id).where(PlayerBasic.name == player_name)
        candidate_ids = {row[0] for row in self.session.execute(stmt).fetchall()}
        return self._cache_single_or_ambiguous(cache_key, player_name, team_code, season, uniform_no, candidate_ids)

    def _resolve_relaxed_and_cache(
        self,
        player_name: str,
        team_code: str,
        season: int,
        cache_key: str,
    ) -> int | None:
        relaxed_id = self._resolve_relaxed(player_name, team_code, season)
        if relaxed_id:
            self._cache[cache_key] = relaxed_id
            return relaxed_id
        return None

    def _resolve_unknown_registration(
        self,
        player_name: str,
        team_code: str,
        season: int,
        uniform_no: str | None,
        cache_key: str,
    ) -> int | None:
        if not self.allow_unknown_registration:
            logger.warning(
                "   [UNKNOWN PLAYER] %s (%s, %s) was not resolved. Leaving player_id NULL; automatic local profile registration is disabled.",
                player_name,
                team_code,
                season,
            )
            self._cache[cache_key] = None
            return None

        if not team_code:
            logger.warning(
                "   [UNKNOWN PLAYER] %s (%s) has no team context. Leaving player_id NULL instead of auto-registering.",
                player_name,
                season,
            )
            self._cache[cache_key] = None
            return None

        new_id = self.register_unknown_player(player_name, team_code, uniform_no)
        if new_id:
            self._cache[cache_key] = new_id
            return new_id
        return None

    def _resolve_non_strict_fallbacks(
        self,
        player_name: str,
        team_code: str,
        season: int,
        uniform_no: str | None,
        cache_key: str,
    ) -> int | None:
        historical_id = self._resolve_unique_historical_name(player_name, team_code, season, uniform_no, cache_key)
        if historical_id is not None:
            return historical_id

        relaxed_id = self._resolve_relaxed_and_cache(player_name, team_code, season, cache_key)
        if relaxed_id is not None:
            return relaxed_id

        return self._resolve_unknown_registration(player_name, team_code, season, uniform_no, cache_key)

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

        team_code = self._canonical_team_code(team_code)

        override_id = self._resolve_static_override(player_name, team_code, season, is_pitcher)
        if override_id:
            return override_id

        samsung_id = self._resolve_samsung_lee_seunghyun(player_name, team_code, season, uniform_no, is_pitcher)
        if samsung_id:
            return samsung_id

        hanwha_id = self._resolve_hanwha_park_junyoung(player_name, team_code, season, uniform_no, is_pitcher)
        if hanwha_id:
            return hanwha_id

        if player_name in self.NAME_ALIASES:
            player_name = self.NAME_ALIASES[player_name]

        cache_key = self._cache_key(player_name, team_code, season, uniform_no, is_pitcher)
        if cache_key in self._cache:
            return self._cache[cache_key]

        resolver_steps = (
            lambda: self._resolve_from_season_stats(player_name, team_code, season, uniform_no, is_pitcher, cache_key),
            lambda: self._resolve_from_player_basic_context(player_name, team_code, season, uniform_no, cache_key),
            lambda: self._resolve_by_uniform_no(player_name, team_code, season, uniform_no, cache_key),
        )
        for resolve_step in resolver_steps:
            resolved_id = resolve_step()
            if resolved_id is not None:
                return resolved_id

        if self.strict_game_resolution:
            return self._resolve_strict_game_facts_or_none(
                player_name, team_code, season, uniform_no, is_pitcher, cache_key
            )

        return self._resolve_non_strict_fallbacks(player_name, team_code, season, uniform_no, cache_key)

    def _unknown_profile_team(self, team_code: str) -> str:
        return self.TEAM_NAME_MAP.get(team_code, team_code)

    def _find_existing_unknown_player(self, name: str, team_code: str, uniform_no: str | None) -> int | None:
        team_name = self._unknown_profile_team(team_code)
        stmt = select(PlayerBasic.player_id).where(
            PlayerBasic.player_id >= SURROGATE_PLAYER_ID_BOUNDARY,
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

    def _resolve_from_same_season_game_facts(
        self,
        player_name: str,
        team_code: str,
        season: int,
        *,
        uniform_no: str | None,
        is_pitcher: bool | None,
    ) -> int | None:
        """Use already-linked same-season game rows as strict pregame evidence."""
        if not player_name or not team_code or not season:
            return None

        from src.models.game import GameBattingStat, GameLineup, GamePitchingStat

        if is_pitcher is True:
            models = [GamePitchingStat]
        elif is_pitcher is False:
            models = [GameBattingStat, GameLineup]
        else:
            models = [GameBattingStat, GameLineup, GamePitchingStat]

        connection = self.session.connection()
        if connection is not None:
            inspector = inspect(connection)
            models = [model for model in models if inspector.has_table(model.__tablename__)]

        candidate_ids: set[int] = set()
        for model in models:
            stmt = select(model.player_id).where(
                model.game_id.like(f"{season}%"),
                model.team_code == team_code,
                model.player_name == player_name,
                model.player_id.isnot(None),
            )
            if uniform_no:
                stmt = stmt.where(model.uniform_no == str(uniform_no))

            for (player_id,) in self.session.execute(stmt).fetchall():
                if player_id is None:
                    continue
                candidate_ids.add(int(player_id))

        official_ids = {
            pid for pid in self._filter_surrogate_ids(candidate_ids, player_name) if pid < SURROGATE_PLAYER_ID_BOUNDARY
        }
        if len(official_ids) == 1:
            return next(iter(official_ids))
        return None

    def register_unknown_player(self, name: str, team_code: str, uniform_no: str | None) -> int | None:
        existing_id = self._find_existing_unknown_player(name, team_code, uniform_no)
        if existing_id:
            logger.info("   [UNKNOWN PLAYER REUSED] %s (%s) -> %s", name, team_code, existing_id)
            return existing_id

        logger.info("   [NEW PLAYER ADDED] Auto-registering local profile for %s (%s)", name, team_code)
        # Generate a large fake ID (>900000)
        stmt = (
            select(PlayerBasic.player_id)
            .where(PlayerBasic.player_id >= SURROGATE_PLAYER_ID_BOUNDARY)
            .order_by(PlayerBasic.player_id.desc())
            .limit(1)
        )
        max_id = self.session.execute(stmt).scalar()
        if max_id is None or max_id < SURROGATE_PLAYER_ID_BOUNDARY:
            new_id = SURROGATE_PLAYER_ID_BOUNDARY
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
        except (SQLAlchemyError, RuntimeError, ValueError, TypeError):
            self.session.rollback()
            logger.exception("   ❌ Error auto-registering player")
            return None
        else:
            return new_id

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
