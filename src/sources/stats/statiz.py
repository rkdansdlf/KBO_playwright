"""STATIZ season-stat adapter with explicit login/access detection."""

from __future__ import annotations

from urllib.parse import urlencode

from bs4 import BeautifulSoup

from .base import (
    ExternalStatRecord,
    ExternalStatsAccessError,
    ExternalStatsAdapter,
    StatTableParseConfig,
    ensure_stat_table,
    find_stat_table,
    parse_stat_table,
    query_source_keys,
)

STATIZ_BASE_URL = "https://www.statiz.co.kr/stats/"
STATIZ_PARSER_VERSION = "statiz-kbo-v1"

_BATTING_HEADERS = {
    "선수": "player_name",
    "선수명": "player_name",
    "이름": "player_name",
    "team": "team_name",
    "팀": "team_name",
    "소속": "team_name",
    "g": "games",
    "pa": "plate_appearances",
    "타석": "plate_appearances",
    "ab": "at_bats",
    "타수": "at_bats",
    "h": "hits",
    "안타": "hits",
    "2b": "doubles",
    "2루타": "doubles",
    "3b": "triples",
    "3루타": "triples",
    "hr": "home_runs",
    "홈런": "home_runs",
    "r": "runs",
    "득점": "runs",
    "rbi": "rbi",
    "타점": "rbi",
    "bb": "walks",
    "볼넷": "walks",
    "so": "strikeouts",
    "삼진": "strikeouts",
    "sb": "stolen_bases",
    "도루": "stolen_bases",
    "cs": "caught_stealing",
    "타율": "avg",
    "avg": "avg",
    "출루율": "obp",
    "obp": "obp",
    "장타율": "slg",
    "slg": "slg",
    "ops": "ops",
    "iso": "iso",
    "babip": "babip",
    "woba": "woba",
    "wrc+": "wrc_plus",
    "war": "war",
}

_PITCHING_HEADERS = {
    "선수": "player_name",
    "선수명": "player_name",
    "이름": "player_name",
    "team": "team_name",
    "팀": "team_name",
    "소속": "team_name",
    "g": "games",
    "경기": "games",
    "gs": "games_started",
    "선발": "games_started",
    "ip": "innings_pitched",
    "이닝": "innings_pitched",
    "h": "hits_allowed",
    "피안타": "hits_allowed",
    "r": "runs_allowed",
    "실점": "runs_allowed",
    "er": "earned_runs",
    "자책": "earned_runs",
    "era": "era",
    "평균자책": "era",
    "w": "wins",
    "승": "wins",
    "l": "losses",
    "패": "losses",
    "sv": "saves",
    "세이브": "saves",
    "hld": "holds",
    "홀드": "holds",
    "so": "strikeouts",
    "삼진": "strikeouts",
    "bb": "walks_allowed",
    "볼넷": "walks_allowed",
    "whip": "whip",
    "fip": "fip",
    "k/9": "k_per_nine",
    "bb/9": "bb_per_nine",
    "k/bb": "kbb",
    "war": "war_pitch",
}


class StatizKboAdapter(ExternalStatsAdapter):
    """Parse STATIZ season tables when the caller has permitted access."""

    provider = "statiz"
    source_keys = query_source_keys(provider)
    host = "www.statiz.co.kr"

    def build_url(self, season: int, stat_type: str) -> str:
        """Build a STATIZ season-stat URL without attempting authentication."""
        if stat_type not in self.source_keys:
            msg = f"Unsupported STATIZ stat type: {stat_type}"
            raise ValueError(msg)
        params = {"m": "main", "m2": "1" if stat_type == "batting" else "2", "year": str(season)}
        return f"{STATIZ_BASE_URL}?{urlencode(params)}"

    def parse_html(self, html: str, season: int, stat_type: str, source_url: str) -> list[ExternalStatRecord]:
        """Parse one STATIZ response and reject the login page explicitly."""
        if "로그인 후 이용" in html or "/member/?m=login" in html:
            msg = "STATIZ season statistics require an authenticated session"
            raise ExternalStatsAccessError(msg)
        header_map = _BATTING_HEADERS if stat_type == "batting" else _PITCHING_HEADERS
        required = {"player_name"}
        table = ensure_stat_table(
            find_stat_table(BeautifulSoup(html, "html.parser"), required, header_map=header_map),
            self.provider,
            stat_type,
        )
        return parse_stat_table(
            table,
            StatTableParseConfig(
                provider=self.provider,
                source_key=self.source_keys[stat_type],
                season=season,
                stat_type=stat_type,
                source_url=source_url,
                header_map=header_map,
                required_metric_headers=set(header_map) - {"선수", "선수명", "이름", "team", "팀", "소속"},
                parser_version=STATIZ_PARSER_VERSION,
            ),
        )
