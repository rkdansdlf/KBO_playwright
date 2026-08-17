"""FanGraphs KBO leaderboard adapter."""

from __future__ import annotations

import json
from urllib.parse import urlencode

from bs4 import BeautifulSoup

from .base import (
    ExternalStatRecord,
    ExternalStatsAdapter,
    ExternalStatsParseError,
    StatTableParseConfig,
    ensure_stat_table,
    find_stat_table,
    normalize_header,
    normalize_player_name,
    parse_metric_value,
    parse_stat_table,
    query_source_keys,
    resolve_external_team_code,
)

FANGRAPHS_BASE_URL = "https://www.fangraphs.com/leaders/international/kbo"
FANGRAPHS_DATA_URL = "https://www.fangraphs.com/api/leaders/international/kbo/data"
FANGRAPHS_PARSER_VERSION = "fangraphs-kbo-v1"

_COMMON_HEADERS = {
    "name": "player_name",
    "team": "team_name",
    "g": "games",
    "pa": "plate_appearances",
    "ab": "at_bats",
    "h": "hits",
    "1b": "singles",
    "2b": "doubles",
    "3b": "triples",
    "hr": "home_runs",
    "r": "runs",
    "rbi": "rbi",
    "bb": "walks",
    "ibb": "intentional_walks",
    "hbp": "hbp",
    "so": "strikeouts",
    "sb": "stolen_bases",
    "cs": "caught_stealing",
    "sh": "sacrifice_hits",
    "sf": "sacrifice_flies",
    "avg": "avg",
    "obp": "obp",
    "slg": "slg",
    "ops": "ops",
    "iso": "iso",
    "babip": "babip",
    "woba": "woba",
    "wrc+": "wrc_plus",
    "bb%": "bb_pct",
    "k%": "k_pct",
    "bb/k": "bb_per_k",
    "spd": "spd",
    "wrc": "wrc",
    "wraa": "wraa",
    "wbsr": "wbsr",
    "age": "age",
    "war": "war",
}

_PITCHING_HEADERS = {
    **_COMMON_HEADERS,
    "h": "hits_allowed",
    "r": "runs_allowed",
    "hr": "home_runs_allowed",
    "er": "earned_runs",
    "bb": "walks_allowed",
    "ibb": "intentional_walks",
    "w": "wins",
    "l": "losses",
    "gs": "games_started",
    "cg": "complete_games",
    "sho": "shutouts",
    "sv": "saves",
    "bs": "blown_saves",
    "hld": "holds",
    "ip": "innings_pitched",
    "tbf": "tbf",
    "wp": "wild_pitches",
    "bk": "balks",
    "era": "era",
    "k/9": "k_per_nine",
    "bb/9": "bb_per_nine",
    "hr/9": "hr_per_nine",
    "whip": "whip",
    "fip": "fip",
    "lob%": "lob_pct",
    "k-bb%": "k_minus_bb_pct",
    "pitches": "pitches",
    "war": "war_pitch",
}


class FanGraphsKboAdapter(ExternalStatsAdapter):
    """Parse public FanGraphs international KBO leaderboard tables."""

    provider = "fangraphs"
    source_keys = query_source_keys(provider)
    host = "www.fangraphs.com"

    def build_url(self, season: int, stat_type: str) -> str:
        """Build the FanGraphs KBO data API URL for one season."""
        if stat_type not in self.source_keys:
            msg = f"Unsupported FanGraphs stat type: {stat_type}"
            raise ValueError(msg)
        params = {
            "lg": "kbo",
            "pos": "all",
            "qual": "0",
            "stats": "bat" if stat_type == "batting" else "pit",
            "type": "0",
            "seasonstart": str(season),
            "seasonend": str(season),
            "team": "0",
            "season": str(season),
            "org": "",
            "ind": "0",
        }
        return f"{FANGRAPHS_DATA_URL}?{urlencode(params)}"

    def parse_html(self, html: str, season: int, stat_type: str, source_url: str) -> list[ExternalStatRecord]:
        """Parse one FanGraphs JSON API or rendered leaderboard response."""
        if html.lstrip().startswith(("[", "{")):
            return self._parse_json_rows(html, season, stat_type, source_url)
        header_map = _COMMON_HEADERS if stat_type == "batting" else _PITCHING_HEADERS
        required = {"player_name", "team_name"}
        required_metrics = set(header_map) - {"name", "team"}
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
                required_metric_headers=required_metrics,
                parser_version=FANGRAPHS_PARSER_VERSION,
            ),
        )

    def _parse_json_rows(
        self,
        body: str,
        season: int,
        stat_type: str,
        source_url: str,
    ) -> list[ExternalStatRecord]:
        """Parse the season-aware FanGraphs data API response."""
        try:
            payload = json.loads(body)
        except json.JSONDecodeError as exc:
            msg = "FanGraphs data API returned invalid JSON"
            raise ExternalStatsParseError(msg) from exc
        if not isinstance(payload, list):
            msg = "FanGraphs data API returned a non-list payload"
            raise ExternalStatsParseError(msg)
        header_map = _COMMON_HEADERS if stat_type == "batting" else _PITCHING_HEADERS
        records: list[ExternalStatRecord] = []
        for row in payload:
            if not isinstance(row, dict):
                continue
            record = self._parse_json_row(row, season, stat_type, source_url, header_map)
            if record is not None:
                records.append(record)
        return records

    def _parse_json_row(
        self,
        row: dict[str, object],
        season: int,
        stat_type: str,
        source_url: str,
        header_map: dict[str, str],
    ) -> ExternalStatRecord | None:
        """Parse one FanGraphs API row after checking its season."""
        row_season = row.get("Season")
        try:
            if row_season is not None and int(row_season) != season:
                return None
        except (TypeError, ValueError):
            return None
        player_name = str(row.get("KName") or "").strip() or _player_name_from_html(str(row.get("Name") or ""))
        if not player_name:
            return None
        metrics: dict[str, int | float | str | None] = {}
        normalized_row = {normalize_header(str(key)): value for key, value in row.items()}
        for raw_header, metric_key in header_map.items():
            if raw_header not in {"name", "team"} and raw_header in normalized_row:
                value = normalized_row[raw_header]
                metrics.update(parse_metric_value(metric_key, None if value is None else str(value)))
        if not metrics:
            return None
        team_name = str(row.get("Team") or "").strip() or None
        external_player_id = str(row.get("playerids") or "").strip() or None
        return ExternalStatRecord(
            provider=self.provider,
            source_key=self.source_keys[stat_type],
            stat_type=stat_type,
            season=season,
            player_name=player_name,
            team_name=team_name,
            team_code=resolve_external_team_code(team_name, season),
            external_player_id=external_player_id,
            metrics=metrics,
            source_url=source_url,
            metric_metadata={"headers": list(row), "parser_version": FANGRAPHS_PARSER_VERSION},
        )


def _player_name_from_html(value: str) -> str:
    """Extract the normalized player name from an API Name anchor."""
    soup = BeautifulSoup(value, "html.parser")
    return normalize_player_name(soup.get_text(" ", strip=True))
