"""Parser modules for transforming raw HTML/text into structured data."""

from __future__ import annotations

from src.parsers.base_parser import (
    BaseHtmlParser,
    BaseJsonParser,
    BaseParser,
    BaseStadiumParser,
)
from src.parsers.dto import (
    GameDetailParsed,
    ParseResult,
    RetiredPlayerStatsParsed,
    ScheduleGameParsed,
    TeamEventParsed,
    TicketPriceParsed,
)
from src.parsers.game_detail_parser import GameDetailParser, parse_game_detail_html
from src.parsers.player_profile_parser import PlayerProfileParsed, PlayerProfileParser, parse_profile
from src.parsers.retired_player_parser import (
    RetiredPlayerParser,
    parse_retired_hitter_tables,
    parse_retired_pitcher_table,
)
from src.parsers.schedule_parser import ScheduleParser, parse_schedule_html
from src.parsers.team_event_parser import TeamEventParser, parse_team_events
from src.parsers.ticket_parser import TicketParser, parse_ticket_page

__all__ = [
    "BaseHtmlParser",
    "BaseJsonParser",
    "BaseParser",
    "BaseStadiumParser",
    "GameDetailParsed",
    "GameDetailParser",
    "ParseResult",
    "PlayerProfileParsed",
    "PlayerProfileParser",
    "RetiredPlayerParser",
    "RetiredPlayerStatsParsed",
    "ScheduleGameParsed",
    "ScheduleParser",
    "TeamEventParsed",
    "TeamEventParser",
    "TicketParser",
    "TicketPriceParsed",
    "parse_game_detail_html",
    "parse_profile",
    "parse_retired_hitter_tables",
    "parse_retired_pitcher_table",
    "parse_schedule_html",
    "parse_team_events",
    "parse_ticket_page",
]
