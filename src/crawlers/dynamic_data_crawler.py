"""
Crawler for dynamic structured data: schedules, ticket open times, and rosters.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.crawlers.daily_roster_crawler import DailyRosterCrawler
from src.models.game import Game
from src.models.ticket_schedule import TicketSchedule

logger = logging.getLogger(__name__)

# Team ticketing rules mapping
# (home_team) -> (days_before_game, hour_of_day, platform, default_url)
TEAM_TICKET_RULES = {
    "두산": (
        10,
        11,
        "인터파크",
        "https://ticket.interpark.com/Contents/Sports/GoodsInfo?SportsCode=07001&TeamCode=PB004",
    ),
    "키움": (
        7,
        14,
        "인터파크",
        "https://ticket.interpark.com/Contents/Sports/GoodsInfo?SportsCode=07001&TeamCode=PB003",
    ),
    "LG": (7, 11, "티켓링크", "https://www.ticketlink.co.kr/sports/baseball/59#reservation"),
    "KT": (7, 14, "티켓링크", "https://www.ticketlink.co.kr/sports/baseball/62#reservation"),
    "SSG": (7, 11, "티켓링크", "https://www.ticketlink.co.kr/sports/baseball/435#reservation"),
    "KIA": (7, 11, "티켓링크", "https://www.ticketlink.co.kr/sports/baseball/57#reservation"),
    "삼성": (7, 11, "티켓링크", "https://www.ticketlink.co.kr/sports/baseball/55#reservation"),
    "한화": (7, 11, "티켓링크", "https://www.ticketlink.co.kr/sports/baseball/60#reservation"),
    "NC": (7, 11, "티켓링크", "https://www.ticketlink.co.kr/sports/baseball/63#reservation"),
    "롯데": (7, 14, "티켓링크", "https://www.ticketlink.co.kr/sports/baseball/58#reservation"),
}


class DynamicDataCrawler:
    """
    Manages daily crawls of schedules, ticket open times, and roster entries.
    """

    def __init__(self, db_session: Session) -> None:
        self.db_session = db_session
        self.roster_crawler = DailyRosterCrawler()

    async def crawl_roster_changes(self, start_date: str, end_date: str) -> list[dict[str, Any]]:
        """
        Crawls daily 1st team registration changes using the existing DailyRosterCrawler.
        """
        logger.info("📋 Crawling roster changes from %s to %s...", start_date, end_date)
        try:
            records = await self.roster_crawler.crawl_date_range(start_date, end_date)
            logger.info("   Collected %s roster movements.", len(records))
            return records
        except Exception as e:
            logger.exception("⚠️ Error crawling roster")
            raise e

    def crawl_and_update_ticket_times(self, lookahead_days: int = 14) -> list[TicketSchedule]:
        """
        Calculates upcoming game ticketing open times based on KBO team rules
        and saves them to the ticket_schedules table.
        """
        today_val = datetime.now().date()
        future_val = today_val + timedelta(days=lookahead_days)
        logger.info("🎟️ Calculating ticket opening times for games between %s and %s...", today_val, future_val)

        # Fetch upcoming games from database
        query = select(Game).where(Game.game_date >= today_val, Game.game_date <= future_val)
        games = self.db_session.scalars(query).all()
        logger.info("   Found %s scheduled games in local DB.", len(games))

        ticket_records = []
        for g in games:
            # Match home team to ticketing rule
            home_name = None
            for key in TEAM_TICKET_RULES:
                if key in g.home_team:
                    home_name = key
                    break

            if not home_name:
                continue

            days_before, hour, platform, url = TEAM_TICKET_RULES[home_name]

            # Open date calculation
            open_date = g.game_date - timedelta(days=days_before)
            open_time = datetime(open_date.year, open_date.month, open_date.day, hour, 0, 0)

            # Check if this record already exists
            existing_ticket = self.db_session.scalar(
                select(TicketSchedule).where(
                    TicketSchedule.game_date == g.game_date,
                    TicketSchedule.home_team == g.home_team,
                    TicketSchedule.platform == platform,
                ),
            )

            if existing_ticket:
                # Update existing
                existing_ticket.away_team = g.away_team
                existing_ticket.stadium = g.stadium or ""
                existing_ticket.open_time = open_time
                existing_ticket.url = url
                existing_ticket.updated_at = datetime.now()
                ticket_records.append(existing_ticket)
            else:
                # Create new
                new_ticket = TicketSchedule(
                    game_date=g.game_date,
                    home_team=g.home_team,
                    away_team=g.away_team,
                    stadium=g.stadium or "",
                    open_time=open_time,
                    platform=platform,
                    url=url,
                )
                self.db_session.add(new_ticket)
                ticket_records.append(new_ticket)

        self.db_session.commit()
        logger.info("   Successfully upserted %s ticketing schedules.", len(ticket_records))
        return ticket_records
