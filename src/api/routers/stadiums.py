"""Stadiums and Stadium Facilities API Router."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

from src.api.auth import get_api_key
from src.api.cache import cached_api
from src.api.schemas import (
    StadiumFacilitiesResponse,
    StadiumFoodSchema,
    StadiumItemSchema,
    StadiumParkingSchema,
    StadiumSeatSectionSchema,
    StadiumTicketOpenRuleSchema,
    StadiumTicketPriceSchema,
    StadiumTicketScheduleSchema,
)
from src.db.engine import get_db_session
from src.models.parking_lot import ParkingLot
from src.models.stadium_food_menu_item import StadiumFoodMenuItem
from src.models.stadium_food_vendor import StadiumFoodVendor
from src.models.stadium_info import StadiumInfo
from src.models.stadium_seat_section import StadiumSeatSection
from src.models.ticket_open_rule import TicketOpenRule
from src.models.ticket_price import TicketPrice
from src.models.ticket_schedule import TicketSchedule

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Stadiums & Facilities"])


@router.get(
    "/api/v1/stadiums",
    dependencies=[Depends(get_api_key)],
    response_model=list[StadiumItemSchema],
    summary="전체 KBO 경기장 목록 조회",
)
@cached_api(ttl_seconds=3600.0, key_prefix="stadiums_list")
def get_stadiums() -> list[dict[str, Any]]:
    """Query list of all registered KBO stadiums."""
    try:
        with get_db_session() as session:
            stmt = select(StadiumInfo).where(StadiumInfo.is_active.is_(True)).order_by(StadiumInfo.stadium_code)
            stadiums = list(session.execute(stmt).scalars().all())

            results = []
            for s in stadiums:
                home_teams: list[str] = []
                if s.home_team_id:
                    home_teams = [t.strip() for t in s.home_team_id.split(",")]

                results.append(
                    {
                        "stadium_code": s.stadium_code,
                        "stadium_name": s.name_kr,
                        "home_teams": home_teams,
                        "capacity": s.capacity,
                        "city": s.location,
                        "address": s.address,
                    }
                )
            return results
    except Exception as e:
        logger.exception("Failed to query stadiums")
        raise HTTPException(status_code=500, detail="Database query failure") from e


def _build_parking_items(session: Session, stadium_code: str) -> list[dict[str, Any]]:
    parking_stmt = select(ParkingLot).where(
        (ParkingLot.stadium_id == stadium_code) | (ParkingLot.stadium_id.ilike(f"%{stadium_code}%")),
    )
    parkings = list(session.execute(parking_stmt).scalars().all())
    return [
        StadiumParkingSchema(
            name=p.name,
            fee_type=p.lot_type,
            capacity=p.capacity,
            tip=p.operating_hours or p.address,
            address=p.address,
            walking_minutes=p.walking_minutes,
            is_event_day_available=p.is_event_day_available if p.is_event_day_available is not None else True,
            reservation_required=p.reservation_required if p.reservation_required is not None else False,
            operating_hours=p.operating_hours,
        ).model_dump()
        for p in parkings
    ]


def _build_food_items(session: Session, stadium_code: str) -> list[dict[str, Any]]:
    vendor_stmt = select(StadiumFoodVendor).where(
        (StadiumFoodVendor.stadium_id == stadium_code) | (StadiumFoodVendor.stadium_id.ilike(f"%{stadium_code}%")),
    )
    vendors = list(session.execute(vendor_stmt).scalars().all())
    vendor_ids = [vendor.id for vendor in vendors]
    menu_items_by_vendor: dict[int, list[StadiumFoodMenuItem]] = {}
    if vendor_ids:
        menu_stmt = select(StadiumFoodMenuItem).where(StadiumFoodMenuItem.vendor_id.in_(vendor_ids))
        for menu in session.execute(menu_stmt).scalars().all():
            menu_items_by_vendor.setdefault(menu.vendor_id, []).append(menu)

    def _menu_label(menu: StadiumFoodMenuItem) -> str:
        if menu.price is None:
            return menu.menu_name
        return f"{menu.menu_name} ({menu.price:,}원)"

    return [
        StadiumFoodSchema(
            vendor_name=v.vendor_name,
            location=v.location_text or v.floor_level,
            popular_menu=next(
                (_menu_label(menu) for menu in menu_items_by_vendor.get(v.id, []) if menu.is_signature),
                next((_menu_label(menu) for menu in menu_items_by_vendor.get(v.id, [])), None),
            ),
            category=next(
                (menu.category for menu in menu_items_by_vendor.get(v.id, []) if menu.is_signature),
                None,
            ),
            floor_level=v.floor_level,
            base_side=v.base_side,
            gate_info=v.gate_info,
            order_method=v.order_method,
            confidence=v.confidence,
        ).model_dump()
        for v in vendors
    ]


def _build_seat_items(session: Session, stadium_code: str) -> list[dict[str, Any]]:
    seat_stmt = select(StadiumSeatSection).where(
        (StadiumSeatSection.stadium_id == stadium_code) | (StadiumSeatSection.stadium_id.ilike(f"%{stadium_code}%")),
    )
    seats = list(session.execute(seat_stmt).scalars().all())

    price_stmt = (
        select(TicketPrice)
        .where(TicketPrice.stadium_id == stadium_code)
        .order_by(TicketPrice.season.desc(), TicketPrice.seat_grade, TicketPrice.day_type)
    )
    ticket_prices = list(session.execute(price_stmt).scalars().all())
    general_prices: dict[tuple[int, str, str], int] = {}
    for price in ticket_prices:
        if price.audience_type in (None, "general"):
            general_prices.setdefault((price.season, price.seat_grade, price.day_type), price.price)
    latest_price_season = max((price.season for price in ticket_prices), default=None)

    def _seat_price(seat_grade: str | None, day_type: str) -> int | None:
        if not seat_grade or latest_price_season is None:
            return None
        return general_prices.get((latest_price_season, seat_grade, day_type))

    return [
        StadiumSeatSectionSchema(
            section_name=s.section_name,
            section_code=s.section_code,
            seat_grade=s.seat_grade or s.price_grade_key,
            weekday_price=_seat_price(s.seat_grade or s.price_grade_key, "weekday"),
            weekend_price=_seat_price(s.seat_grade or s.price_grade_key, "weekend"),
            description=s.floor_level or s.gate_info,
            base_side=s.base_side,
            floor_level=s.floor_level,
            gate_info=s.gate_info,
            seat_map_url=s.seat_map_url,
        ).model_dump()
        for s in seats
    ]


def _raise_stadium_not_found(stadium_code: str) -> None:
    msg = f"Stadium '{stadium_code}' not found"
    raise HTTPException(status_code=404, detail=msg)


@router.get(
    "/api/v1/stadiums/{stadium_code}/facilities",
    dependencies=[Depends(get_api_key)],
    response_model=StadiumFacilitiesResponse,
    summary="구장별 편의시설 (주차/음식/좌석) 통합 조회",
)
@cached_api(ttl_seconds=1800.0, key_prefix="stadium_facilities")
def get_stadium_facilities(stadium_code: str) -> dict[str, Any]:
    """Query parking, food vendors, and seat sections for a specific stadium."""
    try:
        with get_db_session() as session:
            stadium = session.get(StadiumInfo, stadium_code)
            if not stadium:
                _raise_stadium_not_found(stadium_code)

            stadium_name = stadium.name_kr
            home_teams = [team.strip() for team in (stadium.home_team_id or "").split(",") if team.strip()]

            parking_items = _build_parking_items(session, stadium_code)
            vendor_items = _build_food_items(session, stadium_code)
            seat_items = _build_seat_items(session, stadium_code)

            schedule_stmt = select(TicketSchedule).where(
                (TicketSchedule.stadium == stadium_name) | (TicketSchedule.stadium.ilike(f"%{stadium_code}%")),
            )
            ticket_schedules = list(session.execute(schedule_stmt).scalars().all())
            schedule_items = [
                StadiumTicketScheduleSchema(
                    game_date=str(schedule.game_date),
                    home_team=schedule.home_team,
                    away_team=schedule.away_team,
                    stadium=schedule.stadium,
                    open_time=schedule.open_time.isoformat(),
                    platform=schedule.platform,
                    url=schedule.url,
                ).model_dump()
                for schedule in ticket_schedules
            ]

            rule_stmt = select(TicketOpenRule).where(TicketOpenRule.team_id.in_(home_teams))
            ticket_rules = list(session.execute(rule_stmt).scalars().all()) if home_teams else []
            rule_items = [
                StadiumTicketOpenRuleSchema(
                    team_id=rule.team_id,
                    platform=rule.platform,
                    open_offset_days=rule.open_offset_days,
                    open_time=rule.open_time.isoformat(),
                    sales_close_rule=rule.sales_close_rule,
                    max_tickets_per_user=rule.max_tickets_per_user,
                    fee_rule=rule.fee_rule,
                    cancel_rule=rule.cancel_rule,
                    note=rule.note,
                ).model_dump()
                for rule in ticket_rules
            ]

            price_stmt = (
                select(TicketPrice)
                .where(TicketPrice.stadium_id == stadium_code)
                .order_by(TicketPrice.season.desc(), TicketPrice.seat_grade, TicketPrice.day_type)
            )
            ticket_prices = list(session.execute(price_stmt).scalars().all())
            ticket_price_items = [
                StadiumTicketPriceSchema(
                    team_id=price.team_id,
                    season=price.season,
                    seat_grade=price.seat_grade,
                    day_type=price.day_type,
                    audience_type=price.audience_type,
                    price=price.price,
                    currency=price.currency or "KRW",
                    source_url=price.source_url,
                ).model_dump()
                for price in ticket_prices
            ]

            return {
                "stadium_code": stadium_code,
                "stadium_name": stadium_name,
                "home_teams": home_teams,
                "parkings": parking_items,
                "food_vendors": vendor_items,
                "seat_sections": seat_items,
                "ticket_schedules": schedule_items,
                "ticket_open_rules": rule_items,
                "ticket_prices": ticket_price_items,
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to query stadium facilities for %s", stadium_code)
        raise HTTPException(status_code=500, detail="Database query failure") from e
