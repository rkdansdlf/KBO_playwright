from __future__ import annotations

from sqlalchemy import Boolean, ForeignKey, Index, Integer, String, UniqueConstraint
from sqlalchemy.dialects.sqlite import TEXT as SQLiteText
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import JSON

from .base import Base, TimestampMixin


class StadiumSeatSection(Base, TimestampMixin):
    __tablename__ = "stadium_seat_sections"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    stadium_id: Mapped[str] = mapped_column(
        String(10),
        ForeignKey("stadium_info.stadium_code", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Stadium code",
    )
    section_code: Mapped[str | None] = mapped_column(
        String(50), nullable=True, comment="Section code (e.g. 101B, 1루 내야)",
    )
    section_name: Mapped[str] = mapped_column(String(100), nullable=False, comment="Section display name")
    seat_grade: Mapped[str | None] = mapped_column(
        String(50), nullable=True, comment="Seat grade for price matching (e.g. 블루석)",
    )
    base_side: Mapped[str | None] = mapped_column(
        String(20), nullable=True, comment="first_base / third_base / center / outfield",
    )
    floor_level: Mapped[str | None] = mapped_column(String(50), nullable=True, comment="Floor level (1F/2F/3F)")
    gate_info: Mapped[str | None] = mapped_column(String(100), nullable=True, comment="Entry gate info")
    is_home_cheering: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default="0", comment="Home cheering section",
    )
    is_away_cheering: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default="0", comment="Away cheering section",
    )
    is_table_seat: Mapped[bool] = mapped_column(Boolean, default=False, server_default="0", comment="Table seat")
    is_family_seat: Mapped[bool] = mapped_column(Boolean, default=False, server_default="0", comment="Family zone seat")
    is_wheelchair_accessible: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default="0", comment="Wheelchair accessible",
    )
    price_grade_key: Mapped[str | None] = mapped_column(
        String(50), nullable=True, comment="Key to match with TicketPrice.seat_grade",
    )
    seat_map_url: Mapped[str | None] = mapped_column(String(500), nullable=True, comment="Seat map image URL")
    geometry_json: Mapped[dict | None] = mapped_column(
        JSON().with_variant(SQLiteText, "sqlite"), nullable=True, comment="Section geometry coordinates",
    )
    source_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("data_sources.id", ondelete="SET NULL"), nullable=True, comment="DataSource ID",
    )

    __table_args__ = (
        UniqueConstraint("stadium_id", "section_code", name="uq_seat_section_code"),
        UniqueConstraint("stadium_id", "section_name", name="uq_seat_section_name"),
        Index("idx_ss_grade", "seat_grade"),
        Index("idx_ss_side", "base_side"),
    )

    def __repr__(self) -> str:
        return f"<StadiumSeatSection(stadium='{self.stadium_id}', name='{self.section_name}')>"
