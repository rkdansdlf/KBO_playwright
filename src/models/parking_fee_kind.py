"""Data model: stadium parking fee kind.

``parking_fee_rules`` is keyed by vehicle class -- compact/sedan/van/bus -- and
says how that class is charged in minutes. The stadium pages say something else:
they list a fee *kind* -- 기본/추가/일일/행사/경기/무료 -- against an amount in
won. Those are two different axes of the same page, and only the second one is
what the crawler can actually read. This table holds what the crawler reads;
the vehicle-class table stays for the seed data that carries it.
"""

from __future__ import annotations

from sqlalchemy import ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class ParkingFeeKind(Base, TimestampMixin):
    """A single fee kind (기본/추가/일일/행사/경기/무료) for one parking lot."""

    __tablename__ = "parking_fee_kinds"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    parking_lot_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("parking_lots.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="FK to parking_lots",
    )
    fee_kind: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        comment="fee kind label as printed (기본/추가/일일/행사/경기/무료)",
    )
    amount_krw: Mapped[int] = mapped_column(Integer, nullable=False, comment="Fee amount in KRW")
    source_url: Mapped[str | None] = mapped_column(String(500), nullable=True, comment="Page the fee was parsed from")

    __table_args__ = (UniqueConstraint("parking_lot_id", "fee_kind", name="uq_parking_fee_kind"),)

    def __repr__(self) -> str:
        """Return a string representation of this object."""
        return f"<ParkingFeeKind(lot_id={self.parking_lot_id}, kind='{self.fee_kind}', amount={self.amount_krw})>"
