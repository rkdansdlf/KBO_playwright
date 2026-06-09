"""
FA Contract model representing KBO Free Agent contracts.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base, TimestampMixin

if TYPE_CHECKING:
    from .player import PlayerBasic


class FAContract(Base, TimestampMixin):
    """
    Structured details of FA contracts including duration, amount, and old/new team mappings.
    """

    __tablename__ = "fa_contracts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Player references
    player_name: Mapped[str] = mapped_column(String(100), nullable=False, comment="Original player name from source")
    player_basic_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("player_basic.player_id", ondelete="RESTRICT"),
        nullable=True,
        index=True,
        comment="Resolved player_basic.player_id if available",
    )

    # Contract metadata
    year: Mapped[int] = mapped_column(Integer, nullable=False, comment="Season year of the contract")
    fa_type: Mapped[str] = mapped_column(String(20), nullable=False, comment="'retained' | 'transferred'")

    # Team information
    old_team: Mapped[str | None] = mapped_column(String(50), nullable=True, comment="Previous team name")
    new_team: Mapped[str | None] = mapped_column(String(50), nullable=True, comment="Contract team name")
    team_code: Mapped[str | None] = mapped_column(String(20), nullable=True, comment="Resolved teams.team_id code")

    # Financial details
    contract_duration: Mapped[str | None] = mapped_column(
        String(50), nullable=True, comment="Original contract duration text (e.g. '4년')",
    )
    total_amount: Mapped[str | None] = mapped_column(
        String(50), nullable=True, comment="Original total contract amount text (e.g. '75억원')",
    )
    total_amount_krw: Mapped[int | None] = mapped_column(
        Integer, nullable=True, comment="Parsed total contract amount in 10,000 KRW (만원) units",
    )
    signing_bonus: Mapped[str | None] = mapped_column(String(50), nullable=True, comment="Original signing bonus text")
    annual_salary: Mapped[str | None] = mapped_column(String(50), nullable=True, comment="Original annual salary text")

    remarks: Mapped[str | None] = mapped_column(Text, nullable=True, comment="Additional contract clauses/options")
    source_url: Mapped[str | None] = mapped_column(String(500), nullable=True, comment="Crawling source URL")

    # Relationships
    player_basic: Mapped[PlayerBasic | None] = relationship("PlayerBasic", backref="fa_contracts")

    __table_args__ = (
        UniqueConstraint(
            "player_name",
            "year",
            "fa_type",
            "new_team",
            name="uq_fa_contract_record",
        ),
        Index("idx_fa_contracts_player", "player_name"),
        Index("idx_fa_contracts_year", "year"),
    )

    def __repr__(self) -> str:
        return f"<FAContract(player='{self.player_name}', year={self.year}, type='{self.fa_type}', team='{self.new_team}', amount='{self.total_amount}')>"
