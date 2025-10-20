"""
KBO Season metadata model.
"""
from sqlalchemy import Integer, String, Date
from sqlalchemy.orm import Mapped, mapped_column
from typing import Optional

from .base import Base, TimestampMixin


class KboSeason(Base, TimestampMixin):
    """
    Represents a specific KBO season (e.g., 2024 Regular Season).
    Mirrors `Docs/schema/KBO_시즌별 메타 테이블 제약조건.csv`.
    """
    __tablename__ = "kbo_seasons"

    season_id: Mapped[int] = mapped_column(Integer, primary_key=True, comment="PK, 기본 키")
    season_year: Mapped[int] = mapped_column(Integer, nullable=False, comment="시즌 연도")
    league_type_code: Mapped[int] = mapped_column(Integer, nullable=False, comment="시즌 종류 코드")
    league_type_name: Mapped[str] = mapped_column(String(50), nullable=False, comment="시즌 종류 이름")
    start_date: Mapped[Optional[Date]] = mapped_column(Date, nullable=True, comment="시즌 시작일")
    end_date: Mapped[Optional[Date]] = mapped_column(Date, nullable=True, comment="시즌 종료일")

    def __repr__(self) -> str:
        return f"<KboSeason(season_id={self.season_id}, year={self.season_year}, name='{self.league_type_name}')>"
