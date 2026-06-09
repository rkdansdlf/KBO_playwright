from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models.stadium_info import StadiumInfo, StadiumRegulation


class StadiumInfoRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def save_stadium_info(self, data: dict) -> StadiumInfo:
        code = data["stadium_code"]
        existing = self.session.get(StadiumInfo, code)
        if existing:
            for key, value in data.items():
                if value is not None:
                    setattr(existing, key, value)
            return existing
        new_record = StadiumInfo(**data)
        self.session.add(new_record)
        return new_record

    def get_all(self) -> list[StadiumInfo]:
        stmt = select(StadiumInfo).order_by(StadiumInfo.stadium_code)
        return list(self.session.execute(stmt).scalars().all())

    def get_by_code(self, code: str) -> StadiumInfo | None:
        return self.session.get(StadiumInfo, code)

    def save_regulation(self, data: dict) -> StadiumRegulation:
        new_record = StadiumRegulation(**data)
        self.session.add(new_record)
        return new_record

    def get_regulations_by_stadium(self, stadium_code: str) -> list[StadiumRegulation]:
        stmt = select(StadiumRegulation).where(StadiumRegulation.stadium_code == stadium_code)
        return list(self.session.execute(stmt).scalars().all())
