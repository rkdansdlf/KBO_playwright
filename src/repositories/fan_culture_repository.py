from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

from src.models.fan_culture import CheerChant, CheerSong, TeamRivalry

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class FanCultureRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    # TeamRivalry
    def save_rivalry(self, data: dict) -> TeamRivalry:
        team_id_a = data["team_id_a"]
        team_id_b = data["team_id_b"]
        # Normalize ordering
        if team_id_a > team_id_b:
            team_id_a, team_id_b = team_id_b, team_id_a
            data["team_id_a"] = team_id_a
            data["team_id_b"] = team_id_b

        stmt = select(TeamRivalry).where(
            TeamRivalry.team_id_a == team_id_a,
            TeamRivalry.team_id_b == team_id_b,
        )
        existing = self.session.execute(stmt).scalar_one_or_none()
        if existing:
            for key, value in data.items():
                if key not in ("team_id_a", "team_id_b") and value is not None:
                    setattr(existing, key, value)
            return existing
        new_record = TeamRivalry(**data)
        self.session.add(new_record)
        return new_record

    def get_all_rivalries(self) -> list[TeamRivalry]:
        stmt = select(TeamRivalry).order_by(TeamRivalry.team_id_a)
        return list(self.session.execute(stmt).scalars().all())

    # CheerSong
    def save_cheer_song(self, data: dict) -> CheerSong:
        team_id = data["team_id"]
        song_name = data["song_name"]
        song_type = data["song_type"]

        stmt = select(CheerSong).where(
            CheerSong.team_id == team_id,
            CheerSong.song_name == song_name,
            CheerSong.song_type == song_type,
        )
        existing = self.session.execute(stmt).scalar_one_or_none()
        if existing:
            for key, value in data.items():
                if key not in ("team_id", "song_name", "song_type") and value is not None:
                    setattr(existing, key, value)
            return existing
        new_record = CheerSong(**data)
        self.session.add(new_record)
        return new_record

    def get_cheer_songs_by_team(self, team_id: str) -> list[CheerSong]:
        stmt = select(CheerSong).where(CheerSong.team_id == team_id).order_by(CheerSong.song_type)
        return list(self.session.execute(stmt).scalars().all())

    # CheerChant
    def save_cheer_chant(self, data: dict) -> CheerChant:
        team_id = data["team_id"]
        chant_text = data["chant_text"]

        stmt = select(CheerChant).where(
            CheerChant.team_id == team_id,
            CheerChant.chant_text == chant_text,
        )
        existing = self.session.execute(stmt).scalar_one_or_none()
        if existing:
            for key, value in data.items():
                if key not in ("team_id", "chant_text") and value is not None:
                    setattr(existing, key, value)
            return existing
        new_record = CheerChant(**data)
        self.session.add(new_record)
        return new_record

    def get_cheer_chants_by_team(self, team_id: str) -> list[CheerChant]:
        stmt = select(CheerChant).where(CheerChant.team_id == team_id)
        return list(self.session.execute(stmt).scalars().all())
