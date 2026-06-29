from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.fan_culture import CheerChant, CheerSong, TeamRivalry
from src.repositories.fan_culture_repository import FanCultureRepository


class TestFanCultureRepository:
    def _engine(self):
        return create_engine("sqlite:///:memory:")

    def _session(self, engine):
        return sessionmaker(bind=engine)()

    def _init_tables(self, engine):
        TeamRivalry.__table__.create(engine)
        CheerSong.__table__.create(engine)
        CheerChant.__table__.create(engine)

    # --- TeamRivalry ---

    def test_save_rivalry_creates_record(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = FanCultureRepository(session)

        r = repo.save_rivalry(
            {
                "team_id_a": "LG",
                "team_id_b": "KIW",
                "rivalry_name": "Korean Series",
                "intensity": "HIGH",
            },
        )
        session.commit()

        assert r.id is not None
        assert r.rivalry_name == "Korean Series"

    def test_save_rivalry_normalizes_order(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = FanCultureRepository(session)

        r = repo.save_rivalry(
            {
                "team_id_a": "SSG",
                "team_id_b": "LG",
                "rivalry_name": "Cannons Derby",
                "intensity": "MEDIUM",
            },
        )
        session.commit()

        assert r.team_id_a == "LG"
        assert r.team_id_b == "SSG"

    def test_save_rivalry_upsert_returns_same(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = FanCultureRepository(session)

        r1 = repo.save_rivalry(
            {
                "team_id_a": "LG",
                "team_id_b": "KIW",
                "rivalry_name": "Korean Series",
                "intensity": "HIGH",
            },
        )
        session.commit()

        r2 = repo.save_rivalry(
            {
                "team_id_a": "LG",
                "team_id_b": "KIW",
                "rivalry_name": "Korean Series",
                "intensity": "LOW",  # updated
            },
        )
        session.commit()

        assert r1.id == r2.id
        assert r2.intensity == "LOW"

    def test_get_all_rivalries(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = FanCultureRepository(session)

        repo.save_rivalry({"team_id_a": "LG", "team_id_b": "KIW", "rivalry_name": "KS", "intensity": "HIGH"})
        repo.save_rivalry({"team_id_a": "DOO", "team_id_b": "HAN", "rivalry_name": "OB_Bears", "intensity": "HIGH"})
        session.commit()

        results = repo.get_all_rivalries()
        assert len(results) == 2

    def test_get_all_rivalries_empty(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = FanCultureRepository(session)

        assert repo.get_all_rivalries() == []

    # --- CheerSong ---

    def test_save_cheer_song_creates_record(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = FanCultureRepository(session)

        s = repo.save_cheer_song(
            {
                "team_id": "LG",
                "song_name": "Viva LG",
                "song_type": "TEAM",
                "lyrics": "We are LG...",
            },
        )
        session.commit()

        assert s.id is not None
        assert s.song_name == "Viva LG"

    def test_save_cheer_song_upsert_returns_same(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = FanCultureRepository(session)

        s1 = repo.save_cheer_song(
            {
                "team_id": "LG",
                "song_name": "Viva LG",
                "song_type": "TEAM",
                "lyrics": "v1",
            },
        )
        session.commit()

        s2 = repo.save_cheer_song(
            {
                "team_id": "LG",
                "song_name": "Viva LG",
                "song_type": "TEAM",
                "lyrics": "v2",
            },
        )
        session.commit()

        assert s1.id == s2.id
        assert s2.lyrics == "v2"

    def test_get_cheer_songs_by_team(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = FanCultureRepository(session)

        repo.save_cheer_song({"team_id": "LG", "song_name": "Song1", "song_type": "TEAM"})
        repo.save_cheer_song({"team_id": "LG", "song_name": "Song2", "song_type": "PERSONAL"})
        repo.save_cheer_song({"team_id": "SSG", "song_name": "Song3", "song_type": "TEAM"})
        session.commit()

        results = repo.get_cheer_songs_by_team("LG")
        assert len(results) == 2

    def test_get_cheer_songs_by_team_empty(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = FanCultureRepository(session)

        assert repo.get_cheer_songs_by_team("NONE") == []

    # --- CheerChant ---

    def test_save_cheer_chant_creates_record(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = FanCultureRepository(session)

        c = repo.save_cheer_chant(
            {
                "team_id": "LG",
                "chant_text": "Let's go LG!",
                "situation": "TOP_1ST",
            },
        )
        session.commit()

        assert c.id is not None
        assert c.chant_text == "Let's go LG!"

    def test_save_cheer_chant_upsert_returns_same(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = FanCultureRepository(session)

        c1 = repo.save_cheer_chant(
            {
                "team_id": "LG",
                "chant_text": "Let's go LG!",
                "situation": None,
            },
        )
        session.commit()

        c2 = repo.save_cheer_chant(
            {
                "team_id": "LG",
                "chant_text": "Let's go LG!",
                "situation": "TOP_1ST",
            },
        )
        session.commit()

        assert c1.id == c2.id
        assert c2.situation == "TOP_1ST"

    def test_get_cheer_chants_by_team(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = FanCultureRepository(session)

        repo.save_cheer_chant({"team_id": "LG", "chant_text": "Chant1"})
        repo.save_cheer_chant({"team_id": "LG", "chant_text": "Chant2"})
        repo.save_cheer_chant({"team_id": "SSG", "chant_text": "Chant3"})
        session.commit()

        results = repo.get_cheer_chants_by_team("LG")
        assert len(results) == 2

    def test_get_cheer_chants_by_team_empty(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = FanCultureRepository(session)

        assert repo.get_cheer_chants_by_team("NONE") == []
