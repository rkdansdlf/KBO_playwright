from __future__ import annotations

from src.urls import (
    GAME_CENTER,
    HITTER_BASIC1,
    HITTER_DETAIL,
    KBO_BASE,
    PITCHER_BASIC1,
    PITCHER_BASIC2,
    PITCHER_DETAIL,
    REGISTER,
    SCHEDULE,
)


class TestKBOBase:
    def test_base_url(self):
        assert KBO_BASE == "https://www.koreabaseball.com"

    def test_base_starts_with_https(self):
        assert KBO_BASE.startswith("https://")


class TestScheduleUrls:
    def test_schedule_url(self):
        assert f"{KBO_BASE}/Schedule/Schedule.aspx" == SCHEDULE

    def test_game_center_url(self):
        assert f"{KBO_BASE}/Schedule/GameCenter/Main.aspx" == GAME_CENTER

    def test_register_url(self):
        assert f"{KBO_BASE}/Player/Register.aspx" == REGISTER


class TestHitterUrls:
    def test_hitter_basic1(self):
        assert f"{KBO_BASE}/Record/Player/HitterBasic/Basic1.aspx" == HITTER_BASIC1

    def test_hitter_detail(self):
        assert f"{KBO_BASE}/Record/Player/HitterDetail/Basic.aspx" == HITTER_DETAIL


class TestPitcherUrls:
    def test_pitcher_basic1(self):
        assert f"{KBO_BASE}/Record/Player/PitcherBasic/Basic1.aspx" == PITCHER_BASIC1

    def test_pitcher_basic2(self):
        assert f"{KBO_BASE}/Record/Player/PitcherBasic/Basic2.aspx" == PITCHER_BASIC2

    def test_pitcher_detail(self):
        assert f"{KBO_BASE}/Record/Player/PitcherDetail/Basic.aspx" == PITCHER_DETAIL


class TestUrlAllStartWithBase:
    def test_all_urls_start_with_base(self):
        urls = [
            SCHEDULE,
            GAME_CENTER,
            HITTER_BASIC1,
            PITCHER_BASIC1,
            PITCHER_BASIC2,
            HITTER_DETAIL,
            PITCHER_DETAIL,
            REGISTER,
        ]
        for url in urls:
            assert url.startswith(KBO_BASE), f"{url} does not start with {KBO_BASE}"
