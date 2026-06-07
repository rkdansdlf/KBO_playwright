from datetime import datetime

from src.parsers.team_event_parser import (
    SOURCE_CONFIG_MAP,
    TEAM_CODE_FROM_SOURCE_KEY,
    _classify_event,
    parse_team_events,
)


class TestClassifyEvent:
    def test_giveaway(self):
        assert _classify_event("경품 이벤트") == "giveaway"
        assert _classify_event("굿즈 증정") == "giveaway"

    def test_first_pitch(self):
        assert _classify_event("시구 안내") == "first_pitch"

    def test_discount(self):
        assert _classify_event("할인 프로모션") == "discount"

    def test_fan_participation(self):
        assert _classify_event("사인회") == "fan_participation"

    def test_festival(self):
        assert _classify_event("페스티벌") == "festival"

    def test_promotion(self):
        assert _classify_event("모집 공고") == "promotion"

    def test_notice(self):
        assert _classify_event("개막 안내") == "notice"

    def test_notice_keyword(self):
        assert _classify_event("공지") == "notice"

    def test_default_promotion(self):
        assert _classify_event("기타 소식") == "promotion"


class TestParseTeamEvents:
    LG_HTML = """
    <html><body>
    <table>
        <tr>
            <td><a class="subject" href="/service/notice/123">2025 시즌 이벤트 안내</a></td>
            <td><span class="date">2025-03-15</span></td>
        </tr>
        <tr>
            <td><a class="subject" href="/service/notice/124">사인회 및 팬미팅 개최</a></td>
            <td><span class="date">2025-03-20</span></td>
        </tr>
        <tr>
            <td><a class="subject" href="/service/notice/125">할인 프로모션 안내</a></td>
            <td><span class="date">2025-04-01</span></td>
        </tr>
        <tr>
            <td><a class="subject" href="/service/notice/126">선수단 훈련 일정입니다</a></td>
            <td><span class="date">2025-03-10</span></td>
        </tr>
    </table>
    </body></html>
    """

    def test_parse_lg_events(self):
        metadata = {
            "url": "https://www.lgtwins.com/service/announcement?pageNo=1",
            "cutoff_days": 90,
            "fetched_at": "2025-06-01T00:00:00",
        }
        result = parse_team_events(self.LG_HTML, "lg_twins_events", metadata)
        events = [e for e in result if e["source_url"]]
        assert len(events) == 3
        titles = [e["title"] for e in events]
        assert "2025 시즌 이벤트 안내" in titles
        assert "사인회 및 팬미팅 개최" in titles
        assert "할인 프로모션 안내" in titles

    def test_skips_non_event_keywords(self):
        metadata = {
            "url": "https://www.lgtwins.com/service/announcement?pageNo=1",
            "cutoff_days": 90,
            "fetched_at": "2025-06-01T00:00:00",
        }
        result = parse_team_events(self.LG_HTML, "lg_twins_events", metadata)
        titles = [e["title"] for e in result]
        assert "선수단 훈련 일정입니다" not in titles

    def test_source_url_with_link_prefix(self):
        metadata = {
            "url": "https://www.lgtwins.com/service/announcement?pageNo=1",
            "cutoff_days": 90,
            "fetched_at": "2025-06-01T00:00:00",
        }
        result = parse_team_events(self.LG_HTML, "lg_twins_events", metadata)
        for event in result:
            assert event["source_url"].startswith("https://www.lgtwins.com")

    def test_filters_by_cutoff_date(self):
        metadata = {
            "url": "https://www.lgtwins.com/service/announcement?pageNo=1",
            "cutoff_days": 7,
            "fetched_at": "2025-04-10T00:00:00",
        }
        result = parse_team_events(self.LG_HTML, "lg_twins_events", metadata)
        assert all(e["published_at"] >= datetime(2025, 4, 3) for e in result)

    def test_unknown_source_key_returns_empty(self):
        result = parse_team_events("<html></html>", "unknown_source")
        assert result == []

    def test_parse_hanwha_json_response(self):
        html = """
        {
          "result": {
            "data": [
              {"TITLE": "[공지] 2026 KBO리그 암표 근절 캠페인", "PUB_DATE": "2026.03.25", "ID": 1836},
              {"TITLE": "2026 한화이글스 홈경기 입장권 안내", "PUB_DATE": "2026.03.18", "ID": 1829},
              {"TITLE": "한화이글스,‘러닝 클래스’운영…팬과 일상 속 접점 확대", "PUB_DATE": "2026.04.24", "ID": 1853},
              {"TITLE": "한화이글스, 두산과 손아섭 트레이드", "PUB_DATE": "2026.04.14", "ID": 1839}
            ]
          }
        }
        """
        metadata = {
            "url": "https://www.hanwhaeagles.co.kr/FA/CN/PCFACN01.do?page=1",
            "cutoff_days": 90,
            "fetched_at": "2026-06-07T00:00:00",
        }

        result = parse_team_events(html, "hanwha_eagles_events", metadata)

        titles = [e["title"] for e in result]
        assert titles == [
            "[공지] 2026 KBO리그 암표 근절 캠페인",
            "2026 한화이글스 홈경기 입장권 안내",
            "한화이글스,‘러닝 클래스’운영…팬과 일상 속 접점 확대",
        ]
        assert all(e["team_id"] == "HH" for e in result)
        assert all(e["source_url"].startswith(metadata["url"] + "#id=") for e in result)

    def test_json_response_respects_cutoff(self):
        html = """
        {"result":{"data":[
          {"TITLE":"2026 한화이글스 홈경기 입장권 안내","PUB_DATE":"2026.03.18","ID":1829}
        ]}}
        """
        metadata = {
            "url": "https://www.hanwhaeagles.co.kr/FA/CN/PCFACN01.do?page=1",
            "cutoff_days": 30,
            "fetched_at": "2026-06-07T00:00:00",
        }

        result = parse_team_events(html, "hanwha_eagles_events", metadata)

        assert result == []

    def test_parse_lg_events_feed_html(self):
        html = """
        <ul class="news_list event">
          <li class="ing">
            <a href="/twins/feed/events/detail?sneSeq=1776&page=">
              <span class="number">445</span>
              <div class="title_status_wrap">
                <div class="title_wrap">
                  <span class="title">그라운드 투어 (6월 14일)</span>
                </div>
              </div>
              <span class="date">2026/06/04</span>
            </a>
          </li>
        </ul>
        """
        metadata = {
            "url": "https://www.lgtwins.com/twins/feed/events?page=1",
            "cutoff_days": 30,
            "fetched_at": "2026-06-07T00:00:00",
        }

        result = parse_team_events(html, "lg_twins_events", metadata)

        assert [event["title"] for event in result] == ["그라운드 투어 (6월 14일)"]
        assert result[0]["published_at"] == datetime(2026, 6, 4)
        assert result[0]["source_url"].startswith("https://www.lgtwins.com/twins/feed/events/detail")

    def test_parse_doosan_event_json_content(self):
        html = """
        {"success":true,"content":[
          {"id":312,"title":"<현충일 추모 특별 행사>","createdDate":"2026-05-29T13:18:59.761"},
          {"id":311,"title":"<SPECIAL MATCH> 6.3(수) vs 한화 이글스","createdDate":"2026-05-22T13:50:54.092"}
        ]}
        """
        metadata = {
            "url": "https://www.doosanbears.com/doosan/v1/web/doorun/events?page=0&size=8",
            "cutoff_days": 30,
            "fetched_at": "2026-06-07T00:00:00",
        }

        result = parse_team_events(html, "doosan_bears_events", metadata)

        assert [event["team_id"] for event in result] == ["OB", "OB"]
        assert result[0]["published_at"] == datetime(2026, 5, 29)
        assert result[0]["source_url"].endswith("#id=312")

    def test_parse_ssg_media_news_cards(self):
        html = """
        <div style="width: 325px; height: 400px;">
          <div onclick="location.href='/media/news/detail?idx=19881&page='" style="cursor: pointer;"></div>
          <div style="text-align: center; padding: 20px;">
            <h4 class="text-dotdotdot">SSG랜더스, 6월 3일(수) 키움전 '깜자 데이' 진행</h4>
            <div class="text-dotdotdot">포토존, 특별 굿즈 부스, 팬 사인회 등</div>
            <div style="font-size: 12px;">2026.05.29</div>
          </div>
        </div>
        """
        metadata = {
            "url": "https://www.ssglanders.com/media/news?page=1",
            "cutoff_days": 30,
            "fetched_at": "2026-06-07T00:00:00",
        }

        result = parse_team_events(html, "ssg_landers_events", metadata)

        assert [event["title"] for event in result] == ["SSG랜더스, 6월 3일(수) 키움전 '깜자 데이' 진행"]
        assert result[0]["source_url"] == "https://www.ssglanders.com/media/news/detail?idx=19881&page="
        assert result[0]["published_at"] == datetime(2026, 5, 29)

    def test_parse_nc_event_board(self):
        html = """
        <div id="board_list_event">
          <ul>
            <li>
              <span class="cate">이벤트</span>
              <a href="/dinos/event/view.do?seq=44991&newsType=all&pageNo=1" class="title">
                6월 16-18일 홈 3연전 이벤트 안내
              </a>
              <div class="sort-wrap"><span class="date">2026-06-08</span></div>
            </li>
          </ul>
        </div>
        """
        metadata = {
            "url": "https://www.ncdinos.com/dinos/news.do?newsType=event&pageNo=1",
            "cutoff_days": 30,
            "fetched_at": "2026-06-07T00:00:00",
        }

        result = parse_team_events(html, "nc_dinos_events", metadata)

        assert [event["title"] for event in result] == ["6월 16-18일 홈 3연전 이벤트 안내"]
        assert result[0]["published_at"] == datetime(2026, 6, 8)
        assert result[0]["source_url"].startswith("https://www.ncdinos.com/dinos/event/view.do")

    def test_parse_kiwoom_heroes_news_events(self):
        html = """
        <div class="headNotice">
          <ul>
            <li><a href="view.do?num=22589">10일(수) NC전 '히어로데이 Part 4. Remember' 진행</a><span>2026.06.07</span></li>
          </ul>
        </div>
        <ul class="teamNews">
          <li>
            <h4><a href="view.do?num=22572">29일(금) KT전 '서울여자대학교 DAY' 진행</a></h4>
            <span>2026.05.27</span>
            <p><a href="view.do?num=22572">홈경기에 행사를 진행한다.</a></p>
          </li>
          <li>
            <h4><a href="view.do?num=22575">부상 대체 외국인 선수 로젠버그와 연장 계약 체결</a></h4>
            <span>2026.05.28</span>
          </li>
        </ul>
        """
        metadata = {
            "url": "https://www.heroesbaseball.co.kr/story/heroesNews/list.do?page=1",
            "cutoff_days": 30,
            "fetched_at": "2026-06-07T00:00:00",
        }

        result = parse_team_events(html, "kiwoom_heroes_events", metadata)

        titles = [event["title"] for event in result]
        assert titles == [
            "10일(수) NC전 '히어로데이 Part 4. Remember' 진행",
            "29일(금) KT전 '서울여자대학교 DAY' 진행",
        ]
        assert result[0]["source_url"] == "https://www.heroesbaseball.co.kr/story/heroesNews/view.do?num=22589"
        assert all(event["team_id"] == "WO" for event in result)

    def test_output_schema(self):
        metadata = {
            "url": "https://www.lgtwins.com/service/announcement?pageNo=1",
            "cutoff_days": 90,
            "fetched_at": "2025-06-01T00:00:00",
        }
        result = parse_team_events(self.LG_HTML, "lg_twins_events", metadata)
        assert len(result) >= 3
        event = result[0]
        assert event["event_scope"] == "team"
        assert event["team_id"] == "LG"
        assert event["title"]
        assert event["event_type"] in (
            "giveaway",
            "first_pitch",
            "discount",
            "fan_participation",
            "festival",
            "promotion",
            "notice",
        )
        assert isinstance(event["published_at"], datetime)
        assert event["source_url"].startswith("http")
        assert isinstance(event["last_seen_at"], datetime)
        assert event["status"] == "unknown"


class TestSourceConfigMap:
    def test_all_sources_have_selectors(self):
        for source_key, config in SOURCE_CONFIG_MAP.items():
            assert "title_sel" in config, f"{source_key} missing title_sel"
            assert "date_sel" in config, f"{source_key} missing date_sel"
            assert "link_prefix" in config, f"{source_key} missing link_prefix"

    def test_all_sources_have_mapping(self):
        for source_key in SOURCE_CONFIG_MAP:
            assert source_key in TEAM_CODE_FROM_SOURCE_KEY, f"{source_key} not in TEAM_CODE_FROM_SOURCE_KEY"
