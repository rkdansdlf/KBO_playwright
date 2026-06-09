import pytest

from src.crawlers.parking_crawler import ParkingCrawler


class TestParseParkingPage:
    def setup_method(self):
        self.crawler = ParkingCrawler()

    def test_parses_parking_fees(self):
        html = "<html><body>기본 요금: 5,000원 추가 1,000원</body></html>"
        info = {"stadium_id": "MUNHAK"}
        result = self.crawler._parse_parking_page(html, info)
        assert len(result) == 1
        assert result[0]["lot"]["stadium_id"] == "MUNHAK"
        fees = result[0]["fee_rules"]
        assert len(fees) >= 1
        assert any(f["label"] == "기본" for f in fees)

    def test_no_fees_still_returns_lot(self):
        html = "<html><body>주차장 정보만 있습니다.</body></html>"
        info = {"stadium_id": "DAEGU"}
        result = self.crawler._parse_parking_page(html, info)
        assert len(result) == 1
        assert result[0]["fee_rules"] == []

    def test_lot_metadata(self):
        html = "<html><body>주차 가능</body></html>"
        info = {"stadium_id": "JAMSIL"}
        result = self.crawler._parse_parking_page(html, info)
        assert result[0]["lot"]["lot_type"] == "official"
        assert result[0]["lot"]["is_event_day_available"] is True
    