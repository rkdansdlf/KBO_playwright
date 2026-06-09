
from src.crawlers.food_crawler import FoodCrawler


class TestParseFoodPage:
    def setup_method(self):
        self.crawler = FoodCrawler()

    def test_parses_menu_with_price(self):
        html = "<html><body>더블치즈버거: 8,500원</body></html>"
        info = {"stadium_id": "JAMSIL"}
        result = self.crawler._parse_food_page(html, info)
        assert len(result) == 1
        menus = result[0]["menus"]
        assert len(menus) >= 1
        assert menus[0]["menu_name"] == "더블치즈버거"
        assert menus[0]["price"] == 8500

    def test_no_price_no_result(self):
        html = "<html><body>메뉴 정보가 없습니다.</body></html>"
        info = {"stadium_id": "JAMSIL"}
        result = self.crawler._parse_food_page(html, info)
        assert result == []

    def test_vendor_metadata(self):
        html = "<html><body>떡볶이 3,000원</body></html>"
        info = {"stadium_id": "SAJIK"}
        result = self.crawler._parse_food_page(html, info)
        assert result[0]["vendor"]["stadium_id"] == "SAJIK"
        assert result[0]["vendor"]["order_method"] == "onsite"
