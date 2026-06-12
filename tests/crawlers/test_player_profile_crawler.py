from src.crawlers.player_profile_crawler import _clean_photo_url, _parse_debut_year, _parse_hands, _parse_height_weight


class TestParseHands:
    def test_right_throw_right_bat(self):
        result = _parse_hands("투수(우투우타)")
        assert result == {"throws": "R", "bats": "R"}

    def test_left_throw_left_bat(self):
        result = _parse_hands("투수(좌투좌타)")
        assert result == {"throws": "L", "bats": "L"}

    def test_switch_hitter(self):
        result = _parse_hands("투수(우투양타)")
        assert result == {"throws": "R", "bats": "S"}

    def test_no_match(self):
        result = _parse_hands("투수")
        assert result == {"throws": None, "bats": None}

    def test_empty_string(self):
        result = _parse_hands("")
        assert result == {"throws": None, "bats": None}


class TestParseDebutYear:
    def test_four_digit_year(self):
        assert _parse_debut_year("2015 두산") == 2015

    def test_two_digit_year_2000s(self):
        assert _parse_debut_year("15 두산") == 2015

    def test_two_digit_year_1900s(self):
        assert _parse_debut_year("99 삼성") == 1999

    def test_no_digits_returns_none(self):
        assert _parse_debut_year(None) is None
        assert _parse_debut_year("") is None


class TestParseHeightWeight:
    def test_parses_both(self):
        result = _parse_height_weight("185cm/92kg")
        assert result == {"height_cm": 185, "weight_kg": 92}

    def test_with_spaces(self):
        result = _parse_height_weight("185 cm / 92 kg")
        assert result == {"height_cm": 185, "weight_kg": 92}

    def test_no_match(self):
        result = _parse_height_weight(None)
        assert result == {"height_cm": None, "weight_kg": None}

    def test_empty_string(self):
        result = _parse_height_weight("")
        assert result == {"height_cm": None, "weight_kg": None}


class TestCleanPhotoUrl:
    def test_normal_url_passthrough(self):
        url = "https://example.com/photo.jpg"
        assert _clean_photo_url(url) == url

    def test_no_image_sentinel_returns_none(self):
        assert _clean_photo_url("http://example.com/no-Image.png") is None

    def test_protocol_relative_url(self):
        assert _clean_photo_url("//example.com/photo.jpg") == "https://example.com/photo.jpg"

    def test_none_or_empty_returns_none(self):
        assert _clean_photo_url(None) is None
        assert _clean_photo_url("") is None

    def test_data_url_returns_none(self):
        assert _clean_photo_url("data:image/png;base64,abc") is None
