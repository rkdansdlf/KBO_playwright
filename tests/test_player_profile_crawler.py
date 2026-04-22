from src.crawlers.player_profile_crawler import _parse_height_weight


def test_parse_height_weight_extracts_metric_values():
    assert _parse_height_weight("185cm/92kg") == {"height_cm": 185, "weight_kg": 92}
    assert _parse_height_weight("185 cm / 92 kg") == {"height_cm": 185, "weight_kg": 92}


def test_parse_height_weight_handles_missing_value():
    assert _parse_height_weight(None) == {"height_cm": None, "weight_kg": None}
