from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from src.utils.data_lineage import canonical_json, diff_values, sha256_json


def test_canonical_json_is_stable_for_mapping_order_and_decimal() -> None:
    left = {"b": Decimal("1.20"), "a": date(2026, 7, 18)}
    right = {"a": "2026-07-18", "b": "1.20"}

    assert canonical_json(left) == canonical_json(right)
    assert sha256_json(left) == sha256_json(right)


def test_diff_values_reports_missing_and_changed_fields() -> None:
    differences = diff_values({"a": 1, "nested": {"x": True}}, {"a": 2, "nested": {}})

    assert {item["path"] for item in differences} == {"$.a", "$.nested.x"}
    assert differences[1]["actual"] == "<missing>"


def test_canonical_json_rejects_non_finite_float() -> None:
    with pytest.raises(ValueError, match="Non-finite float"):
        canonical_json(float("nan"))
