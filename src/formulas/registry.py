"""Declarative Registry of Standardized KBO Sabermetrics Formulas and Specifications."""

from __future__ import annotations

from typing import Any, ClassVar

from src.formulas.models import (
    FormulaVersion,
    MetricCategory,
    MetricDefinition,
    RuleSeverity,
    ValidationRule,
)

_DEFAULT_V1 = FormulaVersion(
    version="1.0.0",
    effective_season_start=1982,
    changelog="Initial standardized KBO sabermetrics specification.",
)

MAX_SLG_BOUND: float = 4.0
MAX_OPS_BOUND: float = 5.0


def _safe_float(val: object) -> float:
    """Safely convert any input value to float, defaulting to 0.0."""
    if val is None:
        return 0.0
    try:
        return float(val)  # type: ignore[arg-type]
    except (ValueError, TypeError):
        return 0.0


# ==============================================================================
# Pure Functional Mathematical Evaluation Functions (Batting)
# ==============================================================================


def _eval_avg(inputs: dict[str, Any], _c: dict[str, float]) -> float:
    h = _safe_float(inputs.get("hits"))
    ab = _safe_float(inputs.get("at_bats"))
    return round(h / ab, 3) if ab > 0 else 0.000


def _eval_obp(inputs: dict[str, Any], _c: dict[str, float]) -> float:
    h = _safe_float(inputs.get("hits"))
    bb = _safe_float(inputs.get("walks"))
    hbp = _safe_float(inputs.get("hbp"))
    ab = _safe_float(inputs.get("at_bats"))
    sf = _safe_float(inputs.get("sacrifice_flies"))
    if inputs.get("sacrifice_flies") is None and ("plate_appearances" in inputs or "pa" in inputs):
        pa = _safe_float(inputs.get("plate_appearances") or inputs.get("pa"))
        sh = _safe_float(inputs.get("sacrifice_hits") or inputs.get("sh"))
        sf = max(pa - ab - bb - hbp - sh, 0.0)
    den = ab + bb + hbp + sf
    return round((h + bb + hbp) / den, 3) if den > 0 else 0.000


def _eval_slg(inputs: dict[str, Any], _c: dict[str, float]) -> float:
    h = _safe_float(inputs.get("hits"))
    d2 = _safe_float(inputs.get("doubles"))
    d3 = _safe_float(inputs.get("triples"))
    hr = _safe_float(inputs.get("home_runs"))
    ab = _safe_float(inputs.get("at_bats"))
    h1 = max(h - d2 - d3 - hr, 0.0)
    tb = h1 + 2.0 * d2 + 3.0 * d3 + 4.0 * hr
    return round(tb / ab, 3) if ab > 0 else 0.000


def _eval_ops(inputs: dict[str, Any], constants: dict[str, float]) -> float:
    obp = _eval_obp(inputs, constants)
    slg = _eval_slg(inputs, constants)
    return round(obp + slg, 3)


def _eval_iso(inputs: dict[str, Any], constants: dict[str, float]) -> float:
    avg = _eval_avg(inputs, constants)
    slg = _eval_slg(inputs, constants)
    return round(slg - avg, 3)


def _eval_babip_bat(inputs: dict[str, Any], _c: dict[str, float]) -> float:
    h = _safe_float(inputs.get("hits"))
    hr = _safe_float(inputs.get("home_runs"))
    ab = _safe_float(inputs.get("at_bats"))
    so = _safe_float(inputs.get("strikeouts"))
    sf = _safe_float(inputs.get("sacrifice_flies"))
    num = h - hr
    den = ab - so - hr + sf
    return round(num / den, 3) if den > 0 else 0.000


def _eval_bb_pct_bat(inputs: dict[str, Any], _c: dict[str, float]) -> float:
    bb = _safe_float(inputs.get("walks"))
    pa = _safe_float(inputs.get("plate_appearances") or inputs.get("pa"))
    if pa <= 0:
        ab = _safe_float(inputs.get("at_bats"))
        hbp = _safe_float(inputs.get("hbp"))
        sf = _safe_float(inputs.get("sacrifice_flies"))
        pa = ab + bb + hbp + sf
    return round(bb / pa, 3) if pa > 0 else 0.000


def _eval_k_pct_bat(inputs: dict[str, Any], _c: dict[str, float]) -> float:
    so = _safe_float(inputs.get("strikeouts"))
    pa = _safe_float(inputs.get("plate_appearances") or inputs.get("pa"))
    if pa <= 0:
        ab = _safe_float(inputs.get("at_bats"))
        bb = _safe_float(inputs.get("walks"))
        hbp = _safe_float(inputs.get("hbp"))
        sf = _safe_float(inputs.get("sacrifice_flies"))
        pa = ab + bb + hbp + sf
    return round(so / pa, 3) if pa > 0 else 0.000


def _eval_bb_to_k_bat(inputs: dict[str, Any], _c: dict[str, float]) -> float:
    bb = _safe_float(inputs.get("walks"))
    so = _safe_float(inputs.get("strikeouts"))
    return round(bb / so, 2) if so > 0 else 0.00


def _eval_woba(inputs: dict[str, Any], constants: dict[str, float]) -> float:
    h = _safe_float(inputs.get("hits"))
    d2 = _safe_float(inputs.get("doubles"))
    d3 = _safe_float(inputs.get("triples"))
    hr = _safe_float(inputs.get("home_runs"))
    bb = _safe_float(inputs.get("walks"))
    ibb = _safe_float(inputs.get("intentional_walks"))
    hbp = _safe_float(inputs.get("hbp"))
    sf = _safe_float(inputs.get("sacrifice_flies"))
    ab = _safe_float(inputs.get("at_bats"))

    u_bb = max(bb - ibb, 0.0)
    h1 = max(h - d2 - d3 - hr, 0.0)

    w_bb = constants.get("w_bb", 0.690)
    w_hbp = constants.get("w_hbp", 0.720)
    w_1b = constants.get("w_1b", 0.890)
    w_2b = constants.get("w_2b", 1.270)
    w_3b = constants.get("w_3b", 1.620)
    w_hr = constants.get("w_hr", 2.100)

    num = w_bb * u_bb + w_hbp * hbp + w_1b * h1 + w_2b * d2 + w_3b * d3 + w_hr * hr
    den = ab + u_bb + hbp + sf
    return round(num / den, 3) if den > 0 else 0.000


def _eval_wraa(inputs: dict[str, Any], constants: dict[str, float]) -> float:
    woba = _eval_woba(inputs, constants)
    lg_woba = constants.get("lg_woba", 0.330)
    woba_scale = constants.get("woba_scale", 1.240)
    pa = _safe_float(inputs.get("plate_appearances") or inputs.get("pa"))
    if pa <= 0:
        ab = _safe_float(inputs.get("at_bats"))
        bb = _safe_float(inputs.get("walks"))
        hbp = _safe_float(inputs.get("hbp"))
        sf = _safe_float(inputs.get("sacrifice_flies"))
        pa = ab + bb + hbp + sf

    return round(((woba - lg_woba) / max(woba_scale, 0.001)) * pa, 2) if pa > 0 else 0.00


def _eval_wrc(inputs: dict[str, Any], constants: dict[str, float]) -> float:
    wraa = _eval_wraa(inputs, constants)
    lg_r_per_pa = constants.get("lg_r_per_pa", 0.120)
    pa = _safe_float(inputs.get("plate_appearances") or inputs.get("pa"))
    if pa <= 0:
        ab = _safe_float(inputs.get("at_bats"))
        bb = _safe_float(inputs.get("walks"))
        hbp = _safe_float(inputs.get("hbp"))
        sf = _safe_float(inputs.get("sacrifice_flies"))
        pa = ab + bb + hbp + sf

    return round(wraa + (pa * lg_r_per_pa), 2) if pa > 0 else 0.00


def _eval_wrc_index_no_park(inputs: dict[str, Any], constants: dict[str, float]) -> float:
    wraa = _eval_wraa(inputs, constants)
    pa = _safe_float(inputs.get("plate_appearances") or inputs.get("pa"))
    if pa <= 0:
        ab = _safe_float(inputs.get("at_bats"))
        bb = _safe_float(inputs.get("walks"))
        hbp = _safe_float(inputs.get("hbp"))
        sf = _safe_float(inputs.get("sacrifice_flies"))
        pa = ab + bb + hbp + sf

    if pa <= 0:
        return 100.0
    lg_r_per_pa = constants.get("lg_r_per_pa", 0.120)
    val = (((wraa / pa) + lg_r_per_pa) / max(lg_r_per_pa, 0.001)) * 100.0
    return round(val, 1)


def _eval_ops_index_no_park(inputs: dict[str, Any], constants: dict[str, float]) -> float:
    obp = _eval_obp(inputs, constants)
    slg = _eval_slg(inputs, constants)
    lg_obp = constants.get("lg_obp", 0.340)
    lg_slg = constants.get("lg_slg", 0.410)
    val = 100.0 * ((obp / max(lg_obp, 0.001)) + (slg / max(lg_slg, 0.001)) - 1.0)
    return round(val, 1)


def _eval_gpa(inputs: dict[str, Any], constants: dict[str, float]) -> float:
    obp = _eval_obp(inputs, constants)
    slg = _eval_slg(inputs, constants)
    return round((1.8 * obp + slg) / 4.0, 3)


def _eval_seca(inputs: dict[str, Any], _c: dict[str, float]) -> float:
    h = _safe_float(inputs.get("hits"))
    d2 = _safe_float(inputs.get("doubles"))
    d3 = _safe_float(inputs.get("triples"))
    hr = _safe_float(inputs.get("home_runs"))
    bb = _safe_float(inputs.get("walks"))
    sb = _safe_float(inputs.get("stolen_bases"))
    cs = _safe_float(inputs.get("caught_stealing"))
    ab = _safe_float(inputs.get("at_bats"))

    h1 = max(h - d2 - d3 - hr, 0.0)
    tb = h1 + 2.0 * d2 + 3.0 * d3 + 4.0 * hr
    iso_tb = max(tb - h, 0.0)
    num = bb + iso_tb + (sb - cs)
    return round(num / ab, 3) if ab > 0 else 0.000


def _eval_rc(inputs: dict[str, Any], _c: dict[str, float]) -> float:
    h = _safe_float(inputs.get("hits"))
    d2 = _safe_float(inputs.get("doubles"))
    d3 = _safe_float(inputs.get("triples"))
    hr = _safe_float(inputs.get("home_runs"))
    bb = _safe_float(inputs.get("walks"))
    ab = _safe_float(inputs.get("at_bats"))

    h1 = max(h - d2 - d3 - hr, 0.0)
    tb = h1 + 2.0 * d2 + 3.0 * d3 + 4.0 * hr
    num = (h + bb) * tb
    den = ab + bb
    return round(num / den, 2) if den > 0 else 0.00


def _parse_ip_to_outs(ip_val: object) -> float:
    """Accurately convert baseball innings pitched notation (e.g. 10.1, 0.67) to outs."""
    if ip_val is None:
        return 0.0
    try:
        f = float(ip_val)  # type: ignore[arg-type]
        whole = int(f)
        frac = round(f - whole, 2)
        if frac in (0.1, 0.33):
            return float(whole * 3 + 1)
        if frac in (0.2, 0.67):
            return float(whole * 3 + 2)
        return round(f * 3.0, 1)
    except (ValueError, TypeError):
        return 0.0


# ==============================================================================
# Pure Functional Mathematical Evaluation Functions (Pitching)
# ==============================================================================


def _eval_era(inputs: dict[str, Any], _c: dict[str, float]) -> float:
    er = _safe_float(inputs.get("earned_runs") or inputs.get("er"))
    outs = _safe_float(inputs.get("innings_outs") or inputs.get("outs"))
    if outs <= 0 and ("innings_pitched" in inputs or "ip" in inputs):
        outs = _parse_ip_to_outs(inputs.get("innings_pitched") or inputs.get("ip"))
    return round((er * 27.0) / outs, 2) if outs > 0 else 0.00


def _eval_whip(inputs: dict[str, Any], _c: dict[str, float]) -> float:
    h = _safe_float(inputs.get("hits_allowed") or inputs.get("h_allowed") or inputs.get("hits"))
    bb = _safe_float(inputs.get("walks_allowed") or inputs.get("bb_allowed") or inputs.get("walks"))
    outs = _safe_float(inputs.get("innings_outs") or inputs.get("outs"))
    if outs <= 0 and ("innings_pitched" in inputs or "ip" in inputs):
        outs = _parse_ip_to_outs(inputs.get("innings_pitched") or inputs.get("ip"))
    return round((3.0 * (h + bb)) / outs, 2) if outs > 0 else 0.00


def _eval_fip(inputs: dict[str, Any], constants: dict[str, float]) -> float:
    hr = _safe_float(inputs.get("home_runs_allowed") or inputs.get("hr_allowed"))
    bb = _safe_float(inputs.get("walks_allowed") or inputs.get("bb_allowed"))
    hbp = _safe_float(inputs.get("hit_batters") or inputs.get("hbp_allowed"))
    so = _safe_float(inputs.get("strikeouts") or inputs.get("so"))
    outs = _safe_float(inputs.get("innings_outs") or inputs.get("outs"))
    if outs <= 0 and ("innings_pitched" in inputs or "ip" in inputs):
        outs = _parse_ip_to_outs(inputs.get("innings_pitched") or inputs.get("ip"))

    if outs <= 0:
        return 0.00
    c_fip = constants.get("c_fip", 3.850)
    fip_comp = (3.0 * (13.0 * hr + 3.0 * (bb + hbp) - 2.0 * so)) / outs
    return round(fip_comp + c_fip, 2)


def _eval_k_9(inputs: dict[str, Any], _c: dict[str, float]) -> float:
    so = _safe_float(inputs.get("strikeouts") or inputs.get("so"))
    outs = _safe_float(inputs.get("innings_outs") or inputs.get("outs"))
    if outs <= 0 and ("innings_pitched" in inputs or "ip" in inputs):
        outs = _parse_ip_to_outs(inputs.get("innings_pitched") or inputs.get("ip"))
    return round((27.0 * so) / outs, 2) if outs > 0 else 0.00


def _eval_bb_9(inputs: dict[str, Any], _c: dict[str, float]) -> float:
    bb = _safe_float(inputs.get("walks_allowed") or inputs.get("bb_allowed"))
    outs = _safe_float(inputs.get("innings_outs") or inputs.get("outs"))
    if outs <= 0 and ("innings_pitched" in inputs or "ip" in inputs):
        outs = _parse_ip_to_outs(inputs.get("innings_pitched") or inputs.get("ip"))
    return round((27.0 * bb) / outs, 2) if outs > 0 else 0.00


def _eval_hr_9(inputs: dict[str, Any], _c: dict[str, float]) -> float:
    hr = _safe_float(inputs.get("home_runs_allowed") or inputs.get("hr_allowed"))
    outs = _safe_float(inputs.get("innings_outs") or inputs.get("outs"))
    if outs <= 0 and ("innings_pitched" in inputs or "ip" in inputs):
        outs = _parse_ip_to_outs(inputs.get("innings_pitched") or inputs.get("ip"))
    return round((27.0 * hr) / outs, 2) if outs > 0 else 0.00


def _eval_k_pct_pit(inputs: dict[str, Any], _c: dict[str, float]) -> float:
    so = _safe_float(inputs.get("strikeouts") or inputs.get("so"))
    tbf = _safe_float(inputs.get("batters_faced") or inputs.get("tbf"))
    return round(so / tbf, 3) if tbf > 0 else 0.000


def _eval_bb_pct_pit(inputs: dict[str, Any], _c: dict[str, float]) -> float:
    bb = _safe_float(inputs.get("walks_allowed") or inputs.get("bb_allowed"))
    tbf = _safe_float(inputs.get("batters_faced") or inputs.get("tbf"))
    return round(bb / tbf, 3) if tbf > 0 else 0.000


def _eval_k_bb_pit(inputs: dict[str, Any], _c: dict[str, float]) -> float:
    so = _safe_float(inputs.get("strikeouts") or inputs.get("so"))
    bb = _safe_float(inputs.get("walks_allowed") or inputs.get("bb_allowed"))
    tbf = _safe_float(inputs.get("batters_faced") or inputs.get("tbf"))
    return round((so - bb) / tbf, 3) if tbf > 0 else 0.000


def _eval_babip_pit(inputs: dict[str, Any], _c: dict[str, float]) -> float:
    h = _safe_float(inputs.get("hits_allowed") or inputs.get("h_allowed"))
    hr = _safe_float(inputs.get("home_runs_allowed") or inputs.get("hr_allowed"))
    so = _safe_float(inputs.get("strikeouts") or inputs.get("so"))
    tbf = _safe_float(inputs.get("batters_faced") or inputs.get("tbf"))
    sf = _safe_float(inputs.get("sacrifice_flies_allowed"))
    num = h - hr
    den = tbf - so - hr + sf
    return round(num / den, 3) if den > 0 else 0.000


def _eval_lob_pct(inputs: dict[str, Any], _c: dict[str, float]) -> float:
    h = _safe_float(inputs.get("hits_allowed") or inputs.get("h_allowed"))
    bb = _safe_float(inputs.get("walks_allowed") or inputs.get("bb_allowed"))
    hbp = _safe_float(inputs.get("hit_batters") or inputs.get("hbp_allowed"))
    r = _safe_float(inputs.get("runs_allowed") or inputs.get("r_allowed"))
    hr = _safe_float(inputs.get("home_runs_allowed") or inputs.get("hr_allowed"))
    num = h + bb + hbp - r
    den = (h + bb + hbp) - (1.4 * hr)
    return round(max(min(num / den, 1.0), 0.0), 3) if den > 0 else 0.000


def _eval_era_index_no_park(inputs: dict[str, Any], constants: dict[str, float]) -> float:
    era = _eval_era(inputs, constants)
    lg_era = constants.get("lg_era", 4.500)
    return round(100.0 * (lg_era / max(era, 0.01)), 1) if era > 0 else 100.0


def _eval_dice(inputs: dict[str, Any], _c: dict[str, float]) -> float:
    hr = _safe_float(inputs.get("home_runs_allowed") or inputs.get("hr_allowed"))
    bb = _safe_float(inputs.get("walks_allowed") or inputs.get("bb_allowed"))
    hbp = _safe_float(inputs.get("hit_batters") or inputs.get("hbp_allowed"))
    so = _safe_float(inputs.get("strikeouts") or inputs.get("so"))
    outs = _safe_float(inputs.get("innings_outs") or inputs.get("outs"))
    if outs <= 0 and ("innings_pitched" in inputs or "ip" in inputs):
        outs = _parse_ip_to_outs(inputs.get("innings_pitched") or inputs.get("ip"))
    if outs <= 0:
        return 3.00
    comp = (3.0 * (13.0 * hr + 3.0 * (bb + hbp) - 2.0 * so)) / outs
    return round(3.00 + comp, 2)


# ==============================================================================
# Pure Functional Mathematical Evaluation Functions (Baserunning & Fielding)
# ==============================================================================


def _eval_sb_pct(inputs: dict[str, Any], _c: dict[str, float]) -> float:
    sb = _safe_float(inputs.get("stolen_bases") or inputs.get("sb"))
    cs = _safe_float(inputs.get("caught_stealing") or inputs.get("cs"))
    den = sb + cs
    return round(sb / den, 3) if den > 0 else 0.000


def _eval_fpct(inputs: dict[str, Any], _c: dict[str, float]) -> float:
    po = _safe_float(inputs.get("putouts") or inputs.get("po"))
    a = _safe_float(inputs.get("assists") or inputs.get("a"))
    e = _safe_float(inputs.get("errors") or inputs.get("e"))
    chances = po + a + e
    return round((po + a) / chances, 3) if chances > 0 else 1.000


def _eval_rf_9(inputs: dict[str, Any], _c: dict[str, float]) -> float:
    po = _safe_float(inputs.get("putouts") or inputs.get("po"))
    a = _safe_float(inputs.get("assists") or inputs.get("a"))
    outs = _safe_float(inputs.get("innings_outs") or inputs.get("outs"))
    if outs <= 0 and ("innings" in inputs or "inn" in inputs):
        outs = _safe_float(inputs.get("innings") or inputs.get("inn")) * 3.0
    return round((27.0 * (po + a)) / outs, 2) if outs > 0 else 0.00


# ==============================================================================
# Formula Registry Catalog
# ==============================================================================


class FormulaRegistry:
    """Immutable central catalog of standardized, versioned KBO sabermetric formulas."""

    _CATALOG: ClassVar[dict[str, MetricDefinition]] = {
        # --- Batting ---
        "AVG": MetricDefinition(
            metric_id="AVG",
            name="Batting Average",
            korean_name="타율",
            category=MetricCategory.BATTING,
            version=_DEFAULT_V1,
            latex_formula=r"\text{AVG} = \frac{\text{Hits}}{\text{At Bats}} = \frac{H}{AB}",
            eval_fn=_eval_avg,
            input_fields=["hits", "at_bats"],
            validation_rules=[
                ValidationRule(
                    "range",
                    lambda v, _: 0.0 <= float(v) <= 1.0,
                    r"0 \le \text{AVG} \le 1.0",
                    "AVG out of algebraic bounds",
                    severity=RuleSeverity.ALGEBRAIC,
                )
            ],
            precision=3,
            description="Ratio of base hits to official at-bats.",
        ),
        "OBP": MetricDefinition(
            metric_id="OBP",
            name="On-Base Percentage",
            korean_name="출루율",
            category=MetricCategory.BATTING,
            version=_DEFAULT_V1,
            latex_formula=r"\text{OBP} = \frac{H + BB + HBP}{AB + BB + HBP + SF}",
            eval_fn=_eval_obp,
            input_fields=["hits", "walks", "hbp", "at_bats", "sacrifice_flies"],
            validation_rules=[
                ValidationRule(
                    "range",
                    lambda v, _: 0.0 <= float(v) <= 1.0,
                    r"0 \le \text{OBP} \le 1.0",
                    "OBP out of algebraic bounds",
                    severity=RuleSeverity.ALGEBRAIC,
                )
            ],
            precision=3,
            description="Frequency with which a batter reaches base per plate appearance opportunity.",
        ),
        "SLG": MetricDefinition(
            metric_id="SLG",
            name="Slugging Percentage",
            korean_name="장타율",
            category=MetricCategory.BATTING,
            version=_DEFAULT_V1,
            latex_formula=r"\text{SLG} = \frac{1B + 2\cdot 2B + 3\cdot 3B + 4\cdot HR}{AB} = \frac{TB}{AB}",
            eval_fn=_eval_slg,
            input_fields=["hits", "doubles", "triples", "home_runs", "at_bats"],
            validation_rules=[
                ValidationRule(
                    "range",
                    lambda v, _: 0.0 <= float(v) <= MAX_SLG_BOUND,
                    r"0 \le \text{SLG} \le 4.0",
                    "SLG out of algebraic bounds",
                    severity=RuleSeverity.ALGEBRAIC,
                )
            ],
            precision=3,
            description="Total bases achieved per official at-bat.",
        ),
        "OPS": MetricDefinition(
            metric_id="OPS",
            name="On-Base Plus Slugging",
            korean_name="출루율+장타율",
            category=MetricCategory.BATTING,
            version=_DEFAULT_V1,
            latex_formula=r"\text{OPS} = \text{OBP} + \text{SLG}",
            eval_fn=_eval_ops,
            input_fields=["hits", "walks", "hbp", "at_bats", "sacrifice_flies", "doubles", "triples", "home_runs"],
            validation_rules=[
                ValidationRule(
                    "range",
                    lambda v, _: 0.0 <= float(v) <= MAX_OPS_BOUND,
                    r"0 \le \text{OPS} \le 5.0",
                    "OPS out of algebraic bounds",
                    severity=RuleSeverity.ALGEBRAIC,
                )
            ],
            precision=3,
            description="Composite offensive production summing on-base and slugging percentages.",
        ),
        "ISO": MetricDefinition(
            metric_id="ISO",
            name="Isolated Power",
            korean_name="순장타율",
            category=MetricCategory.BATTING,
            version=_DEFAULT_V1,
            latex_formula=r"\text{ISO} = \text{SLG} - \text{AVG} = \frac{2B + 2\cdot 3B + 3\cdot HR}{AB}",
            eval_fn=_eval_iso,
            input_fields=["hits", "doubles", "triples", "home_runs", "at_bats"],
            precision=3,
            description="Measurement of extra-base hitting power independent of singles.",
        ),
        "BABIP_BAT": MetricDefinition(
            metric_id="BABIP_BAT",
            name="Batting BABIP",
            korean_name="인플레이 타구 타율",
            category=MetricCategory.BATTING,
            version=_DEFAULT_V1,
            latex_formula=r"\text{BABIP} = \frac{H - HR}{AB - K - HR + SF}",
            eval_fn=_eval_babip_bat,
            input_fields=["hits", "home_runs", "at_bats", "strikeouts", "sacrifice_flies"],
            precision=3,
            description="Batting average solely on balls put into play against fielders.",
        ),
        "BB_PCT_BAT": MetricDefinition(
            metric_id="BB_PCT_BAT",
            name="Walk Rate (Batting)",
            korean_name="볼넷 비율",
            category=MetricCategory.BATTING,
            version=_DEFAULT_V1,
            latex_formula=r"\text{BB\%} = \frac{BB}{PA}",
            eval_fn=_eval_bb_pct_bat,
            input_fields=["walks", "plate_appearances", "at_bats", "hbp", "sacrifice_flies"],
            precision=3,
            description="Proportion of plate appearances resulting in a base on balls.",
        ),
        "K_PCT_BAT": MetricDefinition(
            metric_id="K_PCT_BAT",
            name="Strikeout Rate (Batting)",
            korean_name="삼진 비율",
            category=MetricCategory.BATTING,
            version=_DEFAULT_V1,
            latex_formula=r"\text{K\%} = \frac{K}{PA}",
            eval_fn=_eval_k_pct_bat,
            input_fields=["strikeouts", "plate_appearances", "at_bats", "walks", "hbp", "sacrifice_flies"],
            precision=3,
            description="Proportion of plate appearances resulting in a strikeout.",
        ),
        "BB_TO_K_BAT": MetricDefinition(
            metric_id="BB_TO_K_BAT",
            name="Walk-to-Strikeout Ratio",
            korean_name="볼삼비",
            category=MetricCategory.BATTING,
            version=_DEFAULT_V1,
            latex_formula=r"\text{BB/K} = \frac{BB}{SO}",
            eval_fn=_eval_bb_to_k_bat,
            input_fields=["walks", "strikeouts"],
            precision=2,
            description="Ratio of walks drawn to strikeouts suffered.",
        ),
        "wOBA": MetricDefinition(
            metric_id="wOBA",
            name="Weighted On-Base Average",
            korean_name="가중 출루율",
            category=MetricCategory.BATTING,
            version=_DEFAULT_V1,
            latex_formula=(
                r"\text{wOBA} = \frac{w_{BB}\cdot uBB + w_{HBP}\cdot HBP + "
                r"w_{1B}\cdot 1B + w_{2B}\cdot 2B + w_{3B}\cdot 3B + w_{HR}\cdot HR}"
                r"{AB + uBB + HBP + SF}"
            ),
            eval_fn=_eval_woba,
            input_fields=[
                "hits",
                "doubles",
                "triples",
                "home_runs",
                "walks",
                "intentional_walks",
                "hbp",
                "sacrifice_flies",
                "at_bats",
            ],
            constants_required=["w_bb", "w_hbp", "w_1b", "w_2b", "w_3b", "w_hr"],
            validation_rules=[
                ValidationRule(
                    "plausibility",
                    lambda v, c: 0.0 <= float(v) <= max(c.get("w_hr", 2.15), 2.20),
                    r"0 \le \text{wOBA} \le w_{HR}",
                    "wOBA exceeds maximum possible linear weight",
                    severity=RuleSeverity.PLAUSIBILITY,
                )
            ],
            precision=3,
            description="Linear weights measure of offensive value attributing accurate run values to each event.",
        ),
        "wRAA": MetricDefinition(
            metric_id="wRAA",
            name="Weighted Runs Above Average",
            korean_name="평균 대비 득점 창출",
            category=MetricCategory.BATTING,
            version=_DEFAULT_V1,
            latex_formula=r"\text{wRAA} = \frac{\text{wOBA} - \text{lg}wOBA}{wOBA\_scale} \cdot PA",
            eval_fn=_eval_wraa,
            input_fields=[
                "hits",
                "doubles",
                "triples",
                "home_runs",
                "walks",
                "intentional_walks",
                "hbp",
                "sacrifice_flies",
                "at_bats",
                "plate_appearances",
            ],
            constants_required=["lg_woba", "woba_scale"],
            precision=2,
            description="Number of offensive runs contributed above the league average baseline.",
        ),
        "wRC": MetricDefinition(
            metric_id="wRC",
            name="Weighted Runs Created",
            korean_name="가중 득점 창출",
            category=MetricCategory.BATTING,
            version=_DEFAULT_V1,
            latex_formula=r"\text{wRC} = \text{wRAA} + (PA \cdot \text{lg}R/\text{lg}PA)",
            eval_fn=_eval_wrc,
            input_fields=[
                "hits",
                "doubles",
                "triples",
                "home_runs",
                "walks",
                "intentional_walks",
                "hbp",
                "sacrifice_flies",
                "at_bats",
                "plate_appearances",
            ],
            constants_required=["lg_woba", "woba_scale", "lg_r_per_pa"],
            precision=2,
            description="Total offensive runs created through all batting outcomes.",
        ),
        "WRC_INDEX_NO_PARK": MetricDefinition(
            metric_id="WRC_INDEX_NO_PARK",
            name="Weighted Runs Created Index (No Park Adjustment)",
            korean_name="조정 득점 창출 지수 (파크팩터 미보정)",
            category=MetricCategory.BATTING,
            version=_DEFAULT_V1,
            latex_formula=(
                r"\text{wRC\_INDEX} = 100 \cdot \frac{\frac{\text{wRAA}}{PA} + "
                r"\frac{\text{lg}R}{\text{lg}PA}}{\frac{\text{lg}R}{\text{lg}PA}}"
            ),
            eval_fn=_eval_wrc_index_no_park,
            input_fields=[
                "hits",
                "doubles",
                "triples",
                "home_runs",
                "walks",
                "intentional_walks",
                "hbp",
                "sacrifice_flies",
                "at_bats",
                "plate_appearances",
            ],
            constants_required=["lg_woba", "woba_scale", "lg_r_per_pa"],
            precision=1,
            description="League-relative offensive production indexed to 100 as average (without park adjustment).",
            is_park_adjusted=False,
        ),
        "OPS_INDEX_NO_PARK": MetricDefinition(
            metric_id="OPS_INDEX_NO_PARK",
            name="OPS Index (No Park Adjustment)",
            korean_name="조정 OPS 지수 (파크팩터 미보정)",
            category=MetricCategory.BATTING,
            version=_DEFAULT_V1,
            latex_formula=(
                r"\text{OPS\_INDEX} = 100 \cdot \left( \frac{\text{OBP}}{\text{lg}OBP} + "
                r"\frac{\text{SLG}}{\text{lg}SLG} - 1 \right)"
            ),
            eval_fn=_eval_ops_index_no_park,
            input_fields=["hits", "walks", "hbp", "at_bats", "sacrifice_flies", "doubles", "triples", "home_runs"],
            constants_required=["lg_obp", "lg_slg"],
            precision=1,
            description="League-normalized OPS scaled so 100 is league average (without park adjustment).",
            is_park_adjusted=False,
        ),
        "GPA": MetricDefinition(
            metric_id="GPA",
            name="Grounded Player Average",
            korean_name="조정 선수 평점",
            category=MetricCategory.BATTING,
            version=_DEFAULT_V1,
            latex_formula=r"\text{GPA} = \frac{1.8\cdot\text{OBP} + \text{SLG}}{4}",
            eval_fn=_eval_gpa,
            input_fields=["hits", "walks", "hbp", "at_bats", "sacrifice_flies", "doubles", "triples", "home_runs"],
            precision=3,
            description="OPS variant applying realistic 1.8x weighting to OBP relative to SLG.",
        ),
        "SecA": MetricDefinition(
            metric_id="SecA",
            name="Secondary Average",
            korean_name="보조 타율",
            category=MetricCategory.BATTING,
            version=_DEFAULT_V1,
            latex_formula=r"\text{SecA} = \frac{BB + (TB - H) + (SB - CS)}{AB}",
            eval_fn=_eval_seca,
            input_fields=[
                "hits",
                "doubles",
                "triples",
                "home_runs",
                "walks",
                "stolen_bases",
                "caught_stealing",
                "at_bats",
            ],
            precision=3,
            description="Bill James metric evaluating power, walks, and base running independent of batting average.",
        ),
        "RC": MetricDefinition(
            metric_id="RC",
            name="Runs Created (Basic Bill James V1)",
            korean_name="득점 기여도 (RC)",
            category=MetricCategory.BATTING,
            version=_DEFAULT_V1,
            latex_formula=r"\text{RC} = \frac{(H + BB) \cdot TB}{AB + BB}",
            eval_fn=_eval_rc,
            input_fields=["hits", "doubles", "triples", "home_runs", "walks", "at_bats"],
            precision=2,
            description="Classical Bill James estimate of total runs generated by a player.",
        ),
        # --- Pitching ---
        "ERA": MetricDefinition(
            metric_id="ERA",
            name="Earned Run Average",
            korean_name="평균자책점",
            category=MetricCategory.PITCHING,
            version=_DEFAULT_V1,
            latex_formula=r"\text{ERA} = \frac{27 \cdot ER}{\text{Outs}} = \frac{9 \cdot ER}{IP}",
            eval_fn=_eval_era,
            input_fields=["earned_runs", "innings_outs"],
            validation_rules=[
                ValidationRule(
                    "positive",
                    lambda v, _: float(v) >= 0.0,
                    r"\text{ERA} \ge 0.0",
                    "ERA must be non-negative",
                    severity=RuleSeverity.ALGEBRAIC,
                )
            ],
            precision=2,
            description="Average earned runs allowed per 9 regulation innings pitched.",
        ),
        "WHIP": MetricDefinition(
            metric_id="WHIP",
            name="Walks Plus Hits per Inning Pitched",
            korean_name="이닝당 출루허용률",
            category=MetricCategory.PITCHING,
            version=_DEFAULT_V1,
            latex_formula=r"\text{WHIP} = \frac{3 \cdot (H + BB)}{\text{Outs}} = \frac{H + BB}{IP}",
            eval_fn=_eval_whip,
            input_fields=["hits_allowed", "walks_allowed", "innings_outs"],
            precision=2,
            description="Baseline rate of baserunners allowed via hit or walk per inning pitched.",
        ),
        "FIP": MetricDefinition(
            metric_id="FIP",
            name="Fielding Independent Pitching",
            korean_name="수비 무관 자책점",
            category=MetricCategory.PITCHING,
            version=_DEFAULT_V1,
            latex_formula=(
                r"\text{FIP} = \frac{3\cdot (13\cdot HR + 3\cdot(uBB + HBP) - 2\cdot K)}"
                r"{\text{Outs}} + c_{FIP}"
            ),
            eval_fn=_eval_fip,
            input_fields=[
                "home_runs_allowed",
                "walks_allowed",
                "intentional_walks_allowed",
                "hit_batters",
                "strikeouts",
                "innings_outs",
            ],
            constants_required=["c_fip"],
            validation_rules=[
                ValidationRule(
                    "plausibility",
                    lambda v, _: float(v) >= 0.0,
                    r"\text{FIP} \ge 0.0",
                    "FIP is negative in small sample",
                    severity=RuleSeverity.PLAUSIBILITY,
                )
            ],
            precision=2,
            description=(
                "Pitching effectiveness metric isolating outcomes directly controlled by the pitcher (HR, BB, K, HBP)."
            ),
        ),
        "K_9": MetricDefinition(
            metric_id="K_9",
            name="Strikeouts per 9 Innings",
            korean_name="9이닝당 탈삼진",
            category=MetricCategory.PITCHING,
            version=_DEFAULT_V1,
            latex_formula=r"\text{K/9} = \frac{27 \cdot K}{\text{Outs}}",
            eval_fn=_eval_k_9,
            input_fields=["strikeouts", "innings_outs"],
            precision=2,
            description="Average number of strikeouts recorded per 9 innings pitched.",
        ),
        "BB_9": MetricDefinition(
            metric_id="BB_9",
            name="Walks per 9 Innings",
            korean_name="9이닝당 볼넷",
            category=MetricCategory.PITCHING,
            version=_DEFAULT_V1,
            latex_formula=r"\text{BB/9} = \frac{27 \cdot BB}{\text{Outs}}",
            eval_fn=_eval_bb_9,
            input_fields=["walks_allowed", "innings_outs"],
            precision=2,
            description="Average number of walks conceded per 9 innings pitched.",
        ),
        "HR_9": MetricDefinition(
            metric_id="HR_9",
            name="Home Runs per 9 Innings",
            korean_name="9이닝당 피홈런",
            category=MetricCategory.PITCHING,
            version=_DEFAULT_V1,
            latex_formula=r"\text{HR/9} = \frac{27 \cdot HR}{\text{Outs}}",
            eval_fn=_eval_hr_9,
            input_fields=["home_runs_allowed", "innings_outs"],
            precision=2,
            description="Average number of home runs surrendered per 9 innings pitched.",
        ),
        "K_PCT_PIT": MetricDefinition(
            metric_id="K_PCT_PIT",
            name="Strikeout Rate (Pitching)",
            korean_name="투수 탈삼진율",
            category=MetricCategory.PITCHING,
            version=_DEFAULT_V1,
            latex_formula=r"\text{K\%} = \frac{K}{TBF}",
            eval_fn=_eval_k_pct_pit,
            input_fields=["strikeouts", "batters_faced"],
            precision=3,
            description="Fraction of total batters faced that end in a strikeout.",
        ),
        "BB_PCT_PIT": MetricDefinition(
            metric_id="BB_PCT_PIT",
            name="Walk Rate (Pitching)",
            korean_name="투수 볼넷 비율",
            category=MetricCategory.PITCHING,
            version=_DEFAULT_V1,
            latex_formula=r"\text{BB\%} = \frac{BB}{TBF}",
            eval_fn=_eval_bb_pct_pit,
            input_fields=["walks_allowed", "batters_faced"],
            precision=3,
            description="Fraction of total batters faced that result in a walk.",
        ),
        "K_BB_PIT": MetricDefinition(
            metric_id="K_BB_PIT",
            name="Strikeout Minus Walk Rate",
            korean_name="K-BB%",
            category=MetricCategory.PITCHING,
            version=_DEFAULT_V1,
            latex_formula=r"\text{K-BB\%} = \frac{K - BB}{TBF}",
            eval_fn=_eval_k_bb_pit,
            input_fields=["strikeouts", "walks_allowed", "batters_faced"],
            precision=3,
            description="Net strikeout to walk dominance percentage.",
        ),
        "BABIP_PIT": MetricDefinition(
            metric_id="BABIP_PIT",
            name="Pitching BABIP",
            korean_name="피인플레이 타구 타율",
            category=MetricCategory.PITCHING,
            version=_DEFAULT_V1,
            latex_formula=r"\text{BABIP} = \frac{H - HR}{TBF - K - HR + SF}",
            eval_fn=_eval_babip_pit,
            input_fields=[
                "hits_allowed",
                "home_runs_allowed",
                "strikeouts",
                "batters_faced",
                "sacrifice_flies_allowed",
            ],
            precision=3,
            description="Opponent batting average on balls put into play against this pitcher.",
        ),
        "LOB_PCT": MetricDefinition(
            metric_id="LOB_PCT",
            name="Left On Base Percentage",
            korean_name="잔루 처리율",
            category=MetricCategory.PITCHING,
            version=_DEFAULT_V1,
            latex_formula=r"\text{LOB\%} = \frac{H + BB + HBP - R}{H + BB + HBP - 1.4\cdot HR}",
            eval_fn=_eval_lob_pct,
            input_fields=["hits_allowed", "walks_allowed", "hit_batters", "runs_allowed", "home_runs_allowed"],
            precision=3,
            description="Percentage of baserunners allowed that fail to score.",
        ),
        "ERA_INDEX_NO_PARK": MetricDefinition(
            metric_id="ERA_INDEX_NO_PARK",
            name="ERA Index (No Park Adjustment)",
            korean_name="조정 평균자책점 지수 (파크팩터 미보정)",
            category=MetricCategory.PITCHING,
            version=_DEFAULT_V1,
            latex_formula=r"\text{ERA\_INDEX} = 100 \cdot \frac{\text{lg}ERA}{\text{ERA}}",
            eval_fn=_eval_era_index_no_park,
            input_fields=["earned_runs", "innings_outs"],
            constants_required=["lg_era"],
            precision=1,
            description="League-normalized ERA where 100 is league average (without park adjustment).",
            is_park_adjusted=False,
        ),
        "DICE": MetricDefinition(
            metric_id="DICE",
            name="Defense-Independent Component ERA",
            korean_name="수비 독립 성분 자책점",
            category=MetricCategory.PITCHING,
            version=_DEFAULT_V1,
            latex_formula=r"\text{DICE} = 3.00 + \frac{3\cdot (13\cdot HR + 3\cdot(BB + HBP) - 2\cdot K)}{\text{Outs}}",
            eval_fn=_eval_dice,
            input_fields=["home_runs_allowed", "walks_allowed", "hit_batters", "strikeouts", "innings_outs"],
            precision=2,
            description="Voorhees classical defense-independent pitching estimator.",
        ),
        # --- Baserunning & Fielding ---
        "SB_PCT": MetricDefinition(
            metric_id="SB_PCT",
            name="Stolen Base Percentage",
            korean_name="도루 성공률",
            category=MetricCategory.BASERUNNING,
            version=_DEFAULT_V1,
            latex_formula=r"\text{SB\%} = \frac{SB}{SB + CS}",
            eval_fn=_eval_sb_pct,
            input_fields=["stolen_bases", "caught_stealing"],
            precision=3,
            description="Success rate of stolen base attempts.",
        ),
        "FPCT": MetricDefinition(
            metric_id="FPCT",
            name="Fielding Percentage",
            korean_name="수비율",
            category=MetricCategory.FIELDING,
            version=_DEFAULT_V1,
            latex_formula=r"\text{FPCT} = \frac{PO + A}{PO + A + E}",
            eval_fn=_eval_fpct,
            input_fields=["putouts", "assists", "errors"],
            precision=3,
            description="Proportion of total defensive chances handled without error.",
        ),
        "RF_9": MetricDefinition(
            metric_id="RF_9",
            name="Range Factor per 9 Innings",
            korean_name="9이닝당 수비 범위 지수",
            category=MetricCategory.FIELDING,
            version=_DEFAULT_V1,
            latex_formula=r"\text{RF/9} = \frac{27 \cdot (PO + A)}{\text{Inn\_Outs}}",
            eval_fn=_eval_rf_9,
            input_fields=["putouts", "assists", "innings_outs"],
            precision=2,
            description="Average number of defensive outs contributed per 9 innings at position.",
        ),
    }

    _ALIASES: ClassVar[dict[str, str]] = {
        "WRC_PLUS": "WRC_INDEX_NO_PARK",
        "WRC+": "WRC_INDEX_NO_PARK",
        "OPS_PLUS": "OPS_INDEX_NO_PARK",
        "OPS+": "OPS_INDEX_NO_PARK",
        "ERA_PLUS": "ERA_INDEX_NO_PARK",
        "ERA+": "ERA_INDEX_NO_PARK",
    }

    @classmethod
    def get(cls, metric_id: str) -> MetricDefinition:
        """Retrieve metric definition by case-insensitive identifier with deprecated alias resolution."""
        norm_key = metric_id.upper().strip()

        # Check direct canonical catalog
        for k, v in cls._CATALOG.items():
            if k.upper().strip() == norm_key:
                return v

        # Check aliases
        if norm_key in cls._ALIASES:
            canonical_key = cls._ALIASES[norm_key]
            canonical_def = cls._CATALOG[canonical_key]
            # Wrap as deprecated alias with warning
            return MetricDefinition(
                metric_id=canonical_def.metric_id,
                name=canonical_def.name,
                korean_name=canonical_def.korean_name,
                category=canonical_def.category,
                version=canonical_def.version,
                latex_formula=canonical_def.latex_formula,
                eval_fn=canonical_def.eval_fn,
                input_fields=canonical_def.input_fields,
                constants_required=canonical_def.constants_required,
                validation_rules=canonical_def.validation_rules,
                precision=canonical_def.precision,
                description=canonical_def.description,
                is_park_adjusted=canonical_def.is_park_adjusted,
                is_deprecated_alias=True,
                deprecation_warning=(
                    f"'{metric_id}' is a deprecated alias for '{canonical_key}'. "
                    "Park factor is not applied to this relative index."
                ),
            )

        msg = f"Metric '{metric_id}' not found in Sabermetrics Formula Registry."
        raise KeyError(msg)

    @classmethod
    def list_all(cls, category: MetricCategory | None = None) -> list[MetricDefinition]:
        """List all registered canonical metrics, optionally filtered by category."""
        if category is None:
            return list(cls._CATALOG.values())
        return [m for m in cls._CATALOG.values() if m.category == category]

    @classmethod
    def list_metric_ids(cls) -> list[str]:
        """List all registered canonical metric identifier keys."""
        return list(cls._CATALOG.keys())

    @classmethod
    def count(cls) -> int:
        """Return total number of registered canonical metrics."""
        return len(cls._CATALOG)


__all__ = [
    "MAX_OPS_BOUND",
    "MAX_SLG_BOUND",
    "FormulaRegistry",
]
