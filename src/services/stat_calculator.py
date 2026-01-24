"""
Batting Stat Calculator Service.
Calculates ratios (AVG, OBP, SLG, etc.) from raw count data.
"""
from typing import Dict, Optional, Any

class BattingStatCalculator:
    @staticmethod
    def calculate_ratios(data: Dict[str, Any]) -> Dict[str, Optional[float]]:
        """
        Calculates advanced/ratio stats from a dictionary of raw stats.
        Required keys: at_bats, hits, walks, hbp, sacrifice_flies, 
                      doubles, triples, home_runs, strikeouts, plate_appearances.
        """
        ab = data.get('at_bats', 0) or 0
        h = data.get('hits', 0) or 0
        bb = data.get('walks', 0) or 0
        hbp = data.get('hbp', 0) or 0
        sf = data.get('sacrifice_flies', 0) or 0
        sh = data.get('sacrifice_hits', 0) or 0 # Sacrifice bunts
        
        d2 = data.get('doubles', 0) or 0
        d3 = data.get('triples', 0) or 0
        hr = data.get('home_runs', 0) or 0
        so = data.get('strikeouts', 0) or 0
        
        # 1. AVG (Batting Average)
        avg = round(h / ab, 3) if ab > 0 else 0.0
        
        # 2. OBP (On-Base Percentage)
        # Formula: (H + BB + HBP) / (AB + BB + HBP + SF)
        obp_denom = ab + bb + hbp + sf
        obp = round((h + bb + hbp) / obp_denom, 3) if obp_denom > 0 else 0.0
        
        # 3. SLG (Slugging Percentage)
        # Total Bases = 1B + (2*2B) + (3*3B) + (4*HR)
        # 1B = H - 2B - 3B - HR
        tb = (h - d2 - d3 - hr) + (2 * d2) + (3 * d3) + (4 * hr)
        slg = round(tb / ab, 3) if ab > 0 else 0.0
        
        # 4. OPS
        ops = round(obp + slg, 3)
        
        # 5. ISO (Isolated Power)
        iso = round(slg - avg, 3)
        
        # 6. BABIP (Batting Average on Balls In Play)
        # Formula: (H - HR) / (AB - SO - HR + SF)
        babip_denom = ab - so - hr + sf
        babip = round((h - hr) / babip_denom, 3) if babip_denom > 0 else 0.0
        
        
        # 7. XR (Extrapolated Runs) - Jim Furtado Version
        # Formula: (0.50 x 1B) + (0.72 x 2B) + (1.04 x 3B) + (1.44 x HR) + (0.34 x (HBP + BB - IBB)) + 
        #          (0.25 x IBB) + (0.18 x SB) - (0.32 x CS) - (0.09 x (AB - H - SO)) - 
        #          (0.098 x SO) - (0.37 x GDP) + (0.37 x SF) + (0.04 x SH)
        
        # Derived inputs
        h_1b = h - d2 - d3 - hr
        ibb = data.get('intentional_walks', 0) or 0
        sb = data.get('stolen_bases', 0) or 0
        cs = data.get('caught_stealing', 0) or 0
        gdp = data.get('gdp', 0) or 0
        
        # Terms
        term_1b = 0.50 * h_1b
        term_2b = 0.72 * d2
        term_3b = 1.04 * d3
        term_hr = 1.44 * hr
        term_walks = 0.34 * (hbp + bb - ibb)
        term_ibb = 0.25 * ibb
        term_sb = 0.18 * sb
        term_cs = 0.32 * cs
        term_outs = 0.09 * (ab - h - so) # BIP Outs
        term_so = 0.098 * so
        term_gdp = 0.37 * gdp
        term_sf = 0.37 * sf
        term_sh = 0.04 * sh
        
        xr = (term_1b + term_2b + term_3b + term_hr + term_walks + term_ibb + term_sb 
               - term_cs - term_outs - term_so - term_gdp + term_sf + term_sh)
              
        return {
            'avg': avg,
            'obp': obp,
            'slg': slg,
            'ops': ops,
            'iso': iso,
            'babip': babip,
            'xr': round(xr, 2)
        }


class PitchingStatCalculator:
    """
    Calculates derived pitching statistics from raw data.
    """
    
    # FIP constant (league average, approximately 3.10 for recent years)
    FIP_CONSTANT = 3.10
    
    @staticmethod
    def calculate_ratios(data: Dict[str, Any]) -> Dict[str, Optional[float]]:
        """
        Calculates pitching ratios from raw stats.
        Required keys: innings_outs, earned_runs, hits_allowed, walks_allowed, strikeouts, 
                       home_runs_allowed, hit_batters, batters_faced.
        """
        # innings_outs: 3 outs = 1 IP
        innings_outs = data.get('innings_outs', 0) or 0
        ip = innings_outs / 3.0  # Convert to innings pitched
        
        er = data.get('earned_runs', 0) or 0
        h = data.get('hits_allowed', 0) or 0
        bb = data.get('walks_allowed', 0) or 0
        so = data.get('strikeouts', 0) or 0
        hr = data.get('home_runs_allowed', 0) or 0
        hbp = data.get('hit_batters', 0) or 0
        
        # 1. ERA (Earned Run Average)
        # ERA = (ER / IP) * 9
        era = round((er / ip) * 9, 2) if ip > 0 else 0.0
        
        # 2. WHIP (Walks + Hits per Innings Pitched)
        # WHIP = (BB + H) / IP
        whip = round((bb + h) / ip, 2) if ip > 0 else 0.0
        
        # 3. K/9 (Strikeouts per 9 innings)
        # K/9 = (SO / IP) * 9
        k_per_nine = round((so / ip) * 9, 2) if ip > 0 else 0.0
        
        # 4. BB/9 (Walks per 9 innings)
        # BB/9 = (BB / IP) * 9
        bb_per_nine = round((bb / ip) * 9, 2) if ip > 0 else 0.0
        
        # 5. K/BB (Strikeout to Walk Ratio)
        # K/BB = SO / BB
        kbb = round(so / bb, 2) if bb > 0 else (float(so) if so > 0 else 0.0)
        
        # 6. FIP (Fielding Independent Pitching)
        # FIP = ((13 * HR) + (3 * (BB + HBP)) - (2 * SO)) / IP + FIP_CONSTANT
        if ip > 0:
            fip = round(((13 * hr) + (3 * (bb + hbp)) - (2 * so)) / ip + PitchingStatCalculator.FIP_CONSTANT, 2)
        else:
            fip = 0.0
        
        return {
            'era': era,
            'whip': whip,
            'k_per_nine': k_per_nine,
            'bb_per_nine': bb_per_nine,
            'kbb': kbb,
            'fip': fip
        }

