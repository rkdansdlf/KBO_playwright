import sys
import os

# Add project root to path
sys.path.append(os.getcwd())

from src.utils.team_codes import resolve_team_code, team_code_from_game_id_segment

def test_resolution(name, year, expected, type="name"):
    try:
        if type == "name":
            result = resolve_team_code(name, year)
        else:
            result = team_code_from_game_id_segment(name, year)
            
        if result == expected:
            print(f"✅ [PASS] {year} {name} -> {result}")
        else:
            print(f"❌ [FAIL] {year} {name} -> Expected {expected}, got {result}")
            return False
    except Exception as e:
        print(f"❌ [ERROR] {year} {name} -> {e}")
        return False
    return True

def main():
    print("Starting Historical Team Code Verification...\n")
    
    failures = 0
    tests = [
        # 1. Haitai Tigers Era (Pre-2001)
        ("해태 타이거즈", 2000, "HT", "name"),
        ("해태", 1995, "HT", "name"),
        ("KIA 타이거즈", 2000, "HT", "name"), # Should resolve to HT if checking for 2000
        ("HT", 2000, "HT", "segment"),
        
        # 2. KIA Tigers Era (Post-2001)
        ("KIA 타이거즈", 2001, "KIA", "name"),
        ("KIA", 2024, "KIA", "name"),
        ("HT", 2024, "KIA", "segment"), # Legacy code mapping for modern game
        ("KIA", 2024, "KIA", "segment"),
        
        # 3. SK Wyverns Era (2000-2020)
        ("SK 와이번스", 2010, "SK", "name"),
        ("SK", 2010, "SK", "name"),
        ("SSG 랜더스", 2010, "SK", "name"), # Logic check: if looking up SSG in 2010, should it return SK? 
                                            # Ideally yes if we map SSG->SK for that year, but resolve_team_code might only check name->code first.
                                            # TEAM_NAME_TO_CODE["SSG 랜더스"] = "SSG". 
                                            # resolve_team_code_for_season("SSG", 2010) -> SK. So yes.
        ("SK", 2010, "SK", "segment"),
        
        # 4. SSG Landers Era (2021-)
        ("SSG 랜더스", 2021, "SSG", "name"),
        ("SSG", 2024, "SSG", "name"),
        ("SK", 2024, "SSG", "segment"), # Legacy code mapping
        
        # 5. OB Bears Era (Pre-1999)
        ("OB 베어스", 1995, "OB", "name"),
        ("OB", 1990, "OB", "name"),
        ("두산 베어스", 1995, "OB", "name"), 
        ("OB", 1995, "OB", "segment"),
        
        # 6. Doosan Bears (Current DB code)
        ("두산 베어스", 2024, "DB", "name"),
        ("두산", 2024, "DB", "name"),
        ("OB", 2024, "DB", "segment"),
        ("DO", 2024, "DB", "segment"),
        
        # 7. Woori/Nexen/Kiwoom
        ("우리 히어로즈", 2008, "WO", "name"),
        ("넥센 히어로즈", 2015, "NX", "name"), 
        ("키움 히어로즈", 2024, "KH", "name"),
        ("WO", 2024, "KH", "segment"),
        ("WO", 2008, "WO", "segment"),
        
        # 8. MBC Blue Dragons / LG Twins
        ("MBC 청룡", 1982, "MBC", "name"),
        ("LG 트윈스", 1994, "LG", "name"),
        
        # 9. Binggrae / Hanwha
        ("빙그레 이글스", 1990, "BE", "name"),
        ("한화 이글스", 2024, "HH", "name"),
    ]
    
    for name, year, expected, test_type in tests:
        if not test_resolution(name, year, expected, type=test_type):
            failures += 1
            
    print(f"\nVerification Complete. {len(tests) - failures}/{len(tests)} passed.")
    if failures > 0:
        print(f"❌ {failures} tests failed.")
        sys.exit(1)
    else:
        print("✅ All tests passed.")

if __name__ == "__main__":
    main()
