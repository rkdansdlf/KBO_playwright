
import sys
import os

# Add project root to path
sys.path.append(os.getcwd())

from src.utils.team_history import _TEAM_HISTORY, resolve_team_code_for_season
from src.utils.team_mapping import get_team_mapper
from src.utils.team_codes import TEAM_NAME_TO_CODE

def verify_mappings():
    mapper = get_team_mapper()
    
    print("Running Team Mapping Verification...")
    print(f"Total History Entries: {len(_TEAM_HISTORY)}")
    
    errors = []
    
    # 1. Reverse lookup map for testing: Code -> Names
    # This helps us find "What names should map to 'OB'?"
    code_to_names = {}
    for name, code in TEAM_NAME_TO_CODE.items():
        if code not in code_to_names:
            code_to_names[code] = []
        code_to_names[code].append(name)
        
    # 2. Iterate through history and verify
    for entry in _TEAM_HISTORY:
        # Check a sample year in the middle of the era
        test_year = entry.start_season
        
        # Get potential names for this team code
        # If the code is 'OB', we want to test "OB", "OB 베어스" etc.
        # But we also need to check if the code from history matches the code mapping logic
        
        expected_code = entry.team_code
        
        # We need to find input names that *should* result in this code
        # For 'OB' (1982-1998), 'OB' should map to 'OB'
        # For 'DB' (1999-Present), 'Doosan' should map to 'DB'
        
        possible_names = code_to_names.get(expected_code, [expected_code])
        
        print(f"Checking {expected_code} ({entry.start_season}-{entry.end_season or 'Now'})...")
        
        for name in possible_names:
            # Test team_mapping.py logic
            mapped_code = mapper.get_team_code(name, test_year)
            
            if mapped_code != expected_code:
                errors.append(
                    f"[TeamMapper] Year {test_year}: '{name}' -> Got '{mapped_code}', Expected '{expected_code}'"
                )
            else:
                # print(f"  OK: {name} -> {mapped_code}")
                pass

    # 3. Specific Edge Cases / Transitions
    edge_cases = [
        ("MBC", 1989, "MBC"),
        ("LG", 1990, "LG"),
        ("OB", 1998, "OB"),
        ("두산", 1999, "DB"),
        ("빙그레", 1993, "BE"),
        ("한화", 1994, "HH"),
        ("해태", 2000, "HT"),
        ("KIA", 2001, "KIA"),
        ("삼미", 1982, "SM"),
        ("청보", 1986, "CB"),
        ("태평양", 1988, "TP"),
        ("현대", 1996, "HU"),
        ("우리", 2008, "WO"),
        ("히어로즈", 2009, "WO"),
        ("넥센", 2010, "NX"),
        ("키움", 2019, "KH"),
        ("쌍방울", 1991, "SL"),
        ("SK", 2000, "SK"),
        ("SSG", 2021, "SSG"),
        ("NC", 2013, "NC"),
        ("KT", 2015, "KT")
    ]
    
    print("\nChecking Edge Cases...")
    for name, year, expected in edge_cases:
        mapped_code = mapper.get_team_code(name, year)
        if mapped_code != expected:
            errors.append(f"[EdgeCase] {year} '{name}' -> Got '{mapped_code}', Expected '{expected}'")

    if errors:
        print("\n❌ Found Discrepancies:")
        for e in errors:
            print(e)
    else:
        print("\n✅ All verifications passed!")

if __name__ == "__main__":
    verify_mappings()
