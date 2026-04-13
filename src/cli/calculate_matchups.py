import argparse
from typing import Sequence
from src.services.matchup_engine import MatchupEngine
from src.db.engine import SessionLocal

def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Recalculate Native Matchup Engine metrics.")
    parser.add_argument("--year", type=int, required=True, help="Season year to recalculate")

    args = parser.parse_args(argv)

    print(f"🔄 Recalculating Matchups (DB Engine) for {args.year}...")
    
    # Run the engine
    with SessionLocal() as session:
        engine = MatchupEngine(session)
        engine.execute_all(args.year)
        
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())
