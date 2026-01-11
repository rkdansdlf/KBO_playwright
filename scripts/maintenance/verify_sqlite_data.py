"""
Verify SQLite data integrity before syncing to Supabase
"""
from src.db.engine import SessionLocal
from src.models.team import Franchise, TeamIdentity, Ballpark, HomeBallparkAssignment
from src.models.game import GameSchedule, Game, GameLineup, PlayerGameStats
from src.utils.safe_print import safe_print as print


def verify_team_data(session):
    """Verify team-related data"""
    print("\n📊 Team Data Verification")
    print("=" * 50)

    # Franchises
    franchises = session.query(Franchise).all()
    print(f"✅ Franchises: {len(franchises)}")
    active = [f for f in franchises if f.status == 'ACTIVE']
    dissolved = [f for f in franchises if f.status == 'DISSOLVED']
    print(f"   - Active: {len(active)}")
    print(f"   - Dissolved: {len(dissolved)}")

    # Team Identities
    identities = session.query(TeamIdentity).all()
    print(f"✅ Team Identities: {len(identities)}")
    current = [i for i in identities if i.is_current == 1]
    historical = [i for i in identities if i.is_current == 0]
    print(f"   - Current: {len(current)}")
    print(f"   - Historical: {len(historical)}")

    # Ballparks
    ballparks = session.query(Ballpark).all()
    print(f"✅ Ballparks: {len(ballparks)}")

    # Ballpark Assignments
    assignments = session.query(HomeBallparkAssignment).all()
    print(f"✅ Ballpark Assignments: {len(assignments)}")

    # Sample data
    print("\n📋 Sample Franchise Data:")
    for f in franchises[:3]:
        print(f"   {f.key}: {f.canonical_name} ({f.status})")

    print("\n📋 Current Team Identities:")
    for i in sorted(current, key=lambda x: x.name_kor):
        print(f"   {i.name_kor} ({i.short_code}) - {i.city_kor}")

    return len(franchises), len(identities), len(ballparks), len(assignments)


def verify_game_data(session):
    """Verify game-related data"""
    print("\n📊 Game Data Verification")
    print("=" * 50)

    schedules = session.query(GameSchedule).all()
    print(f"✅ Game Schedules: {len(schedules)}")

    games = session.query(Game).all()
    print(f"✅ Games: {len(games)}")

    lineups = session.query(GameLineup).all()
    print(f"✅ Game Lineups: {len(lineups)}")

    stats = session.query(PlayerGameStats).all()
    print(f"✅ Player Game Stats: {len(stats)}")

    if schedules:
        print("\n📋 Sample Game Schedule:")
        for s in schedules[:3]:
            print(f"   Game {s.game_id}: {s.away_team_code} @ {s.home_team_code} on {s.game_date} ({s.game_status})")

    return len(schedules), len(games), len(lineups), len(stats)


def check_data_quality(session):
    """Check for data quality issues"""
    print("\n🔍 Data Quality Checks")
    print("=" * 50)

    issues = []

    # Check for NULL critical fields
    franchises_no_name = session.query(Franchise).filter(
        (Franchise.canonical_name == None) | (Franchise.canonical_name == '')
    ).count()
    if franchises_no_name > 0:
        issues.append(f"⚠️  {franchises_no_name} franchises with no canonical_name")

    # Check for orphaned team identities
    identities = session.query(TeamIdentity).all()
    franchise_ids = {f.id for f in session.query(Franchise).all()}
    orphaned = [i for i in identities if i.franchise_id not in franchise_ids]
    if orphaned:
        issues.append(f"⚠️  {len(orphaned)} team identities with invalid franchise_id")

    # Check for duplicate franchise keys
    from sqlalchemy import func
    duplicates = session.query(
        Franchise.key, func.count(Franchise.key)
    ).group_by(Franchise.key).having(func.count(Franchise.key) > 1).all()
    if duplicates:
        issues.append(f"⚠️  Duplicate franchise keys found: {duplicates}")

    if not issues:
        print("✅ No data quality issues found!")
    else:
        for issue in issues:
            print(issue)

    return len(issues)


def main():
    """Run all verification checks"""
    print("\n" + "🔬" * 30)
    print("SQLite Data Verification")
    print("🔬" * 30)

    with SessionLocal() as session:
        try:
            # Verify team data
            team_counts = verify_team_data(session)

            # Verify game data
            game_counts = verify_game_data(session)

            # Check data quality
            issue_count = check_data_quality(session)

            # Summary
            print("\n" + "=" * 50)
            print("📈 Verification Summary")
            print("=" * 50)
            print(f"Team Data:")
            print(f"  - Franchises: {team_counts[0]}")
            print(f"  - Team Identities: {team_counts[1]}")
            print(f"  - Ballparks: {team_counts[2]}")
            print(f"  - Assignments: {team_counts[3]}")
            print(f"\nGame Data:")
            print(f"  - Schedules: {game_counts[0]}")
            print(f"  - Games: {game_counts[1]}")
            print(f"  - Lineups: {game_counts[2]}")
            print(f"  - Player Stats: {game_counts[3]}")
            print(f"\nData Quality: {issue_count} issues found")

            if issue_count == 0:
                print("\n✅ SQLite data is ready for Supabase sync!")
            else:
                print("\n⚠️  Please fix data quality issues before syncing to Supabase")

        except Exception as e:
            print(f"\n❌ Verification error: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    main()
