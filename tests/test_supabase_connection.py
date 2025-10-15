"""
Test different Supabase connection strings
"""
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# Different connection string formats
connection_strings = {
    "Session Pooler": "postgresql://postgres.zyofzvnkputevakepbdm:kboplatform2025@aws-0-ap-northeast-2.pooler.supabase.com:5432/postgres",
    "Transaction Pooler (port 6543)": "postgresql://postgres.zyofzvnkputevakepbdm:kboplatform2025@aws-0-ap-northeast-2.pooler.supabase.com:6543/postgres",
    "Direct Connection": "postgresql://postgres.zyofzvnkputevakepbdm:kboplatform2025@aws-0-ap-northeast-2.pooler.supabase.com:5432/postgres",
    "Direct Connection (db subdomain)": "postgresql://postgres:kboplatform2025@db.zyofzvnkputevakepbdm.supabase.co:5432/postgres",
}

print("🔍 Testing Supabase Connection Strings\n" + "="*60)

for name, conn_str in connection_strings.items():
    print(f"\n📡 Testing: {name}")
    print(f"   URL: {conn_str[:50]}...")

    try:
        engine = create_engine(
            conn_str,
            echo=False,
            connect_args={"connect_timeout": 5}
        )

        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.fetchone()[0]
            print(f"   ✅ SUCCESS!")
            print(f"   PostgreSQL: {version[:60]}...")

        engine.dispose()

    except Exception as e:
        error_msg = str(e)
        if "Tenant or user not found" in error_msg:
            print(f"   ❌ FAILED: Tenant/user not found (wrong format)")
        elif "timeout" in error_msg.lower():
            print(f"   ❌ FAILED: Connection timeout")
        elif "password" in error_msg.lower():
            print(f"   ❌ FAILED: Authentication error")
        else:
            print(f"   ❌ FAILED: {error_msg[:100]}")

print("\n" + "="*60)
print("💡 TIP: DBeaver가 연결되는 설정을 확인하세요:")
print("   - Host: db.zyofzvnkputevakepbdm.supabase.co")
print("   - Port: 5432 (또는 6543)")
print("   - Database: postgres")
print("   - User: postgres")
print("   - Password: kboplatform2025")
