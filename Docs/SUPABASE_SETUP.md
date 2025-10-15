# Supabase Setup Guide

KBO_playwright 프로젝트의 이중 저장소 패턴 설정 가이드

## Architecture: Dual Repository Pattern

```
┌─────────────────┐
│   Web Scraping  │ (Playwright)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  SQLite (Local) │ ← Development & Validation
│  - Fast writes  │
│  - Easy debug   │
│  - Data verify  │
└────────┬────────┘
         │ (After validation)
         ▼
┌─────────────────┐
│    Supabase     │ ← Production Storage
│  (PostgreSQL)   │
│  - Persistent   │
│  - API access   │
│  - Realtime     │
└─────────────────┘
```

**Data Flow**: Crawl → SQLite → Validate → Supabase

## Prerequisites

1. **Supabase Account**: [https://supabase.com](https://supabase.com)
2. **Supabase Project**: Create a new project
3. **Database Password**: Set during project creation

## Step 1: Get Supabase Credentials

### 1.1 Database Connection String

1. Go to Supabase Dashboard → Project Settings → Database
2. Find "Connection string" section
3. Select **"Session pooler"** (recommended for better performance)
4. Copy the connection string (URI format):

```
postgresql://postgres.[PROJECT_REF]:[YOUR_PASSWORD]@aws-0-[REGION].pooler.supabase.com:5432/postgres
```

### 1.2 API Keys

1. Go to Supabase Dashboard → Project Settings → API
2. Copy these keys:
   - `anon` / `public` key (for client-side access)
   - `service_role` key (for server-side admin access)

### 1.3 Project URL

```
https://[PROJECT_REF].supabase.co
```

## Step 2: Configure Environment Variables

Create or update `.env` file in project root:

```bash
# Supabase REST API
SUPABASE_URL=https://zyofzvnkputevakepbdm.supabase.co
SUPABASE_ANON_KEY=your-anon-key-here
SUPABASE_SERVICE_ROLE_KEY=your-service-role-key-here

# Supabase Database (Session Pooler - Recommended)
SUPABASE_DB_URL=postgresql://postgres.zyofzvnkputevakepbdm:[YOUR_PASSWORD]@aws-0-ap-northeast-2.pooler.supabase.com:5432/postgres

# Replace [YOUR_PASSWORD] with your actual database password
```

## Step 3: Run Database Migrations

### 3.1 Option A: Using Supabase SQL Editor (Recommended)

1. Go to Supabase Dashboard → SQL Editor
2. Open `migrations/supabase/001_create_team_tables.sql`
3. Copy and paste the entire SQL script
4. Click "Run" to execute
5. Repeat for `migrations/supabase/002_create_game_tables.sql`

### 3.2 Option B: Using psql CLI

```bash
# Install psql if not available
brew install postgresql  # macOS
sudo apt install postgresql-client  # Ubuntu

# Run migrations
psql "postgresql://postgres.xxx:[PASSWORD]@xxx.pooler.supabase.com:5432/postgres" \
  -f migrations/supabase/001_create_team_tables.sql

psql "postgresql://postgres.xxx:[PASSWORD]@xxx.pooler.supabase.com:5432/postgres" \
  -f migrations/supabase/002_create_game_tables.sql
```

### 3.3 Verify Schema Creation

In Supabase Dashboard → Table Editor, you should see:

**Team Tables:**
- `franchises`
- `team_identities`
- `franchise_events`
- `ballparks`
- `home_ballpark_assignments`

**Game Tables:**
- `game_schedules`
- `games`
- `game_lineups`
- `player_game_stats`

## Step 4: Authenticate Supabase MCP (Optional)

If you're using Claude Code with Supabase MCP:

```bash
# Add Supabase MCP server
claude mcp add --transport http supabase \
  "https://mcp.supabase.com/mcp?project_ref=zyofzvnkputevakepbdm"

# Authenticate (will open browser)
claude mcp auth supabase
```

## Step 5: Test Data Sync

### 5.1 Ensure SQLite Has Data

```bash
# Initialize SQLite database
./venv/bin/python3 init_db.py

# Seed team data
./venv/bin/python3 seed_teams.py

# Verify SQLite data
./venv/bin/python3 verify_sqlite_data.py
```

### 5.2 Run Supabase Sync

```bash
# Set environment variable
export SUPABASE_DB_URL='postgresql://postgres.xxx:[PASSWORD]@xxx.pooler.supabase.com:5432/postgres'

# Run sync test
./venv/bin/python3 src/sync/supabase_sync.py
```

Expected output:
```
✅ Supabase connection successful
📦 Syncing team data...
✅ Synced 11 franchises to Supabase
✅ Synced 21 team identities to Supabase
✅ Synced 9 ballparks to Supabase
✅ Synced 7 ballpark assignments to Supabase
```

### 5.3 Verify Data in Supabase

Go to Supabase Dashboard → Table Editor:
- `franchises`: Should have 11 rows
- `team_identities`: Should have 21 rows
- `ballparks`: Should have 9 rows
- `home_ballpark_assignments`: Should have 7 rows

## Step 6: Integration into Crawler Workflow

### Automatic Sync After Crawling

Update your crawler to sync after successful validation:

```python
from src.db.engine import SessionLocal
from src.sync.supabase_sync import SupabaseSync
import os

# 1. Crawl and save to SQLite
with SessionLocal() as session:
    # ... crawling logic ...
    # ... save to SQLite ...
    pass

# 2. Validate SQLite data
# ... run validation checks ...

# 3. Sync to Supabase
supabase_url = os.getenv('SUPABASE_DB_URL')
if supabase_url:
    with SessionLocal() as session:
        sync = SupabaseSync(supabase_url, session)
        sync.sync_all_team_data()
        sync.close()
```

## Troubleshooting

### Connection Timeout

**Issue**: `connection timeout` error

**Solution**:
- Use "Session pooler" instead of "Direct connection"
- Check firewall/network settings
- Verify database password is correct

### Authentication Error

**Issue**: `authentication failed for user "postgres.xxx"`

**Solution**:
- Verify password in connection string
- Password should NOT have special characters that need URL encoding
- If it does, encode them (e.g., `@` → `%40`, `!` → `%21`)

### Table Already Exists

**Issue**: `ERROR: relation "franchises" already exists`

**Solution**:
- Safe to ignore if running migrations multiple times
- Use `CREATE TABLE IF NOT EXISTS` (already in migration files)

### ID Mapping Issues

**Issue**: Foreign key constraint violations during sync

**Solution**:
- Ensure franchises are synced BEFORE team_identities
- Ensure ballparks are synced BEFORE home_ballpark_assignments
- Follow sync order: franchises → team_identities → ballparks → assignments

## Security Best Practices

1. **Never commit `.env` file** to git (already in `.gitignore`)
2. **Use service_role key** only in backend/server code
3. **Use anon key** for client-side applications
4. **Enable Row Level Security (RLS)** in Supabase for production:
   ```sql
   ALTER TABLE franchises ENABLE ROW LEVEL SECURITY;
   ```

## Performance Optimization

### Enable Connection Pooling

Already configured in `SupabaseSync`:
```python
engine = create_engine(
    supabase_url,
    pool_pre_ping=True,
    connect_args={
        "connect_timeout": 10,
        "application_name": "KBO_Crawler_Sync"
    }
)
```

### Batch Inserts

For large datasets, consider batching:
```python
# Instead of one-by-one UPSERT
for item in items:
    session.execute(insert_stmt)

# Use bulk operations
session.bulk_insert_mappings(Model, items)
```

## Next Steps

1. ✅ Setup Supabase project
2. ✅ Configure environment variables
3. ✅ Run database migrations
4. ✅ Test data sync
5. ⏳ Integrate sync into crawler workflow
6. ⏳ Setup scheduled sync jobs (cron/scheduler)
7. ⏳ Implement Supabase Realtime listeners (optional)
8. ⏳ Build analytics dashboard using Supabase API

## Resources

- [Supabase Documentation](https://supabase.com/docs)
- [PostgreSQL Connection Pooling](https://supabase.com/docs/guides/database/connecting-to-postgres#connection-pooler)
- [SQLAlchemy PostgreSQL Dialect](https://docs.sqlalchemy.org/en/20/dialects/postgresql.html)
