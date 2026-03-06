# Snowflake SQL Scripts

Run these scripts in order from a Snowflake worksheet or SnowSQL session. Each script is self-contained and idempotent (uses `CREATE OR REPLACE` / `IF NOT EXISTS`).

## Scripts

| Script | Purpose |
|--------|---------|
| `01_setup.sql` | Creates the `LD_SYNC_DEMO` database and `SYNCED_SEGMENTS` schema. |
| `02_network_access.sql` | Creates a network rule and external access integration to allow outbound HTTPS calls from stored procedures. |
| `03_tables.sql` | Creates three tables: `SEGMENTS` (segment definitions), `SEGMENT_MEMBERS` (membership), `SYNC_LOG` (audit trail). |
| `04_sync_procedure.sql` | Creates the `SYNC_SEGMENT_TO_LD` Python stored procedure that detects changes and sends them to the middleware. |
| `05_task.sql` | Creates the `SYNC_ALL_SEGMENTS` helper procedure, a Stream on `SEGMENT_MEMBERS` for change detection, and a scheduled Task that syncs automatically when data changes. |
| `06_seed_data.sql` | Example seed data: 10 premium users and 5 beta testers. Use as a reference for populating your own segments. Safe to re-run (clears previous data first). |

## Before You Run

Update these placeholders to match your environment:

| File | What to change |
|------|---------------|
| `01_setup.sql` | `USE WAREHOUSE ...` -- your warehouse name |
| `02_network_access.sql` | `VALUE_LIST` -- your middleware hostname (e.g. `your-app.vercel.app:443`) |
| `05_task.sql` | Warehouse name in `CREATE TASK` and middleware URL in `CALL SYNC_ALL_SEGMENTS(...)` |

## Quick Reference

**Manual sync of one segment:**
```sql
CALL SYNC_SEGMENT_TO_LD('premium-users', 'https://your-middleware.vercel.app');
```

**Manual sync of all segments:**
```sql
CALL SYNC_ALL_SEGMENTS('https://your-middleware.vercel.app');
```

**Enable automatic syncing:**
```sql
ALTER TASK SYNC_SEGMENTS_TASK RESUME;
```

**Pause automatic syncing:**
```sql
ALTER TASK SYNC_SEGMENTS_TASK SUSPEND;
```

**Check task status:**
```sql
SHOW TASKS LIKE 'SYNC_SEGMENTS_TASK';
```

**View sync history:**
```sql
SELECT * FROM SYNC_LOG ORDER BY SYNCED_AT DESC LIMIT 20;
```

**Reset demo data (re-run from scratch):**
```sql
-- Just re-run 06_seed_data.sql -- it clears previous data first.
```
