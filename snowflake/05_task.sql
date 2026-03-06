-- =============================================================================
-- 05_task.sql -- Automated segment syncing with Snowflake Tasks and Streams
-- =============================================================================
-- This script creates:
--   1. A helper procedure that iterates over all segments
--   2. A Stream on SEGMENT_MEMBERS for change detection
--   3. A scheduled Task that syncs only when changes are detected
--
-- The Task checks the stream before doing any work, so it's safe to run on
-- a short schedule (e.g. every 1 minute) without wasting compute.

USE DATABASE LD_SYNC_DEMO;
USE SCHEMA SYNCED_SEGMENTS;

-- ---------------------------------------------------------------------------
-- Helper procedure: sync ALL segments in the SEGMENTS table
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SYNC_ALL_SEGMENTS(
    SYNC_ENDPOINT_URL VARCHAR
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests')
EXTERNAL_ACCESS_INTEGRATIONS = (LD_SYNC_ACCESS_INTEGRATION)
HANDLER = 'run'
AS
$$
import json

def run(session, sync_endpoint_url: str) -> str:
    """Iterate over every segment and sync each one."""
    segments = session.sql("SELECT SEGMENT_KEY FROM SEGMENTS").collect()

    if not segments:
        return json.dumps({"status": "no_segments", "message": "No segments found in SEGMENTS table"})

    results = []
    for row in segments:
        key = row["SEGMENT_KEY"]
        result_json = session.sql(
            f"CALL SYNC_SEGMENT_TO_LD('{key.replace(chr(39), chr(39)+chr(39))}', "
            f"'{sync_endpoint_url.replace(chr(39), chr(39)+chr(39))}')"
        ).collect()[0][0]
        results.append({"segment_key": key, "result": json.loads(result_json)})

    return json.dumps({"status": "complete", "segments_synced": len(results), "results": results}, default=str)
$$;

-- ---------------------------------------------------------------------------
-- Stream: captures changes (inserts, updates, deletes) on SEGMENT_MEMBERS
-- ---------------------------------------------------------------------------
CREATE OR REPLACE STREAM SEGMENT_MEMBERS_STREAM
    ON TABLE SEGMENT_MEMBERS
    APPEND_ONLY = FALSE;

-- ---------------------------------------------------------------------------
-- Scheduled task: checks the stream and syncs only when data has changed
-- ---------------------------------------------------------------------------
-- IMPORTANT: Replace the URL below with your actual middleware endpoint.
-- Adjust the warehouse name and cron schedule to match your environment.
CREATE OR REPLACE TASK SYNC_SEGMENTS_TASK
    WAREHOUSE = LD_EXPORT_WH          -- Change to your warehouse name
    SCHEDULE  = 'USING CRON */1 * * * * UTC'
    WHEN SYSTEM$STREAM_HAS_DATA('SEGMENT_MEMBERS_STREAM')
AS
    CALL SYNC_ALL_SEGMENTS('https://your-middleware-host.vercel.app');

-- The task is created SUSPENDED. Resume it when you're ready:
--   ALTER TASK SYNC_SEGMENTS_TASK RESUME;
--
-- To pause:
--   ALTER TASK SYNC_SEGMENTS_TASK SUSPEND;
--
-- To check status:
--   SHOW TASKS LIKE 'SYNC_SEGMENTS_TASK';
--
-- To see execution history:
--   SELECT *
--   FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'SYNC_SEGMENTS_TASK'))
--   ORDER BY SCHEDULED_TIME DESC
--   LIMIT 20;
