-- =============================================================================
-- 04_sync_procedure.sql -- Python stored procedure to sync a segment to LD
-- =============================================================================
-- Uses External Access Integration from 02_network_access.sql to make outbound
-- HTTPS calls to the middleware.

USE DATABASE LD_SYNC_DEMO;
USE SCHEMA SYNCED_SEGMENTS;

CREATE OR REPLACE PROCEDURE SYNC_SEGMENT_TO_LD(
    SEGMENT_KEY VARCHAR,
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
import requests
from datetime import datetime

BATCH_SIZE = 1000


def run(session, segment_key: str, sync_endpoint_url: str) -> str:
    """Sync a single segment's changes to LaunchDarkly via the middleware."""

    # ------------------------------------------------------------------
    # 1. Look up the segment definition
    # ------------------------------------------------------------------
    seg_rows = session.sql(
        f"SELECT SYNC_VERSION, LAST_SYNCED_AT "
        f"FROM SEGMENTS WHERE SEGMENT_KEY = '{_esc(segment_key)}'"
    ).collect()

    if not seg_rows:
        return json.dumps({"status": "error", "message": f"Segment '{segment_key}' not found in SEGMENTS table"})

    current_version = seg_rows[0]["SYNC_VERSION"]
    last_synced_at = seg_rows[0]["LAST_SYNCED_AT"]

    # ------------------------------------------------------------------
    # 2. Query for changed members since last sync
    # ------------------------------------------------------------------
    if last_synced_at is not None:
        ts = last_synced_at.strftime("%Y-%m-%d %H:%M:%S.%f")
        included_rows = session.sql(
            f"SELECT CONTEXT_KEY FROM SEGMENT_MEMBERS "
            f"WHERE SEGMENT_KEY = '{_esc(segment_key)}' "
            f"  AND IS_ACTIVE = TRUE "
            f"  AND UPDATED_AT > '{ts}'"
        ).collect()

        excluded_rows = session.sql(
            f"SELECT CONTEXT_KEY FROM SEGMENT_MEMBERS "
            f"WHERE SEGMENT_KEY = '{_esc(segment_key)}' "
            f"  AND IS_ACTIVE = FALSE "
            f"  AND UPDATED_AT > '{ts}'"
        ).collect()
    else:
        included_rows = session.sql(
            f"SELECT CONTEXT_KEY FROM SEGMENT_MEMBERS "
            f"WHERE SEGMENT_KEY = '{_esc(segment_key)}' "
            f"  AND IS_ACTIVE = TRUE"
        ).collect()
        excluded_rows = []

    included_keys = [r["CONTEXT_KEY"] for r in included_rows]
    excluded_keys = [r["CONTEXT_KEY"] for r in excluded_rows]

    # ------------------------------------------------------------------
    # 3. Short-circuit if nothing changed
    # ------------------------------------------------------------------
    if not included_keys and not excluded_keys:
        return json.dumps({
            "status": "no_changes",
            "segment_key": segment_key,
            "version": current_version,
        })

    # ------------------------------------------------------------------
    # 4. Send batched requests (max 1000 per included/excluded array)
    # ------------------------------------------------------------------
    included_batches = _chunk(included_keys, BATCH_SIZE) or [[]]
    excluded_batches = _chunk(excluded_keys, BATCH_SIZE) or [[]]
    total_batches = max(len(included_batches), len(excluded_batches))

    results = []
    url = f"{sync_endpoint_url.rstrip('/')}/api/snowflake-sync"

    for i in range(total_batches):
        inc = included_batches[i] if i < len(included_batches) else []
        exc = excluded_batches[i] if i < len(excluded_batches) else []

        new_version = current_version + i + 1

        payload = {
            "audience": segment_key,
            "included": inc,
            "excluded": exc,
            "version": new_version,
        }

        try:
            resp = requests.post(url, json=payload, headers={"Content-Type": "application/json"}, timeout=30)
            status = "success" if resp.status_code == 200 else "error"
            resp_code = resp.status_code
            resp_body = resp.text[:5000]
        except Exception as e:
            status = "error"
            resp_code = 0
            resp_body = str(e)[:5000]

        _log_sync(session, segment_key, new_version, len(inc), len(exc),
                  i + 1, total_batches, status, resp_code, resp_body)

        results.append({
            "batch": i + 1,
            "version": new_version,
            "included_count": len(inc),
            "excluded_count": len(exc),
            "status": status,
            "response_code": resp_code,
        })

    # ------------------------------------------------------------------
    # 5. Update segment metadata on success
    # ------------------------------------------------------------------
    final_version = current_version + total_batches
    any_error = any(r["status"] != "success" for r in results)

    if not any_error:
        session.sql(
            f"UPDATE SEGMENTS "
            f"SET SYNC_VERSION = {final_version}, "
            f"    LAST_SYNCED_AT = CURRENT_TIMESTAMP() "
            f"WHERE SEGMENT_KEY = '{_esc(segment_key)}'"
        ).collect()

    return json.dumps({
        "status": "error" if any_error else "success",
        "segment_key": segment_key,
        "final_version": final_version,
        "total_included": len(included_keys),
        "total_excluded": len(excluded_keys),
        "batches": results,
    }, default=str)


# -- helpers ---------------------------------------------------------------

def _esc(val: str) -> str:
    """Minimal SQL string escaping."""
    return val.replace("'", "''")


def _chunk(lst: list, size: int) -> list:
    """Split a list into chunks of at most `size`."""
    if not lst:
        return []
    return [lst[i:i + size] for i in range(0, len(lst), size)]


def _log_sync(session, segment_key, version, inc_count, exc_count,
              batch_num, total_batches, status, resp_code, resp_body):
    """Insert a row into SYNC_LOG."""
    session.sql(
        f"INSERT INTO SYNC_LOG "
        f"  (SEGMENT_KEY, SYNC_VERSION, INCLUDED_COUNT, EXCLUDED_COUNT, "
        f"   BATCH_NUMBER, TOTAL_BATCHES, STATUS, RESPONSE_CODE, RESPONSE_BODY) "
        f"VALUES ("
        f"  '{_esc(segment_key)}', {version}, {inc_count}, {exc_count}, "
        f"  {batch_num}, {total_batches}, '{_esc(status)}', {resp_code}, "
        f"  '{_esc(resp_body)}')"
    ).collect()
$$;
