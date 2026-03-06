-- =============================================================================
-- 07_demo_walkthrough.sql -- Interactive CRUD lifecycle demo
-- =============================================================================
-- Run each section one at a time in a Snowflake worksheet. Read the comments
-- to understand what each step does and what to expect.
--
-- PREREQUISITE: Run scripts 01 through 06 first.
-- =============================================================================

USE DATABASE LD_SYNC_DEMO;
USE SCHEMA SYNCED_SEGMENTS;
USE WAREHOUSE LD_EXPORT_WH;  -- Change to your warehouse name

-- Replace this with your actual middleware URL.
SET SYNC_URL = 'https://your-middleware-host.vercel.app';


-- =========================================================================
-- STEP 1: Review the starting data
-- =========================================================================
-- You should see the premium-users segment with 10 active members,
-- and the beta-testers segment with 5 active members.

SELECT * FROM SEGMENTS ORDER BY SEGMENT_KEY;
SELECT * FROM SEGMENT_MEMBERS WHERE SEGMENT_KEY = 'premium-users' ORDER BY CONTEXT_KEY;


-- =========================================================================
-- STEP 2: Initial sync -- push all premium-users members to LaunchDarkly
-- =========================================================================
-- This is the first sync, so all 10 active members will be sent as "included".
-- Expected: version goes from 0 to 1, all 10 users sent.

CALL SYNC_SEGMENT_TO_LD('premium-users', $SYNC_URL);

-- Check the result: SYNC_VERSION should now be 1, LAST_SYNCED_AT should be set.
SELECT SEGMENT_KEY, SYNC_VERSION, LAST_SYNCED_AT FROM SEGMENTS WHERE SEGMENT_KEY = 'premium-users';

-- Check the audit log: one entry with INCLUDED_COUNT = 10, EXCLUDED_COUNT = 0.
SELECT * FROM SYNC_LOG ORDER BY SYNCED_AT DESC LIMIT 5;


-- =========================================================================
-- STEP 3: Add new members (CREATE)
-- =========================================================================
-- Insert 3 new users into the segment. Only these 3 will be sent on the
-- next sync (incremental, not the full list).

INSERT INTO SEGMENT_MEMBERS (SEGMENT_KEY, CONTEXT_KEY, IS_ACTIVE) VALUES
    ('premium-users', 'user-011', TRUE),
    ('premium-users', 'user-012', TRUE),
    ('premium-users', 'user-013', TRUE);

-- Sync. Expected: version 1 -> 2, included_count = 3, excluded_count = 0.
CALL SYNC_SEGMENT_TO_LD('premium-users', $SYNC_URL);

-- Verify the incremental sync in the log.
SELECT * FROM SYNC_LOG ORDER BY SYNCED_AT DESC LIMIT 5;


-- =========================================================================
-- STEP 4: Remove members (UPDATE / soft delete)
-- =========================================================================
-- Deactivate 2 users. They will be sent in the "excluded" array on next sync.

UPDATE SEGMENT_MEMBERS
SET IS_ACTIVE = FALSE, UPDATED_AT = CURRENT_TIMESTAMP()
WHERE SEGMENT_KEY = 'premium-users'
  AND CONTEXT_KEY IN ('user-002', 'user-005');

-- Sync. Expected: version 2 -> 3, included_count = 0, excluded_count = 2.
CALL SYNC_SEGMENT_TO_LD('premium-users', $SYNC_URL);

SELECT * FROM SYNC_LOG ORDER BY SYNCED_AT DESC LIMIT 5;


-- =========================================================================
-- STEP 5: Re-add a previously removed member (UPDATE)
-- =========================================================================
-- Reactivate user-002. It will show up in "included" again.

UPDATE SEGMENT_MEMBERS
SET IS_ACTIVE = TRUE, UPDATED_AT = CURRENT_TIMESTAMP()
WHERE SEGMENT_KEY = 'premium-users'
  AND CONTEXT_KEY = 'user-002';

-- Sync. Expected: version 3 -> 4, included_count = 1, excluded_count = 0.
CALL SYNC_SEGMENT_TO_LD('premium-users', $SYNC_URL);

SELECT * FROM SYNC_LOG ORDER BY SYNCED_AT DESC LIMIT 5;


-- =========================================================================
-- STEP 6: No-op sync (nothing changed)
-- =========================================================================
-- Running sync again immediately should detect no changes.

CALL SYNC_SEGMENT_TO_LD('premium-users', $SYNC_URL);

-- Expected: status = "no_changes", no new SYNC_LOG entry.
SELECT * FROM SYNC_LOG ORDER BY SYNCED_AT DESC LIMIT 5;


-- =========================================================================
-- STEP 7: Sync a second segment
-- =========================================================================
-- Demonstrates that multiple segments are independently managed.

CALL SYNC_SEGMENT_TO_LD('beta-testers', $SYNC_URL);

-- Check both segments' state.
SELECT SEGMENT_KEY, SYNC_VERSION, LAST_SYNCED_AT FROM SEGMENTS ORDER BY SEGMENT_KEY;
SELECT * FROM SYNC_LOG ORDER BY SYNCED_AT DESC LIMIT 10;


-- =========================================================================
-- STEP 8: Sync all segments at once (using the helper procedure)
-- =========================================================================
-- This is what the scheduled task calls. It iterates over every segment.
-- Since we just synced both, this should produce "no_changes" for each.

CALL SYNC_ALL_SEGMENTS($SYNC_URL);


-- =========================================================================
-- STEP 9: Review the full audit trail
-- =========================================================================

SELECT
    SYNC_ID,
    SEGMENT_KEY,
    SYNC_VERSION,
    INCLUDED_COUNT,
    EXCLUDED_COUNT,
    STATUS,
    RESPONSE_CODE,
    SYNCED_AT
FROM SYNC_LOG
ORDER BY SYNCED_AT;


-- =========================================================================
-- STEP 10: Review current segment state
-- =========================================================================

-- Segment definitions with sync metadata
SELECT * FROM SEGMENTS ORDER BY SEGMENT_KEY;

-- Current membership (active and inactive)
SELECT
    SEGMENT_KEY,
    CONTEXT_KEY,
    IS_ACTIVE,
    UPDATED_AT
FROM SEGMENT_MEMBERS
ORDER BY SEGMENT_KEY, CONTEXT_KEY;


-- =========================================================================
-- NEXT: Verify in LaunchDarkly
-- =========================================================================
-- 1. Log into LaunchDarkly.
-- 2. Navigate to Segments in your project.
-- 3. Look for the "premium-users" and "beta-testers" segments.
-- 4. Verify that the membership matches what you see in SEGMENT_MEMBERS
--    (active = included, inactive = removed).
-- 5. Try targeting a feature flag with one of these segments.
