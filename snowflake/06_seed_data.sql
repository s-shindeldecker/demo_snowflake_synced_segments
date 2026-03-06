-- =============================================================================
-- 06_seed_data.sql -- Sample segment and members for the demo walkthrough
-- =============================================================================

USE DATABASE LD_SYNC_DEMO;
USE SCHEMA SYNCED_SEGMENTS;

-- Clear any previous demo data (idempotent re-runs)
DELETE FROM SYNC_LOG    WHERE SEGMENT_KEY IN ('premium-users', 'beta-testers');
DELETE FROM SEGMENT_MEMBERS WHERE SEGMENT_KEY IN ('premium-users', 'beta-testers');
DELETE FROM SEGMENTS    WHERE SEGMENT_KEY IN ('premium-users', 'beta-testers');

-- ---------------------------------------------------------------------------
-- Segment 1: premium-users (primary demo segment)
-- ---------------------------------------------------------------------------
INSERT INTO SEGMENTS (SEGMENT_KEY, SEGMENT_NAME, CONTEXT_KIND)
VALUES ('premium-users', 'Premium Users', 'user');

INSERT INTO SEGMENT_MEMBERS (SEGMENT_KEY, CONTEXT_KEY, IS_ACTIVE) VALUES
    ('premium-users', 'user-001', TRUE),
    ('premium-users', 'user-002', TRUE),
    ('premium-users', 'user-003', TRUE),
    ('premium-users', 'user-004', TRUE),
    ('premium-users', 'user-005', TRUE),
    ('premium-users', 'user-006', TRUE),
    ('premium-users', 'user-007', TRUE),
    ('premium-users', 'user-008', TRUE),
    ('premium-users', 'user-009', TRUE),
    ('premium-users', 'user-010', TRUE);

-- ---------------------------------------------------------------------------
-- Segment 2: beta-testers (secondary segment to show multi-segment support)
-- ---------------------------------------------------------------------------
INSERT INTO SEGMENTS (SEGMENT_KEY, SEGMENT_NAME, CONTEXT_KIND)
VALUES ('beta-testers', 'Beta Testers', 'user');

INSERT INTO SEGMENT_MEMBERS (SEGMENT_KEY, CONTEXT_KEY, IS_ACTIVE) VALUES
    ('beta-testers', 'user-003', TRUE),
    ('beta-testers', 'user-007', TRUE),
    ('beta-testers', 'user-011', TRUE),
    ('beta-testers', 'user-012', TRUE),
    ('beta-testers', 'user-013', TRUE);

-- Verify
SELECT 'SEGMENTS' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM SEGMENTS
UNION ALL
SELECT 'SEGMENT_MEMBERS', COUNT(*) FROM SEGMENT_MEMBERS
UNION ALL
SELECT 'SYNC_LOG', COUNT(*) FROM SYNC_LOG;
