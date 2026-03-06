-- =============================================================================
-- 03_tables.sql -- Core tables for segment management and sync tracking
-- =============================================================================

USE DATABASE LD_SYNC_DEMO;
USE SCHEMA SYNCED_SEGMENTS;

-- ---------------------------------------------------------------------------
-- SEGMENTS: one row per segment definition
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS SEGMENTS (
    SEGMENT_KEY     VARCHAR(255)    NOT NULL,
    SEGMENT_NAME    VARCHAR(500)    NOT NULL,
    CONTEXT_KIND    VARCHAR(100)    DEFAULT 'user',
    SYNC_VERSION    INT             DEFAULT 0,
    LAST_SYNCED_AT  TIMESTAMP_NTZ,
    CREATED_AT      TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),

    PRIMARY KEY (SEGMENT_KEY)
);

-- ---------------------------------------------------------------------------
-- SEGMENT_MEMBERS: one row per context belonging to a segment
--
-- is_active = TRUE  --> context will be sent in the "included" array
-- is_active = FALSE --> context will be sent in the "excluded" array
--
-- updated_at is compared against SEGMENTS.last_synced_at to detect changes.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS SEGMENT_MEMBERS (
    SEGMENT_KEY     VARCHAR(255)    NOT NULL,
    CONTEXT_KEY     VARCHAR(500)    NOT NULL,
    IS_ACTIVE       BOOLEAN         DEFAULT TRUE,
    UPDATED_AT      TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    CREATED_AT      TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),

    PRIMARY KEY (SEGMENT_KEY, CONTEXT_KEY),
    FOREIGN KEY (SEGMENT_KEY) REFERENCES SEGMENTS(SEGMENT_KEY)
);

-- ---------------------------------------------------------------------------
-- SYNC_LOG: append-only audit trail of every sync attempt
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS SYNC_LOG (
    SYNC_ID         INT AUTOINCREMENT,
    SEGMENT_KEY     VARCHAR(255),
    SYNC_VERSION    INT,
    INCLUDED_COUNT  INT,
    EXCLUDED_COUNT  INT,
    BATCH_NUMBER    INT             DEFAULT 1,
    TOTAL_BATCHES   INT             DEFAULT 1,
    STATUS          VARCHAR(50),
    RESPONSE_CODE   INT,
    RESPONSE_BODY   VARCHAR(10000),
    SYNCED_AT       TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),

    PRIMARY KEY (SYNC_ID)
);
