-- =============================================================================
-- 02_network_access.sql -- External access integration for outbound HTTPS
-- =============================================================================
-- Snowflake blocks all egress by default. These objects allow stored procedures
-- to make HTTPS calls to the middleware endpoint.
--
-- Requires ACCOUNTADMIN (or a role with CREATE INTEGRATION privilege).

USE ROLE ACCOUNTADMIN;
USE DATABASE LD_SYNC_DEMO;
USE SCHEMA SYNCED_SEGMENTS;

-- Network rule: allow outbound HTTPS to the middleware.
-- Update VALUE_LIST to match where you deploy the middleware.
CREATE OR REPLACE NETWORK RULE LD_SYNC_EGRESS_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('your-middleware-host.vercel.app:443');

-- External access integration: grants stored procedures permission to use the rule.
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION LD_SYNC_ACCESS_INTEGRATION
    ALLOWED_NETWORK_RULES = (LD_SYNC_EGRESS_RULE)
    ENABLED = TRUE;

-- Grant usage so procedures created by other roles can reference the integration.
GRANT USAGE ON INTEGRATION LD_SYNC_ACCESS_INTEGRATION TO ROLE ACCOUNTADMIN;
