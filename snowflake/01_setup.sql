-- =============================================================================
-- 01_setup.sql -- Database and schema for the synced segments demo
-- =============================================================================
-- Run this as ACCOUNTADMIN (or a role with CREATE DATABASE privilege).
-- Update the USE WAREHOUSE line to match your own warehouse name.

USE ROLE ACCOUNTADMIN;

-- Use your existing warehouse (change this to your warehouse name)
USE WAREHOUSE LD_EXPORT_WH;

-- Database
CREATE DATABASE IF NOT EXISTS LD_SYNC_DEMO;

-- Schema
CREATE SCHEMA IF NOT EXISTS LD_SYNC_DEMO.SYNCED_SEGMENTS;

-- Set context for subsequent scripts
USE DATABASE LD_SYNC_DEMO;
USE SCHEMA SYNCED_SEGMENTS;
