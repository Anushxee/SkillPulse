-- sql/snowflake_setup.sql
-- Run once per Snowflake account. No student prefix needed here — this is
-- your own account, unlike the shared Databricks workspace.
-- Credentials are NEVER embedded here: connect via SnowSQL/Snowsight with
-- your own login, or via env vars (see .env.example) if scripting the load.

CREATE DATABASE IF NOT EXISTS SKILLPULSE;
USE DATABASE SKILLPULSE;

CREATE SCHEMA IF NOT EXISTS ANALYTICS;
USE SCHEMA ANALYTICS;

-- File format matching the CSVs exported by databricks/06_export
CREATE OR REPLACE FILE FORMAT CSV_STANDARD
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL', 'null');

-- Internal stage for the Gold CSV exports
CREATE OR REPLACE STAGE SKILLPULSE_STAGE
    FILE_FORMAT = CSV_STANDARD
    COMMENT = 'Landing area for SkillPulse Gold-layer CSV exports from Databricks';

-- Upload files with SnowSQL, e.g.:
--   PUT file:///local/path/gold_skill_month.csv @SKILLPULSE_STAGE AUTO_COMPRESS=TRUE;
--   PUT file:///local/path/gold_skill_pair_month.csv @SKILLPULSE_STAGE AUTO_COMPRESS=TRUE;
