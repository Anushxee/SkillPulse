-- sql/copy_into.sql
-- Idempotent load: Snowflake's COPY INTO tracks file metadata (name, size,
-- MD5) per stage/table combination for 64 days by default, so re-running the
-- SAME file a second time loads zero rows unless FORCE=TRUE is passed. That
-- is the mechanism the brief wants demonstrated — run this script twice
-- without truncating the table first, and confirm the second run reports
-- 0 rows loaded / an already-loaded skip in the result.

USE DATABASE SKILLPULSE;
USE SCHEMA ANALYTICS;

COPY INTO GOLD_SKILL_MONTH
    FROM @SKILLPULSE_STAGE/gold_skill_month.csv
    FILE_FORMAT = (FORMAT_NAME = CSV_STANDARD)
    ON_ERROR = 'ABORT_STATEMENT';

COPY INTO GOLD_SKILL_PAIR_MONTH
    FROM @SKILLPULSE_STAGE/gold_skill_pair_month.csv
    FILE_FORMAT = (FORMAT_NAME = CSV_STANDARD)
    ON_ERROR = 'ABORT_STATEMENT';

COPY INTO SILVER_REJECTS
    FROM @SKILLPULSE_STAGE/silver_rejects.csv
    FILE_FORMAT = (FORMAT_NAME = CSV_STANDARD)
    ON_ERROR = 'ABORT_STATEMENT';

-- Prove idempotency: run this a second time in the same session/day.
-- COPY_HISTORY shows the file already loaded and skipped.
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'GOLD_SKILL_MONTH',
    START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
));

-- Sanity counts after load
SELECT COUNT(*) AS gold_skill_month_rows FROM GOLD_SKILL_MONTH;          -- expect 360
SELECT COUNT(*) AS gold_pair_month_rows  FROM GOLD_SKILL_PAIR_MONTH;     -- expect 6660
