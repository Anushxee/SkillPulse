-- sql/tables.sql
-- Target analytical tables in Snowflake. Column types match the Gold-layer
-- shape produced by databricks/04_gold and scripts/pipeline_core.py.

USE DATABASE SKILLPULSE;
USE SCHEMA ANALYTICS;

CREATE OR REPLACE TABLE GOLD_SKILL_MONTH (
    skill                VARCHAR(50)     NOT NULL,
    month                VARCHAR(7)      NOT NULL,   -- 'YYYY-MM'
    postings_with_skill  NUMBER(10)      NOT NULL,
    postings_that_month  NUMBER(10)      NOT NULL,
    skill_share          FLOAT           NOT NULL,
    rank_in_month         NUMBER(4)       NOT NULL,
    PRIMARY KEY (skill, month)
);

CREATE OR REPLACE TABLE GOLD_SKILL_PAIR_MONTH (
    skill_a             VARCHAR(50)     NOT NULL,
    skill_b             VARCHAR(50)     NOT NULL,
    month               VARCHAR(7)      NOT NULL,
    pair_postings       NUMBER(10)      NOT NULL,
    postings_a          NUMBER(10)      NOT NULL,
    confidence_a_to_b   FLOAT           NOT NULL,
    lift                FLOAT           NOT NULL,
    PRIMARY KEY (skill_a, skill_b, month)
);

-- Optional: staging-detail table if you want row-level Silver data in
-- Snowflake too (not required for the three faculty questions, which are
-- fully answerable from the two Gold tables above).
CREATE OR REPLACE TABLE SILVER_REJECTS (
    posting_id          VARCHAR(20),
    company_id          VARCHAR(20),
    rejection_reason    VARCHAR(50),
    rejected_at          TIMESTAMP_NTZ,
    _source_file         VARCHAR(100),
    rejection_details    VARCHAR(500)
);
