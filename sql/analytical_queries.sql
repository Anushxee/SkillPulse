-- sql/analytical_queries.sql
-- The three faculty-defined analytical questions, answered directly in
-- Snowflake SQL against GOLD_SKILL_MONTH / GOLD_SKILL_PAIR_MONTH.

USE DATABASE SKILLPULSE;
USE SCHEMA ANALYTICS;


Q1. Top 10 skills by posting count, per month
SELECT month, skill, postings_with_skill, skill_share, rank_in_month
FROM GOLD_SKILL_MONTH
WHERE rank_in_month <= 10
ORDER BY month, rank_in_month;

-- Q2. Which skills are growing fastest quarter-on-quarter?
-- Quarter = 3 consecutive months of `month` ('YYYY-MM'); aggregate first,
-- then divide once at the end (ratio of sums, not average of ratios).

WITH monthly AS (
    SELECT skill, month, postings_with_skill,
           TO_CHAR(DATE_TRUNC('quarter', TO_DATE(month || '-01', 'YYYY-MM-DD')), 'YYYY"Q"Q') AS quarter
    FROM GOLD_SKILL_MONTH
),
quarterly AS (
    SELECT skill, quarter, SUM(postings_with_skill) AS quarter_postings
    FROM monthly
    GROUP BY skill, quarter
),
qoq AS (
    SELECT skill, quarter, quarter_postings,
           LAG(quarter_postings) OVER (PARTITION BY skill ORDER BY quarter) AS prev_quarter_postings
    FROM quarterly
)
SELECT skill, quarter, prev_quarter_postings, quarter_postings,
       quarter_postings - prev_quarter_postings AS qoq_abs_change,
       ROUND(100.0 * (quarter_postings - prev_quarter_postings)
             / NULLIF(prev_quarter_postings, 0), 1) AS qoq_pct_change
FROM qoq
WHERE prev_quarter_postings IS NOT NULL
ORDER BY qoq_pct_change DESC NULLS LAST;

-- Q3. Which skills appear together? (companion skills for a chosen skill)
-- Example: Snowflake's top companions, most recent month.
SELECT skill_b AS companion_skill, month, pair_postings, postings_a,
       ROUND(confidence_a_to_b * 100, 1) AS confidence_pct,
       ROUND(lift, 2) AS lift
FROM GOLD_SKILL_PAIR_MONTH
WHERE skill_a = 'snowflake'
  AND month = (SELECT MAX(month) FROM GOLD_SKILL_PAIR_MONTH)
ORDER BY pair_postings DESC
LIMIT 10;

-- Support / confidence / lift, defined:
--   support(A,B)    = postings containing both A and B / postings that month
--   confidence(A->B) = postings containing both A and B / postings containing A
--   lift(A->B)       = confidence(A->B) / (postings containing B / postings that month)
-- lift > 1 means A and B co-occur more than chance would predict.
