# SkillPulse

## Job Market Skill Intelligence and Demand Analytics Platform

SkillPulse is a data engineering and analytics project based on **Project 26 - Job Postings Skill Demand**.

The project processes 18 months of job posting data and converts a raw skills column into structured, queryable data. The pipeline handles duplicate records, inconsistent skill delimiters, capitalization differences, padded values, invalid dates, unknown companies, and missing skill information.

The processed data is used to study skill demand over time, compare skills by month, identify frequently co-occurring skills, and provide the results through an interactive dashboard.

---

## Project Overview

Job postings often contain skills in an inconsistent free-text format. The same skill may appear with different capitalization, separators, or additional spaces.

For example:

```text
Python;SQL;Snowflake
Python,SQL,Snowflake
 python ; sql ; snowflake
PYTHON;SQL;SNOWFLAKE
