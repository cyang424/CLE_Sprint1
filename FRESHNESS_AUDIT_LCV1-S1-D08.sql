-- Freshness audit table 
CREATE OR REPLACE TABLE dev_analytics_db.customer_lifecycle.FRESHNESS_AUDIT ( 
run_id string, 
checked_ts timestamp_ntz, 
rule_name string, 
decision_id string,
target_table string, 
expected_value varchar, 
actual_value varchar, 
severity string, 
status string,                 -- PASSED / FAILED 
details string 
); 
-- ===============
-- LCV1-S1-D01
-- ===============
-- Step 1: Create temp summary 
CREATE OR REPLACE TEMPORARY TABLE PERSON_SPINE_DQ_TEMP_SUMMARY AS
WITH agg AS (
  SELECT
    DECISION_ID,
    RUN_ID,
    OBJECT_CONSTRUCT(
      'TABLE_FRESHNESS', MAX(DATE(RECORD_UPDATED_TS)),
      'RUN_ID_FRESHNESS', TO_DATE(SUBSTRING(RUN_ID, 5, 8), 'YYYYMMDD')
    ) AS METRICS_OBJ
  FROM dev_analytics_db.customer_lifecycle.PERSON_SPINE
  GROUP BY DECISION_ID, RUN_ID
  QUALIFY ROW_NUMBER() OVER (PARTITION BY DECISION_ID ORDER BY RUN_ID DESC) = 1
)  
SELECT
  DECISION_ID,
  RUN_ID,
  CURRENT_TIMESTAMP AS CHECKED_TS,
  'PERSON_SPINE' AS TARGET_TABLE,
  f.key::STRING AS RULE_NAME,
  f.value::STRING AS ACTUAL_VALUE
FROM agg,
LATERAL FLATTEN(input => METRICS_OBJ) f;



INSERT INTO dev_analytics_db.customer_lifecycle.FRESHNESS_AUDIT (
  run_id,
  checked_ts,
  rule_name,
  decision_id,
  target_table,
  expected_value,
  actual_value,
  severity,
  status,
  details
)
SELECT
  s.RUN_ID,
  s.CHECKED_TS,
  s.RULE_NAME,
  s.decision_id,
  s.TARGET_TABLE,
  TO_VARCHAR(CURRENT_DATE) AS EXPECTED_VALUE,
  TO_VARCHAR(s.ACTUAL_VALUE) AS ACTUAL_VALUE,
  r.SEVERITY,
  CASE 
    WHEN s.ACTUAL_VALUE = EXPECTED_VALUE THEN 'PASSED'
    ELSE r.SEVERITY
  END AS STATUS,
  'Checked at ' || CURRENT_TIMESTAMP AS DETAILS
FROM PERSON_SPINE_DQ_TEMP_SUMMARY s
INNER JOIN dev_analytics_db.customer_lifecycle.FRESHNESS_RULES r
  ON s.DECISION_ID = r.DECISION_ID
  AND s.RULE_NAME = r.RULE_NAME
WHERE r.IS_ACTIVE = TRUE;


-- ===============
-- LCV1-S1-D02
-- ===============
-- Step 1: Create temp summary 
CREATE OR REPLACE TEMPORARY TABLE PERSON_IDENTIFIER_XREF_DQ_TEMP_SUMMARY AS
WITH agg AS (
  SELECT
    DECISION_ID,
    RUN_ID,
    OBJECT_CONSTRUCT(
      'TABLE_FRESHNESS', MAX(DATE(RECORD_UPDATED_TS)),
      'RUN_ID_FRESHNESS', TO_DATE(SUBSTRING(RUN_ID, 5, 8), 'YYYYMMDD')
    ) AS METRICS_OBJ
  FROM dev_analytics_db.customer_lifecycle.PERSON_IDENTIFIER_XREF
  GROUP BY DECISION_ID, RUN_ID
  QUALIFY ROW_NUMBER() OVER (PARTITION BY DECISION_ID ORDER BY RUN_ID DESC) = 1
)  
SELECT
  DECISION_ID,
  RUN_ID,
  CURRENT_TIMESTAMP AS CHECKED_TS,
  'PERSON_IDENTIFIER_XREF' AS TARGET_TABLE,
  f.key::STRING AS RULE_NAME,
  f.value::DATE AS ACTUAL_VALUE
FROM agg,
LATERAL FLATTEN(input => METRICS_OBJ) f;


INSERT INTO dev_analytics_db.customer_lifecycle.FRESHNESS_AUDIT (
  run_id,
  checked_ts,
  rule_name,
  decision_id,
  target_table,
  expected_value,
  actual_value,
  severity,
  status,
  details
)
SELECT
  s.RUN_ID,
  s.CHECKED_TS,
  s.RULE_NAME,
  s.decision_id,
  s.TARGET_TABLE,
  CURRENT_DATE AS EXPECTED_VALUE,
  s.ACTUAL_VALUE,
  r.SEVERITY,
  CASE 
    WHEN s.ACTUAL_VALUE = EXPECTED_VALUE THEN 'PASSED'
    ELSE SEVERITY
  END AS STATUS,
  'Checked at ' || CURRENT_TIMESTAMP AS DETAILS
FROM PERSON_IDENTIFIER_XREF_DQ_TEMP_SUMMARY s
INNER JOIN dev_analytics_db.customer_lifecycle.FRESHNESS_RULES r
  ON s.DECISION_ID = r.DECISION_ID
  AND s.RULE_NAME = r.RULE_NAME
WHERE r.IS_ACTIVE = TRUE;

-- ===============
-- LCV1-S1-D3
-- ===============
CREATE OR REPLACE TEMPORARY TABLE CUSTOMER_BEHAVIOR_DAILY_DQ_TEMP_SUMMARY AS
WITH agg AS (
  SELECT
    DECISION_ID,
    NULL AS RUN_ID,
    OBJECT_CONSTRUCT(
      'TABLE_FRESHNESS', TO_VARCHAR(MAX(DATE(RECORD_UPDATED_TS))),
      'LAG_DAY', TO_VARCHAR(DATEDIFF(day, MAX(BUSINESS_DATE), CURRENT_DATE()))
    ) AS METRICS_OBJ
  FROM dev_analytics_db.customer_lifecycle.CUSTOMER_BEHAVIOR_DAILY
  GROUP BY DECISION_ID
)  
SELECT
  DECISION_ID,
  RUN_ID,
  CURRENT_TIMESTAMP AS CHECKED_TS,
  'CUSTOMER_BEHAVIOR_DAILY' AS TARGET_TABLE,
  f.key::STRING AS RULE_NAME,
  f.value::STRING AS ACTUAL_VALUE
FROM agg,
LATERAL FLATTEN(input => METRICS_OBJ) f;


INSERT INTO dev_analytics_db.customer_lifecycle.FRESHNESS_AUDIT (
  run_id,
  checked_ts,
  rule_name,
  decision_id,
  target_table,
  expected_value,
  actual_value,
  severity,
  status,
  details
)
SELECT
  s.RUN_ID,
  s.CHECKED_TS,
  s.RULE_NAME,
  s.decision_id,
  s.TARGET_TABLE,
  CASE 
    WHEN s.RULE_NAME = 'TABLE_FRESHNESS' THEN TO_VARCHAR(DATEADD(day, -COALESCE(r.EXPECTED_LAG_DAYS, 0), CURRENT_DATE()))
    WHEN s.RULE_NAME = 'LAG_DAY' THEN TO_VARCHAR(COALESCE(r.EXPECTED_LAG_DAYS, 0))
  END AS EXPECTED_VALUE,
  s.ACTUAL_VALUE,
  r.SEVERITY,
  CASE 
    WHEN s.RULE_NAME = 'TABLE_FRESHNESS' AND TO_DATE(s.ACTUAL_VALUE) >= DATEADD(day, -COALESCE(r.EXPECTED_LAG_DAYS, 0), CURRENT_DATE()) THEN 'PASSED'
    WHEN s.RULE_NAME = 'LAG_DAY' AND TO_NUMBER(s.ACTUAL_VALUE) <= COALESCE(r.EXPECTED_LAG_DAYS, 0) THEN 'PASSED'
    ELSE r.SEVERITY
  END AS STATUS,
  'Checked at ' || CURRENT_TIMESTAMP AS DETAILS
FROM CUSTOMER_BEHAVIOR_DAILY_DQ_TEMP_SUMMARY s
INNER JOIN dev_analytics_db.customer_lifecycle.FRESHNESS_RULES r
  ON s.DECISION_ID = r.DECISION_ID
  AND s.RULE_NAME = r.RULE_NAME
WHERE r.IS_ACTIVE = TRUE;



-- ===============
-- LCV1-S1-12
-- ===============
CREATE OR REPLACE TEMPORARY TABLE PERSON_CONTACT_PROFILE_DQ_TEMP_SUMMARY AS
WITH agg AS (
  SELECT
    DECISION_ID,
    RUN_ID,
    OBJECT_CONSTRUCT(
      'TABLE_FRESHNESS', MAX(DATE(RECORD_UPDATED_TS)),
      'RUN_ID_FRESHNESS', TO_DATE(SUBSTRING(RUN_ID, 5, 8), 'YYYYMMDD')
    ) AS METRICS_OBJ
  FROM dev_analytics_db.customer_lifecycle.PERSON_CONTACT_PROFILE
  GROUP BY DECISION_ID, RUN_ID
  QUALIFY ROW_NUMBER() OVER (PARTITION BY DECISION_ID ORDER BY RUN_ID DESC) = 1
)  
SELECT
  DECISION_ID,
  RUN_ID,
  CURRENT_TIMESTAMP AS CHECKED_TS,
  'PERSON_CONTACT_PROFILE' AS TARGET_TABLE,
  f.key::STRING AS RULE_NAME,
  f.value::DATE AS ACTUAL_VALUE
FROM agg,
LATERAL FLATTEN(input => METRICS_OBJ) f;


INSERT INTO dev_analytics_db.customer_lifecycle.FRESHNESS_AUDIT (
  run_id,
  checked_ts,
  rule_name,
  decision_id
  target_table,
  expected_value,
  actual_value,
  severity,
  status,
  details
)
SELECT
  s.RUN_ID,
  s.CHECKED_TS,
  s.RULE_NAME,
  s.decision_id,
  s.TARGET_TABLE,
  CURRENT_DATE AS EXPECTED_VALUE,
  s.ACTUAL_VALUE,
  r.SEVERITY,
  CASE 
    WHEN s.ACTUAL_VALUE = EXPECTED_VALUE THEN 'PASSED'
    ELSE SEVERITY
  END AS STATUS,
  'Checked at ' || CURRENT_TIMESTAMP AS DETAILS
FROM  PERSON_CONTACT_PROFILE_DQ_TEMP_SUMMARY s
INNER JOIN dev_analytics_db.customer_lifecycle.FRESHNESS_RULES r
  ON s.DECISION_ID = r.DECISION_ID
  AND s.RULE_NAME = r.RULE_NAME
WHERE r.IS_ACTIVE = TRUE;


select * from dev_analytics_db.customer_lifecycle.FRESHNESS_AUDIT

select * from dev_analytics_db.customer_lifecycle.FRESHNESS_RULES