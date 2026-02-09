create or replace table dev_analytics_db.customer_lifecycle.DQ_AUDIT ( 
  dq_run_id string, 
  pipeline_name string, 
  decision_id string,
  business_date date, 
  check_name string, 
  severity string,           -- FAIL / WARN 
  violation_count number, 
  threshold number, 
  status string,             -- PASSED / FAILED 
  created_ts timestamp_ntz 
); 

-- ============================================
-- PERSON_SPINE DQ TEMP SUMMARY
-- ============================================
CREATE OR REPLACE TEMPORARY TABLE PERSON_SPINE_DQ_TEMP_SUMMARY AS
WITH agg AS (
  SELECT
    DECISION_ID,
    COUNT(*) AS TOTAL_VOL,
    COUNT(DISTINCT PERSON_ID) AS DISTINCT_PERSON_ID,
    OBJECT_CONSTRUCT(
      -- Metadata
    --   'TOTAL_VOL', TOTAL_VOL,
    --   'DISTINCT_PERSON_ID', DISTINCT_PERSON_ID,
      
      -- FAIL Checks (Threshold = 0)
      'REQ_PERSON_ID', COUNT_IF(PERSON_ID IS NULL),
      'UNQ_PERSON_ID', IFF(TOTAL_VOL > DISTINCT_PERSON_ID, TOTAL_VOL - DISTINCT_PERSON_ID, 0),
      'REQ_IDENTITY_TYPE_USED', COUNT_IF(IDENTITY_TYPE_USED IS NULL),
      'REQ_SRC_SYSTEM_CD', COUNT_IF(SRC_SYSTEM_CD IS NULL),
      'REQ_SRC_CUSTOMER_ID', COUNT_IF(SRC_CUSTOMER_ID IS NULL),
      'REQ_CREATE_TS', COUNT_IF(RECORD_CREATED_TS IS NULL),
      'LOG_CREATE_LS_UPDATE_TS', COUNT_IF(
        RECORD_CREATED_TS IS NOT NULL 
        AND RECORD_UPDATED_TS IS NOT NULL
        AND RECORD_CREATED_TS > RECORD_UPDATED_TS),
      'REQ_DECISION_VERSION', COUNT_IF(DECISION_VERSION IS NULL),
      
      -- WARN Checks
      'LOG_FIRST_LS_LAST_SEEN', COUNT_IF(
        FIRST_SEEN_DATE IS NOT NULL
        AND LAST_SEEN_DATE IS NOT NULL
        AND FIRST_SEEN_DATE > LAST_SEEN_DATE),
      'LOG_DOB', COUNT_IF(
        DATE_OF_BIRTH IS NOT NULL
        AND YEAR(CURRENT_DATE()) - YEAR(DATE_OF_BIRTH) < 18),
      'THR_SSN_NULL', COUNT_IF(SSN IS NULL),
      'THR_DOB_NULL', COUNT_IF(DATE_OF_BIRTH IS NULL),
      'THR_IS_ACTIVE_FALSE', COUNT_IF(IS_ACTIVE = FALSE),
      'THR_RECORD_DROP_PCT', DIV0(
        COUNT_IF(IS_ACTIVE = FALSE AND DATE(RECORD_UPDATED_TS) = CURRENT_DATE()),
        TOTAL_VOL
      ) * 100
    ) AS METRICS_OBJ
  FROM dev_analytics_db.customer_lifecycle.PERSON_SPINE
  GROUP BY DECISION_ID
)  
SELECT
  DECISION_ID,
  'PERSON_SPINE' AS TABLE_NAME,
  f.key::STRING AS CHECK_NAME,
  f.value::FLOAT AS CHECK_VALUE
FROM agg,
LATERAL FLATTEN(input => METRICS_OBJ) f;


--==============================================
-- INSERT INTO DQ_AUDIT
--==============================================
INSERT INTO dev_analytics_db.customer_lifecycle.DQ_AUDIT (
  dq_run_id,
  pipeline_name,
  decision_id,
  business_date,
  check_name,
  severity,
  violation_count,
  threshold,
  status,
  created_ts
)
SELECT
  s.DECISION_ID || '_' || TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS') AS DQ_RUN_ID,
  s.TABLE_NAME AS PIPELINE_NAME,
  s.decision_id,
  CURRENT_DATE() AS BUSINESS_DATE,
  s.CHECK_NAME,
  t.SEVERITY,
  s.CHECK_VALUE AS VIOLATION_COUNT,
  t.THRESHOLD_VALUE AS THRESHOLD,
  CASE 
    -- Skip metadata fields
    WHEN s.CHECK_NAME IN ('TOTAL_VOL', 'DISTINCT_PERSON_ID') THEN 'METADATA'
    
    -- All threshold types: simple comparison (violation_count vs threshold)
    WHEN s.CHECK_VALUE <= t.THRESHOLD_VALUE THEN 'PASSED'
    WHEN s.CHECK_VALUE > t.THRESHOLD_VALUE THEN t.SEVERITY
    
    ELSE 'UNKNOWN'
  END AS STATUS,
  CURRENT_TIMESTAMP AS CREATED_TS
FROM PERSON_SPINE_DQ_TEMP_SUMMARY s
INNER JOIN dev_analytics_db.customer_lifecycle.DQ_THRESHOLDS t
  ON s.TABLE_NAME = t.TABLE_NAME
  AND s.CHECK_NAME = t.CHECK_NAME
  AND s.DECISION_ID=t.DECISION_ID
WHERE t.IS_ACTIVE = TRUE
  AND s.CHECK_NAME NOT IN ('TOTAL_VOL', 'DISTINCT_PERSON_ID');

-- ============================================
-- PERSON_IDENTIFIER_XREF DQ TEMP SUMMARY
-- ============================================
CREATE OR REPLACE TEMPORARY TABLE PERSON_IDENTIFIER_XREF_DQ_TEMP_SUMMARY AS
WITH agg AS (
    SELECT 
        DECISION_ID,
        COUNT(*) AS TOTAL_VOL,
        COUNT(DISTINCT SRC_SYSTEM_CD, SOURCE_IDENTIFIER_TYPE, SOURCE_IDENTIFIER_VALUE, PERSON_ID) AS DISTINCT_PK,
        OBJECT_CONSTRUCT(
            -- Metadata
            'TOTAL_VOL', TOTAL_VOL,
            'DISTINCT_PK', DISTINCT_PK,
            
            -- FAIL Checks (Threshold = 0)
            'REQ_PK', COUNT_IF(
                SRC_SYSTEM_CD IS NULL 
                OR SOURCE_IDENTIFIER_TYPE IS NULL 
                OR SOURCE_IDENTIFIER_VALUE IS NULL 
                OR PERSON_ID IS NULL),
            'UNQ_PK', IFF(TOTAL_VOL > DISTINCT_PK, TOTAL_VOL - DISTINCT_PK, 0),
            'REQ_PERSON_ID_EXISTS', COUNT_IF(
                PERSON_ID IS NOT NULL
                AND PERSON_ID NOT IN (
                    SELECT DISTINCT PERSON_ID 
                    FROM dev_analytics_db.customer_lifecycle.PERSON_SPINE
                )
            ),
            'REQ_CREATE_TS', COUNT_IF(RECORD_CREATED_TS IS NULL),
            'LOG_CREATE_LS_UPDATE_TS', COUNT_IF(
                RECORD_CREATED_TS IS NOT NULL 
                AND RECORD_UPDATED_TS IS NOT NULL
                AND RECORD_CREATED_TS > RECORD_UPDATED_TS),
            
            -- WARN Checks
            'LOG_EFFECTIVE_START_LS_END_TS', COUNT_IF(
                EFFECTIVE_START_DATE IS NOT NULL 
                AND EFFECTIVE_END_DATE IS NOT NULL 
                AND EFFECTIVE_START_DATE > EFFECTIVE_END_DATE),
            'THR_RECORD_DROP_PCT', DIV0(
                COUNT_IF(
                    EFFECTIVE_END_DATE IS NOT NULL 
                    AND DATE(EFFECTIVE_END_DATE) = CURRENT_DATE()),
                TOTAL_VOL
            ) * 100
        ) AS METRICS_OBJ
    FROM dev_analytics_db.customer_lifecycle.PERSON_IDENTIFIER_XREF
    WHERE EFFECTIVE_END_DATE IS NULL
    GROUP BY DECISION_ID
)
SELECT
    DECISION_ID,
    'PERSON_IDENTIFIER_XREF' AS TABLE_NAME,
    f.key::STRING AS CHECK_NAME,
    f.value::FLOAT AS CHECK_VALUE
FROM agg,
LATERAL FLATTEN(input => METRICS_OBJ) f;


--==============================================
-- INSERT INTO DQ_AUDIT
--==============================================
INSERT INTO dev_analytics_db.customer_lifecycle.DQ_AUDIT (
  dq_run_id,
  pipeline_name,
  decision_id,
  business_date,
  check_name,
  severity,
  violation_count,
  threshold,
  status,
  created_ts
)
SELECT
  s.DECISION_ID || '_' || TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS') AS DQ_RUN_ID,
  s.TABLE_NAME AS PIPELINE_NAME,
  s.decision_id,
  CURRENT_DATE() AS BUSINESS_DATE,
  s.CHECK_NAME,
  t.SEVERITY,
  s.CHECK_VALUE AS VIOLATION_COUNT,
  t.THRESHOLD_VALUE AS THRESHOLD,
  CASE 
    -- Skip metadata fields
    WHEN s.CHECK_NAME IN ('TOTAL_VOL', 'DISTINCT_PK') THEN 'METADATA'
    
    -- All threshold types: simple comparison
    WHEN s.CHECK_VALUE <= t.THRESHOLD_VALUE THEN 'PASSED'
    WHEN s.CHECK_VALUE > t.THRESHOLD_VALUE THEN t.SEVERITY
    
    ELSE 'UNKNOWN'
  END AS STATUS,
  CURRENT_TIMESTAMP AS CREATED_TS
FROM PERSON_IDENTIFIER_XREF_DQ_TEMP_SUMMARY s
INNER JOIN dev_analytics_db.customer_lifecycle.DQ_THRESHOLDS t
  ON s.CHECK_NAME = t.CHECK_NAME
  AND s.DECISION_ID = t.DECISION_ID
WHERE t.IS_ACTIVE = TRUE
  AND s.CHECK_NAME NOT IN ('TOTAL_VOL', 'DISTINCT_PK');




-- ============================================
-- CUSTOMER_BEHAVIOR_DAILY_SNAPSHOT DQ TEMP SUMMARY
-- ============================================
CREATE OR REPLACE TEMPORARY TABLE CUSTOMER_BEHAVIOR_DAILY_SNAPSHOT_DQ_TEMP_SUMMARY AS
WITH agg AS (
    SELECT 
        DECISION_ID,
        COUNT(*) AS TOTAL_VOL,
        COUNT(DISTINCT PERSON_ID, BUSINESS_DATE) AS DISTINCT_PK,
        OBJECT_CONSTRUCT(
            -- Metadata
            'TOTAL_VOL', TOTAL_VOL,
            'DISTINCT_PK', DISTINCT_PK,
            
            -- FAIL Checks (Threshold = 0)
            'REQ_PK', COUNT_IF(
                PERSON_ID IS NULL 
                OR BUSINESS_DATE IS NULL),
            'UNQ_PK', IFF(TOTAL_VOL > DISTINCT_PK, TOTAL_VOL - DISTINCT_PK, 0),
            'REQ_PERSON_ID_EXISTS', COUNT_IF(
                PERSON_ID IS NOT NULL
                AND PERSON_ID NOT IN (
                    SELECT DISTINCT PERSON_ID 
                    FROM dev_analytics_db.customer_lifecycle.PERSON_SPINE
                )
            ),
            'REQ_SRC_SYSTEM_CD', COUNT_IF(SRC_SYSTEM_CD IS NULL),
            'REQ_IS_ACTIVE_FLAG', COUNT_IF(IS_ACTIVE_CUSTOMER_FLAG IS NULL),
            'DEP_INACTIVE_REQUIRES_DAYS', COUNT_IF(
                IS_ACTIVE_CUSTOMER_FLAG = FALSE AND LAST_ACTIVITY_DATE IS NOT NULL
                AND DAYS_SINCE_INACTIVE IS NULL),
            'LOG_CREATE_LS_UPDATE_TS', COUNT_IF(
                RECORD_CREATED_TS IS NOT NULL 
                AND RECORD_UPDATED_TS IS NOT NULL
                AND RECORD_CREATED_TS > RECORD_UPDATED_TS),
            
            -- WARN Checks
            'THR_SSN_NULL', COUNT_IF(SSN IS NULL),
            'THR_RECORD_DROP_PCT', DIV0(
                COUNT_IF(BUSINESS_DATE=CURRENT_DATE-1)-COUNT_IF(BUSINESS_DATE=CURRENT_DATE),
                COUNT_IF(BUSINESS_DATE=CURRENT_DATE-1)
            ) * 100
        ) AS METRICS_OBJ
    FROM dev_analytics_db.customer_lifecycle.CUSTOMER_BEHAVIOR_DAILY
    GROUP BY DECISION_ID
)
SELECT
    DECISION_ID,
    'CUSTOMER_BEHAVIOR_DAILY' AS TABLE_NAME,
    f.key::STRING AS CHECK_NAME,
    f.value::FLOAT AS CHECK_VALUE
FROM agg,
LATERAL FLATTEN(input => METRICS_OBJ) f;



--==============================================
-- INSERT INTO DQ_AUDIT
--==============================================
INSERT INTO dev_analytics_db.customer_lifecycle.DQ_AUDIT (
  dq_run_id,
  pipeline_name,
  decision_id,
  business_date,
  check_name,
  severity,
  violation_count,
  threshold,
  status,
  created_ts
)
SELECT
  s.DECISION_ID || '_' || TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS') AS DQ_RUN_ID,
  s.TABLE_NAME AS PIPELINE_NAME,
  s.decision_id,
  CURRENT_DATE() AS BUSINESS_DATE,
  s.CHECK_NAME,
  t.SEVERITY,
  s.CHECK_VALUE AS VIOLATION_COUNT,
  t.THRESHOLD_VALUE AS THRESHOLD,
  CASE 
    -- Skip metadata fields
    WHEN s.CHECK_NAME IN ('TOTAL_VOL', 'DISTINCT_PK') THEN 'METADATA'
    
    -- All threshold types: simple comparison
    WHEN s.CHECK_VALUE <= t.THRESHOLD_VALUE THEN 'PASSED'
    WHEN s.CHECK_VALUE > t.THRESHOLD_VALUE THEN t.SEVERITY
    
    ELSE 'UNKNOWN'
  END AS STATUS,
  CURRENT_TIMESTAMP AS CREATED_TS
FROM CUSTOMER_BEHAVIOR_DAILY_SNAPSHOT_DQ_TEMP_SUMMARY s
INNER JOIN dev_analytics_db.customer_lifecycle.DQ_THRESHOLDS t
  ON s.DECISION_ID = t.DECISION_ID
  AND s.CHECK_NAME=t.CHECK_NAME
WHERE t.IS_ACTIVE = TRUE
  AND s.CHECK_NAME NOT IN ('TOTAL_VOL', 'DISTINCT_PK');


-- ============================================
-- PERSON_CONTACT_PROFILE_DAILY DQ TEMP SUMMARY
-- ============================================
CREATE OR REPLACE TEMPORARY TABLE PERSON_CONTACT_PROFILE_DQ_TEMP_SUMMARY AS
WITH agg AS (
    SELECT 
        DECISION_ID,
        SUM(CASE WHEN IS_ACTIVE=TRUE THEN 1 ELSE 0 END) AS TOTAL_VOL,
        COUNT(DISTINCT CASE 
             WHEN IS_ACTIVE = TRUE 
             THEN PERSON_ID || '-' || SRC_SYSTEM_CD 
             END) AS DISTINCT_PK,
        OBJECT_CONSTRUCT(
            -- Metadata
            'TOTAL_VOL', TOTAL_VOL,
            'DISTINCT_PK', DISTINCT_PK,
            
            -- FAIL Checks - Core Requirements
            'REQ_PK', COUNT_IF(IS_ACTIVE=TRUE AND (PERSON_ID IS NULL OR SRC_SYSTEM_CD IS NULL OR IS_ACTIVE IS NULL)),
            'UNQ_PK', IFF(TOTAL_VOL > DISTINCT_PK, TOTAL_VOL - DISTINCT_PK, 0),
            'REQ_PERSON_ID_EXISTS', COUNT_IF(IS_ACTIVE=TRUE AND
                PERSON_ID IS NOT NULL
                AND PERSON_ID NOT IN (
                    SELECT DISTINCT PERSON_ID 
                    FROM dev_analytics_db.customer_lifecycle.PERSON_SPINE
                )
            ),
            'REQ_RECORD_CREATED_TS', COUNT_IF(IS_ACTIVE=TRUE AND RECORD_CREATED_TS IS NULL),
            'LOG_RECORD_CREATED_LS_UPDATED', COUNT_IF(
                IS_ACTIVE=TRUE AND
                RECORD_CREATED_TS IS NOT NULL 
                AND RECORD_UPDATED_TS IS NOT NULL
                AND RECORD_CREATED_TS > RECORD_UPDATED_TS),
            
            -- FAIL Checks - Format Validation
            'VAL_EMAIL_FORMAT', COUNT_IF(IS_ACTIVE=TRUE AND
                PRIMARY_EMAIL_ADDRESS IS NOT NULL 
                AND PRIMARY_EMAIL_ADDRESS NOT LIKE '%@%'),
            'VAL_PHONE_FORMAT', COUNT_IF(IS_ACTIVE=TRUE AND
                PRIMARY_PHONE_NUMBER IS NOT NULL 
                AND (LENGTH(REGEXP_REPLACE(PRIMARY_PHONE_NUMBER, '[^0-9]', '')) < 10)),
            'VAL_STATE_CODE', COUNT_IF(IS_ACTIVE=TRUE AND
                MAILING_ADDRESS_STATE_CD IS NOT NULL 
                AND LENGTH(MAILING_ADDRESS_STATE_CD) != 2),
        
            -- WARN Checks - Coverage & Quality Monitoring
            'THR_EMAIL_COVERAGE_DROP', DIV0(COUNT_IF(RECORD_UPDATED_TS>=CURRENT_DATE AND PRIMARY_EMAIL_ADDRESS IS NULL), DISTINCT_PK) * 100,
            'THR_PHONE_COVERAGE_DROP', DIV0(COUNT_IF(RECORD_UPDATED_TS>=CURRENT_DATE AND PRIMARY_PHONE_NUMBER IS NULL), DISTINCT_PK) * 100,
            'THR_ADDRESS_COVERAGE_DROP', DIV0(COUNT_IF(RECORD_UPDATED_TS>=CURRENT_DATE AND MAILING_ADDRESS_LINE1 IS NULL), DISTINCT_PK) * 100,
            'THR_RECORD_DROP_PCT', DIV0(COUNT_IF(IS_ACTIVE = FALSE AND DATE(RECORD_UPDATED_TS) = CURRENT_DATE()),DISTINCT_PK) * 100,
            'THR_NAME_NULL', COUNT_IF(FIRST_NAME IS NULL OR LAST_NAME IS NULL)
        ) AS METRICS_OBJ
    FROM dev_analytics_db.customer_lifecycle.PERSON_CONTACT_PROFILE
    GROUP BY DECISION_ID
)
SELECT
    DECISION_ID,
    'PERSON_CONTACT_PROFILE' AS TABLE_NAME,
    f.key::STRING AS CHECK_NAME,
    f.value::FLOAT AS CHECK_VALUE
FROM agg,
LATERAL FLATTEN(input => METRICS_OBJ) f
WHERE f.key NOT IN ('TOTAL_VOL', 'DISTINCT_PK')
ORDER BY CHECK_NAME;



--==============================================
-- INSERT INTO DQ_AUDIT
--==============================================
INSERT INTO dev_analytics_db.customer_lifecycle.DQ_AUDIT (
  dq_run_id,
  pipeline_name,
  decision_id,
  business_date,
  check_name,
  severity,
  violation_count,
  threshold,
  status,
  created_ts
)
SELECT
  s.DECISION_ID || '_' || TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS') AS DQ_RUN_ID,
  s.TABLE_NAME AS PIPELINE_NAME,
  s.decision_id,
  CURRENT_DATE() AS BUSINESS_DATE,
  s.CHECK_NAME,
  t.SEVERITY,
  s.CHECK_VALUE AS VIOLATION_COUNT,
  t.THRESHOLD_VALUE AS THRESHOLD,
  CASE 
    -- Skip metadata fields
    WHEN s.CHECK_NAME IN ('TOTAL_VOL', 'DISTINCT_PK') THEN 'METADATA'
    
    -- All threshold types: simple comparison
    WHEN s.CHECK_VALUE <= t.THRESHOLD_VALUE THEN 'PASSED'
    WHEN s.CHECK_VALUE > t.THRESHOLD_VALUE THEN t.SEVERITY
    
    ELSE 'UNKNOWN'
  END AS STATUS,
  CURRENT_TIMESTAMP AS CREATED_TS
FROM PERSON_CONTACT_PROFILE_DQ_TEMP_SUMMARY s
INNER JOIN dev_analytics_db.customer_lifecycle.DQ_THRESHOLDS t
  ON s.DECISION_ID = t.DECISION_ID
  AND s.CHECK_NAME = t.CHECK_NAME
WHERE t.IS_ACTIVE = TRUE
  AND s.CHECK_NAME NOT IN ('TOTAL_VOL', 'DISTINCT_PK');

