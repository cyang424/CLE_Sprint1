-- DQ_THRESHOLDS
-- 1. Data Collection Procedures (one per table)
--    ├── SP_DQ_PERSON_SPINE
--    ├── SP_DQ_PERSON_IDENTIFIER_XREF
--    ├── SP_DQ_PERSON_CONTACT_PROFILE_DAILY
--    └── SP_DQ_CUSTOMER_BEHAVIOR_DAILY
   
-- 2. Threshold Configuration Table
--    └── DQ_THRESHOLDS
   
-- 3. Threshold Evaluation Procedure
--    └── SP_DQ_EVALUATE_THRESHOLDS

-- ============================================
-- DQ_THRESHOLDS TABLE DDL
-- ============================================
CREATE OR REPLACE TABLE dev_analytics_db.customer_lifecycle.DQ_THRESHOLDS (
    threshold_id NUMBER AUTOINCREMENT,
    table_name STRING NOT NULL,
    check_name STRING NOT NULL,
    decision_id STRING NOT NULL,  -- Maps to Decision ID (e.g., LCV1-S1-D01)
    severity STRING NOT NULL,     -- FAIL / WARN / INFO
    threshold_type STRING NOT NULL,  -- NULL_COUNT, DUPLICATE_COUNT, LOGICAL_CONSISTENCY, etc.
    threshold_value NUMBER NOT NULL,
    lookback_days NUMBER,  -- For relative checks (e.g., 7-day avg)
    is_active BOOLEAN DEFAULT TRUE,
    created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    notes STRING,  -- Why this threshold was set
    PRIMARY KEY (threshold_id)
);

-- ============================================
-- PERSON_SPINE THRESHOLDS
-- ============================================
INSERT INTO dev_analytics_db.customer_lifecycle.DQ_THRESHOLDS 
(table_name, check_name, decision_id, severity, threshold_type, threshold_value, lookback_days, is_active, notes) 
VALUES
-- FAIL Checks (Threshold = 0)
('PERSON_SPINE', 'REQ_PERSON_ID', 'LCV1-S1-D01', 'FAIL', 'NULL_COUNT', 0, NULL, TRUE, 
 'Required field - primary key'),

('PERSON_SPINE', 'UNQ_PERSON_ID', 'LCV1-S1-D01', 'FAIL', 'DUPLICATE_COUNT', 0, NULL, TRUE, 
 'Primary key uniqueness constraint'),

('PERSON_SPINE', 'REQ_IDENTITY_TYPE_USED', 'LCV1-S1-D01', 'FAIL', 'NULL_COUNT', 0, NULL, TRUE, 
 'Required field - identity classification'),

('PERSON_SPINE', 'REQ_SRC_SYSTEM_CD', 'LCV1-S1-D01', 'FAIL', 'NULL_COUNT', 0, NULL, TRUE, 
 'Required field - source system identifier'),

('PERSON_SPINE', 'REQ_SRC_CUSTOMER_ID', 'LCV1-S1-D01', 'FAIL', 'NULL_COUNT', 0, NULL, TRUE, 
 'Required field - source customer reference'),

('PERSON_SPINE', 'REQ_CREATE_TS', 'LCV1-S1-D01', 'FAIL', 'NULL_COUNT', 0, NULL, TRUE, 
 'Required field - update timestamp'),

('PERSON_SPINE', 'LOG_CREATE_LS_UPDATE_TS', 'LCV1-S1-D01', 'FAIL', 'LOGICAL_CONSISTENCY', 0, NULL, TRUE, 
 'CREATE_TS must be <= UPDATE_TS'),

-- WARN Checks
('PERSON_SPINE', 'LOG_FIRST_LS_LAST_SEEN', 'LCV1-S1-D01', 'WARN', 'LOGICAL_CONSISTENCY', 50, NULL, TRUE, 
 'FIRST_SEEN_DATE must be <= LAST_SEEN_DATE'),

('PERSON_SPINE', 'LOG_DOB', 'LCV1-S1-D01', 'WARN', 'LOGICAL_CONSISTENCY', 200, NULL, TRUE, 
 'age must be >=18'),

('PERSON_SPINE', 'THR_SSN_NULL', 'LCV1-S1-D01', 'WARN', 'ABSOLUTE_COUNT', 200, NULL, TRUE, 
 'Alert if SSN null count exceeds threshold - may indicate upstream issue'),

('PERSON_SPINE', 'THR_DOB_NULL', 'LCV1-S1-D01', 'WARN', 'ABSOLUTE_COUNT', 200, NULL, TRUE, 
 'Alert if DOB null count exceeds threshold - should be rare'),

('PERSON_SPINE', 'THR_IS_ACTIVE_FALSE', 'LCV1-S1-D01', 'WARN', 'ABSOLUTE_COUNT', 500, NULL, TRUE, 
 'Alert if inactive record count exceeds threshold - may indicate data quality issue'),

('PERSON_SPINE', 'THR_RECORD_DROP_PCT', 'LCV1-S1-D01', 'WARN', 'PERCENTAGE', 10, NULL, TRUE, 
 'Alert if record count drops >10% vs prior period - indicates upstream issue'),

('PERSON_SPINE', 'REQ_DECISION_VERSION', 'LCV1-S1-D01', 'FAIL', 'NULL_COUNT', 0, NULL, TRUE, 
 'Required field - decision version identifier');

-- ============================================
-- PERSON_IDENTIFIER_XREF THRESHOLDS
-- ============================================
INSERT INTO dev_analytics_db.customer_lifecycle.DQ_THRESHOLDS 
(table_name, check_name, decision_id, severity, threshold_type, threshold_value, lookback_days, is_active, notes) 
VALUES
-- FAIL Checks
('PERSON_IDENTIFIER_XREF', 'REQ_PK', 'LCV1-S1-D02', 'FAIL', 'NULL_COUNT', 0, NULL, TRUE, 
 'Required field - primary key'),

('PERSON_IDENTIFIER_XREF', 'UNQ_PK', 'LCV1-S1-D02', 'FAIL', 'DUPLICATE_COUNT', 0, NULL, TRUE, 
 'Primary key uniqueness constraint'),

('PERSON_IDENTIFIER_XREF', 'REQ_PERSON_ID_EXISTS', 'LCV1-S1-D02', 'FAIL', 'REFERENTIAL_INTEGRITY', 0, NULL, TRUE, 
 'FK to PERSON_SPINE.PERSON_ID'),

('PERSON_IDENTIFIER_XREF', 'REQ_CREATE_TS', 'LCV1-S1-D02', 'FAIL', 'NULL_COUNT', 0, NULL, TRUE, 
 'Required field - update timestamp'),

('PERSON_IDENTIFIER_XREF', 'LOG_CREATE_LS_UPDATE_TS', 'LCV1-S1-D02', 'FAIL', 'LOGICAL_CONSISTENCY', 0, NULL, TRUE, 
 'RECORD_CREATED_TS must be <= RECORD_UPDATED_TS'),

-- WARN Checks
('PERSON_IDENTIFIER_XREF', 'LOG_EFFECTIVE_START_LS_END_TS', 'LCV1-S1-D02', 'WARN', 'LOGICAL_CONSISTENCY', 200, NULL, TRUE, 
 'EFFECTIVE_START_DATE must be <= EFFECTIVE_END_DATE'),

('PERSON_IDENTIFIER_XREF', 'THR_RECORD_DROP_PCT', 'LCV1-S1-D02', 'WARN', 'PERCENTAGE', 10, NULL, TRUE, 
 'Alert if record count drops >10% vs prior period - indicates upstream issue');


-- ============================================
-- CUSTOMER_BEHAVIOR_DAILY_SNAPSHOT THRESHOLDS
-- ============================================
INSERT INTO dev_analytics_db.customer_lifecycle.DQ_THRESHOLDS 
(table_name, check_name, decision_id, severity, threshold_type, threshold_value, lookback_days, is_active, notes) 
VALUES
-- FAIL Checks
('CUSTOMER_BEHAVIOR_DAILY', 'REQ_PK', 'LCV1-S1-D03', 'FAIL', 'NULL_COUNT', 0, NULL, TRUE, 
 'Required field - primary key'),

('CUSTOMER_BEHAVIOR_DAILY', 'UNQ_PK', 'LCV1-S1-D03', 'FAIL', 'DUPLICATE_COUNT', 0, NULL, TRUE, 
 'Primary key uniqueness constraint'),

('CUSTOMER_BEHAVIOR_DAILY', 'REQ_PERSON_ID_EXISTS', 'LCV1-S1-D03', 'FAIL', 'REFERENTIAL_INTEGRITY', 0, NULL, TRUE, 
 'FK to PERSON_SPINE.PERSON_ID'),

('CUSTOMER_BEHAVIOR_DAILY', 'REQ_SRC_SYSTEM_CD', 'LCV1-S1-D03', 'FAIL', 'NULL_COUNT', 0, NULL, TRUE, 
 'Required field - source system identifier'),

('CUSTOMER_BEHAVIOR_DAILY', 'REQ_IS_ACTIVE_FLAG', 'LCV1-S1-D03', 'FAIL', 'NULL_COUNT', 0, NULL, TRUE, 
 'Required field - status indicator'),

('CUSTOMER_BEHAVIOR_DAILY', 'DEP_INACTIVE_REQUIRES_DAYS', 'LCV1-S1-D03', 'FAIL', 'CROSS_FIELD_CONSISTENCY', 0, NULL, TRUE, 
 'If IS_ACTIVE_FLAG=false, DAYS_SINCE_INACTIVE must not be null'),

('CUSTOMER_BEHAVIOR_DAILY', 'LOG_CREATE_LS_UPDATE_TS', 'LCV1-S1-D03', 'FAIL', 'LOGICAL_CONSISTENCY', 0, NULL, TRUE, 
 'RECORD_CREATED_TS must be <= RECORD_UPDATED_TS'),

-- WARN Checks
('CUSTOMER_BEHAVIOR_DAILY', 'THR_SSN_NULL', 'LCV1-S1-D03', 'WARN', 'ABSOLUTE_COUNT', 200, NULL, TRUE, 
 'Alert if SSN null count exceeds threshold - may indicate upstream issue'),

('CUSTOMER_BEHAVIOR_DAILY', 'THR_RECORD_DROP_PCT', 'LCV1-S1-D03', 'WARN', 'PERCENTAGE', 1, 1, TRUE, 
 'Alert if record count drops >10% vs prior period - indicates upstream issue');

-- ============================================
-- PERSON_CONTACT_PROFILE THRESHOLDS
-- ============================================
INSERT INTO dev_analytics_db.customer_lifecycle.DQ_THRESHOLDS 
(table_name, check_name, decision_id, severity, threshold_type, threshold_value, lookback_days, is_active, notes) 
VALUES
-- Core Requirements (FAIL)
('PERSON_CONTACT_PROFILE', 'REQ_PK', 'LCV1-S1-D12', 'FAIL', 'NULL_COUNT', 0, NULL, TRUE, 'Required field - primary key'),
('PERSON_CONTACT_PROFILE', 'UNQ_PK', 'LCV1-S1-D12', 'FAIL', 'DUPLICATE_COUNT', 0, NULL, TRUE, 'Primary key uniqueness constraint'),
('PERSON_CONTACT_PROFILE', 'REQ_PERSON_ID_EXISTS', 'LCV1-S1-D12', 'FAIL', 'REFERENTIAL_INTEGRITY', 0, NULL, TRUE, 'FK to PERSON_SPINE.PERSON_ID'),
('PERSON_CONTACT_PROFILE', 'REQ_RECORD_CREATED_TS', 'LCV1-S1-D12', 'FAIL', 'NULL_COUNT', 0, NULL, TRUE, 'Create timestamp required'),

-- Validation Rules (FAIL)
('PERSON_CONTACT_PROFILE', 'VAL_EMAIL_FORMAT', 'LCV1-S1-D12', 'WARN', 'PATTERN_MISMATCH', 100, NULL, TRUE, 'Valid email format'),
('PERSON_CONTACT_PROFILE', 'VAL_PHONE_FORMAT', 'LCV1-S1-D12', 'WARN', 'PATTERN_MISMATCH',100, NULL, TRUE, 'Valid phone format'),
('PERSON_CONTACT_PROFILE', 'VAL_STATE_CODE', 'LCV1-S1-D12', 'FAIL', 'PATTERN_MISMATCH', 0, NULL, TRUE, 'Valid state code'),

-- Logical Consistency (FAIL)
('PERSON_CONTACT_PROFILE', 'LOG_RECORD_CREATED_LS_UPDATED', 'LCV1-S1-D12', 'FAIL', 'LOGICAL_CONSISTENCY', 0, NULL, TRUE, 'RECORD_CREATED_TS must be <= RECORD_UPDATED_TS'),

-- Monitoring Thresholds (WARN)
('PERSON_CONTACT_PROFILE', 'THR_EMAIL_COVERAGE_DROP', 'LCV1-S1-D12', 'WARN', 'PERCENTAGE', 5, 1, TRUE, 'Alert if Email coverage >5% drop'),
('PERSON_CONTACT_PROFILE', 'THR_PHONE_COVERAGE_DROP', 'LCV1-S1-D12', 'WARN', 'PERCENTAGE', 5, 1, TRUE, 'Alert if Phone coverage >5% drop'),
('PERSON_CONTACT_PROFILE', 'THR_ADDRESS_COVERAGE_DROP', 'LCV1-S1-D12', 'WARN', 'PERCENTAGE', 5, 1, TRUE, 'Alert if Address coverage >5% drop'),
('PERSON_CONTACT_PROFILE', 'THR_RECORD_DROP_PCT', 'LCV1-S1-D12', 'WARN', 'PERCENTAGE', 5, 1, TRUE, 'Alert if record count >5% drop'),
('PERSON_CONTACT_PROFILE', 'THR_NAME_NULL', 'LCV1-S1-D12', 'WARN', 'ABSOLUTE_COUNT', 100, 1, TRUE, 'Name nulls >100');

-- ============================================
-- VERIFICATION QUERY
-- ============================================
SELECT 
    table_name,
    COUNT(*) AS total_checks,
    SUM(CASE WHEN severity = 'FAIL' THEN 1 ELSE 0 END) AS fail_checks,
    SUM(CASE WHEN severity = 'WARN' THEN 1 ELSE 0 END) AS warn_checks,
    SUM(CASE WHEN is_active = TRUE THEN 1 ELSE 0 END) AS active_checks
FROM dev_analytics_db.customer_lifecycle.DQ_THRESHOLDS
GROUP BY table_name
ORDER BY table_name;

select * from dev_analytics_db.customer_lifecycle.DQ_THRESHOLDS

--==============================================
-- UPDATE PROC
--==============================================
CREATE OR REPLACE PROCEDURE dev_analytics_db.customer_lifecycle.UPDATE_DQ_THRESHOLD(
    p_table_name STRING,
    p_check_name STRING,
    p_decision_id STRING,
    p_severity STRING,
    p_threshold_type STRING,
    p_threshold_value NUMBER,
    p_lookback_days NUMBER,
    p_notes STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Deactivate existing active threshold
    UPDATE dev_analytics_db.customer_lifecycle.DQ_THRESHOLDS
    SET is_active = FALSE, 
        updated_ts = CURRENT_TIMESTAMP()
    WHERE table_name = :p_table_name
      AND check_name = :p_check_name
      AND is_active = TRUE;
    
    -- Insert new active threshold
    INSERT INTO dev_analytics_db.customer_lifecycle.DQ_THRESHOLDS (
        table_name, check_name, decision_id, severity, threshold_type,
        threshold_value, lookback_days, is_active, notes
    )
    VALUES (
        :p_table_name, :p_check_name, :p_decision_id, :p_severity,
        :p_threshold_type, :p_threshold_value, :p_lookback_days, TRUE, :p_notes
    );
    
    RETURN '✅ Updated: ' || :p_table_name || '.' || :p_check_name;
END;
$$;



--==============================================
-- EXAMPLE
--==============================================
-- Update threshold value
CALL dev_analytics_db.customer_lifecycle.UPDATE_DQ_THRESHOLD(
    'PERSON_SPINE', 'THR_SSN_NULL', 'LCV1-S1-D01', 'WARN',
    'ABSOLUTE_COUNT', 150, NULL, 'Tightened after 30 days'
);

-- Change severity
CALL dev_analytics_db.customer_lifecycle.UPDATE_DQ_THRESHOLD(
    'PERSON_IDENTIFIER_XREF', 'LOG_EFFECTIVE_START_LS_END_TS', 'LCV1-S1-D02', 'FAIL',
    'LOGICAL_CONSISTENCY', 0, NULL, 'Elevated to FAIL - critical rule'
);

-- Update percentage threshold
CALL dev_analytics_db.customer_lifecycle.UPDATE_DQ_THRESHOLD(
    'CUSTOMER_BEHAVIOR_DAILY_SNAPSHOT', 'THR_RECORD_DROP_PCT', 'LCV1-S1-D03', 'WARN',
    'PERCENTAGE', 5, 1, 'Reduced from 10% to 5%'
);

select *  from dev_analytics_db.customer_lifecycle.DQ_THRESHOLDS
