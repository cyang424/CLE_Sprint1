CREATE OR REPLACE TABLE dev_analytics_db.customer_lifecycle.FRESHNESS_RULES (
  TARGET_TABLE VARCHAR(255),
  DECISION_ID VARCHAR(50),
  RULE_NAME VARCHAR(100),
  DATE_COLUMN VARCHAR(100),
  EXPECTED_LAG_DAYS NUMBER(10),
  SLA_HOUR_ET NUMBER(2),
  SEVERITY VARCHAR(10),
  IS_ACTIVE BOOLEAN,
  CREATED_TS TIMESTAMP,
  UPDATED_TS TIMESTAMP,
  NOTES VARCHAR(500)
);

-- Insert the data
INSERT INTO dev_analytics_db.customer_lifecycle.FRESHNESS_RULES 
(TARGET_TABLE, DECISION_ID,RULE_NAME, DATE_COLUMN, EXPECTED_LAG_DAYS, SLA_HOUR_ET, SEVERITY, IS_ACTIVE, CREATED_TS, UPDATED_TS, NOTES)
VALUES
('CUSTOMER_BEHAVIOR_DAILY','LCV1-S1-D03','LAG_DAY','BUSINESS_DATE', 1, 6, 'FAIL', TRUE, CURRENT_TIMESTAMP,CURRENT_TIMESTAMP, 'Critical table - must be updated daily by 6 AM ET'),
('CUSTOMER_BEHAVIOR_DAILY', 'LCV1-S1-D03','TABLE_FRESHNESS', 'RECORD_UPDATED_TS', NULL, 6, 'FAIL', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'FAIL level due to downstream impact'),
('PERSON_CONTACT_PROFILE', 'LCV1-S1-D12','RUN_ID_FRESHNESS', 'RUN_ID', NULL, 6, 'FAIL', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'Critical table - must be updated daily by 6 AM ET'),
('PERSON_CONTACT_PROFILE', 'LCV1-S1-D12', 'TABLE_FRESHNESS', 'RECORD_UPDATED_TS', NULL, 6, 'WARN', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'FAIL level due to downstream impact'),
('PERSON_IDENTIFIER_XREF', 'LCV1-S1-D02','RUN_ID_FRESHNESS', 'RUN_ID', NULL, 6, 'FAIL', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'Critical table - must be updated daily by 6 AM ET'),
('PERSON_IDENTIFIER_XREF', 'LCV1-S1-D02','TABLE_FRESHNESS', 'RECORD_UPDATED_TS', NULL, 6, 'WARN', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'WARN -refreshed without any updates'),
('PERSON_SPINE', 'LCV1-S1-D01','RUN_ID_FRESHNESS', 'RUN_ID', NULL, 6, 'FAIL', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'Critical table - must be updated daily by 6 AM ET'),
('PERSON_SPINE', 'LCV1-S1-D01','TABLE_FRESHNESS', 'RECORD_UPDATED_TS', NULL, 6, 'WARN', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'WARN -refreshed without any updates');



--==============================================
-- UPDATE PROC FOR FRESHNESS_RULES
--==============================================
CREATE OR REPLACE PROCEDURE dev_analytics_db.customer_lifecycle.UPDATE_FRESHNESS_RULE(
    p_target_table STRING,
    p_decision_id STRING,
    p_rule_name STRING,
    p_date_column STRING,
    p_expected_lag_days NUMBER,
    p_sla_hour_et NUMBER,
    p_severity STRING,
    p_notes STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Deactivate existing active rule
    UPDATE dev_analytics_db.customer_lifecycle.FRESHNESS_RULES
    SET is_active = FALSE, 
        updated_ts = CURRENT_TIMESTAMP()
    WHERE  decision_id = :p_decision_id
      AND rule_name = :p_rule_name
      AND is_active = TRUE;
    
    -- Insert new active rule
    INSERT INTO dev_analytics_db.customer_lifecycle.FRESHNESS_RULES (
        target_table, decision_id,rule_name, date_column, expected_lag_days,
        sla_hour_et, severity, is_active, created_ts, updated_ts, notes
    )
    VALUES (
        :p_target_table, :p_decision_id,:p_rule_name,:p_date_column, :p_expected_lag_days,
        :p_sla_hour_et, :p_severity, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), :p_notes
    );
    
    RETURN '✅ Updated: ' || :p_target_table || '.' || :p_date_column || ' (' || :p_decision_id || ')';
END;
$$;


--==============================================
-- EXAMPLES
--==============================================

-- Update LAG_DAY threshold for CUSTOMER_BEHAVIOR_DAILY
CALL dev_analytics_db.customer_lifecycle.UPDATE_FRESHNESS_RULE(
    'CUSTOMER_BEHAVIOR_DAILY', 
    'LCV1-S1-D03', 
    'LAG_DAY',
    'BUSINESS_DATE', 
    2,  -- Changed from 1 to 2 days
    6, 
    'WARN',  -- Downgraded from FAIL to WARN
    'Relaxed threshold after review'
);

-- Update TABLE_FRESHNESS rule for PERSON_SPINE
CALL dev_analytics_db.customer_lifecycle.UPDATE_FRESHNESS_RULE(
    'PERSON_SPINE', 
    'LCV1-S1-D01', 
    'RECORD_UPDATED_TS', 
    NULL,  -- No lag days for freshness check
    6, 
    'FAIL', 
    'Must be refreshed daily'
);

-- Tighten SLA for PERSON_CONTACT_PROFILE_DAILY
CALL dev_analytics_db.customer_lifecycle.UPDATE_FRESHNESS_RULE(
    'PERSON_CONTACT_PROFILE_DAILY', 
    'LCV1-S1-D12', 
    'BUSINESS_DATE', 
    1, 
    5,  -- Changed from 6 AM to 5 AM ET
    'FAIL', 
    'Earlier SLA required for downstream jobs'
);



