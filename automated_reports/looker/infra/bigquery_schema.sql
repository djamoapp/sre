-- Target (final) 
CREATE TABLE IF NOT EXISTS `djamo_data.sre.jsm_tickets` (
  `key` STRING NOT NULL,
  summary STRING,
  description STRING,
  issue_type STRING,
  status STRING,
  priority STRING,
  resolution STRING,
  created TIMESTAMP,
  updated TIMESTAMP,
  resolved TIMESTAMP,
  assignee STRING,
  reporter STRING,
  operational_categorization STRING,
  linked_intercom_conversation_ids STRING,
  team ARRAY<STRING>,
  filiale ARRAY<STRING>,
  start_date DATE,
  ttr_raw_json STRING,
  tffr_raw_json STRING,
  sla_breached BOOL,
  last_sync TIMESTAMP
)
PARTITION BY DATE(updated)
CLUSTER BY `key`;

-- Staging
CREATE TABLE IF NOT EXISTS `djamo_data.sre.jsm_tickets_staging` AS
SELECT * FROM `djamo_data.sre.jsm_tickets` WHERE 1=0;

-- Stored Procedure (MERGE)
CREATE OR REPLACE PROCEDURE `djamo_data.sre.sp_merge_jsm`()
BEGIN
  MERGE `djamo_data.sre.jsm_tickets` T
  USING `djamo_data.sre.jsm_tickets_staging` S
  ON T.`key` = S.`key`
  WHEN MATCHED THEN UPDATE SET
    summary                         = S.summary,
    description                     = S.description,
    issue_type                      = S.issue_type,
    status                          = S.status,
    priority                        = S.priority,
    resolution                      = S.resolution,
    created                         = S.created,
    updated                         = S.updated,
    resolved                        = S.resolved,
    assignee                        = S.assignee,
    reporter                        = S.reporter,
    operational_categorization      = S.operational_categorization,
    linked_intercom_conversation_ids= S.linked_intercom_conversation_ids,
    team                            = S.team,
    filiale                         = S.filiale,
    start_date                      = S.start_date,
    ttr_raw_json                    = S.ttr_raw_json,
    tffr_raw_json                   = S.tffr_raw_json,
    sla_breached                    = S.sla_breached,
    last_sync                       = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN
    INSERT ROW;

  DELETE FROM `djamo_data.sre.jsm_tickets_staging` WHERE TRUE;
END;
