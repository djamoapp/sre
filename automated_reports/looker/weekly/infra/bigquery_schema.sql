-- Note: MERGE logic is handled in the application code (jsm-puller/src/index.ts)
--       to keep a single source of truth and avoid drift between SQL and app logic.

-- Target (final table)
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

-- Staging table (for incremental loads)
CREATE TABLE IF NOT EXISTS `djamo_data.sre.jsm_tickets_staging` AS
SELECT * FROM `djamo_data.sre.jsm_tickets` WHERE 1=0;