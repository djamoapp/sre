# SRE  — Looker Automated Reports

This repo hosts the JSM → BigQuery incremental puller (Cloud Run Job) and an on-demand trigger, plus BigQuery schema/procedure.

## Folders
- `infra/` — BigQuery schema + stored procedure 
- `jsm-puller/` — Node puller to fetch from Jira Cloud and write to BigQuery
- `trigger-refresh/` — HTTPS trigger that starts the Cloud Run Job

## Quick start
1. Create BigQuery tables and stored procedure from `infra/bigquery_schema.sql`
2. Build & deploy `jsm-puller` container to Cloud Run Job
3. (Optional) Deploy `trigger-refresh` Cloud Function and link it in Looker Studio
