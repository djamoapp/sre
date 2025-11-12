import fetch from "node-fetch";
import { BigQuery } from "@google-cloud/bigquery";
import { SecretManagerServiceClient } from "@google-cloud/secret-manager";

const bq = new BigQuery();
const secrets = new SecretManagerServiceClient();

/* ---------------------------- Types & Interfaces -------------------------- */

interface LogData {
  [key: string]: unknown;
}

interface JiraUser {
  displayName?: string;
}

interface JiraCascadingSelect {
  value?: string;
  child?: {
    value?: string;
  };
}

interface JiraMultiSelectOption {
  value?: string;
}

interface JiraIssueFields {
  summary?: string;
  description?: string;
  issuetype?: { name?: string };
  status?: { name?: string };
  priority?: { name?: string };
  resolution?: { name?: string };
  created?: string;
  updated?: string;
  resolutiondate?: string;
  assignee?: JiraUser;
  reporter?: JiraUser;
  customfield_10061?: JiraCascadingSelect; // Operational categorization
  customfield_10065?: string; // Linked Intercom conversation IDs
  customfield_10090?: JiraMultiSelectOption[]; // Team
  customfield_10083?: JiraMultiSelectOption[]; // Filiale
  customfield_10015?: string; // Start date
  customfield_10055?: unknown; // TTR SLA object
  customfield_10056?: unknown; // TFFR SLA object
}

interface JiraIssue {
  key?: string;
  fields?: JiraIssueFields;
}

interface JiraSearchResponse {
  issues?: JiraIssue[];
  startAt?: number;
  maxResults?: number;
  total?: number;
}

interface BigQueryRow {
  key: string | null;
  summary: string | null;
  description: string | null;
  issue_type: string | null;
  status: string | null;
  priority: string | null;
  resolution: string | null;
  created: string | null;
  updated: string | null;
  resolved: string | null;
  assignee: string | null;
  reporter: string | null;
  operational_categorization: string | null;
  linked_intercom_conversation_ids: string | null;
  team: string[] | null;
  filiale: string[] | null;
  start_date: string | null;
  ttr_raw_json: string | null;
  tffr_raw_json: string | null;
  sla_breached: null;
  last_sync: null;
}

interface FetchIssuesParams {
  base: string;
  user: string;
  token: string;
  sinceIso: string;
}

interface InsertToStagingParams {
  dataset: string;
  stagingTable: string;
  rows: BigQueryRow[];
}

interface RunMergeParams {
  project: string;
  dataset: string;
  location: string;
}

/* -------------------------- Utility Functions ---------------------------- */

// Structured logging for Cloud Logging
function log(severity: string, message: string, data: LogData = {}): void {
  console.log(JSON.stringify({
    severity,
    message,
    timestamp: new Date().toISOString(),
    ...data
  }));
}

function env(name: string, fallback?: string): string | undefined {
  return process.env[name] ?? fallback;
}

// Validate ISO-8601 UTC timestamp (e.g., 2025-01-01T12:34:56Z or with fractional seconds)
function isIso8601Z(value: string): boolean {
  return /^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?Z$/.test(value);
}

// Strict identifier validation to prevent SQL identifier injection
// Per user request, allow only lowercase letters, digits, and hyphens
const IDENT_RE = /^[a-z0-9-]+$/;

async function getSecret(name: string): Promise<string> {
  const [v] = await secrets.accessSecretVersion({
    name: `projects/${process.env.GCP_PROJECT}/secrets/${name}/versions/latest`,
  });
  return v.payload?.data?.toString() || "";
}

/* ------------------------- Field helpers & mappers ------------------------- */

const asDisplay = (user?: JiraUser): string | null =>
  user ? user.displayName ?? null : null;

function cascadingToString(cs?: JiraCascadingSelect): string | null {
  if (!cs || typeof cs !== "object") return null;
  const parent = cs.value ?? null;
  const child = cs.child?.value ?? null;
  if (parent && child) return `${parent} > ${child}`;
  if (parent) return parent;
  return null;
}

function multiSelectToArray(ms?: JiraMultiSelectOption[]): string[] | null {
  if (!Array.isArray(ms)) return null;
  const vals = ms.map((o) => o?.value).filter(Boolean) as string[];
  return vals.length ? vals : null;
}

function toBQDate(dateStr?: string): string | null {
  if (!dateStr) return null;
  const d = new Date(dateStr);
  if (Number.isNaN(d.getTime())) {
    if (/^\d{4}-\d{2}-\d{2}$/.test(String(dateStr))) return dateStr;
    return null;
  }
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function toRawJson(value: unknown): string | null {
  if (value === undefined || value === null) return null;
  try {
    return JSON.stringify(value);
  } catch {
    return null;
  }
}

/* ------------------------------ Core puller ------------------------------- */

async function fetchIssuesSince({ base, user, token, sinceIso }: FetchIssuesParams): Promise<BigQueryRow[]> {
  const fields = [
    "summary",
    "description",
    "issuetype",
    "status",
    "priority",
    "resolution",
    "created",
    "updated",
    "resolutiondate",
    "assignee",
    "reporter",
    "customfield_10061", // Operational categorization (cascading)
    "customfield_10065", // Linked Intercom conversation ids (string)
    "customfield_10090", // Team (multiselect)
    "customfield_10083", // Filiale (multiselect)
    "customfield_10015", // Start date (date)
    "customfield_10055", // TTR (SLA object)
    "customfield_10056", // TFFR (SLA object)
  ].join(",");

  const auth = "Basic " + Buffer.from(`${user}:${token}`).toString("base64");
  const jql = `updated >= "${sinceIso}" order by updated asc`;

  let startAt = 0;
  const rows: BigQueryRow[] = [];

  while (true) {
    const url =
      `${base}/rest/api/3/search` +
      `?jql=${encodeURIComponent(jql)}` +
      `&fields=${encodeURIComponent(fields)}` +
      `&maxResults=200&startAt=${startAt}`;

    const res = await fetch(url, {
      headers: { Authorization: auth, Accept: "application/json" },
    });

    if (!res.ok) {
      const errorText = await res.text();
      log("ERROR", "Jira API request failed", {
        status: res.status,
        error: errorText,
        jql,
        startAt
      });
      throw new Error(`Jira ${res.status}: ${errorText}`);
    }

    const data = await res.json() as JiraSearchResponse;

    for (const issue of data.issues ?? []) {
      const f = issue.fields ?? {};

      const row: BigQueryRow = {
        key: issue.key ?? null,

        summary: f.summary ?? null,
        description: f.description ?? null,
        issue_type: f.issuetype?.name ?? null,
        status: f.status?.name ?? null,
        priority: f.priority?.name ?? null,
        resolution: f.resolution?.name ?? null,
        created: f.created ?? null,
        updated: f.updated ?? null,
        resolved: f.resolutiondate ?? null,
        assignee: asDisplay(f.assignee),
        reporter: asDisplay(f.reporter),
        operational_categorization: cascadingToString(f.customfield_10061),
        linked_intercom_conversation_ids: f.customfield_10065 ?? null,
        team: multiSelectToArray(f.customfield_10090),
        filiale: multiSelectToArray(f.customfield_10083),
        start_date: toBQDate(f.customfield_10015),
        ttr_raw_json: toRawJson(f.customfield_10055),
        tffr_raw_json: toRawJson(f.customfield_10056),
        sla_breached: null,
        last_sync: null,
      };

      if (row.key) rows.push(row);
    }

    const { startAt: s = 0, maxResults: m = 0, total = 0 } = data;
    if (s + m >= total) break;
    startAt += m;
  }

  return rows;
}

async function insertToStaging({ dataset, stagingTable, rows }: InsertToStagingParams): Promise<number> {
  if (!rows.length) return 0;
  await bq.dataset(dataset).table(stagingTable).insert(rows);
  return rows.length;
}

async function runMerge({ project, dataset, location }: RunMergeParams): Promise<void> {
  const [job] = await bq.createQueryJob({
    query: `
      BEGIN
        MERGE \`${project}.${dataset}.jsm_tickets\` T
        USING \`${project}.${dataset}.jsm_tickets_staging\` S
        ON T.\`key\` = S.\`key\`
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

        DELETE FROM \`${project}.${dataset}.jsm_tickets_staging\` WHERE TRUE;
      END
    `,
    location,
  });
  await job.getQueryResults();
}

async function main(): Promise<void> {
  const project = env("GCP_PROJECT");
  const dataset = env("BQ_DATASET", "sre") as string;
  const stagingTable = env("BQ_STAGING_TABLE", "jsm_tickets_staging") as string;
  const location = env("BQ_LOCATION", "EU") as string;

  if (!project) {
    throw new Error("GCP_PROJECT environment variable is required");
  }
  if (!dataset) {
    throw new Error("BQ_DATASET environment variable is required");
  }

  // Validate identifiers strictly
  if (!IDENT_RE.test(project)) {
    throw new Error("Invalid GCP_PROJECT format. Expected ^[a-z0-9-]+$");
  }
  if (!IDENT_RE.test(dataset)) {
    throw new Error("Invalid BQ_DATASET format. Expected ^[a-z0-9-]+$");
  }

  const sinceEnv = env("SINCE", "");
  let sinceIso: string;
  if (sinceEnv) {
    if (!isIso8601Z(sinceEnv)) {
      throw new Error("Invalid SINCE value. Must be ISO-8601 UTC (e.g., 2025-01-01T00:00:00Z)");
    }
    sinceIso = sinceEnv;
  } else {
    sinceIso = new Date(Date.now() - 60 * 60 * 1000).toISOString();
  }

  log("INFO", "Starting JSM puller", {
    project,
    dataset,
    stagingTable,
    location,
    since: sinceIso
  });

  const base = await getSecret("jsm_base");
  const user = await getSecret("jsm_user");
  const token = await getSecret("jsm_token");

  log("INFO", "Fetching issues from Jira", { since: sinceIso });
  const rows = await fetchIssuesSince({ base, user, token, sinceIso });
  log("INFO", "Fetched issues from Jira", { count: rows.length });

  const inserted = await insertToStaging({ dataset, stagingTable, rows });
  log("INFO", "Inserted rows to staging table", {
    count: inserted,
    table: `${dataset}.${stagingTable}`
  });

  if (inserted > 0) {
    log("INFO", "Starting merge to target table");
    await runMerge({ project, dataset, location });
    log("INFO", "Merge completed successfully");
  } else {
    log("INFO", "No data to merge, skipping merge operation");
  }
}

main().catch((e: Error) => {
  log("ERROR", "Fatal error in JSM puller", {
    error: e.message,
    stack: e.stack
  });
  process.exit(1);
});
