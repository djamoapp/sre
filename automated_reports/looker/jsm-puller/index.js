import fetch from "node-fetch";
import { BigQuery } from "@google-cloud/bigquery";
import { SecretManagerServiceClient } from "@google-cloud/secret-manager";

const bq = new BigQuery();
const secrets = new SecretManagerServiceClient();

function env(name, fallback) {
  return process.env[name] ?? fallback;
}

async function getSecret(name) {
  const [v] = await secrets.accessSecretVersion({
    name: `projects/${process.env.GCP_PROJECT}/secrets/${name}/versions/latest`,
  });
  return v.payload.data.toString();
}

/* ------------------------- Field helpers & mappers ------------------------- */

const asDisplay = (user) => (user ? user.displayName ?? null : null);

function cascadingToString(cs) {
  if (!cs || typeof cs !== "object") return null;
  const parent = cs.value ?? null;
  const child = cs.child?.value ?? null;
  if (parent && child) return `${parent} > ${child}`;
  if (parent) return parent;
  return null;
}

function multiSelectToArray(ms) {
  if (!Array.isArray(ms)) return null;
  const vals = ms.map((o) => o?.value).filter(Boolean);
  return vals.length ? vals : null;
}


function toBQDate(dateStr) {
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

function toRawJson(value) {
  if (value === undefined || value === null) return null;
  try {
    return JSON.stringify(value);
  } catch {
    return null;
  }
}

/* ------------------------------ Core puller ------------------------------- */

async function fetchIssuesSince({ base, user, token, sinceIso }) {
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
  const rows = [];

  while (true) {
    const url =
      `${base}/rest/api/3/search` +
      `?jql=${encodeURIComponent(jql)}` +
      `&fields=${encodeURIComponent(fields)}` +
      `&maxResults=200&startAt=${startAt}`;

    const res = await fetch(url, {
      headers: { Authorization: auth, Accept: "application/json" },
    });
    if (!res.ok) throw new Error(`Jira ${res.status}: ${await res.text()}`);
    const data = await res.json();

    for (const issue of data.issues ?? []) {
      const f = issue.fields ?? {};

      
      const row = {
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

async function insertToStaging({ dataset, stagingTable, rows }) {
  if (!rows.length) return 0;
  await bq.dataset(dataset).table(stagingTable).insert(rows);
  return rows.length;
}

async function runMerge({ project, dataset, location }) {
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

async function main() {
  const project = env("GCP_PROJECT");
  const dataset = env("BQ_DATASET", "support");
  const stagingTable = env("BQ_STAGING_TABLE", "jsm_tickets_staging");
  const location = env("BQ_LOCATION", "EU");

  const sinceEnv = env("SINCE", "");
  const sinceIso = sinceEnv || new Date(Date.now() - 60 * 60 * 1000).toISOString();

  const base = await getSecret("jsm_base");
  const user = await getSecret("jsm_user");
  const token = await getSecret("jsm_token");

  console.log(`Pulling issues updated since ${sinceIso}...`);
  const rows = await fetchIssuesSince({ base, user, token, sinceIso });
  console.log(`Fetched ${rows.length} issues.`);

  const inserted = await insertToStaging({ dataset, stagingTable, rows });
  console.log(`Inserted ${inserted} rows to staging.`);

  if (inserted > 0) {
    console.log("Merging to target...");
    await runMerge({ project, dataset, location });
    console.log("Merge complete.");
  } else {
    console.log("Nothing to merge.");
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
