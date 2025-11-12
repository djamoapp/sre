import fetch, { Response as FetchResponse } from "node-fetch";
import type { Request, Response } from "@google-cloud/functions-framework";

/* ---------------------------- Types & Interfaces -------------------------- */

interface LogData {
  [key: string]: unknown;
}

interface MetadataServiceResponse {
  access_token: string;
  expires_in: number;
  token_type: string;
}

interface CloudRunJobResponse {
  metadata?: {
    name?: string;
  };
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

// Validate ISO-8601 UTC timestamp (e.g., 2025-01-01T12:34:56Z or with fractional seconds)
function isIso8601Z(value: string): boolean {
  return /^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?Z$/.test(value);
}

// Strict identifier validation per request: lowercase letters, digits, and hyphens
const IDENT_RE = /^[a-z0-9-]+$/;

/* ---------------------------- Main Handler -------------------------------- */

export default async (req: Request, res: Response): Promise<void> => {
  try {
    // Validate Authorization header (Bearer token)
    const secret = process.env.SECRET;
    const authHeader = req.headers.authorization || "";
    const token = authHeader.startsWith("Bearer ") ? authHeader.substring(7) : "";

    if (!token || token !== secret) {
      log("WARNING", "Unauthorized request attempt", {
        sourceIP: req.ip,
        hasAuthHeader: !!authHeader
      });
      res.status(401).json({ error: "Unauthorized" });
      return;
    }

    const project = process.env.PROJECT;
    const region = process.env.REGION;
    const job = process.env.JOB;
    const since = (req.query.since as string) || "";

    if (!project || !region || !job) {
      log("ERROR", "Missing required environment variables", {
        hasProject: !!project,
        hasRegion: !!region,
        hasJob: !!job
      });
      res.status(500).json({ error: "Server misconfiguration" });
      return;
    }

    // Validate env var formats strictly
    if (!IDENT_RE.test(project) || !IDENT_RE.test(region) || !IDENT_RE.test(job)) {
      log("ERROR", "Invalid environment variable format", {
        project,
        region,
        job
      });
      res.status(500).json({ error: "Server misconfiguration" });
      return;
    }

    // Validate optional since parameter when provided
    if (since && !isIso8601Z(since)) {
      log("WARNING", "Invalid since parameter format", { since });
      res.status(400).json({ error: "Invalid 'since'. Must be ISO-8601 UTC, e.g., 2025-01-01T00:00:00Z" });
      return;
    }

    log("INFO", "Triggering job", { job, project, region, since: since || "default" });

    // Get access token from metadata service
    const meta: FetchResponse = await fetch(
      "http://metadata/computeMetadata/v1/instance/service-accounts/default/token",
      { headers: { "Metadata-Flavor": "Google" } }
    );

    if (!meta.ok) {
      throw new Error(`Failed to get access token: ${meta.status}`);
    }

    const metaData = await meta.json() as MetadataServiceResponse;
    const { access_token } = metaData;

    // Trigger Cloud Run Job
    const url = `https://${region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${project}/jobs/${job}:run`;

    const body = since ? {
      overrides: {
        containerOverrides: [{
          name: job,
          env: [{ name: "SINCE", value: since }]
        }]
      }
    } : {};

    const r: FetchResponse = await fetch(url, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${access_token}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify(body)
    });

    if (!r.ok) {
      const errorText = await r.text();
      log("ERROR", "Failed to trigger job", {
        status: r.status,
        error: errorText,
        job,
        url
      });
      res.status(500).json({
        error: "Failed to trigger job",
        details: errorText
      });
      return;
    }

    const result = await r.json() as CloudRunJobResponse;
    log("INFO", "Job triggered successfully", { job, executionId: result?.metadata?.name });

    res.json({
      status: "started",
      job,
      executionId: result?.metadata?.name
    });

  } catch (error) {
    const err = error as Error;
    log("ERROR", "Unexpected error in trigger function", {
      error: err.message,
      stack: err.stack
    });
    res.status(500).json({
      error: "Internal server error",
      message: err.message
    });
  }
};
