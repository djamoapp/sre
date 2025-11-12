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

/* -------------------------- Security Validators --------------------------- */

/**
 * Validates ISO 8601 datetime string to prevent injection attacks
 * Accepts formats: YYYY-MM-DDTHH:mm:ss.sssZ or YYYY-MM-DDTHH:mm:ssZ
 */
function validateISO8601(dateString: string): boolean {
  // Strict regex for ISO 8601 UTC datetime
  const iso8601Regex = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?Z$/;

  if (!iso8601Regex.test(dateString)) {
    return false;
  }

  // Ensure it's a valid date by parsing
  const date = new Date(dateString);
  return !isNaN(date.getTime());
}

/**
 * Validates GCP identifiers (project, region, job names)
 * Allows: lowercase letters, numbers, hyphens, underscores
 */
function validateGCPIdentifier(identifier: string): boolean {
  const gcpIdentifierRegex = /^[a-z0-9_-]+$/;
  return gcpIdentifierRegex.test(identifier);
}

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

    // Validate required environment variables exist
    if (!project || !region || !job) {
      log("ERROR", "Missing required environment variables", {
        hasProject: !!project,
        hasRegion: !!region,
        hasJob: !!job
      });
      res.status(500).json({ error: "Server misconfiguration" });
      return;
    }

    // Validate environment variable formats to prevent injection
    if (!validateGCPIdentifier(project)) {
      log("ERROR", "Invalid PROJECT format", { project });
      res.status(500).json({ error: "Server misconfiguration: invalid PROJECT" });
      return;
    }

    if (!validateGCPIdentifier(region)) {
      log("ERROR", "Invalid REGION format", { region });
      res.status(500).json({ error: "Server misconfiguration: invalid REGION" });
      return;
    }

    if (!validateGCPIdentifier(job)) {
      log("ERROR", "Invalid JOB format", { job });
      res.status(500).json({ error: "Server misconfiguration: invalid JOB" });
      return;
    }

    // Validate 'since' parameter if provided
    if (since && !validateISO8601(since)) {
      log("WARNING", "Invalid 'since' parameter format", { since, sourceIP: req.ip });
      res.status(400).json({
        error: "Invalid 'since' parameter",
        message: "Must be ISO 8601 UTC datetime (e.g., 2024-01-01T00:00:00Z)"
      });
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