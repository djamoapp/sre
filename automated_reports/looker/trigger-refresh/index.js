import fetch from "node-fetch";

export default async (req, res) => {
  const secret = process.env.SECRET;
  if ((req.query.token || "") !== secret) return res.status(401).send("unauthorized");

  const project = process.env.PROJECT;
  const region  = process.env.REGION;
  const job     = process.env.JOB;
  const since   = req.query.since || "";

  const meta = await fetch("http://metadata/computeMetadata/v1/instance/service-accounts/default/token",
    { headers: { "Metadata-Flavor": "Google" }});
  const { access_token } = await meta.json();

  const url = `https://${region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${project}/jobs/${job}:run`;

  const body = since ? {
    overrides: { containerOverrides: [{ name: job, env: [{ name: "SINCE", value: since }] }] }
  } : {};

  const r = await fetch(url, {
    method: "POST",
    headers: { "Authorization": `Bearer ${access_token}`, "Content-Type": "application/json" },
    body: JSON.stringify(body)
  });

  if (!r.ok) return res.status(500).send(await r.text());
  res.send("job started");
};
