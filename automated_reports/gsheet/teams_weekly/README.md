# GSheet Jira Weekly Reports

Google Apps Script bound to a Google Sheet to:
- Fetch Jira issues for the last N days
- Store them in sheets (`SourceData`, `LastWeekData`, `Logs`)
- Analyse per team + guild
- Send weekly SRE reports to Slack via webhooks

## Dev workflow

From this folder:

```bash
# pull latest Apps Script code into this repo
clasp pull

# push local changes back to Apps Script
clasp push

Test in GSheet (📊 Weekly Reports etc.).

When you are happy, then :
git add .
git commit -m "Update weekly Jira reports logic"
git push

