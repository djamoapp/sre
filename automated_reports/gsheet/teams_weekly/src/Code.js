const SHEETS = {
  SOURCE_DATA: 'SourceData',
  LAST_WEEK_DATA: 'LastWeekData',
  LOGS: 'Logs'
};

const FIELDS = {
  SUBSIDIARY: 'customfield_10083',
  TEAM: 'customfield_10090',
  OPS_CAT: 'customfield_10061',
  TTR_RAW: 'customfield_10055'
};

const TEAM_NAMES = {
  PAYMENTS: 'Payments',
  SAVINGS: 'Savings',
  LENDING: 'Lending_Business'
};

const TEAM_MENTIONS = {
  'Payments': {
    techLead: 'U04861FTC94',        
    pm: 'U04NVHWP9M5',              
    supportLead: 'U0485MS883G',
    sreLead: 'U058E2SSF1V',      
  },
  'Savings': {
    techLead: 'U049223MNKA',
    pm: 'U04EL8X0WGN',
    supportLead: 'U04GV028Y2J',
    sreLead: 'U058E2SSF1V',
  },
  'Lending Business': {
    techLead: 'U08L55C87K7',
    pm: 'U08RQC82ZFY',
    supportLead: 'U09ANBTDE2F',
    sreLead: 'U058E2SSF1V',
  },
  'Guild': {
    sreLead: 'U058E2SSF1V',
  }
};

const SLACK_WEBHOOKS = {
  'team-payments': '',
  'team-savings': '',
  'team-lending-business': '',
  'guild-support-tech': ''
};

const CONFIG = {
  DAYS_TO_FETCH: 7,
  COMPARISON_DAYS: 7,
  TOP_OLD_TICKETS: 5,
  API_DELAY: 120,
  RETRY_ATTEMPTS: 3
};

class Logger {
  constructor(sheetName = 'Logs') {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    this.sheet = ss.getSheetByName(sheetName) || ss.insertSheet(sheetName);
    this.setupSheet_();
  }
  
  setupSheet_() {
    if (this.sheet.getLastRow() === 0) {
      this.sheet.getRange(1, 1, 1, 5).setValues([
        ['Timestamp', 'Level', 'Function', 'Message', 'Details']
      ]).setFontWeight('bold').setBackground('#f0f0f0');
      this.sheet.setFrozenRows(1);
      this.sheet.setColumnWidths(1, 5, 150);
    }
  }
  
  log(level, func, message, details = '') {
    const timestamp = new Date();
    const row = [timestamp, level, func, message, details];
    this.sheet.appendRow(row);
    const lastRow = this.sheet.getLastRow();
    const colors = { 'ERROR': '#ffcccc', 'WARN': '#fff3cd', 'INFO': '#d1ecf1' };
    if (colors[level]) this.sheet.getRange(lastRow, 1, 1, 5).setBackground(colors[level]);
    console.log(`[${level}] ${func}: ${message}`);
    if (lastRow > 1001) this.sheet.deleteRows(2, lastRow - 1001);
  }
  
  info(f, m, d = '') { this.log('INFO', f, m, d); }
  warn(f, m, d = '') { this.log('WARN', f, m, d); }
  error(f, m, d = '') { this.log('ERROR', f, m, d); }
}

const logger = new Logger();

function props() { return PropertiesService.getScriptProperties(); }

function getJiraCreds() {
  const p = props();
  const domain = p.getProperty('JIRA_DOMAIN');
  const email = p.getProperty('JIRA_EMAIL');
  const token = p.getProperty('JIRA_TOKEN');
  if (!domain || !email || !token) throw new Error('Jira credentials not configured');
  return { domain, email, token, auth: 'Basic ' + Utilities.base64Encode(email + ':' + token) };
}

function toYMD(date) {
  const d = new Date(date);
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

function getFieldText(f) {
  if (!f) return '';
  if (typeof f === 'string' || typeof f === 'number') return String(f);
  if (Array.isArray(f)) return f.map(getFieldText).join(', ');
  if (f.value) return f.value;
  if (f.name) return f.name;
  return '';
}

function getCascadingFieldText(f) {
  if (!f) return '';
  const parent = f.value || '';
  const child = (f.child && f.child.value) ? f.child.value : '';
  return (parent && child) ? `${parent} > ${child}` : parent;
}

function extractTtrRawAndStatus(slaObj) {
  if (!slaObj) return { raw: '', status: '' };
  const completed = Array.isArray(slaObj.completedCycles) ? slaObj.completedCycles : [];
  const cycle = slaObj.ongoingCycle || (completed.length ? completed[completed.length - 1] : null);
  if (!cycle) return { raw: '', status: '' };
  const raw = (cycle.remainingTime?.friendly || cycle.elapsedTime?.friendly || '').trim();
  const status = raw ? (raw.startsWith('-') ? 'Breached' : 'Not breached') : '';
  return { raw, status };
}

function jiraJqlGetPage({ domain, auth, jql, fields, nextPageToken }) {
  const base = `https://${domain}/rest/api/3/search/jql`;
  const params = [`jql=${encodeURIComponent(jql)}`, `fields=${encodeURIComponent(fields.join(','))}`];
  if (nextPageToken) params.push(`nextPageToken=${encodeURIComponent(nextPageToken)}`);
  const url = `${base}?${params.join('&')}`;
  
  for (let attempt = 0; attempt < CONFIG.RETRY_ATTEMPTS; attempt++) {
    try {
      const resp = UrlFetchApp.fetch(url, {
        method: 'get',
        headers: { Authorization: auth, Accept: 'application/json' },
        muteHttpExceptions: true
      });
      const code = resp.getResponseCode();
      if (code === 200) {
        const data = JSON.parse(resp.getContentText());
        return { issues: data.issues || [], nextPageToken: data.nextPageToken || null, total: data.total || null };
      }
      if (code >= 500 || code === 429) {
        if (attempt < CONFIG.RETRY_ATTEMPTS - 1) {
          Utilities.sleep(1000 * Math.pow(2, attempt));
          continue;
        }
      }
      throw new Error(`Jira API ${code}`);
    } catch (error) {
      if (attempt >= CONFIG.RETRY_ATTEMPTS - 1) throw error;
      Utilities.sleep(1000 * Math.pow(2, attempt));
    }
  }
}

function fetchWeeklyData(fromDate, toDate, sheetName) {
  logger.info('fetchWeeklyData', `Fetching ${fromDate} to ${toDate}`);
  const creds = getJiraCreds();
  const jql = `created >= "${fromDate}" AND created <= "${toDate} 23:59" ORDER BY created DESC`;
  const fields = ['summary', 'status', 'created', 'resolutiondate', 'priority', 'reporter', 'assignee', 
                  'labels', 'issuetype', 'project', FIELDS.SUBSIDIARY, FIELDS.TEAM, FIELDS.OPS_CAT, FIELDS.TTR_RAW];
  
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName(sheetName) || ss.insertSheet(sheetName);
  sheet.clear();
  
  const header = ['Key', 'Summary', 'Status', 'Created', 'Resolved', 'Priority', 'Reporter', 'Assignee',
                  'Subsidiary', 'Team', 'Operational categorization', 'TTR Raw', 'OLA TTR Status',
                  'Labels', 'IssueType', 'Project', 'TimeToResolve_hours'];
  sheet.getRange(1, 1, 1, header.length).setValues([header]).setFontWeight('bold').setBackground('#f0f0f0');
  
  let allRows = [], token = null, pageCount = 0;
  do {
    const page = jiraJqlGetPage({ domain: creds.domain, auth: creds.auth, jql, fields, nextPageToken: token });
    if (page.issues.length) {
      const rows = page.issues.map(it => {
        const f = it.fields || {};
        const created = f.created ? new Date(f.created) : '';
        const resolved = f.resolutiondate ? new Date(f.resolutiondate) : '';
        const ttr = (created && resolved) ? Math.round(((resolved - created) / 36e5) * 100) / 100 : '';
        const { raw: ttrRaw, status: ola } = extractTtrRawAndStatus(f[FIELDS.TTR_RAW]);
        return [
          it.key || '', f.summary || '', f.status?.name || '', created, resolved, f.priority?.name || '',
          f.reporter?.displayName || '', f.assignee?.displayName || '', getFieldText(f[FIELDS.SUBSIDIARY]),
          getFieldText(f[FIELDS.TEAM]), getCascadingFieldText(f[FIELDS.OPS_CAT]), ttrRaw, ola,
          (f.labels || []).join(', '), f.issuetype?.name || '', f.project?.key || '', ttr
        ];
      });
      allRows = allRows.concat(rows);
    }
    token = page.nextPageToken;
    pageCount++;
    Utilities.sleep(CONFIG.API_DELAY);
  } while (token && pageCount < 50);
  
  if (allRows.length) sheet.getRange(2, 1, allRows.length, header.length).setValues(allRows);
  logger.info('fetchWeeklyData', `Fetched ${allRows.length} issues`);
  return allRows.length;
}

function analyzeTeamData(sheetName, teamName) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(sheetName);
  if (!sheet || sheet.getLastRow() <= 1) return { error: 'No data', totalIssues: 0 };
  
  const data = sheet.getDataRange().getValues();
  const headers = data[0];
  const teamRows = data.slice(1).filter(row => row[headers.indexOf('Team')] === teamName);
  if (teamRows.length === 0) return { error: `No data for ${teamName}`, totalIssues: 0 };
  
  const analysis = { 
    totalIssues: teamRows.length, resolved: 0, unresolved: 0, byCountry: {}, 
    oldestUnresolved: [], topIssueCategories: {}, slaBreached: 0, slaNotBreached: 0 
  };
  
  const cols = { 
    resolved: headers.indexOf('Resolved'), subsidiary: headers.indexOf('Subsidiary'),
    created: headers.indexOf('Created'), key: headers.indexOf('Key'), 
    priority: headers.indexOf('Priority'), opsCat: headers.indexOf('Operational categorization'),
    ola: headers.indexOf('OLA TTR Status'), assignee: headers.indexOf('Assignee') 
  };
  
  teamRows.forEach(row => {
    const resolved = row[cols.resolved];
    const country = row[cols.subsidiary] || 'Unknown';
    const ola = row[cols.ola] || '';
    const opsCat = row[cols.opsCat] || 'Unknown';
    
    if (resolved && resolved !== '') {
      analysis.resolved++;
    } else {
      analysis.unresolved++;
      const created = row[cols.created];
      if (created) {
        const age = (Date.now() - new Date(created).getTime()) / (1000 * 60 * 60);
        analysis.oldestUnresolved.push({
          key: row[cols.key], age, ageDays: Math.round(age / 24), 
          priority: row[cols.priority], assignee: row[cols.assignee] || 'Unassigned'
        });
      }
    }
    
    analysis.byCountry[country] = (analysis.byCountry[country] || 0) + 1;
    analysis.topIssueCategories[opsCat] = (analysis.topIssueCategories[opsCat] || 0) + 1;
    
    if (ola.toLowerCase().includes('not')) {
      analysis.slaNotBreached++;
    } else if (ola.toLowerCase().includes('breach')) {
      analysis.slaBreached++;
    }
  });
  
  analysis.oldestUnresolved.sort((a, b) => b.age - a.age).splice(CONFIG.TOP_OLD_TICKETS);
  analysis.topCategories = Object.entries(analysis.topIssueCategories).sort((a, b) => b[1] - a[1]).slice(0, 5);
  analysis.resolvedPercent = Math.round((analysis.resolved / analysis.totalIssues) * 100);
  analysis.unresolvedPercent = Math.round((analysis.unresolved / analysis.totalIssues) * 100);
  analysis.slaBreachedPercent = analysis.totalIssues > 0 ? Math.round((analysis.slaBreached / analysis.totalIssues) * 100) : 0;
  
  return analysis;
}

function analyzeGuildData(sheetName) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(sheetName);
  if (!sheet || sheet.getLastRow() <= 1) return { error: 'No data', totalIssues: 0 };
  
  const data = sheet.getDataRange().getValues();
  const headers = data[0];
  const rows = data.slice(1);
  
  const cols = { 
    team: headers.indexOf('Team'), resolved: headers.indexOf('Resolved'), ttr: headers.indexOf('TimeToResolve_hours'),
    ola: headers.indexOf('OLA TTR Status'), created: headers.indexOf('Created'), key: headers.indexOf('Key'),
    priority: headers.indexOf('Priority'), opsCat: headers.indexOf('Operational categorization'),
    assignee: headers.indexOf('Assignee') 
  };
  
  const analysis = { 
    totalIssues: rows.length, resolved: 0, unresolved: 0, slaBreached: 0,
    byTeam: {}, teamResolutionTimes: {}, topIssueCategories: {}, 
    criticalOldTickets: [], oldestUnresolved: [], avgResolutionTime: 0, resolutionTimes: [] 
  };
  
  rows.forEach(row => {
    const resolved = row[cols.resolved];
    const team = row[cols.team] || 'Unknown';
    const ttr = parseFloat(row[cols.ttr]) || 0;
    const ola = row[cols.ola] || '';
    const opsCat = row[cols.opsCat] || 'Unknown';
    const created = row[cols.created];
    const key = row[cols.key];
    const priority = row[cols.priority];
    const assignee = row[cols.assignee] || 'Unassigned';
    
    if (resolved && resolved !== '') {
      analysis.resolved++;
      if (ttr > 0) {
        analysis.resolutionTimes.push(ttr);
        if (!analysis.teamResolutionTimes[team]) analysis.teamResolutionTimes[team] = [];
        analysis.teamResolutionTimes[team].push(ttr);
      }
    } else {
      analysis.unresolved++;
      if (created) {
        const age = (Date.now() - new Date(created).getTime()) / (1000 * 60 * 60);
        analysis.oldestUnresolved.push({ key, age, ageDays: Math.round(age / 24), priority, team, assignee });
      }
      if (created && (priority === 'High' || priority === 'Highest')) {
        const age = (Date.now() - new Date(created).getTime()) / (1000 * 60 * 60);
        if (age > 120) analysis.criticalOldTickets.push({ key, age, ageDays: Math.round(age / 24), priority, team });
      }
    }
    
    if (ola.toLowerCase().includes('not')) {
      analysis.slaNotBreached++;
    } else if (ola.toLowerCase().includes('breach')) {
      analysis.slaBreached++;
    }
    
    if (!analysis.byTeam[team]) analysis.byTeam[team] = { total: 0, resolved: 0 };
    analysis.byTeam[team].total++;
    if (resolved && resolved !== '') analysis.byTeam[team].resolved++;
    
    analysis.topIssueCategories[opsCat] = (analysis.topIssueCategories[opsCat] || 0) + 1;
  });
  
  if (analysis.resolutionTimes.length > 0) {
    analysis.avgResolutionTime = analysis.resolutionTimes.reduce((a, b) => a + b, 0) / analysis.resolutionTimes.length;
  }
  
  analysis.teamLeaderboard = Object.entries(analysis.byTeam).map(([team, data]) => ({
    team, total: data.total, resolved: data.resolved,
    resolutionRate: Math.round((data.resolved / data.total) * 100),
    avgTime: analysis.teamResolutionTimes[team] 
      ? analysis.teamResolutionTimes[team].reduce((a,b) => a+b, 0) / analysis.teamResolutionTimes[team].length : 0
  })).sort((a, b) => b.resolutionRate - a.resolutionRate);
  
  analysis.topCategories = Object.entries(analysis.topIssueCategories).sort((a, b) => b[1] - a[1]).slice(0, 5);
  analysis.criticalOldTickets.sort((a, b) => b.age - a.age).splice(8);
  analysis.oldestUnresolved.sort((a, b) => b.age - a.age).splice(5);
  analysis.resolvedPercent = Math.round((analysis.resolved / analysis.totalIssues) * 100);
  analysis.slaBreachedPercent = Math.round((analysis.slaBreached / analysis.totalIssues) * 100);
  
  return analysis;
}

function createTeamReportMessage(teamName, thisWeek, lastWeek) {
  const now = new Date();
  const weekStart = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
  const dateStr = `${weekStart.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })} - ${now.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })}`;
  
  const totalChange = thisWeek.totalIssues - lastWeek.totalIssues;
  
  let countryText = '';
  Object.entries(thisWeek.byCountry).sort((a, b) => b[1] - a[1]).forEach(([c, n]) => {
    countryText += `• ${c}: ${n}\n`;
  });
  
  let oldestText = '';
  thisWeek.oldestUnresolved.forEach((t, i) => {
    oldestText += `${i+1}. ${t.key} - ${t.ageDays} days (${t.priority})\n`;
  });
  
  let categoriesText = '';
  if (thisWeek.topCategories && thisWeek.topCategories.length > 0) {
    thisWeek.topCategories.forEach(([cat, count], i) => {
      categoriesText += `${i+1}. ${cat}: ${count}\n`;
    });
  }
  
  const slaEmoji = thisWeek.slaBreachedPercent > 10 ? '🔴' : thisWeek.slaBreachedPercent > 5 ? '🟡' : '🟢';
  
  // 👥 Récupérer les mentions pour cette équipe
  const mentions = TEAM_MENTIONS[teamName];
  let mentionText = '';
  if (mentions) {
    mentionText = `<@${mentions.techLead}> <@${mentions.pm}> <@${mentions.supportLead}> <@${mentions.sreLead}>`;
  }
  
  const blocks = [];
  blocks.push({ type: 'header', text: { type: 'plain_text', text: `📊 Team ${teamName} - SRE Weekly Report`, emoji: true } });
  
  // 👥 Ajouter les mentions dans le contexte
  if (mentionText) {
    blocks.push({ 
      type: 'context', 
      elements: [{ 
        type: 'mrkdwn', 
        text: `Week of ${dateStr} • ${mentionText}` 
      }] 
    });
  } else {
    blocks.push({ type: 'context', elements: [{ type: 'mrkdwn', text: `Week of ${dateStr}` }] });
  }
  
  blocks.push({ type: 'divider' });
  blocks.push({ type: 'section', text: { type: 'mrkdwn', text: `*📈 This Week's Performance*\n\nTotal Issues: *${thisWeek.totalIssues}* ${totalChange !== 0 ? `(${totalChange > 0 ? '+' : ''}${totalChange} from last week)` : ''}\nResolved: *${thisWeek.resolved}* (${thisWeek.resolvedPercent}%)\nUnresolved: *${thisWeek.unresolved}* (${thisWeek.unresolvedPercent}%)\nSLA Breached: *${thisWeek.slaBreached}* (${thisWeek.slaBreachedPercent}%) ${slaEmoji}` } });
  blocks.push({ type: 'section', text: { type: 'mrkdwn', text: `*🌍 Total Issues by Country*\n\n${countryText || 'No data'}` } });
  
  if (oldestText) {
    blocks.push({ type: 'section', text: { type: 'mrkdwn', text: `*⚠️ Top ${CONFIG.TOP_OLD_TICKETS} Oldest Open Tickets*\n\n${oldestText}` } });
  }
  
  if (categoriesText) {
    blocks.push({ type: 'section', text: { type: 'mrkdwn', text: `*📊 Top 5 Issue Categories*\n\n${categoriesText}` } });
  }
  
  blocks.push({ type: 'section', text: { type: 'mrkdwn', text: `*🎯 Week-over-Week Comparison*\n\nIssues: ${thisWeek.totalIssues} vs ${lastWeek.totalIssues}\nResolution Rate: ${thisWeek.resolvedPercent}% vs ${lastWeek.resolvedPercent}%` } });
  blocks.push({ type: 'context', elements: [{ type: 'mrkdwn', text: `Generated on ${now.toLocaleString()}` }] });
  
  return { blocks };
}

function createGuildReportMessage(thisWeek, lastWeek) {
  const now = new Date();
  const weekStart = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
  const dateStr = `${weekStart.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })} - ${now.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })}`;
  
  let leaderboardText = '';
  let fastestText = '';
  let categoriesText = '';
  let criticalText = '';
  let oldestText = '';
  
  if (thisWeek.teamLeaderboard && thisWeek.teamLeaderboard.length) {
    thisWeek.teamLeaderboard.slice(0, 3).forEach((t, i) => {
      const medal = ['🥇', '🥈', '🥉'][i];
      leaderboardText += `${medal} ${t.team}: ${t.resolutionRate}% (${t.resolved}/${t.total})\n`;
    });
    
    const fastestTeams = thisWeek.teamLeaderboard.filter(t => t.avgTime > 0).sort((a, b) => a.avgTime - b.avgTime).slice(0, 3);
    fastestTeams.forEach((t, i) => {
      fastestText += `${i+1}. ${t.team}: ${t.avgTime.toFixed(1)} hours\n`;
    });
  }
  
  if (thisWeek.topCategories && thisWeek.topCategories.length) {
    thisWeek.topCategories.forEach(([cat, count], i) => {
      categoriesText += `${i+1}. ${cat}: ${count}\n`;
    });
  }
  
  if (thisWeek.criticalOldTickets && thisWeek.criticalOldTickets.length) {
    thisWeek.criticalOldTickets.slice(0, 5).forEach(t => {
      criticalText += `• ${t.key} - ${t.ageDays} days (${t.team}, ${t.priority})\n`;
    });
  }
  
  if (thisWeek.oldestUnresolved && thisWeek.oldestUnresolved.length) {
    thisWeek.oldestUnresolved.forEach((t, i) => {
      oldestText += `${i+1}. ${t.key} - ${t.ageDays} days - ${t.assignee}\n`;
    });
  }
  
  const slaEmoji = thisWeek.slaBreachedPercent > 10 ? '🔴' : thisWeek.slaBreachedPercent > 5 ? '🟡' : '🟢';
  const totalChange = thisWeek.totalIssues - lastWeek.totalIssues;
  const resolvedChange = thisWeek.resolved - lastWeek.resolved;
  const slaChange = thisWeek.slaBreached - lastWeek.slaBreached;
  
  // 👥 Récupérer les mentions pour la Guild
  const mentions = TEAM_MENTIONS['Guild'];
  let mentionText = '';
  if (mentions) {
    mentionText = `<@${mentions.techLead}> <@${mentions.pm}> <@${mentions.supportLead}> <@${mentions.sreLead}>`;
  }
  
  const blocks = [];
  blocks.push({ type: 'header', text: { type: 'plain_text', text: '📊 SRE L3 Support - Weekly Overview', emoji: true } });
  
  // 👥 Ajouter les mentions dans le contexte
  if (mentionText) {
    blocks.push({ 
      type: 'context', 
      elements: [{ 
        type: 'mrkdwn', 
        text: `Week of ${dateStr} • ${mentionText}` 
      }] 
    });
  } else {
    blocks.push({ type: 'context', elements: [{ type: 'mrkdwn', text: `Week of ${dateStr}` }] });
  }
  
  blocks.push({ type: 'divider' });
  blocks.push({ type: 'section', fields: [
    { type: 'mrkdwn', text: `*Total Issues:*\n${thisWeek.totalIssues} (all teams)` },
    { type: 'mrkdwn', text: `*Resolved:*\n${thisWeek.resolved} (${thisWeek.resolvedPercent}%)` },
    { type: 'mrkdwn', text: `*SLA Breached:*\n${thisWeek.slaBreached} (${thisWeek.slaBreachedPercent}%) ${slaEmoji}` },
    { type: 'mrkdwn', text: `*Avg Resolution:*\n${thisWeek.avgResolutionTime.toFixed(1)} hours` }
  ]});
  blocks.push({ type: 'divider' });
  blocks.push({ type: 'section', text: { type: 'mrkdwn', text: `*🏆 Team Leaderboard*\n\n${leaderboardText || 'No data'}` } });
  blocks.push({ type: 'section', text: { type: 'mrkdwn', text: `*⚡ Fastest Teams*\n\n${fastestText || 'No data'}` } });
  blocks.push({ type: 'section', text: { type: 'mrkdwn', text: `*📊 Top 5 Issue Categories*\n\n${categoriesText || 'No data'}` } });
  
  if (oldestText) {
    blocks.push({ type: 'section', text: { type: 'mrkdwn', text: `*⏰ Top 5 Oldest Open Tickets*\n\n${oldestText}` } });
  }
  
  if (criticalText) {
    blocks.push({ type: 'section', text: { type: 'mrkdwn', text: `*🚨 Critical Issues*\n\n${criticalText}` } });
  }
  
  blocks.push({ type: 'section', text: { type: 'mrkdwn', text: `*🎯 Week-over-Week Comparison*\n\nTotal Issues: ${thisWeek.totalIssues} vs ${lastWeek.totalIssues} (${totalChange >= 0 ? '+' : ''}${totalChange})\nResolved: ${thisWeek.resolved} vs ${lastWeek.resolved} (${resolvedChange >= 0 ? '+' : ''}${resolvedChange})\nSLA Breached: ${thisWeek.slaBreached} vs ${lastWeek.slaBreached} (${slaChange >= 0 ? '+' : ''}${slaChange})\nResolution Rate: ${thisWeek.resolvedPercent}% vs ${lastWeek.resolvedPercent}%` } });
  blocks.push({ type: 'context', elements: [{ type: 'mrkdwn', text: `Generated on ${now.toLocaleString()}` }] });
  
  return { blocks };
}

function sendToSlack(webhookUrl, message) {
  if (!webhookUrl || webhookUrl === '') throw new Error('Webhook URL not configured');
  const resp = UrlFetchApp.fetch(webhookUrl, {
    method: 'post', contentType: 'application/json', 
    payload: JSON.stringify(message), muteHttpExceptions: true
  });
  if (resp.getResponseCode() !== 200) throw new Error(`Slack API ${resp.getResponseCode()}`);
  return true;
}

function generateAllWeeklyReports() {
  try {
    logger.info('generateAllWeeklyReports', 'Starting');
    loadWebhooksFromProperties();
    
    const now = new Date();
    const thisWeekEnd = toYMD(now);
    const thisWeekStart = toYMD(new Date(now.getTime() - CONFIG.DAYS_TO_FETCH * 24 * 60 * 60 * 1000));
    const lastWeekEnd = toYMD(new Date(now.getTime() - CONFIG.DAYS_TO_FETCH * 24 * 60 * 60 * 1000));
    const lastWeekStart = toYMD(new Date(now.getTime() - (CONFIG.DAYS_TO_FETCH + CONFIG.COMPARISON_DAYS) * 24 * 60 * 60 * 1000));
    
    const thisWeekCount = fetchWeeklyData(thisWeekStart, thisWeekEnd, SHEETS.SOURCE_DATA);
    const lastWeekCount = fetchWeeklyData(lastWeekStart, lastWeekEnd, SHEETS.LAST_WEEK_DATA);
    
    if (thisWeekCount === 0) throw new Error('No data fetched');
    
    const reports = [];
    
    for (const [key, name] of Object.entries({ PAYMENTS: 'Payments', SAVINGS: 'Savings', LENDING: 'Lending Business' })) {
      const thisWeek = analyzeTeamData(SHEETS.SOURCE_DATA, TEAM_NAMES[key]);
      const lastWeek = analyzeTeamData(SHEETS.LAST_WEEK_DATA, TEAM_NAMES[key]);
      if (!thisWeek.error) {
        reports.push({ 
          channel: key === 'PAYMENTS' ? 'team-payments' : key === 'SAVINGS' ? 'team-savings' : 'team-lending-business',
          message: createTeamReportMessage(name, thisWeek, lastWeek), 
          team: name 
        });
      }
    }
    
    const guildThis = analyzeGuildData(SHEETS.SOURCE_DATA);
    const guildLast = analyzeGuildData(SHEETS.LAST_WEEK_DATA);
    if (!guildThis.error) {
      reports.push({ channel: 'guild-support-tech', message: createGuildReportMessage(guildThis, guildLast), team: 'Guild' });
    }
    
    let successCount = 0;
    reports.forEach(r => {
      try {
        const webhook = SLACK_WEBHOOKS[r.channel];
        if (!webhook) throw new Error(`Webhook not configured for ${r.channel}`);
        sendToSlack(webhook, r.message);
        successCount++;
        Utilities.sleep(1000);
      } catch (error) {
        logger.error('generateAllWeeklyReports', `Failed ${r.channel}: ${error.message}`);
      }
    });
    
    SpreadsheetApp.getUi().alert(`✅ Complete!\n\nData: ${thisWeekCount} issues\nReports sent: ${successCount}/${reports.length}`);
    
  } catch (error) {
    logger.error('generateAllWeeklyReports', error.message, error.stack);
    SpreadsheetApp.getUi().alert(`❌ Failed: ${error.message}`);
  }
}

function setupWeeklyReports() {
  const ui = SpreadsheetApp.getUi();
  ui.alert('Setup', 'Configure Jira credentials and Slack webhooks.\n\nReady?', ui.ButtonSet.OK_CANCEL);
  
  const domain = ui.prompt('Jira Domain', 'e.g., company.atlassian.net:', ui.ButtonSet.OK_CANCEL);
  if (domain.getSelectedButton() !== ui.Button.OK) return;
  const email = ui.prompt('Jira Email', 'Your email:', ui.ButtonSet.OK_CANCEL);
  if (email.getSelectedButton() !== ui.Button.OK) return;
  const token = ui.prompt('Jira API Token', 'Your API token:', ui.ButtonSet.OK_CANCEL);
  if (token.getSelectedButton() !== ui.Button.OK) return;
  
  const p = props();
  p.setProperty('JIRA_DOMAIN', domain.getResponseText().trim());
  p.setProperty('JIRA_EMAIL', email.getResponseText().trim());
  p.setProperty('JIRA_TOKEN', token.getResponseText().trim());
  
  ui.alert('✅ Jira credentials saved!');
  
  const webhooks = [
    { key: 'TEAM_PAYMENTS', name: 'Team Payments' },
    { key: 'TEAM_SAVINGS', name: 'Team Savings' },
    { key: 'TEAM_LENDING_BUSINESS', name: 'Team Lending Business' },
    { key: 'GUILD_SUPPORT_TECH', name: 'Guild Support Tech' }
  ];
  
  webhooks.forEach(wh => {
    const response = ui.prompt(`${wh.name} Webhook`, `Webhook URL for ${wh.name}:`, ui.ButtonSet.OK_CANCEL);
    if (response.getSelectedButton() === ui.Button.OK) {
      const url = response.getResponseText().trim();
      if (url) p.setProperty(`SLACK_WEBHOOK_${wh.key}`, url);
    }
  });
  
  ui.alert('✅ Setup Complete!', 'Use "Generate All Weekly Reports" to test.', ui.ButtonSet.OK);
  logger.info('setupWeeklyReports', 'Setup completed');
}

function loadWebhooksFromProperties() {
  const p = props();
  SLACK_WEBHOOKS['team-payments'] = p.getProperty('SLACK_WEBHOOK_TEAM_PAYMENTS') || '';
  SLACK_WEBHOOKS['team-savings'] = p.getProperty('SLACK_WEBHOOK_TEAM_SAVINGS') || '';
  SLACK_WEBHOOKS['team-lending-business'] = p.getProperty('SLACK_WEBHOOK_TEAM_LENDING_BUSINESS') || '';
  SLACK_WEBHOOKS['guild-support-tech'] = p.getProperty('SLACK_WEBHOOK_GUILD_SUPPORT_TECH') || '';
}

function scheduleWeeklyReports() {
  try {
    const triggers = ScriptApp.getProjectTriggers();
    triggers.forEach(t => {
      if (t.getHandlerFunction() === 'generateAllWeeklyReports') ScriptApp.deleteTrigger(t);
    });
    
    ScriptApp.newTrigger('generateAllWeeklyReports')
      .timeBased()
      .onWeekDay(ScriptApp.WeekDay.FRIDAY)
      .atHour(12)
      .create();
    
    logger.info('scheduleWeeklyReports', 'Scheduled for Friday 12 PM');
    SpreadsheetApp.getUi().alert('✅ Scheduled for every Friday at 12:00 PM');
  } catch (error) {
    logger.error('scheduleWeeklyReports', error.message);
    SpreadsheetApp.getUi().alert(`❌ Failed: ${error.message}`);
  }
}

function removeSchedule() {
  try {
    const triggers = ScriptApp.getProjectTriggers();
    let count = 0;
    triggers.forEach(t => {
      if (t.getHandlerFunction() === 'generateAllWeeklyReports') {
        ScriptApp.deleteTrigger(t);
        count++;
      }
    });
    logger.info('removeSchedule', `Removed ${count} trigger(s)`);
    SpreadsheetApp.getUi().alert(`✅ Removed ${count} schedule(s)`);
  } catch (error) {
    logger.error('removeSchedule', error.message);
    SpreadsheetApp.getUi().alert(`❌ Failed: ${error.message}`);
  }
}

function viewLogs() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const logsSheet = ss.getSheetByName(SHEETS.LOGS);
  if (logsSheet) ss.setActiveSheet(logsSheet);
  else SpreadsheetApp.getUi().alert('No logs found.');
}

function clearLogs() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const logsSheet = ss.getSheetByName(SHEETS.LOGS);
  if (logsSheet) {
    logsSheet.clear();
    new Logger().setupSheet_();
  }
  SpreadsheetApp.getUi().alert('✅ Logs cleared');
}

function initialize() {
  const ui = SpreadsheetApp.getUi();
  const response = ui.alert('Initialize', 'Create all sheets?\n\nContinue?', ui.ButtonSet.YES_NO);
  if (response !== ui.Button.YES) return;
  
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  Object.values(SHEETS).forEach(name => {
    if (!ss.getSheetByName(name)) {
      ss.insertSheet(name);
      logger.info('initialize', `Created: ${name}`);
    }
  });
  
  ui.alert('✅ Done!', 'Sheets created.\n\nNext: Close and reopen to see menu.', ui.ButtonSet.OK);
}

function onOpen() {
  loadWebhooksFromProperties();
  
  SpreadsheetApp.getUi().createMenu('📊 Weekly Reports')
    .addItem('🚀 Generate All Weekly Reports', 'generateAllWeeklyReports')
    .addSeparator()
    .addItem('⚙️ Setup Weekly Reports', 'setupWeeklyReports')
    .addItem('⏰ Schedule Weekly (Monday 9 AM)', 'scheduleWeeklyReports')
    .addItem('🗑️ Remove Schedule', 'removeSchedule')
    .addSeparator()
    .addItem('📋 View Logs', 'viewLogs')
    .addItem('🗑️ Clear Logs', 'clearLogs')
    .addToUi();
}