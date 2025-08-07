const { parse } = require('csv-parse/lib/sync.js');
const FormData = require('form-data');

const POLL_INTERVAL_MS = 5000;
const MAX_POLL_ATTEMPTS = 6;  // 6 * 5 sec = 30 sec max polling

export async function handler(event) {
  if (event.httpMethod !== 'POST') {
    return {
      statusCode: 405,
      body: JSON.stringify({ error: 'Method Not Allowed' }),
    };
  }

  try {
    const body = JSON.parse(event.body);
    const csvData = body.csv_data;
    const enableTestMode = body.enable_test_mode || false;
    const testEmail = body.test_email || 'test@example.com';
    const customSubject = body.custom_subject || 'P0 Alert: [Deployment Name]';
    const customBody = body.custom_body || 'This is a generated ticket for [Deployment Name].';

    if (!csvData) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: 'No CSV data provided.' }),
      };
    }

    const MODE_AUTH_TOKEN = process.env.MODE_AUTH_TOKEN;
    const FRESHDESK_API_KEY = process.env.FRESHDESK_API_KEY;

    const MODE_RUN_URL = 'https://app.mode.com/api/blend/reports/77c0a6f31c3c/runs';
    const MODE_CSV_URL = 'https://app.mode.com/api/blend/reports/77c0a6f31c3c/results/content.csv';
    const FRESHDESK_API_URL = 'https://blendsupport.freshdesk.com/api/v2/tickets';

    const FRESHDESK_TRIAGE_GROUP_ID = 156000870331;
    const FRESHDESK_RESPONDER_ID = 156006674011;

    // Parse user CSV data
    const userRecords = parse(csvData, {
      columns: true,
      skip_empty_lines: true,
    });

    // 1. Trigger Mode report run
    const runResp = await fetch(MODE_RUN_URL, {
      method: 'POST',
      headers: {
        Authorization: `Basic ${MODE_AUTH_TOKEN}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({}),
    });

    if (!runResp.ok) {
      const errorText = await runResp.text();
      throw new Error(`Failed to trigger Mode report run: ${errorText}`);
    }

    const runData = await runResp.json();
    const runToken = runData.token;

    // 2. Poll Mode report status with limited attempts
    let succeeded = false;
    const pollUrl = `${MODE_RUN_URL}/${runToken}`;

    for (let attempt = 0; attempt < MAX_POLL_ATTEMPTS; attempt++) {
      const statusResp = await fetch(pollUrl, {
        headers: { Authorization: `Basic ${MODE_AUTH_TOKEN}` },
      });

      if (!statusResp.ok) {
        throw new Error(`Error polling Mode report status: ${await statusResp.text()}`);
      }

      const statusData = await statusResp.json();

      if (statusData.state === 'succeeded') {
        succeeded = true;
        break;
      }

      if (['failed', 'cancelled'].includes(statusData.state)) {
        throw new Error(`Mode report run failed: ${statusData.state}`);
      }

      await new Promise((res) => setTimeout(res, POLL_INTERVAL_MS));
    }

    if (!succeeded) {
      // Poll timed out, return partial status so client can retry poll later
      return {
        statusCode: 202,
        body: JSON.stringify({
          message: 'Mode report is still processing. Please try again later with the same run token.',
          run_token: runToken,
        }),
      };
    }

    // 3. Fetch Mode CSV content
    const modeCsvResp = await fetch(MODE_CSV_URL, {
      headers: {
        Authorization: `Basic ${MODE_AUTH_TOKEN}`,
        Accept: 'text/csv',
      },
    });

    if (!modeCsvResp.ok) {
      throw new Error(`Failed to fetch Mode report CSV: ${await modeCsvResp.text()}`);
    }

    const modeCsvText = await modeCsvResp.text();

    const modeRecords = parse(modeCsvText, {
      columns: true,
      skip_empty_lines: true,
    });

    // 4. Group user records by deployment (case insensitive)
    const deployments = {};
    for (const row of userRecords) {
      const deploymentKey = (row.Tenant || row.TENANT || row.Deployment || row.DEPLOYMENT || '').toLowerCase().trim();
      if (!deployments[deploymentKey]) deployments[deploymentKey] = [];
      deployments[deploymentKey].push(row);
    }

    // 5. Match and create Freshdesk tickets
    const results = [];

    for (const [depKey, rows] of Object.entries(deployments)) {
      // Find matching Mode deployment records (case-insensitive)
      const matched = modeRecords.filter((m) => (m.Deployment || '').toLowerCase().trim() === depKey);

      // Collect emails (contacts + AMs)
      const emails = new Set();

      for (const match of matched) {
        if (match.email && match.email.includes('@')) emails.add(match.email.trim());
        if (match.account_manager_email) {
          const ams = match.account_manager_email.split(/[;,\s]+/).filter((e) => e.includes('@'));
          ams.forEach((e) => emails.add(e.trim()));
        }
      }

      const requesterEmail = enableTestMode ? testEmail : [...emails][0] || null;
      const ccEmails = [...emails].filter((e) => e !== requesterEmail);

      if (!requesterEmail) {
        results.push({ deployment: depKey, status: 'Skipped (no email)' });
        continue;
      }

      const subject = customSubject.replace('[Deployment Name]', depKey);
      const description = customBody.replace('[Deployment Name]', depKey);

      const impactCsv = rows.map((r) => Object.values(r).join(',')).join('\n');

      const form = new FormData();
      form.append('subject', subject);
      form.append('description', `${description}<br><br>--- Auto Generated ---`, { contentType: 'text/html' });
      form.append('email', requesterEmail);
      form.append('status', '5');
      form.append('priority', '1');
      form.append('group_id', FRESHDESK_TRIAGE_GROUP_ID.toString());
      form.append('responder_id', FRESHDESK_RESPONDER_ID.toString());
      form.append('tags[]', 'Support-emergency');
      form.append('custom_fields[cf_blend_product]', 'Mortgage');
      form.append('custom_fields[cf_type_of_case]', 'Issue');
      form.append('custom_fields[cf_disposition477339]', 'P0 Comms');
      form.append('custom_fields[cf_blend_platform]', 'Lending Platform');
      form.append('custom_fields[cf_survey_automation]', 'No');

      ccEmails.forEach((cc) => form.append('cc_emails[]', cc));

      form.append('attachments[]', Buffer.from(impactCsv), {
        filename: `Impact_List_${depKey}.csv`,
        contentType: 'text/csv',
      });

      const fdResp = await fetch(FRESHDESK_API_URL, {
        method: 'POST',
        body: form,
        headers: {
          Authorization: `Basic ${Buffer.from(FRESHDESK_API_KEY + ':X').toString('base64')}`,
        },
      });

      const fdResult = await fdResp.json();

      results.push({
        deployment: depKey,
        status: fdResp.ok ? 'Success' : 'Failed',
        ticket_id: fdResult.id || null,
      });
    }

    // 6. Return results
    return {
      statusCode: 200,
      body: JSON.stringify({ freshdesk_results: results }),
    };
  } catch (err) {
    console.error('Handler error:', err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: err.message || 'Internal Server Error' }),
    };
  }
}
