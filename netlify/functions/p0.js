const { parse } = require('csv-parse/sync');
const FormData = require('form-data');

const POLL_INTERVAL_MS = 5000;
const MAX_POLL_ATTEMPTS = 6;  // 6 * 5 sec = 30 sec max polling

exports.handler = async function(event) {
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
    console.log('User CSV records parsed. Found deployments to process:', Object.keys(userRecords.reduce((acc, row) => {
        const key = (row.Tenant || row.TENANT || row.Deployment || row.DEPLOYMENT || row.tenant || '').toLowerCase().trim();
        if (key) acc[key] = true;
        return acc;
    }, {})));

    // 1. Trigger Mode report run
    console.log('Step 1: Triggering Mode report run...');
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
    console.log('Mode report run triggered. Run token:', runToken);

    // 2. Poll Mode report status with limited attempts
    let succeeded = false;
    const pollUrl = `${MODE_RUN_URL}/${runToken}`;

    console.log('Step 2: Polling Mode report status...');
    for (let attempt = 0; attempt < MAX_POLL_ATTEMPTS; attempt++) {
      const statusResp = await fetch(pollUrl, {
        headers: { Authorization: `Basic ${MODE_AUTH_TOKEN}` },
      });

      if (!statusResp.ok) {
        throw new Error(`Error polling Mode report status: ${await statusResp.text()}`);
      }

      const statusData = await statusResp.json();
      console.log(`- Attempt ${attempt + 1}: Status is '${statusData.state}'...`);

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
      console.log('Mode report polling timed out.');
      return {
        statusCode: 202,
        body: JSON.stringify({
          message: 'Mode report is still processing. Please try again later with the same run token.',
          run_token: runToken,
        }),
      };
    }
    console.log('Mode report succeeded.');

    // 3. Fetch Mode CSV content
    console.log('Step 3: Fetching Mode report content...');
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
    console.log(`Mode report fetched. Found ${modeRecords.length} records.`);
    if (modeRecords.length > 0) {
        console.log('Mode report column headers:', Object.keys(modeRecords[0]));
    }


    // 4. Group user records by deployment (case insensitive)
    console.log('Step 4: Grouping user records...');
    const deployments = {};
    for (const row of userRecords) {
      // Added 'tenant' to the list of acceptable deployment keys
      const deploymentKey = (row.Tenant || row.TENANT || row.Deployment || row.DEPLOYMENT || row.tenant || '').toLowerCase().trim();
      if (deploymentKey) {
          if (!deployments[deploymentKey]) deployments[deploymentKey] = [];
          deployments[deploymentKey].push(row);
      }
    }
    console.log('User records grouped into these deployments:', Object.keys(deployments));


    // 5. Match and create Freshdesk tickets
    console.log('Step 5: Matching and creating Freshdesk tickets...');
    const results = [];
    const modeDeploymentCol = modeRecords.length > 0 ? Object.keys(modeRecords[0]).find(key => key.toLowerCase() === 'deployment') : null;
    
    if (!modeDeploymentCol) {
        console.error("Mode report is missing a 'deployment' column (case-insensitive). Skipping ticket creation.");
        results.push({ deployment: 'all', status: 'Failed', error: 'Mode report missing deployment column' });
    } else {
      console.log(`Mode report deployment column found: '${modeDeploymentCol}'`);
      for (const [depKey, rows] of Object.entries(deployments)) {
          console.log(`- Processing deployment: '${depKey}'...`);
          
          // Use the dynamically found column name for matching
          const matched = modeRecords.filter((m) => (m[modeDeploymentCol] || '').toLowerCase().trim() === depKey);

          console.log(`-- Found ${matched.length} matching records in Mode report.`);

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
          
          console.log(`-- Requester email: ${requesterEmail}`);
          console.log(`-- CC emails: ${ccEmails.join(', ')}`);

          if (!requesterEmail) {
            console.warn(`-- Skipping ticket for '${depKey}': No valid requester email found.`);
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

          console.log('-- Creating Freshdesk ticket...');
          // Get headers with boundary from form-data and add Authorization header
          const headers = form.getHeaders();
          headers.Authorization = `Basic ${Buffer.from(FRESHDESK_API_KEY + ':X').toString('base64')}`;

          const fdResp = await fetch(FRESHDESK_API_URL, {
            method: 'POST',
            body: form,
            headers: headers,
            // The following option is necessary to handle stream data with Node.js fetch
            duplex: 'half'
          });

          const fdResult = await fdResp.json();
          if (fdResp.ok) {
              console.log(`-- Successfully created ticket with ID: ${fdResult.id}`);
          } else {
              console.error(`-- Failed to create ticket. Status: ${fdResp.status}, Response: ${JSON.stringify(fdResult)}`);
          }

          results.push({
            deployment: depKey,
            status: fdResp.ok ? 'Success' : 'Failed',
            ticket_id: fdResult.id || null,
          });
      }
    }


    // 6. Return results
    console.log('Step 6: Returning final results:', results);
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
