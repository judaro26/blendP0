const { parse } = require('csv-parse/sync');
const FormData = require('form-data');
const fetch = require('node-fetch');

const POLL_INTERVAL_MS = 5000;
const MAX_POLL_ATTEMPTS = 6;  // 6 * 5 sec = 30 sec max polling

exports.handler = async function(event) {
  const log = [];
  const startTimestamp = new Date().toISOString();
  log.push(`Function started at ${startTimestamp}`);

  if (event.httpMethod !== 'POST') {
    const errorMessage = 'Method Not Allowed';
    log.push(`ERROR: ${errorMessage}`);
    return {
      statusCode: 405,
      body: JSON.stringify({ error: errorMessage, log }),
    };
  }

  try {
    const body = JSON.parse(event.body);
    const csvData = body.csv_data;
    const enableTestMode = body.enable_test_mode || false;
    const testEmail = body.test_email || 'test@example.com';
    const customSubject = body.custom_subject || 'P0 Alert: [Deployment Name]';
    const customBody = body.custom_body || 'This is a generated ticket for [Deployment Name].';
    const FRESHDESK_RESPONDER_ID = 156006674011;
    
    // Retrieve credentials from the payload instead of environment variables
    const MODE_AUTH_TOKEN = body.mode_auth_token;
    const FRESHDESK_API_KEY = body.freshdesk_api_key;

    if (!MODE_AUTH_TOKEN || !FRESHDESK_API_KEY) {
        const errorMessage = 'Missing API credentials in the request body.';
        log.push(`ERROR: ${errorMessage}`);
        return {
            statusCode: 400,
            body: JSON.stringify({ error: errorMessage, log }),
        };
    }
    
    if (!csvData) {
      const errorMessage = 'No CSV data provided.';
      log.push(`ERROR: ${errorMessage}`);
      return {
        statusCode: 400,
        body: JSON.stringify({ error: errorMessage, log }),
      };
    }

    log.push('Received CSV data and configuration.');

    const MODE_RUN_URL = 'https://app.mode.com/api/blend/reports/77c0a6f31c3c/runs';
    const MODE_CSV_URL = 'https://app.mode.com/api/blend/reports/77c0a6f31c3c/results/content.csv';
    const FRESHDESK_API_URL = 'https://blendsupport.freshdesk.com/api/v2/tickets';
    const FRESHDESK_TRIAGE_GROUP_ID = 156000870331;

    // Parse user CSV data
    const userRecords = parse(csvData, {
      columns: true,
      skip_empty_lines: true,
    });
    log.push(`Successfully parsed user CSV data. Found ${userRecords.length} records.`);

    if (!userRecords.length || (!userRecords[0].tenant && !userRecords[0].Tenant)) {
      const errorMessage = "CSV must include a 'tenant' column.";
      log.push(`ERROR: ${errorMessage}`);
      return {
        statusCode: 400,
        body: JSON.stringify({ error: errorMessage, log }),
      };
    }
    
    // Check if the impact list contains a valid loan ID column
    const loanIdColumn = findImpactListColumn(userRecords[0]);
    const hasImpactList = !!loanIdColumn;
    if (hasImpactList) {
      log.push(`Found impact list column: '${loanIdColumn}'. An attachment will be created.`);
    } else {
      log.push('No valid impact list column found. No attachment will be created.');
    }

    // Group user records by deployment (case insensitive)
    const deployments = {};
    for (const row of userRecords) {
      const deploymentKey = (row.Tenant || row.TENANT || row.Deployment || row.DEPLOYMENT || row.tenant || '').toLowerCase().trim();
      if (deploymentKey) {
        if (!deployments[deploymentKey]) deployments[deploymentKey] = [];
        deployments[deploymentKey].push(row);
      }
    }
    log.push(`Grouped user records into ${Object.keys(deployments).length} deployments.`);

    // 1. Trigger Mode report run
    log.push('Triggering Mode report run...');
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
    log.push(`Mode report run triggered successfully with token: ${runToken}`);

    // 2. Poll Mode report status with limited attempts
    let succeeded = false;
    const pollUrl = `${MODE_RUN_URL}/${runToken}`;

    log.push('Polling Mode report status...');
    for (let attempt = 0; attempt < MAX_POLL_ATTEMPTS; attempt++) {
      const statusResp = await fetch(pollUrl, {
        headers: { Authorization: `Basic ${MODE_AUTH_TOKEN}` },
      });

      if (!statusResp.ok) {
        throw new Error(`Error polling Mode report status: ${await statusResp.text()}`);
      }

      const statusData = await statusResp.json();
      log.push(`- Poll attempt ${attempt + 1}: Status is '${statusData.state}'`);

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
      const warningMessage = 'Mode report is still processing after maximum attempts. Please try again later.';
      log.push(`WARNING: ${warningMessage}`);
      return {
        statusCode: 202,
        body: JSON.stringify({
          message: warningMessage,
          run_token: runToken,
          log,
        }),
      };
    }
    log.push('Mode report run succeeded.');

    // 3. Fetch Mode CSV content
    log.push('Fetching Mode report CSV content...');
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
    log.push(`Successfully parsed Mode report. Found ${modeRecords.length} records.`);

    // 4. Match Mode and Impact data by deployment
    const MODE_DEPLOYMENT_COL = 'DEPLOYMENT';

    if (!modeRecords[0] || !modeRecords[0][MODE_DEPLOYMENT_COL]) {
      const errorMessage = `Mode report is missing the required '${MODE_DEPLOYMENT_COL}' column.`;
      log.push(`ERROR: ${errorMessage}`);
      throw new Error(errorMessage);
    }

    const matchedData = {};
    for (const modeRow of modeRecords) {
      const deploymentKey = (modeRow[MODE_DEPLOYMENT_COL] || '').toLowerCase().trim();
      if (deploymentKey && deployments[deploymentKey]) {
        const impactRows = deployments[deploymentKey];
        const impactCsv = impactRows.map(r => Object.values(r).join(',')).join('\n');

        const emails = new Set();
        if (modeRow.EMAIL && modeRow.EMAIL.includes('@')) {
          emails.add(modeRow.EMAIL.trim());
        }
        if (modeRow.ACCOUNT_MANAGER_EMAIL) {
          const amEmails = modeRow.ACCOUNT_MANAGER_EMAIL.split(/[;,\s]+/).filter(e => e.includes('@'));
          amEmails.forEach(e => emails.add(e.trim()));
        }
        
        if (emails.size > 0) {
          matchedData[deploymentKey] = {
            impact_list: impactCsv,
            contacts: [...emails],
          };
          log.push(`Match found for deployment '${deploymentKey}'. Collected ${emails.size} contact emails.`);
        } else {
          log.push(`Match found for deployment '${deploymentKey}', but no valid email contacts were found. Skipping ticket creation for this deployment.`);
        }
      }
    }
    log.push(`Found matches for ${Object.keys(matchedData).length} deployments.`);

    // 5. Create Freshdesk tickets
    const freshdeskResults = [];

    for (const [depKey, data] of Object.entries(matchedData)) {
      const requesterEmail = enableTestMode ? testEmail : data.contacts[0] || null;
      
      if (!requesterEmail) {
        log.push(`SKIPPED: Ticket for deployment '${depKey}' skipped because no requester email could be determined.`);
        freshdeskResults.push({ deployment: depKey, status: 'Skipped (no email)' });
        continue;
      }
      
      const ccEmails = enableTestMode ? [] : data.contacts.filter((e) => e !== requesterEmail);
      
      const subject = customSubject.replace('[Deployment Name]', depKey);

      // --- NEW LOGIC TO PRESERVE FORMATTING ---
      // Replace newline characters with HTML <br> tags.
      const formattedBody = customBody.replace(/\n/g, '<br>');
      const description = formattedBody.replace('[Deployment Name]', depKey);
      // --- END NEW LOGIC ---

      log.push(`Creating ticket for deployment '${depKey}'. Requester: ${requesterEmail}, CCs: ${ccEmails.join(', ')}.`);

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

      if (hasImpactList) {
        const impactCsv = data.impact_list;
        const impactBuffer = Buffer.from(impactCsv);
        form.append('attachments[]', impactBuffer, {
          filename: `Impact_List_${depKey}.csv`,
          contentType: 'text/csv',
          knownLength: impactBuffer.length,
        });
        log.push(`- Attached impact list for '${depKey}'.`);
      }

      const headers = form.getHeaders();
      headers.Authorization = `Basic ${Buffer.from(FRESHDESK_API_KEY + ':X').toString('base64')}`;

      const fdResp = await fetch(FRESHDESK_API_URL, {
        method: 'POST',
        headers: headers,
        body: form,
        duplex: 'half',
      });

      const fdResult = await fdResp.json();
      if (fdResp.ok) {
        log.push(`SUCCESS: Freshdesk ticket created for '${depKey}' with ID: ${fdResult.id}`);
      } else {
        log.push(`FAILED: Freshdesk ticket for '${depKey}' failed with status ${fdResp.status}. Error: ${JSON.stringify(fdResult)}`);
      }
      
      freshdeskResults.push({
        deployment: depKey,
        status: fdResp.ok ? 'Success' : 'Failed',
        ticket_id: fdResult.id || null,
        error_details: fdResp.ok ? null : fdResult,
      });
    }

    // 6. Return results
    const endTimestamp = new Date().toISOString();
    log.push(`Function finished at ${endTimestamp}`);

    return {
      statusCode: 200,
      body: JSON.stringify({
        message: 'Processing complete. See log for details.',
        freshdesk_results: freshdeskResults,
        matched_data: matchedData,
        log,
      }),
    };
  } catch (err) {
    const errorMessage = err.message || 'An unexpected error occurred.';
    log.push(`CRITICAL ERROR: ${errorMessage}`);
    console.error('Handler error:', err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: errorMessage, log }),
    };
  }
};

function findImpactListColumn(record) {
  const possibleHeaders = ['loanId', 'LOANID', 'GUID', 'guid', 'Guid', 'BlendGuid', 'Blend_Guid', 'BLEND_GUID'];
  const recordHeaders = Object.keys(record);
  
  for (const header of recordHeaders) {
    if (possibleHeaders.includes(header)) {
      return header;
    }
  }
  
  return null;
}
