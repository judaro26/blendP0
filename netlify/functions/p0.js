const { parse } = require('csv-parse/sync');
const FormData = require('form-data');
const fetch = require('node-fetch');

const POLL_INTERVAL_MS = 5000;
const MAX_POLL_ATTEMPTS = 2;  // Reduced from 6 to stay within Netlify's 10-second timeout

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
    const FRESHDESK_RESPONDER_ID = 156008293335;
    
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
    const FRESHDESK_CONVERSATIONS_URL = 'https://blendsupport.freshdesk.com/api/v2/tickets';
    // const FRESHDESK_TRIAGE_GROUP_ID = 156000870331; // This variable is no longer needed

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
      log.push('Mode report did not succeed within max attempts. Attempting to proceed anyway.');
      // NO return statement here. The function will continue to the next steps.
    } else {
      log.push('Mode report run succeeded.');
    }

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
        const impactCsv = [Object.keys(impactRows[0]).join(',')].concat(
            impactRows.map(r => Object.values(r).join(','))
        ).join('\n');

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
          log.push(`Match found for deployment '${depKey}', but no valid email contacts were found. Skipping ticket creation for this deployment.`);
        }
      }
    }
    log.push(`Found matches for ${Object.keys(matchedData).length} deployments.`);

    // 5. Create Freshdesk tickets and send a public reply
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

      // Replace newline characters with HTML <br> tags.
      const formattedBody = customBody.replace(/\n/g, '<br>');
      const description = formattedBody.replace('[Deployment Name]', depKey);

      // --- 5.1 Create the Freshdesk ticket with the full body and attachment ---
      log.push(`Creating ticket for deployment '${depKey}' with full body and attachment. Requester: ${requesterEmail}, CCs: ${ccEmails.join(', ')}.`);

      const ticketForm = new FormData();
      ticketForm.append('subject', subject);
      // Use the full, formatted description here for the initial ticket creation.
      ticketForm.append('description', `<div>${description}</div>`, { contentType: 'text/html' });
      ticketForm.append('email', requesterEmail);
      ticketForm.append('status', '2'); // Status 2 for Open
      ticketForm.append('priority', '1');
      ticketForm.append('responder_id', FRESHDESK_RESPONDER_ID.toString());
      ticketForm.append('tags[]', 'Support-emergency');
      ticketForm.append('tags[]', 'org_nochange');
      ticketForm.append('custom_fields[cf_blend_product]', 'Mortgage');
      ticketForm.append('custom_fields[cf_type_of_case]', 'Issue');
      ticketForm.append('custom_fields[cf_disposition477339]', 'P0 Comms');
      ticketForm.append('custom_fields[cf_blend_platform]', 'Lending Platform');
      ticketForm.append('custom_fields[cf_survey_automation]', 'No');

      ccEmails.forEach((cc) => ticketForm.append('cc_emails[]', cc));

      if (hasImpactList) {
        const impactCsv = data.impact_list;
        const impactBuffer = Buffer.from(impactCsv);
        ticketForm.append('attachments[]', impactBuffer, {
          filename: `Impact_List_${depKey}.csv`,
          contentType: 'text/csv',
          knownLength: impactBuffer.length,
        });
        log.push(`- Attached impact list for '${depKey}' during ticket creation.`);
      }

      const ticketHeaders = ticketForm.getHeaders();
      ticketHeaders.Authorization = `Basic ${Buffer.from(FRESHDESK_API_KEY + ':X').toString('base64')}`;

      const ticketResp = await fetch(FRESHDESK_API_URL, {
        method: 'POST',
        headers: ticketHeaders,
        body: ticketForm,
        duplex: 'half',
      });

      const ticketResult = await ticketResp.json();
      if (!ticketResp.ok) {
        log.push(`FAILED: Freshdesk ticket for '${depKey}' failed with status ${ticketResp.status}. Error: ${JSON.stringify(ticketResult)}`);
        freshdeskResults.push({
            deployment: depKey,
            status: 'Failed',
            ticket_id: null,
        });
        continue;
      }
      log.push(`SUCCESS: Freshdesk ticket created for '${depKey}' with ID: ${ticketResult.id}`);

      // --- 5.2 We no longer need to send a reply with the body and attachment,
      // as they are already included in the initial ticket creation.
      // This is the intended behavior to ensure all data is in the first email.
      
      freshdeskResults.push({
        deployment: depKey,
        status: 'Success',
        ticket_id: ticketResult.id,
        error_details: null,
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
