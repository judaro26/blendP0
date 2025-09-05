const { parse } = require('csv-parse/sync');
const FormData = require('form-data');
const fetch = require('node-fetch');

const POLL_INTERVAL_MS = 5000;
const MAX_POLL_ATTEMPTS = 6;  // Background functions have a 15-minute timeout

exports.handler = async function(event) {
  const log = [];
  const startTimestamp = new Date().toISOString();
  log.push(`Background function started at ${startTimestamp}`);

  try {
    // The payload is passed from the main p0.js function
    const body = JSON.parse(event.body);
    const runToken = body.runToken;
    const csvData = body.csvData;
    const enableTestMode = body.enableTestMode;
    const testEmail = body.testEmail;
    const customSubject = body.customSubject;
    const customBody = body.customBody;
    const MODE_AUTH_TOKEN = body.MODE_AUTH_TOKEN;
    const FRESHDESK_API_KEY = body.FRESHDESK_API_KEY;
    const FRESHDESK_RESPONDER_ID = body.FRESHDESK_RESPONDER_ID;
    const deployments = body.deployments;
    const hasImpactList = body.hasImpactList;
    if (body.log) {
      log.push(...body.log); // Continue the log from the main function
    }

    const MODE_RUN_URL = `https://app.mode.com/api/blend/reports/77c0a6f31c3c/runs`;
    const MODE_CSV_URL = `https://app.mode.com/api/blend/reports/77c0a6f31c3c/results/content.csv`;
    const FRESHDESK_API_URL = `https://blendsupport.freshdesk.com/api/v2/tickets`;

    // 2. Poll Mode report status with a longer timeout
    let succeeded = false;
    const pollUrl = `${MODE_RUN_URL}/${runToken}`;
    log.push('Polling Mode report status in background...');
    for (let attempt = 0; attempt < MAX_POLL_ATTEMPTS; attempt++) {
      const statusResp = await fetch(pollUrl, {
        headers: { Authorization: `Basic ${MODE_AUTH_TOKEN}` },
      });
      const statusData = await statusResp.json();

      if (!statusResp.ok) {
        log.push(`Error polling Mode report status on attempt ${attempt + 1}: ${JSON.stringify(statusData)}`);
        break;
      }
      log.push(`- Poll attempt ${attempt + 1}: Status is '${statusData.state}'`);
      if (statusData.state === 'succeeded') {
        succeeded = true;
        break;
      }
      if (['failed', 'cancelled'].includes(statusData.state)) {
        log.push(`Mode report run failed or was cancelled. State: ${statusData.state}. Exiting.`);
        return { statusCode: 200, body: JSON.stringify({ message: 'Mode report failed or was cancelled. See logs for details.' }) };
      }
      await new Promise((res) => setTimeout(res, POLL_INTERVAL_MS));
    }

    if (!succeeded) {
      log.push('Mode report did not succeed within max attempts. Proceeding to fetch anyway (may fail).');
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
      log.push(`Failed to fetch Mode report CSV: ${await modeCsvResp.text()}`);
      return { statusCode: 200, body: JSON.stringify({ message: 'Failed to fetch Mode CSV. See logs for details.' }) };
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
      return { statusCode: 200, body: JSON.stringify({ message: 'Missing required column in Mode report. See logs.' }) };
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
        log.push(`SKIPPED: Ticket for deployment '${depKey}' skipped because no requester email was found.`);
        freshdeskResults.push({ deployment: depKey, status: 'Skipped (no email)' });
        continue;
      }
      
      const ccEmails = enableTestMode ? [] : data.contacts.filter((e) => e !== requesterEmail);
      const subject = customSubject.replace('[Deployment Name]', depKey);
      const formattedBody = customBody.replace(/\n/g, '<br>');
      const description = formattedBody.replace('[Deployment Name]', depKey);
      
      log.push(`Creating ticket for deployment '${depKey}'. Requester: ${requesterEmail}, CCs: ${ccEmails.join(', ')}.`);
      const ticketForm = new FormData();
      ticketForm.append('subject', subject);
      ticketForm.append('description', `<div>${description}</div>`, { contentType: 'text/html' });
      ticketForm.append('email', requesterEmail);
      ticketForm.append('status', '5'); // Status 5 to close the ticket
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
        log.push(`FAILED: Freshdesk ticket for '${depKey}' failed. Error: ${JSON.stringify(ticketResult)}`);
        freshdeskResults.push({ deployment: depKey, status: 'Failed', ticket_id: null, error_details: ticketResult });
        continue;
      }
      log.push(`SUCCESS: Freshdesk ticket created for '${depKey}' with ID: ${ticketResult.id}`);
      freshdeskResults.push({ deployment: depKey, status: 'Success', ticket_id: ticketResult.id, error_details: null });
    }
    
    // This is a background function, so we don't need to return anything to the user.
    log.push(`Background function finished at ${new Date().toISOString()}`);
    return {
      statusCode: 200,
      body: JSON.stringify({ message: "Background process complete.", log }),
    };
  } catch (err) {
    const errorMessage = err.message || 'An unexpected error occurred in the background function.';
    log.push(`CRITICAL ERROR: ${errorMessage}`);
    console.error('Background function handler error:', err);
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
