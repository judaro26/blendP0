const { parse } = require('csv-parse/sync');
const fetch = require('node-fetch');

const POLL_INTERVAL_MS = 5000;
const MAX_POLL_ATTEMPTS = 6;

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

    // 2. Poll Mode report status
    let succeeded = false;
    const pollUrl = `${MODE_RUN_URL}/${runToken}`;
    log.push('Polling Mode report status...');
    for (let attempt = 0; attempt < MAX_POLL_ATTEMPTS; attempt++) {
        const statusResp = await fetch(pollUrl, {
            headers: { Authorization: `Basic ${MODE_AUTH_TOKEN}` },
        });
        const statusData = await statusResp.json();

        if (!statusResp.ok) {
            log.push(`Error polling Mode report status on attempt ${attempt + 1}: ${JSON.stringify(statusData)}`);
            throw new Error('Failed to poll Mode status.');
        }
        log.push(`- Poll attempt ${attempt + 1}: Status is '${statusData.state}'`);
        if (statusData.state === 'succeeded') {
            succeeded = true;
            break;
        }
        if (['failed', 'cancelled'].includes(statusData.state)) {
            log.push(`Mode report run failed or was cancelled. State: ${statusData.state}. Exiting.`);
            throw new Error('Mode report failed or was cancelled.');
        }
        await new Promise((res) => setTimeout(res, POLL_INTERVAL_MS));
    }

    if (!succeeded) {
      log.push('Mode report did not succeed within max attempts. Proceeding anyway.');
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
      throw new Error('Failed to fetch Mode CSV.');
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
        const errorMessage = `Mode report is missing the required '${MODE_DEPLOYMENT_COL}' column.`
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
                log.push(`Match found for deployment '${deploymentKey}', but no valid email contacts were found. Skipping ticket creation for this deployment.`);
            }
        }
    }
    log.push(`Found matches for ${Object.keys(matchedData).length} deployments.`);
    
    // 5. Trigger background function for each matched deployment
    log.push('Triggering background function for each deployment...');
    const freshdeskResults = [];

    for (const [depKey, data] of Object.entries(matchedData)) {
      const payload = {
        deploymentKey: depKey,
        data: data,
        enableTestMode,
        testEmail,
        customSubject,
        customBody,
        FRESHDESK_API_KEY,
        FRESHDESK_RESPONDER_ID,
        hasImpactList,
        log: [...log],
      };

      await fetch('https://' + event.headers.host + '/.netlify/functions/p0-background', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload),
      });
      freshdeskResults.push({ deployment: depKey, status: 'Initiated' });
    }

    // 6. Return results immediately
    const endTimestamp = new Date().toISOString();
    log.push(`Function finished at ${endTimestamp}`);

    return {
      statusCode: 202,
      body: JSON.stringify({
        message: 'Processing successfully initiated! Please check Freshdesk for ticket status.',
        deployments_initiated: freshdeskResults,
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
