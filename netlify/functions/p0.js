const { parse } = require('csv-parse/sync');
const FormData = require('form-data');
const fetch = require('node-fetch');

const POLL_INTERVAL_MS = 5000;
const MAX_POLL_ATTEMPTS = 6;  // Background functions have a longer timeout (15 mins)

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
    const FRESHDESK_API_URL = 'https://blendsupport.freshdesk.com/api/v2/tickets';
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
    
    // Trigger the background function and pass all data
    log.push('Triggering background function for ticket creation...');
    await fetch('https://' + event.headers.host + '/.netlify/functions/p0-background', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        runToken,
        csvData,
        enableTestMode,
        testEmail,
        customSubject,
        customBody,
        MODE_AUTH_TOKEN,
        FRESHDESK_API_KEY,
        FRESHDESK_RESPONDER_ID,
        deployments,
        hasImpactList,
        log: [...log],
      }),
    });

    // 6. Return results immediately
    const endTimestamp = new Date().toISOString();
    log.push(`Function finished at ${endTimestamp}`);

    return {
      statusCode: 202,
      body: JSON.stringify({
        message: 'Processing successfully initiated! Please check Freshdesk for ticket status.',
        run_token: runToken,
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
