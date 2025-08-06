import fetch from 'node-fetch';
import { parse } from 'csv-parse/sync';

export async function handler(event, context) {
  if (event.httpMethod !== 'POST') {
    return {
      statusCode: 405,
      body: JSON.stringify({ error: 'Method Not Allowed' })
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
        body: JSON.stringify({ error: 'No CSV data provided.' })
      };
    }

    const userRecords = parse(csvData, {
      columns: true,
      skip_empty_lines: true
    });

    const MODE_AUTH_TOKEN = process.env.MODE_AUTH_TOKEN;
    const FRESHDESK_API_KEY = process.env.FRESHDESK_API_KEY;
    const MODE_RUN_URL = 'https://app.mode.com/api/blend/reports/77c0a6f31c3c/runs';
    const MODE_CSV_URL = 'https://app.mode.com/api/blend/reports/77c0a6f31c3c/results/content.csv';

    // Trigger Mode Report Run
    const runResp = await fetch(MODE_RUN_URL, {
      method: 'POST',
      headers: {
        Authorization: `Basic ${MODE_AUTH_TOKEN}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({})
    });

    if (!runResp.ok) {
      throw new Error('Failed to trigger Mode report run');
    }

    const runData = await runResp.json();
    const runToken = runData.token;

    // Poll status
    let succeeded = false;
    const pollUrl = `${MODE_RUN_URL}/${runToken}`;
    for (let i = 0; i < 60; i++) {
      const statusResp = await fetch(pollUrl, {
        headers: { Authorization: `Basic ${MODE_AUTH_TOKEN}` }
      });
      const statusData = await statusResp.json();
      if (statusData.state === 'succeeded') {
        succeeded = true;
        break;
      } else if (['failed', 'cancelled'].includes(statusData.state)) {
        throw new Error(`Report run failed: ${statusData.state}`);
      }
      await new Promise(res => setTimeout(res, 5000));
    }

    if (!succeeded) {
      throw new Error('Report polling timed out');
    }

    // Fetch Mode CSV content
    const modeCsvResp = await fetch(MODE_CSV_URL, {
      headers: {
        Authorization: `Basic ${MODE_AUTH_TOKEN}`,
        Accept: 'text/csv'
      }
    });
    const modeCsvText = await modeCsvResp.text();
    const modeRecords = parse(modeCsvText, {
      columns: true,
      skip_empty_lines: true
    });

    // TODO: Match userRecords with modeRecords and send Freshdesk tickets.
    // For now, we just return parsed counts.

    return {
      statusCode: 200,
      body: JSON.stringify({
        message: 'Parsed successfully',
        userRecords: userRecords.length,
        modeRecords: modeRecords.length
      })
    };
  } catch (err) {
    console.error('Handler error:', err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: err.message || 'Internal Server Error' })
    };
  }
}
