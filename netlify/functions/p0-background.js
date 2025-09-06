const { parse } = require('csv-parse/sync');
const FormData = require('form-data');
const fetch = require('node-fetch');

exports.handler = async function(event) {
  const log = [];
  const startTimestamp = new Date().toISOString();
  log.push(`Background function started at ${startTimestamp}`);

  try {
    const body = JSON.parse(event.body);
    const enableTestMode = body.enableTestMode;
    const testEmail = body.testEmail;
    const customSubject = body.customSubject;
    const customBody = body.customBody;
    const FRESHDESK_API_KEY = body.FRESHDESK_API_KEY;
    const FRESHDESK_RESPONDER_ID = body.FRESHDESK_RESPONDER_ID;
    const hasImpactList = body.hasImpactList;
    const deploymentKey = body.deploymentKey;
    const data = body.data;
    if (body.log) {
      log.push(...body.log);
    }

    const FRESHDESK_API_URL = `https://blendsupport.freshdesk.com/api/v2/tickets`;
    
    const requesterEmail = enableTestMode ? testEmail : data.contacts[0] || null;
    if (!requesterEmail) {
      log.push(`SKIPPED: Ticket for deployment '${deploymentKey}' skipped because no requester email was found.`);
      return {
        statusCode: 200,
        body: JSON.stringify({
          status: 'Skipped (no email)',
          ticket_id: null,
          error_details: null,
          log,
        }),
      };
    }
    
    const ccEmails = enableTestMode ? [] : data.contacts.filter((e) => e !== requesterEmail);
    const subject = customSubject.replace('[Deployment Name]', deploymentKey);
    const formattedBody = customBody.replace(/\n/g, '<br>');
    const description = formattedBody.replace('[Deployment Name]', deploymentKey);
    
    log.push(`Creating ticket for deployment '${deploymentKey}'. Requester: ${requesterEmail}, CCs: ${ccEmails.join(', ')}.`);
    const ticketForm = new FormData();
    ticketForm.append('subject', subject);
    ticketForm.append('description', `<div>${description}</div>`, { contentType: 'text/html' });
    ticketForm.append('email', requesterEmail);
    ticketForm.append('status', '5');
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
        filename: `Impact_List_${deploymentKey}.csv`,
        contentType: 'text/csv',
        knownLength: impactBuffer.length,
      });
      log.push(`- Attached impact list for '${deploymentKey}' during ticket creation.`);
    }

    const ticketHeaders = ticketForm.getHeaders();
    ticketHeaders.Authorization = `Basic ${Buffer.from(FRESHDESK_API_KEY + ':X').toString('base64')}`;
    
    const ticketResp = await fetch(FRESHDESK_API_URL, {
      method: 'POST',
      headers: ticketHeaders,
      body: ticketForm,
    });
    
    // Check if the response is JSON before parsing
    const contentType = ticketResp.headers.get('content-type');
    if (!contentType || !contentType.includes('application/json')) {
        const errorText = await ticketResp.text();
        log.push(`Freshdesk API returned non-JSON response. Status: ${ticketResp.status}, Body: ${errorText}`);
        return {
          statusCode: 500,
          body: JSON.stringify({
            status: 'Failed',
            ticket_id: null,
            error_details: { message: 'Non-JSON response from Freshdesk API', body: errorText },
            log,
          }),
        };
    }
    
    const ticketResult = await ticketResp.json();
    if (!ticketResp.ok) {
      log.push(`FAILED: Freshdesk ticket for '${deploymentKey}' failed. Error: ${JSON.stringify(ticketResult)}`);
      return {
        statusCode: ticketResp.status,
        body: JSON.stringify({
          status: 'Failed',
          ticket_id: null,
          error_details: ticketResult,
          log,
        }),
      };
    }
    log.push(`SUCCESS: Freshdesk ticket created for '${deploymentKey}' with ID: ${ticketResult.id}`);
    
    return {
      statusCode: 200,
      body: JSON.stringify({
        status: 'Success',
        ticket_id: ticketResult.id,
        error_details: null,
        log,
      }),
    };
  } catch (err) {
    const errorMessage = err.message || 'An unexpected error occurred in the background function.';
    log.push(`CRITICAL ERROR: ${errorMessage}`);
    console.error('Background function handler error:', err);
    return {
      statusCode: 500,
      body: JSON.stringify({
        status: 'Error',
        ticket_id: null,
        error_details: { message: errorMessage },
        log,
      }),
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
