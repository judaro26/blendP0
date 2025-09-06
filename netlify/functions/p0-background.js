const { parse } = require('csv-parse/sync');
const FormData = require('form-data');
const fetch = require('node-fetch');

exports.handler = async function(event) {
  const log = [];
  const startTimestamp = new Date().toISOString();
  log.push(`Background function started at ${startTimestamp}`);

  try {
    // The payload is passed from the main p0.js function
    const body = JSON.parse(event.body);
    const enableTestMode = body.enableTestMode;
    const testEmail = body.testEmail;
    const customSubject = body.customSubject;
    const customBody = body.customBody;
    const FRESHDESK_API_KEY = body.FRESHDESK_API_KEY;
    const FRESHDESK_RESPONDER_ID = body.FRESHDESK_RESPONDER_ID;
    const hasImpactList = body.hasImpactList;
    const matchedData = body.matchedData;
    if (body.log) {
      log.push(...body.log);
    }

    const FRESHDESK_API_URL = `https://blendsupport.freshdesk.com/api/v2/tickets`;
    
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
