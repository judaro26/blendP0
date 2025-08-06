
import json
import base64
import os
import io
import requests
import time
import pandas as pd

# --- Configuration ---
MODE_REPORT_RUNS_URL = "https://app.mode.com/api/blend/reports/77c0a6f31c3c/runs"
MODE_RESULTS_CONTENT_CSV_URL = (
    "https://app.mode.com/api/blend/reports/77c0a6f31c3c/results/content.csv"
)
# Use environment variables for sensitive data
MODE_AUTH_TOKEN = os.environ.get("MODE_AUTH_TOKEN") 
FRESHDESK_API_KEY = os.environ.get("FRESHDESK_API_KEY")

FRESHDESK_DOMAIN = "blendsupport.freshdesk.com"
FRESHDESK_API_URL = f"https://{FRESHDESK_DOMAIN}/api/v2/tickets"

# IMPORTANT: Configure Freshdesk specific IDs and values
FRESHDESK_TRIAGE_GROUP_ID = 156000870331
FRESHDESK_RESPONDER_ID = 156006674011
DEPLOYMENT_COLUMN_NAMES = ['Tenant', 'TENANT', 'Deployment', 'DEPLOYMENT']

# --- Helper Functions (same as your original script) ---
# NOTE: The Streamlit-specific 'st' calls are removed and replaced with print statements
# or logging for debugging within the Netlify Function environment.
def log(message):
    print(f"[LOG] {message}")

def log_error(message, exception=None):
    print(f"[ERROR] {message}")
    if exception:
        print(f"[EXCEPTION] {exception}")

def trigger_mode_report_run(auth_token):
    headers = {
        'Authorization': f'Basic {auth_token}',
        'Content-Type': 'application/json'
    }
    try:
        response = requests.post(MODE_REPORT_RUNS_URL, headers=headers, json={})
        response.raise_for_status()
        log("Successfully triggered Mode report run.")
        return response.json()['token']
    except requests.exceptions.RequestException as e:
        log_error(f"Failed to trigger Mode report run: {e}")
        return None

def poll_mode_report_status(run_token, auth_token, timeout=300, interval=5):
    status_url = f"{MODE_REPORT_RUNS_URL}/{run_token}"
    headers = {
        'Authorization': f'Basic {auth_token}',
        'Content-Type': 'application/json'
    }
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get(status_url, headers=headers)
            response.raise_for_status()
            data = response.json()
            log(f"Mode report status: {data['state']}")
            if data['state'] == 'succeeded':
                return True
            elif data['state'] in ['failed', 'cancelled']:
                log_error(f"Mode report run failed or was cancelled. State: {data['state']}")
                return False
            else:
                time.sleep(interval)
        except requests.exceptions.RequestException as e:
            log_error(f"Error polling Mode report status: {e}")
            return False
    log_error("Timeout reached while waiting for Mode report to complete.")
    return False

def fetch_mode_report_content(auth_token):
    headers = {
        'Authorization': f'Basic {auth_token}',
        'Accept': 'text/csv'
    }
    try:
        response = requests.get(MODE_RESULTS_CONTENT_CSV_URL, headers=headers)
        response.raise_for_status()
        log("Successfully fetched Mode report content.")
        return response.text
    except requests.exceptions.RequestException as e:
        log_error(f"Failed to fetch Mode report content: {e}")
        return None

def match_data(user_df, mode_df):
    matched_data = {}
    user_deployment_column = None
    user_cols_lower = [col.lower() for col in user_df.columns]
    for col_name in DEPLOYMENT_COLUMN_NAMES:
        if col_name.lower() in user_cols_lower:
            user_deployment_column = next(c for c in user_df.columns if c.lower() == col_name.lower())
            break
    if not user_deployment_column:
        log_error(f"Could not find a valid deployment column in user CSV. Expected one of: {', '.join(DEPLOYMENT_COLUMN_NAMES)}")
        return {}
    log(f"Identified user CSV deployment column: `{user_deployment_column}`")
    user_df_cleaned = user_df.copy()
    user_df_cleaned[user_deployment_column] = user_df_cleaned[user_deployment_column].astype(str).str.strip().str.lower()
    
    for deployment_value, group in user_df_cleaned.groupby(user_deployment_column):
        matched_data[deployment_value] = {
            'impact_list': user_df[user_df[user_deployment_column].astype(str).str.strip().str.lower() == deployment_value].to_dict(orient='records'),
            'contacts': [],
            'cc_emails': set()
        }
    
    mode_deployment_col_name = None
    for col in mode_df.columns:
        if col.lower() == 'deployment':
            mode_deployment_col_name = col
            break

    if not mode_deployment_col_name:
        log("Mode report is missing a 'DEPLOYMENT' column. Contacts will not be found.")
    else:
        log(f"Identified Mode report deployment column: `{mode_deployment_col_name}`")
        mode_df_cleaned = mode_df.copy()
        mode_df_cleaned[mode_deployment_col_name] = mode_df_cleaned[mode_deployment_col_name].astype(str).str.strip().str.lower()
        
        mode_data_by_deployment = {}
        for deployment_value in mode_df_cleaned[mode_deployment_col_name].unique():
            mode_data_by_deployment[deployment_value] = mode_df_cleaned[mode_df_cleaned[mode_deployment_col_name] == deployment_value]
            
        for user_dep_name in matched_data.keys():
            if user_dep_name in mode_data_by_deployment:
                mode_rows_for_deployment = mode_data_by_deployment[user_dep_name]
                for index, mode_row in mode_rows_for_deployment.iterrows():
                    email_col_name = next((c for c in mode_df.columns if c.lower() == 'email'), None)
                    if email_col_name:
                        contact_email = mode_row.get(email_col_name)
                        if pd.notna(contact_email) and isinstance(contact_email, str) and '@' in contact_email.strip():
                            contact_email = contact_email.strip()
                            contact_name_col_name = next((c for c in mode_df.columns if c.lower() == 'name'), None)
                            contact_name = mode_row.get(contact_name_col_name, 'Unknown Contact') if contact_name_col_name else 'Unknown Contact'
                            if contact_email not in {c['email'] for c in matched_data[user_dep_name]['contacts']}:
                                matched_data[user_dep_name]['contacts'].append({'email': contact_email, 'name': contact_name})
                            matched_data[user_dep_name]['cc_emails'].add(contact_email)
                    
                    am_email_col_name = next((c for c in mode_df.columns if c.lower() == 'account_manager_email'), None)
                    if am_email_col_name:
                        account_manager_emails_raw = mode_row.get(am_email_col_name)
                        if pd.notna(account_manager_emails_raw) and isinstance(account_manager_emails_raw, str):
                            manager_emails = [e.strip() for e in account_manager_emails_raw.replace(';', ',').replace(' ', ',').split(',') if '@' in e.strip()]
                            for email in manager_emails:
                                matched_data[user_dep_name]['cc_emails'].add(email)

    for deployment_name, data in matched_data.items():
        matched_data[deployment_name]['cc_emails'] = list(data['cc_emails'])

    return matched_data

def convert_dict_to_csv_string(data_list):
    if not data_list:
        return ""
    df = pd.DataFrame(data_list)
    return df.to_csv(index=False)

def convert_text_to_freshdesk_html(text_content):
    html_content = text_content.replace('\n', '<br>')
    if not html_content.strip().lower().startswith(('<div', '<p', '<table', '<ul', '<ol', '<h1', '<h2', '<h3', '<h4', '<h5', '<h6')):
        html_content = f"<div>{html_content}</div>"
    return html_content

def create_freshdesk_ticket(subject, description, email, cc_emails=None, attachments=None):
    try:
        html_description_content = convert_text_to_freshdesk_html(description)
        full_description = f"{html_description_content}<br><br>" \
                           f"--- This ticket was automatically generated by the CSV Data Processor. ---<br>" \
                           f"Attachments for this ticket are included below."

        files_payload = [
            ('subject', (None, subject, 'text/plain')),
            ('description', (None, full_description, 'text/html')),
            ('email', (None, email, 'text/plain')),
            ('status', (None, str(5), 'text/plain')),
            ('priority', (None, str(1), 'text/plain')),
            ('group_id', (None, str(FRESHDESK_TRIAGE_GROUP_ID), 'text/plain')),
            ('responder_id', (None, str(FRESHDESK_RESPONDER_ID), 'text/plain')),
            ('tags[]', (None, 'Support-emergency', 'text/plain')),
            ('custom_fields[cf_blend_product]', (None, 'Mortgage', 'text/plain')),
            ('custom_fields[cf_type_of_case]', (None, 'Issue', 'text/plain')),
            ('custom_fields[cf_disposition477339]', (None, 'P0 Comms', 'text/plain')),
            ('custom_fields[cf_blend_platform]', (None, 'Lending Platform', 'text/plain')),
            ('custom_fields[cf_survey_automation]', (None, 'No', 'text/plain')),
        ]
        
        if cc_emails:
            for cc_email in cc_emails:
                files_payload.append(('cc_emails[]', (None, cc_email, 'text/plain')))

        if attachments:
            for field_name, file_tuple in attachments:
                files_payload.append((field_name, file_tuple))

        response = requests.post(FRESHDESK_API_URL, auth=(FRESHDESK_API_KEY, 'X'), files=files_payload)
        response.raise_for_status()
        log(f"Successfully created Freshdesk ticket for '{subject}'. Ticket ID: {response.json().get('id')}")
        return True, response.json().get('id')
    except requests.exceptions.RequestException as e:
        log_error(f"Failed to create Freshdesk ticket for '{subject}': {e}")
        if response is not None:
            log_error(f"Freshdesk API Response Status Code: {response.status_code}")
            log_error(f"Freshdesk API Response Body: {response.text}")
        else:
            log_error("No response received from Freshdesk API.")
        return False, None


# --- Netlify Function Handler ---
def handler(event, context):
    try:
        # Only handle POST requests
        if event["httpMethod"] != "POST":
            return {
                "statusCode": 405,
                "body": json.dumps({"error": "Method Not Allowed"})
            }

        # Parse the JSON payload from the request body
        body = json.loads(event.get("body", "{}"))
        csv_data = body.get('csv_data')
        enable_test_mode = body.get('enable_test_mode', False)
        test_email = body.get('test_email', 'test@example.com')
        custom_subject = body.get('custom_subject')
        custom_body = body.get('custom_body')

        if not csv_data:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "No CSV data provided."})
            }
        
        # Read the user's CSV data
        user_df = pd.read_csv(io.StringIO(csv_data))
        log(f"User CSV loaded successfully. Found {len(user_df)} rows.")

        # Trigger and poll the Mode report
        log("Triggering Mode report run...")
        run_token = trigger_mode_report_run(MODE_AUTH_TOKEN)
        if not run_token:
            return {"statusCode": 500, "body": json.dumps({"error": "Failed to trigger Mode report."})}
        
        log(f"Mode report run triggered. Polling for completion...")
        if not poll_mode_report_status(run_token, MODE_AUTH_TOKEN):
            return {"statusCode": 500, "body": json.dumps({"error": "Mode report run failed or timed out."})}
        
        # Fetch Mode report content
        log("Fetching Mode report content...")
        mode_csv_text = fetch_mode_report_content(MODE_AUTH_TOKEN)
        if not mode_csv_text:
            return {"statusCode": 500, "body": json.dumps({"error": "Failed to fetch Mode report content."})}
        
        mode_df = pd.read_csv(io.StringIO(mode_csv_text))
        log(f"Mode report fetched and parsed. Found {len(mode_df)} rows.")

        # Match data and create tickets
        log("Matching data...")
        matched_data = match_data(user_df, mode_df)
        if not matched_data:
            return {"statusCode": 400, "body": json.dumps({"error": "No matching data found."})}
        
        freshdesk_results = []
        for deployment_name, data in matched_data.items():
            actual_primary_contact_emails = [c['email'] for c in data['contacts'] if c['email'] != 'no_email@example.com']
            requester_email_for_ticket = None
            if enable_test_mode:
                requester_email_for_ticket = test_email
            elif actual_primary_contact_emails:
                requester_email_for_ticket = actual_primary_contact_emails[0]
            
            if not requester_email_for_ticket:
                freshdesk_results.append({'deployment': deployment_name, 'status': 'Skipped (No Requester Email)'})
                continue
            
            cc_emails_for_ticket = [email for email in data['cc_emails'] if email != requester_email_for_ticket]
            if enable_test_mode:
                for actual_primary_email in actual_primary_contact_emails:
                    if actual_primary_email != requester_email_for_ticket and actual_primary_email not in cc_emails_for_ticket:
                        cc_emails_for_ticket.append(actual_primary_email)
            
            ticket_subject = custom_subject.replace("[Deployment Name]", deployment_name)
            ticket_description = custom_body.replace("[Deployment Name]", deployment_name)
            if enable_test_mode:
                ticket_description += f"\n\n--- This ticket was sent in TEST MODE to {test_email} as requester. Original primary contacts and AMs are CC'd. ---"
            
            impact_csv_content = convert_dict_to_csv_string(data['impact_list']).encode('utf-8')
            impact_file_name = f"Impact_List_{deployment_name.replace(' ', '_').replace('/', '_')}.csv"
            attachments = [('attachments[]', (impact_file_name, impact_csv_content, 'text/csv'))]

            ticket_created, ticket_id = create_freshdesk_ticket(ticket_subject, ticket_description, requester_email_for_ticket, cc_emails_for_ticket, attachments)
            freshdesk_results.append({
                'deployment': deployment_name,
                'status': 'Success' if ticket_created else 'Failed',
                'ticket_id': ticket_id
            })

        return {
            "statusCode": 200,
            "body": json.dumps({"freshdesk_results": freshdesk_results})
        }
    except Exception as e:
        log_error(f"An unexpected error occurred: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
