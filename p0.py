import streamlit as st
import pandas as pd
import requests
import time
import io
import base64

# --- Configuration ---
MODE_REPORT_RUNS_URL = "https://app.mode.com/api/blend/reports/77c0a6f31c3c/runs"
MODE_RESULTS_CONTENT_CSV_URL = (
    "https://app.mode.com/api/blend/reports/77c0a6f31c3c/results/content.csv"
)
DEPLOYMENT_COLUMN_NAMES = ['Tenant', 'TENANT', 'Deployment', 'DEPLOYMENT']

# Freshdesk Configuration
FRESHDESK_DOMAIN = "blendsupport.freshdesk.com"
FRESHDESK_API_URL = f"https://{FRESHDESK_DOMAIN}/api/v2/tickets"

# IMPORTANT: Configure Freshdesk specific IDs and values
FRESHDESK_TRIAGE_GROUP_ID = 156000870331
FRESHDESK_RESPONDER_ID = 156008293335

# --- Helper Functions ---

def trigger_mode_report_run(auth_token):
    """Triggers a new run of the Mode report."""
    headers = {
        'Authorization': f'Basic {auth_token}',
        'Content-Type': 'application/json'
    }
    try:
        response = requests.post(MODE_REPORT_RUNS_URL, headers=headers, json={})
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        return response.json()['token']
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to trigger Mode report run: {e}")
        st.exception(e)
        return None

def poll_mode_report_status(run_token, auth_token, timeout=300, interval=5):
    """Polls the Mode API for the status of a report run until it succeeds or fails."""
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

            if data['state'] == 'succeeded':
                return True
            elif data['state'] in ['failed', 'cancelled']:
                st.error(f"Mode report run failed or was cancelled. State: {data['state']}")
                return False
            else:
                st.info(f"Mode report status: {data['state']}. Retrying in {interval} seconds...")
                time.sleep(interval)
        except requests.exceptions.RequestException as e:
            st.error(f"Error polling Mode report status: {e}")
            st.exception(e)
            return False
    st.error("Timeout reached while waiting for Mode report to complete.")
    return False

def fetch_mode_report_content(auth_token):
    """Fetches the content of the Mode report as CSV."""
    headers = {
        'Authorization': f'Basic {auth_token}',
        'Accept': 'text/csv'
    }
    try:
        response = requests.get(MODE_RESULTS_CONTENT_CSV_URL, headers=headers)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to fetch Mode report content: {e}")
        st.exception(e)
        return None

def match_data(user_df, mode_df):
    """
    Matches user CSV data with Mode report data based on 'DEPLOYMENT' column.
    Groups user CSV rows by their deployment value and finds corresponding contacts from Mode.
    Includes unique emails from 'ACCOUNT_MANAGER_EMAIL' and 'EMAIL' columns from Mode.
    Performs robust string matching with stripping and case folding.
    """
    matched_data = {}
    
    user_deployment_column = None
    user_cols_lower = [col.lower() for col in user_df.columns]
    for col_name in DEPLOYMENT_COLUMN_NAMES:
        if col_name.lower() in user_cols_lower:
            user_deployment_column = next(c for c in user_df.columns if c.lower() == col_name.lower())
            break

    if not user_deployment_column:
        st.error(f"Could not find a valid deployment column in your CSV. Expected one of: {', '.join(DEPLOYMENT_COLUMN_NAMES)}")
        return {}

    user_df_cleaned = user_df.copy()
    user_df_cleaned[user_deployment_column] = user_df_cleaned[user_deployment_column].astype(str).str.strip().str.lower()
    
    user_deployments_in_csv = set(user_df_cleaned[user_deployment_column].unique())

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

    if mode_deployment_col_name:
        mode_df_cleaned = mode_df.copy()
        mode_df_cleaned[mode_deployment_col_name] = mode_df_cleaned[mode_deployment_col_name].astype(str).str.strip().str.lower()
        
        mode_data_by_deployment = {}
        for deployment_value in mode_df_cleaned[mode_deployment_col_name].unique():
            mode_data_by_deployment[deployment_value] = mode_df_cleaned[mode_df_cleaned[mode_deployment_col_name] == deployment_value]
        
        for user_dep_name in user_deployments_in_csv:
            if user_dep_name in mode_data_by_deployment:
                mode_rows_for_deployment = mode_data_by_deployment[user_dep_name]

                for _, mode_row in mode_rows_for_deployment.iterrows():
                    email_col_name = next((c for c in mode_df.columns if c.lower() == 'email'), None)
                    if email_col_name:
                        contact_email = mode_row.get(email_col_name)
                        if pd.notna(contact_email) and isinstance(contact_email, str) and '@' in contact_email:
                            contact_email = contact_email.strip()
                            contact_name_col_name = next((c for c in mode_df.columns if c.lower() == 'name'), None)
                            contact_name = mode_row.get(contact_name_col_name, 'Unknown Contact') if contact_name_col_name else 'Unknown Contact'
                            
                            if contact_email not in {c['email'] for c in matched_data[user_dep_name]['contacts']}:
                                matched_data[user_dep_name]['contacts'].append({
                                    'email': contact_email,
                                    'name': contact_name
                                })
                            matched_data[user_dep_name]['cc_emails'].add(contact_email)

                    am_email_col_name = next((c for c in mode_df.columns if c.lower() == 'account_manager_email'), None)
                    if am_email_col_name:
                        account_manager_emails_raw = mode_row.get(am_email_col_name)
                        if pd.notna(account_manager_emails_raw) and isinstance(account_manager_emails_raw, str):
                            manager_emails = [
                                e.strip() for e in account_manager_emails_raw.replace(';', ',').replace(' ', ',').split(',')
                                if '@' in e.strip()
                            ]
                            for email in manager_emails:
                                matched_data[user_dep_name]['cc_emails'].add(email)

    for deployment_name, data in matched_data.items():
        matched_data[deployment_name]['cc_emails'] = list(data['cc_emails'])

    return matched_data

def convert_dict_to_csv_string(data_list):
    """Converts a list of dictionaries to a CSV string."""
    if not data_list:
        return ""
    df = pd.DataFrame(data_list)
    return df.to_csv(index=False)

def convert_text_to_freshdesk_html(text_content):
    """
    Converts plain text content to HTML format suitable for Freshdesk,
    preserving explicit HTML tags and converting newlines to <br>.
    """
    html_content = text_content.replace('\n', '<br>')
    if not html_content.strip().lower().startswith(('<div', '<p', '<table', '<ul', '<ol', '<h1', '<h2', '<h3', '<h4', '<h5', '<h6')):
        html_content = f"<div>{html_content}</div>"
    
    return html_content

def create_freshdesk_ticket(freshdesk_api_key, subject, description, email, cc_emails=None, attachments=None):
    """
    Creates a Freshdesk ticket and then sends a reply to trigger email delivery.
    Args:
        freshdesk_api_key (str): The Freshdesk API key.
        subject (str): The subject of the ticket.
        description (str): The description/body of the ticket.
        email (str): The email of the requester.
        cc_emails (list): Optional list of email addresses to CC on the ticket.
        attachments (list of tuples): List of (filename, file_content_bytes, mime_type) for attachments.
    Returns:
        bool: True if both ticket creation and reply were successful, False otherwise.
    """
    # 1. Create the ticket first (internal action)
    try:
        # Initial ticket creation payload
        files_payload = [
            ('subject', (None, subject, 'text/plain')),
            ('description', (None, f"<div>Ticket created automatically for {subject.replace('Impact Report for ', '')}.</div>", 'text/html')),
            ('email', (None, email, 'text/plain')),
            ('status', (None, str(2), 'text/plain')), # Status 2 for Open
            ('priority', (None, str(1), 'text/plain')), # Priority 1 for Low
            ('group_id', (None, str(FRESHDESK_TRIAGE_GROUP_ID), 'text/plain')), # Set Group to Triage
            ('responder_id', (None, str(FRESHDESK_RESPONDER_ID), 'text/plain')), # Set Responder ID
            ('tags[]', (None, 'Support-emergency', 'text/plain')), # Add Tags
            ('custom_fields[cf_blend_product]', (None, 'Mortgage', 'text/plain')), # Blend Product
            ('custom_fields[cf_type_of_case]', (None, 'Issue', 'text/plain')), # Type of Case
            ('custom_fields[cf_disposition477339]', (None, 'P0 Comms', 'text/plain')), # Disposition
            ('custom_fields[cf_blend_platform]', (None, 'Lending Platform', 'text/plain')), # Blend Platform
            ('custom_fields[cf_survey_automation]', (None, 'No', 'text/plain')), # Survey
        ]

        response = requests.post(FRESHDESK_API_URL, auth=(freshdesk_api_key, 'X'), files=files_payload)
        response.raise_for_status()
        ticket_id = response.json().get('id')
        st.success(f"Successfully created Freshdesk ticket for '{subject}'. Ticket ID: {ticket_id}")

    except requests.exceptions.RequestException as e:
        st.error(f"Failed to create initial Freshdesk ticket for '{subject}': {e}")
        st.exception(e)
        return False

    # 2. Send a public reply to the new ticket to trigger the email
    try:
        html_description_content = convert_text_to_freshdesk_html(description)
        reply_url = f"{FRESHDESK_API_URL}/{ticket_id}/reply"

        # The reply payload with the full body and attachments
        reply_payload = [
            ('body', (None, f"<div>{html_description_content}</div>", 'text/html')),
            ('status', (None, str(2), 'text/plain')), # Keep the status as Open
        ]

        if cc_emails:
            for cc_email in cc_emails:
                reply_payload.append(('cc_emails[]', (None, cc_email, 'text/plain')))
        
        if attachments:
            for field_name, file_tuple in attachments:
                reply_payload.append((field_name, file_tuple))

        reply_response = requests.post(reply_url, auth=(freshdesk_api_key, 'X'), files=reply_payload)
        reply_response.raise_for_status()
        st.success(f"Successfully sent a public reply to ticket ID {ticket_id} to trigger email delivery.")
        return True
    
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to send a public reply to ticket ID {ticket_id}: {e}")
        st.exception(e)
        return False

# --- Streamlit UI ---

st.set_page_config(layout="centered", page_title="CSV Data Processor")

st.title("CSV Data Processor & Mode/Freshdesk Integrator")
st.markdown("""
    Upload your CSV, fetch a report from Mode Analytics, match the data,
    and create Freshdesk tickets.
""")

# --- Credential Inputs ---
st.subheader("API Credentials")
mode_api_token = st.text_input("Mode API Token (Base64 encoded Basic Auth):", type="password")
freshdesk_api_key = st.text_input("Freshdesk API Key:", type="password")

# File Uploader
uploaded_file = st.file_uploader("Upload User CSV File", type=["csv"])

user_df = pd.DataFrame()
if uploaded_file is not None:
    try:
        user_df = pd.read_csv(uploaded_file)
        st.success(f"File '{uploaded_file.name}' loaded successfully. Found {len(user_df)} rows.")
        st.subheader("Preview of your CSV:")
        st.dataframe(user_df.head())
    except Exception as e:
        st.error(f"Error reading CSV file: {e}")
        st.exception(e)

# Test Mode Configuration
st.subheader("Test Mode Settings")
enable_test_mode = st.checkbox("Enable Test Mode (send all Freshdesk tickets to a single email)")
test_email = ""
if enable_test_mode:
    test_email = st.text_input("Enter Test Email Address:", value="test@example.com")
    if not test_email or "@" not in test_email:
        st.warning("Please enter a valid email address for Test Mode.")
        enable_test_mode = False

# Email Content Configuration
st.subheader("Freshdesk Email Content")
default_subject = "Impact Report for [Deployment Name]"
custom_subject = st.text_input("Custom Subject Line:", value=default_subject)
default_body = """Dear team,

We identified an issue affecting the Builder workflow that resulted in some customers experiencing prolonged loading screens. The issue has since been resolved following a successful hotfix that was deployed tonight. 

We are actively remediating any impacted applications and are including a detailed impact lists with the affected clients.


Please find the attached impact list for [Deployment Name].

Please don't hesitate to reach back with any additional question, suggestion or update. I am just a click away!

All the best,

Blend Support"""
custom_body = st.text_area("Custom Email Body: (New lines are automatic. Use HTML for bold: <b>text</b>, color: <span style='color:red;'>text</span>)", value=default_body, height=200)

# Check for all required inputs before enabling the button
can_process = bool(uploaded_file and mode_api_token and freshdesk_api_key)

# Process Button
if st.button("Start Processing", disabled=not can_process):
    st.info("Starting data processing...")

    with st.spinner("Triggering Mode report run..."):
        run_token = trigger_mode_report_run(mode_api_token)
        if not run_token:
            st.stop()

    with st.spinner(f"Mode report run triggered. Run token: {run_token}. Polling for completion..."):
        if not poll_mode_report_status(run_token, mode_api_token):
            st.stop()

    st.success("Mode report run completed successfully. Fetching report content...")

    with st.spinner("Fetching Mode report content..."):
        mode_csv_text = fetch_mode_report_content(mode_api_token)
        if not mode_csv_text:
            st.stop()

        mode_df = pd.read_csv(io.StringIO(mode_csv_text))
        st.success(f"Mode report fetched and parsed. Found {len(mode_df)} rows.")
        st.subheader("Preview of Mode Report Data:")
        st.dataframe(mode_df.head())

    st.info("Matching data from user CSV and Mode report...")
    matched_data = match_data(user_df, mode_df)

    if not matched_data:
        st.warning("No matching data found or deployment column not identified. Please check your CSV.")
        st.stop()

    st.success("Data matching complete. Preparing impact lists and creating Freshdesk tickets.")

    st.subheader("Generated Impact Lists")
    st.markdown("Below are the impact lists generated for each deployment. Click to download the corresponding CSV.")
    
    cols = st.columns(2)
    col_idx = 0
    for deployment_name, data in matched_data.items():
        with cols[col_idx % 2]:
            st.markdown(f"**{deployment_name}**")
            st.write(f"Impacted Rows: {len(data['impact_list'])}")
            st.write(f"Primary Contact Candidates: {len(data['contacts'])}")
            st.write(f"Total Unique CC Emails (including primary and AMs): {len(data['cc_emails'])}")
            
            impact_csv_content = convert_dict_to_csv_string(data['impact_list'])
            
            st.download_button(
                label=f"Download {deployment_name} Impact CSV",
                data=impact_csv_content,
                file_name=f"Impact_List_{deployment_name.replace(' ', '_').replace('/', '_')}.csv",
                mime="text/csv",
                key=f"download_csv_{deployment_name}"
            )
            st.markdown("---")
        col_idx += 1

    st.subheader("Freshdesk Ticket Creation Status")
    if enable_test_mode:
        st.info(f"Test Mode is ENABLED. All tickets will be sent to: **{test_email}** (as requester). **No other CCs will be added.**")
    else:
        st.info("Test Mode is DISABLED. Tickets will be sent to actual contacts with CCs.")

    freshdesk_results = []
    for deployment_name, data in matched_data.items():
        requester_email_for_ticket = None
        cc_emails_for_ticket = []

        if enable_test_mode:
            requester_email_for_ticket = test_email
            st.info(f"For '{deployment_name}' in TEST MODE, requester is '{requester_email_for_ticket}' and no CCs will be added.")
        else:
            actual_primary_contact_emails = [c['email'] for c in data['contacts'] if c['email'] and c['email'] != 'no_email@example.com']
            if actual_primary_contact_emails:
                requester_email_for_ticket = actual_primary_contact_emails[0]
                cc_emails_for_ticket = [email for email in data['cc_emails'] if email != requester_email_for_ticket]
            else:
                st.warning(f"Skipping Freshdesk ticket for '{deployment_name}' as no valid requester email was found.")
                freshdesk_results.append({
                    'deployment': deployment_name,
                    'status': 'Skipped (No Requester Email)',
                    'ticket_id': None
                })
                continue

        ticket_subject = custom_subject.replace("[Deployment Name]", deployment_name)
        ticket_description = custom_body.replace("[Deployment Name]", deployment_name)

        if enable_test_mode:
            ticket_description += f"\n\n--- This ticket was sent in TEST MODE to {test_email} as requester. Original primary contacts and AMs are NOT CC'd. ---"
        
        impact_csv_content_bytes = convert_dict_to_csv_string(data['impact_list']).encode('utf-8')
        impact_file_name = f"Impact_List_{deployment_name.replace(' ', '_').replace('/', '_')}.csv"
        
        attachments = [('attachments[]', (impact_file_name, impact_csv_content_bytes, 'text/csv'))]

        with st.spinner(f"Creating Freshdesk ticket for {deployment_name}..."):
            ticket_created = create_freshdesk_ticket(
                freshdesk_api_key,
                ticket_subject,
                ticket_description,
                requester_email_for_ticket,
                cc_emails_for_ticket,
                attachments
            )
            freshdesk_results.append({
                'deployment': deployment_name,
                'status': 'Success' if ticket_created else 'Failed',
                'ticket_id': 'Check Freshdesk' if ticket_created else None
            })
        st.markdown("---")

    st.subheader("Freshdesk Ticket Summary")
    for result in freshdesk_results:
        if result['status'] == 'Success':
            st.success(f"Ticket for '{result['deployment']}': {result['status']}")
        elif result['status'] == 'Skipped (No Requester Email)':
            st.warning(f"Ticket for '{result['deployment']}': {result['status']}")
        else:
            st.error(f"Ticket for '{result['deployment']}': {result['status']}")
