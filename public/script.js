document.addEventListener('DOMContentLoaded', () => {
    const form = document.getElementById('upload-form');
    const fileInput = document.getElementById('csv-file');
    const testModeCheckbox = document.getElementById('test-mode');
    const testEmailInput = document.getElementById('test-email');
    const testEmailGroup = document.getElementById('test-email-group');
    const modeAuthTokenInput = document.getElementById('mode-auth-token');
    const freshdeskApiKeyInput = document.getElementById('freshdesk-api-key');
    const resultsDiv = document.getElementById('results');
    const processButton = document.getElementById('process-button');

    testModeCheckbox.addEventListener('change', () => {
        testEmailGroup.style.display = testModeCheckbox.checked ? 'block' : 'none';
    });

    const statusMessage = (text, type = 'info') => {
        const p = document.createElement('p');
        p.textContent = text;
        p.className = `status-message ${type}`;
        resultsDiv.appendChild(p);
        resultsDiv.scrollTop = resultsDiv.scrollHeight;
    };

    const createDownloadLink = (filename, content) => {
        const a = document.createElement('a');
        const blob = new Blob([content], { type: 'text/csv' });
        const url = URL.createObjectURL(blob);
        a.href = url;
        a.download = filename;
        a.textContent = `Download ${filename}`;
        a.className = 'download-button';
        return a;
    };

    form.addEventListener('submit', async (e) => {
        e.preventDefault();
        
        resultsDiv.innerHTML = '';
        const file = fileInput.files[0];
        
        if (!file) {
            statusMessage('Please upload a CSV file.', 'error');
            return;
        }

        processButton.disabled = true;
        processButton.textContent = 'Processing...';

        try {
            statusMessage('Starting data processing...');
            statusMessage('Reading file content...', 'info');
            
            const fileContent = await file.text();
            const payload = {
                csv_data: fileContent,
                enable_test_mode: testModeCheckbox.checked,
                test_email: testEmailInput.value,
                custom_subject: document.getElementById('custom-subject').value,
                custom_body: document.getElementById('custom-body').value,
                mode_auth_token: modeAuthTokenInput.value,
                freshdesk_api_key: freshdeskApiKeyInput.value
            };
            
            statusMessage('Sending data to backend for processing...', 'info');
            const response = await fetch('/.netlify/functions/p0', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(payload)
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error || 'Unknown error occurred in backend.');
            }

            const data = await response.json();
            
            statusMessage('Processing complete!', 'success');
            
            // Display Matched Data Previews
            const previewTitle = document.createElement('h3');
            previewTitle.textContent = 'Matched Data Previews:';
            resultsDiv.appendChild(previewTitle);

            for (const depKey in data.matched_data) {
                const deploymentData = data.matched_data[depKey];
                const deploymentSection = document.createElement('div');
                deploymentSection.className = 'deployment-section';
                resultsDiv.appendChild(deploymentSection);

                const deploymentHeader = document.createElement('h4');
                deploymentHeader.textContent = `Deployment: ${depKey}`;
                deploymentSection.appendChild(deploymentHeader);

                // Preview Impact List CSV
                const impactHeader = document.createElement('p');
                impactHeader.innerHTML = `<strong>Impact List CSV:</strong>`;
                deploymentSection.appendChild(impactHeader);
                const impactPreview = document.createElement('pre');
                impactPreview.textContent = deploymentData.impact_list;
                deploymentSection.appendChild(impactPreview);

                // Download Button for Impact List
                const impactFilename = `Impact_List_${depKey}.csv`;
                const downloadLink = createDownloadLink(impactFilename, deploymentData.impact_list);
                deploymentSection.appendChild(downloadLink);

                // Preview Matched Contacts
                const contactsHeader = document.createElement('p');
                contactsHeader.innerHTML = `<strong>Matched Contacts:</strong>`;
                deploymentSection.appendChild(contactsHeader);
                const contactsList = document.createElement('ul');
                if (deploymentData.contacts.length > 0) {
                    deploymentData.contacts.forEach(contact => {
                        const li = document.createElement('li');
                        li.textContent = contact;
                        contactsList.appendChild(li);
                    });
                } else {
                    const li = document.createElement('li');
                    li.textContent = 'No contacts found.';
                    contactsList.appendChild(li);
                }
                deploymentSection.appendChild(contactsList);
            }

            // Display Freshdesk Ticket Creation Summary
            const summaryTitle = document.createElement('h3');
            summaryTitle.textContent = 'Freshdesk Ticket Creation Summary:';
            resultsDiv.appendChild(summaryTitle);

            data.freshdesk_results.forEach(result => {
                const resultP = document.createElement('p');
                let statusText = '';
                let statusClass = '';

                if (result.status === 'Success') {
                    statusText = `Deployment: ${result.deployment}, Status: ${result.status}, Ticket ID: ${result.ticket_id}`;
                    statusClass = 'success';
                } else if (result.status === 'Failed') {
                    statusText = `Deployment: ${result.deployment}, Status: ${result.status}`;
                    statusClass = 'error';
                } else {
                    statusText = `Deployment: ${result.deployment}, Status: ${result.status}`;
                    statusClass = 'warning';
                }
                
                resultP.textContent = statusText;
                resultP.className = statusClass;
                resultsDiv.appendChild(resultP);
            });

        } catch (error) {
            statusMessage(`An error occurred: ${error.message}`, 'error');
            console.error('Error:', error);
        } finally {
            processButton.disabled = false;
            processButton.textContent = 'Start Processing';
        }
    });
});
