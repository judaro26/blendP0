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
        p.innerHTML = text;
        p.className = `status-message ${type}`;
        resultsDiv.appendChild(p);
        resultsDiv.scrollTop = resultsDiv.scrollHeight;
    };

    const displayResults = (results) => {
        if (results.length === 0) {
            statusMessage('No tickets were created.', 'info');
            return;
        }
        
        results.forEach(result => {
            const resultItem = document.createElement('div');
            resultItem.className = 'result-item';

            if (result.status === 'Success') {
                const message = `✅ Ticket created for **${result.deployment}**! Ticket ID: ${result.ticket_id}.`;
                const p = document.createElement('p');
                p.innerHTML = message;
                p.className = 'status-message success';
                resultItem.appendChild(p);

                if (result.impact_list) {
                    const downloadBtn = document.createElement('button');
                    downloadBtn.textContent = 'Download Impact List';
                    downloadBtn.className = 'download-button';
                    downloadBtn.onclick = () => downloadFile(result.impact_list, `Impact_List_${result.deployment}.csv`, 'text/csv');
                    resultItem.appendChild(downloadBtn);
                }

            } else {
                const message = `❌ Ticket creation failed for **${result.deployment}**. Status: ${result.status}`;
                const p = document.createElement('p');
                p.innerHTML = message;
                p.className = 'status-message error';
                resultItem.appendChild(p);
            }
            resultsDiv.appendChild(resultItem);
        });
    };

    const downloadFile = (base64Data, filename, mimeType) => {
      const link = document.createElement('a');
      link.href = `data:${mimeType};base64,${base64Data}`;
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
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
            statusMessage('Starting data processing...', 'info');
            
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
            
            // Display logs first
            if (data.log && Array.isArray(data.log)) {
                data.log.forEach(logEntry => {
                    const type = logEntry.toLowerCase().includes('error') ? 'error' : 'info';
                    statusMessage(logEntry, type);
                });
            }

            // Display final results
            if (data.results && Array.isArray(data.results)) {
                displayResults(data.results);
            }
            
            statusMessage('Processing complete!', 'success');

        } catch (error) {
            statusMessage(`An error occurred: ${error.message}`, 'error');
            console.error('Error:', error);
        } finally {
            processButton.disabled = false;
            processButton.textContent = 'Start Processing';
        }
    });
});
