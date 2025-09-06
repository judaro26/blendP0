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
            
            if (response.status === 202) {
                // The backend has accepted the request and will process it asynchronously
                const data = await response.json();
                statusMessage('Processing successfully initiated! 🎉', 'success');
                statusMessage(`Status: ${data.message}`, 'info');
                statusMessage(`A Mode report run with token **${data.run_token || 'not provided'}** was triggered.`, 'info');
                statusMessage('The Freshdesk tickets will be created shortly. Please check your Freshdesk account for the results.', 'info');
            } else if (!response.ok) {
                // Handle non-202/non-200 errors
                const errorData = await response.json();
                throw new Error(errorData.error || 'Unknown error occurred in backend.');
            } else {
                // This case should not be reached with the new backend logic, but we handle it just in case.
                const data = await response.json();
                statusMessage('Processing complete!', 'success');
                // Display results from the successful backend response
                // ... (existing code to display results) ...
            }

        } catch (error) {
            statusMessage(`An error occurred: ${error.message}`, 'error');
            console.error('Error:', error);
        } finally {
            processButton.disabled = false;
            processButton.textContent = 'Start Processing';
        }
    });
});
