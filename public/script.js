document.addEventListener('DOMContentLoaded', () => {
    const form = document.getElementById('upload-form');
    const fileInput = document.getElementById('csv-file');
    const testModeCheckbox = document.getElementById('test-mode');
    const testEmailInput = document.getElementById('test-email');
    const testEmailGroup = document.getElementById('test-email-group');
    const resultsDiv = document.getElementById('results');
    const processButton = document.getElementById('process-button');

    // Show/hide test email input
    testModeCheckbox.addEventListener('change', () => {
        testEmailGroup.style.display = testModeCheckbox.checked ? 'block' : 'none';
    });

    const statusMessage = (text, type = 'info') => {
        const p = document.createElement('p');
        p.textContent = text;
        p.className = `status-message ${type}`; // Add a class for styling (e.g., info, success, error)
        resultsDiv.appendChild(p);
        resultsDiv.scrollTop = resultsDiv.scrollHeight; // Auto-scroll to the bottom
    };

    form.addEventListener('submit', async (e) => {
        e.preventDefault();
        
        resultsDiv.innerHTML = ''; // Clear previous results
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
            
            // Read file content
            const fileContent = await file.text();

            const payload = {
                csv_data: fileContent,
                enable_test_mode: testModeCheckbox.checked,
                test_email: testEmailInput.value,
                custom_subject: document.getElementById('custom-subject').value,
                custom_body: document.getElementById('custom-body').value
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
            
            statusMessage('Processing complete! Here are the results:', 'success');
            
            // Display results in a clear, formatted way
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
                } else { // Handle 'Skipped' or other statuses
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
