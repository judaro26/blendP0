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

    form.addEventListener('submit', async (e) => {
        e.preventDefault();
        
        resultsDiv.innerHTML = '';
        const statusMessage = (text) => {
            const p = document.createElement('p');
            p.textContent = text;
            resultsDiv.appendChild(p);
        };

        const file = fileInput.files[0];
        if (!file) {
            statusMessage('Please upload a CSV file.');
            return;
        }

        processButton.disabled = true;
        statusMessage('Starting processing...');

        try {
            // Read file content
            const fileContent = await file.text();

            const payload = {
                csv_data: fileContent,
                enable_test_mode: testModeCheckbox.checked,
                test_email: testEmailInput.value,
                custom_subject: document.getElementById('custom-subject').value,
                custom_body: document.getElementById('custom-body').value
            };
            
            statusMessage('Sending data to backend for processing...');
            const response = await fetch('/.netlify/functions/p0', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(payload)
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error || 'Unknown error');
            }

            const data = await response.json();
            
            statusMessage('Processing complete! Here are the results:');
            resultsDiv.innerHTML = '<h3>Ticket Creation Summary:</h3>';
            data.freshdesk_results.forEach(result => {
                const resultP = document.createElement('p');
                resultP.textContent = `Deployment: ${result.deployment}, Status: ${result.status}`;
                resultsDiv.appendChild(resultP);
            });

        } catch (error) {
            statusMessage(`An error occurred: ${error.message}`);
            console.error('Error:', error);
        } finally {
            processButton.disabled = false;
        }
    });
});
