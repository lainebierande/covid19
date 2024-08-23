// Function to handle clicks on result paragraphs
function handleResultClick(event) {
    const clickedElement = event.target;
    if (clickedElement.tagName === 'P') {
        const idMatch = clickedElement.textContent.match(/Datapoint:\s([a-f0-9\-]+)/);
        if (idMatch) {
            const datapointId = idMatch[1];
            // Automatically populate ID fields in different sections
            document.getElementById("datapoint-id").value = datapointId;
            document.getElementById("get-datapoint-id").value = datapointId;
            document.getElementById("visual-id").value = datapointId;
            document.getElementById("get-visual-id").value = datapointId;
        }
    }
}

// Function to fetch and display table data
async function fetchData() {
    const database = document.getElementById("database").value;
    const table = document.getElementById("table").value;

    const response = await fetch("/fetch-data", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ database, table })
    });

    const data = await response.json();
    const overviewResultsDiv = document.getElementById("overview-results");

    if (response.ok) {
        overviewResultsDiv.innerHTML = ''; // Clear previous results

        const tableElement = document.createElement('table');
        tableElement.border = '1';

        const headerRow = document.createElement('tr');
        const headers = ['Column Name', 'Data Type', 'Description', 'Min Value', 'Max Value'];
        headers.forEach(headerText => {
            const headerCell = document.createElement('th');
            headerCell.innerText = headerText;
            headerRow.appendChild(headerCell);
        });
        tableElement.appendChild(headerRow);

        data.description.forEach(row => {
            const tableRow = document.createElement('tr');
            row.forEach(cellData => {
                const cell = document.createElement('td');
                cell.innerText = cellData;
                tableRow.appendChild(cell);
            });
            tableElement.appendChild(tableRow);
        });

        overviewResultsDiv.appendChild(tableElement);
    } else {
        overviewResultsDiv.innerHTML = `<p>Error: ${data.detail || 'Unknown error occurred'}</p>`;
    }
}

// Function to fetch data from a specific endpoint and display the results
async function fetchDataFromEndpoint(endpoint, label) {
    const country = document.getElementById("country").value;
    const date = document.getElementById("date").value + "-01";

    const response = await fetch(endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ country, date })
    });

    const data = await response.json();
    const resultsDiv = document.getElementById("results");

    if (response.ok) {
        const resultParagraph = document.createElement('p');
        resultParagraph.innerHTML = `
            Datapoint: 
            <span class="datapoint-value">
                ${data.id || 'undefined'}
            </span> 
            | ${label} on ${date} in ${country}: 
            ${data[label.toLowerCase().replace(/ /g, '_')]}`;
        
        resultParagraph.addEventListener('click', handleResultClick);
        resultsDiv.appendChild(resultParagraph);
    } else {
        resultsDiv.innerHTML += `<p>Error: ${data.detail || 'Unknown error occurred'}</p>`;
    }
}

// Functions to fetch different data points
async function fetchTotalCases() { await fetchDataFromEndpoint("/fetch-total-cases", "Total Cases"); }
async function fetchPopulation() { await fetchDataFromEndpoint("/fetch-population", "Population"); }
async function fetchVaccinated() { await fetchDataFromEndpoint("/fetch-vaccinated", "Vaccinated"); }
async function fetchUnvaccinated() { await fetchDataFromEndpoint("/fetch-unvaccinated", "Unvaccinated"); }
async function fetchTotalTests() { await fetchDataFromEndpoint("/fetch-total-tests", "Total Tests"); }
async function fetchTotalDeath() { await fetchDataFromEndpoint("/fetch-total-death", "Total Death"); }

// Function to add a comment
async function addComment() {
    const id = document.getElementById("datapoint-id").value;
    const user = document.getElementById("user").value;
    const comment = document.getElementById("comment").value;

    const response = await fetch("/save-comment", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ datapoint_id: id, user: user, comment: comment })
    });

    const data = await response.json();
    if (response.ok) {
        alert("Comment added successfully!");
        document.getElementById("comment").value = ''; // Clear the input field
    } else {
        alert(`Error: ${data.detail || 'Unknown error occurred'}`);
    }
}

// Function to get comments
async function getComments() {
    const id = document.getElementById("get-datapoint-id").value;
    const commentsDiv = document.getElementById("comments-section");

    const response = await fetch("/get-comments", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ datapoint_id: id })
    });

    const data = await response.json();
    commentsDiv.innerHTML = ""; // Clear previous comments

    if (response.ok) {
        if (data.comments.length > 0) {
            data.comments.forEach(comment => {
                const commentParagraph = document.createElement('p');
                commentParagraph.textContent = `User: ${comment.USER}, Comment: ${comment.COMMENT}`;
                commentsDiv.appendChild(commentParagraph);
            });
        } else {
            commentsDiv.innerHTML = "<p>No comments found.</p>";
        }
    } else {
        commentsDiv.innerHTML = `<p>Error: ${data.detail || 'Unknown error occurred'}</p>`;
    }
}

// Function to fetch data for visualizations
async function fetchVisualData(endpoint, label) {
    const country = document.getElementById("country").value;
    const date = document.getElementById("date").value + "-01";

    const response = await fetch(endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ country, date })
    });

    const data = await response.json();
    const resultsDiv = document.getElementById("visual-results");

    if (response.ok) {
        resultsDiv.innerHTML = data.visual;
        document.getElementById("visual-id").value = data.id;
        document.getElementById("get-visual-id").value = data.id;
    } else {
        resultsDiv.innerHTML = `<p>Error: ${data.detail || 'Unknown error occurred'}</p>`;
    }
}

// Function to add a comment for visualizations
async function addVisualComment() {
    const datapointId = document.getElementById("visual-id").value;
    const user = document.getElementById("visual-user").value;
    const comment = document.getElementById("visual-comment").value;

    const response = await fetch("/save-comment", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ datapoint_id: datapointId, user: user, comment: comment })
    });

    const data = await response.json();

    if (response.ok) {
        alert("Comment added successfully!");
        document.getElementById("visual-comment").value = ''; // Clear the input field
    } else {
        alert(`Error: ${data.detail || 'Unknown error occurred'}`);
    }
}

// Function to get comments for visualizations
async function getVisualComments() {
    const datapointId = document.getElementById("get-visual-id").value;
    const commentsDiv = document.getElementById("visual-comments-section");

    const response = await fetch("/get-comments", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ datapoint_id: datapointId })
    });

    const data = await response.json();
    commentsDiv.innerHTML = ""; // Clear previous comments

    if (response.ok) {
        if (data.comments.length > 0) {
            data.comments.forEach(comment => {
                const commentParagraph = document.createElement('p');
                commentParagraph.textContent = `User: ${comment.USER}, Comment: ${comment.COMMENT}`;
                commentsDiv.appendChild(commentParagraph);
            });
        } else {
            commentsDiv.innerHTML = "<p>No comments found.</p>";
        }
    } else {
        commentsDiv.innerHTML = `<p>Error: ${data.detail || 'Unknown error occurred'}</p>`;
    }
}

// Add event listeners after the DOM content is loaded
document.addEventListener('DOMContentLoaded', () => {
    document.getElementById("submit-comment").addEventListener('click', addComment);
    document.getElementById("get-comments").addEventListener('click', getComments);
    document.getElementById("visual-submit-comment").addEventListener('click', addVisualComment);
    document.getElementById("get-visual-comments").addEventListener('click', getVisualComments);
});
