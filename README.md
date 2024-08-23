# COVID-19 Data Dashboard

## Description

This project is a web-based dashboard for visualizing and interacting with COVID-19 data. It allows users to select databases, query specific data points, and add or retrieve comments related to the data. The dashboard is built with HTML, CSS, JavaScript, and FastAPI for the backend.

## Features

- **Database and Table Selection**: Choose between different databases and tables to view data descriptions.
- **Data Query**: Fetch and display data points such as total cases, population, vaccinated, unvaccinated, total tests, and total deaths for selected countries and dates.
- **Comment Section**: Add and retrieve comments related to specific data points.
- **Data Visuals**: Navigate to a separate page for data visualizations.

## Installation

To set up the project locally, follow these steps:

1. **Clone the repository**:

   `git clone https://github.com/your-username/covid19-data-dashboard.git`

2. **Navigate to the project directory**:

   `cd covid19-data-dashboard`

3. **Set up a virtual environment**:

   `python -m venv venv`

4. **Activate the virtual environment**:
   - On Windows:

     `venv\Scripts\activate`

   - On macOS/Linux:

     `source venv/bin/activate`

5. **Install the required packages**:

   `pip install -r requirements.txt`

6. **Run the FastAPI application**:

   `uvicorn main:app --reload`

   The application should be accessible at `http://127.0.0.1:8000`.

## Usage

### Access the Dashboard

- Open your web browser and go to `http://127.0.0.1:8000`.
- Use the interface to select databases, tables, and query data.

### Data Query

- Select the database and table from the dropdown menus.
- Click on the "OVERVIEW" button to fetch the table description.
- Input the country and date, then click on the respective buttons to fetch data points like Total Cases, Population, Vaccinated, Unvaccinated, Total Tests, and Total Deaths.

### Adding and Retrieving Comments

- To add a comment, enter the datapoint ID, user, and comment in the provided fields and click "Add Comment".
- To retrieve comments, enter the datapoint ID and click "Get Comments".

### Data Visuals

- Click on the "Data Visuals" button to navigate to the data visualization page.

## File Structure

- `index.html`: Main HTML file for the dashboard.
- `static/script.js`: JavaScript file containing functions for interacting with the API and handling UI events.
- `templates/index.html`: Jinja2 template for rendering the HTML content with FastAPI.
- `main.py`: FastAPI application file handling routes and API requests.
- `requirements.txt`: List of required Python packages.
