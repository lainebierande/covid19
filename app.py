from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import snowflake.connector
import boto3
import pandas as pd
import plotly.express as px
from datetime import datetime
from sklearn.cluster import KMeans

app = FastAPI()

# Mount static files and templates
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Establish connection to DynamoDB
dynamodb = boto3.resource('dynamodb')
comments_table = dynamodb.Table('COMMENTS')

def get_snowflake_connection():
    """Establish a connection to Snowflake."""
    return snowflake.connector.connect(
        user='LAINEBIERANDE',
        password='Kaskurkad7.',
        account='uj99173.us-east-2.aws',
        warehouse='WH_2',
        role='ACCOUNTADMIN',
        database='GLOBAL',
        schema='PUBLIC'
    )

# Home page route
@app.get("/", response_class=HTMLResponse)
async def read_index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# Describe table based on database selection
def describe_table(database: str, table: str):
    conn = get_snowflake_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"USE DATABASE {database}")
        
        if database == "COVID_DATA":
            if table == "APPLE_MOBILITY":
                cur.execute("DESCRIBE TABLE APPLE_MOBILITY")
                description = cur.fetchall()
                return [(row[0], row[1]) for row in description]
            else:
                raise ValueError("Table not found in COVID_DATA database")
        
        elif database == "GLOBAL":
            if table in ["SUMMARY", "BYDATE"]:
                cur.execute(f"DESCRIBE TABLE {table}")
                description = cur.fetchall()
                return [(row[0], row[1]) for row in description]
            elif table == "UNITED_GLOBAL_EXTENDED":
                cur.execute("SELECT COLUMN_NAME, DATA_TYPE, DESCRIPTION, MIN_VALUE, MAX_VALUE FROM PUBLIC.UNITED_GLOBAL_METADATA")
                metadata = cur.fetchall()
                return metadata
            else:
                cur.execute(f"SELECT id, date, country FROM {table} LIMIT 10")
                records = cur.fetchall()
                return records
        
        else:
            raise ValueError("Invalid database selected")
    
    finally:
        cur.close()
        conn.close()

# Fetch available tables based on the selected database
def get_tables(database: str):
    if database == "COVID_DATA":
        return ["APPLE_MOBILITY"]
    elif database == "GLOBAL":
        return ["SUMMARY", "BYDATE", "UNITED_GLOBAL_EXTENDED"]
    else:
        raise ValueError("Invalid database selected")

# Fetch table data via POST request
@app.post("/fetch-data")
async def fetch_data(request: Request):
    form_data = await request.json()
    database = form_data.get('database')
    table = form_data.get('table')
    
    if not all([database, table]):
        raise HTTPException(status_code=400, detail="Required fields are missing")
    
    try:
        data = describe_table(database, table)
        return {"description": data}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An error occurred: {e}")

# General function to fetch specific metrics
async def fetch_metric(metric: str, label: str, request: Request):
    form_data = await request.json()
    country = form_data.get('country')
    date = form_data.get('date')

    if not country or not date:
        raise HTTPException(status_code=400, detail="Country and Date are required")

    conn = get_snowflake_connection()
    try:
        cur = conn.cursor()
        query = f"""
        SELECT ID, {metric}
        FROM UNITED_GLOBAL_EXTENDED
        WHERE COUNTRY = '{country}' AND DATE = '{date}'
        """
        cur.execute(query)
        result = cur.fetchone()
        if result:
            return {"id": result[0], label.lower().replace(" ", "_"): result[1]}
        else:
            raise HTTPException(status_code=404, detail="No data found for the given country and date")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"An error occurred: {e}")
    finally:
        cur.close()
        conn.close()

# Specific route handlers to fetch metrics
@app.post("/fetch-total-cases")
async def fetch_total_cases(request: Request):
    return await fetch_metric("TOTAL_CASES", "Total Cases", request)

@app.post("/fetch-population")
async def fetch_population(request: Request):
    return await fetch_metric("POPULATION", "Population", request)

@app.post("/fetch-vaccinated")
async def fetch_vaccinated(request: Request):
    return await fetch_metric("VACCINATED", "Vaccinated", request)

@app.post("/fetch-unvaccinated")
async def fetch_unvaccinated(request: Request):
    return await fetch_metric("UNVACCINATED", "Unvaccinated", request)

@app.post("/fetch-total-tests")
async def fetch_total_tests(request: Request):
    return await fetch_metric("TOTAL_TESTS", "Total Tests", request)

@app.post("/fetch-total-death")
async def fetch_total_death(request: Request):
    return await fetch_metric("TOTAL_DEATH", "Total Death", request)

# Save a comment to DynamoDB
@app.post("/save-comment")
async def save_comment(request: Request):
    form_data = await request.json()
    datapoint_id = form_data.get('datapoint_id')
    comment = form_data.get('comment')
    user = form_data.get('user')
    
    if not datapoint_id or not comment or not user:
        raise HTTPException(status_code=400, detail="Datapoint ID, User, and Comment are required")

    try:
        timestamp = datetime.utcnow().isoformat()
        comments_table.put_item(
            Item={
                'ID': datapoint_id,
                'TIMESTAMP': timestamp,
                'COMMENT': comment,
                'USER': user
            }
        )
        return {"message": "Comment added successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")

# Retrieve comments from DynamoDB
@app.post("/get-comments")
async def get_comments(request: Request):
    form_data = await request.json()
    datapoint_id = form_data.get('datapoint_id')
    
    if not datapoint_id:
        raise HTTPException(status_code=400, detail="Datapoint ID is required")

    try:
        response = comments_table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('ID').eq(datapoint_id)
        )
        comments = response.get('Items', [])
        return {"comments": comments}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")

# Fetch distinct country data from Snowflake
def get_country_data():
    conn = get_snowflake_connection()
    try:
        query = """
        SELECT DISTINCT COUNTRY
        FROM UNITED_GLOBAL_EXTENDED
        """
        cur = conn.cursor()
        cur.execute(query)
        result = cur.fetchall()
        
        df = pd.DataFrame(result, columns=['COUNTRY'])
        return df
    finally:
        cur.close()
        conn.close()

# Create a world map plot
def plot_world_map(df):
    fig = px.choropleth(df, locations="COUNTRY",
                        locationmode='country names',
                        color_discrete_sequence=["#5e8fc2"],
                        projection="natural earth",
                        title="Country in which the data was observed")
    return fig.to_html(full_html=False)

# Perform clustering on COVID-19 data
def perform_clustering():
    conn = get_snowflake_connection()
    try:
        query = """
        SELECT COUNTRY, TOTAL_CASES, TOTAL_DEATH, POPULATION
        FROM UNITED_GLOBAL_EXTENDED
        WHERE DATE = '2021-12-01'
        """
        cur = conn.cursor()
        
        cur.execute(query)
        result = cur.fetchall()
        
        df = pd.DataFrame(result, columns=['COUNTRY', 'TOTAL_CASES', 'TOTAL_DEATH', 'POPULATION'])
        
        if df.empty:
            return "<p>No data available for clustering.</p>"

        # Clustering algorithm (K-means)
        X = df[['TOTAL_CASES', 'TOTAL_DEATH', 'POPULATION']].values
        kmeans = KMeans(n_clusters=3)
        df['Cluster'] = kmeans.fit_predict(X)

        # Clustering visualization on a world map
        fig = px.choropleth(df, locations="COUNTRY",
                            locationmode='country names',
                            color='Cluster',
                            color_continuous_scale=px.colors.sequential.Viridis,
                            title="COVID-19 Clustering by Country")

        return fig.to_html(full_html=False)
        
    finally:
        cur.close()
        conn.close()

# Generate 2021 plots
def generate_2021_plot():
    conn = get_snowflake_connection()
    try:
        query = """
        SELECT DATE, COUNTRY, POPULATION, VACCINATED, UNVACCINATED, TOTAL_TESTS, TOTAL_CASES, TOTAL_DEATH
        FROM UNITED_GLOBAL_EXTENDED
        WHERE DATE BETWEEN '2021-01-01' AND '2021-12-31'
        ORDER BY DATE ASC
        """
        cur = conn.cursor()
        cur.execute(query)
        result = cur.fetchall()

        df = pd.DataFrame(result, columns=['DATE', 'COUNTRY', 'POPULATION', 'VACCINATED', 'UNVACCINATED', 'TOTAL_TESTS', 'TOTAL_CASES', 'TOTAL_DEATH'])
        df['MORTALITY_RATE'] = (df['TOTAL_DEATH'] / df['TOTAL_CASES']) * 100
        df['VACCINATION_RATE'] = (df['VACCINATED'] / df['POPULATION']) * 100
        df['UNVACCINATION_RATE'] = (df['UNVACCINATED'] / df['POPULATION']) * 100

        # Create a bar chart for 2021 data
        fig2021 = px.bar(df, x='DATE', y=['VACCINATION_RATE', 'UNVACCINATION_RATE'], 
                         title='Demographic Breakdown: Vaccinated vs Unvaccinated (2021)',
                         labels={'value': 'Percentage', 'variable': 'Group'})

        return fig2021.to_html(full_html=False)

    finally:
        cur.close()
        conn.close()

# Generate total cases for all countries in 2021
def generate_total_cases_all_countries_2021():
    conn = get_snowflake_connection()
    try:
        query = """
        SELECT DATE, COUNTRY, SUM(TOTAL_CASES) as TOTAL_CASES
        FROM UNITED_GLOBAL_EXTENDED
        WHERE DATE BETWEEN '2021-01-01' AND '2021-12-31'
        GROUP BY DATE, COUNTRY
        ORDER BY DATE ASC
        """
        cur = conn.cursor()
        cur.execute(query)
        result = cur.fetchall()

        df = pd.DataFrame(result, columns=['DATE', 'COUNTRY', 'TOTAL_CASES'])

        # Create the line plot for all countries
        fig_all_countries = px.line(df, x='DATE', y='TOTAL_CASES', color='COUNTRY', 
                                    title='Countries with total cases over time')
        return fig_all_countries.to_html(full_html=False)

    finally:
        cur.close()
        conn.close()

# Generate total cases in Latvia in 2021
def generate_total_cases_latvia_2021():
    conn = get_snowflake_connection()
    try:
        query = """
        SELECT DATE, SUM(TOTAL_CASES) as TOTAL_CASES
        FROM UNITED_GLOBAL_EXTENDED
        WHERE COUNTRY = 'Latvia' AND DATE BETWEEN '2021-01-01' AND '2021-12-31'
        GROUP BY DATE
        ORDER BY DATE ASC
        """
        cur = conn.cursor()
        cur.execute(query)
        result = cur.fetchall()

        df = pd.DataFrame(result, columns=['DATE', 'TOTAL_CASES'])

        # Create the line plot for Latvia
        fig_latvia_2021 = px.line(df, x='DATE', y='TOTAL_CASES', 
                                  title='Total cases in Latvia over time')
        return fig_latvia_2021.to_html(full_html=False)

    finally:
        cur.close()
        conn.close()

# Generate various plots including the world map
def generate_plots():
    conn = get_snowflake_connection()
    try:
        query = """
        SELECT DATE, COUNTRY, POPULATION, VACCINATED, UNVACCINATED, TOTAL_TESTS, TOTAL_CASES, TOTAL_DEATH
        FROM UNITED_GLOBAL_EXTENDED
        WHERE COUNTRY = 'Latvia'
        ORDER BY DATE ASC
        """
        cur = conn.cursor()
        cur.execute(query)
        result = cur.fetchall()
        
        df = pd.DataFrame(result, columns=['DATE', 'COUNTRY', 'POPULATION', 'VACCINATED', 'UNVACCINATED', 'TOTAL_TESTS', 'TOTAL_CASES', 'TOTAL_DEATH'])

        df['MORTALITY_RATE'] = (df['TOTAL_DEATH'] / df['TOTAL_CASES']) * 100
        df['VACCINATION_RATE'] = (df['VACCINATED'] / df['POPULATION']) * 100
        df['UNVACCINATION_RATE'] = (df['UNVACCINATED'] / df['POPULATION']) * 100

        # Creating the demographic breakdown plot
        fig4 = px.bar(df, x='DATE', y=['VACCINATION_RATE', 'UNVACCINATION_RATE'],
                      title='Demographic Breakdown: Vaccinated vs Unvaccinated',
                      labels={'value': 'Percentage', 'variable': 'Group'})

        # Generate the world map plot
        country_df = get_country_data()
        fig_world_map = plot_world_map(country_df)

        graph4 = fig4.to_html(full_html=False)

        return fig_world_map, graph4
    finally:
        cur.close()
        conn.close()

# Route to render visual.html and generate visualizations
@app.get("/visual", response_class=HTMLResponse)
async def read_visual(request: Request):
    clustering_graph = perform_clustering()  # Clustering graph
    graphs = generate_plots()
    plot_2021 = generate_2021_plot()  # 2021 plot
    plot_all_countries_2021 = generate_total_cases_all_countries_2021()  # Total cases in all countries (2021)
    plot_latvia_2021 = generate_total_cases_latvia_2021()  # Total cases in Latvia (2021)

    return templates.TemplateResponse("visual.html", {
        "request": request,
        "world_map": graphs[0],
        "clustering_graph": clustering_graph,
        "plot_2021": plot_2021,
        "plot_all_countries_2021": plot_all_countries_2021,
        "plot_latvia_2021": plot_latvia_2021,
    })

# Entry point for running the app
if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000)
