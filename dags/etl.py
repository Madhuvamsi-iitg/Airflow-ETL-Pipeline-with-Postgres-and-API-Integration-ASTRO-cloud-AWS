from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from airflow.utils.dates import days_ago
import json

#define the DAG
with DAG(
    dag_id = 'nasa_apod_postgres',
    start_date = days_ago(1),
    schedule_interval = '@daily',
    catchup = False
) as dag:
    
    ##step1: create a table if it doesnot exist

    @task
    def create_table():
        ##initialize the PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id = 'my_postgres_connection')

        #SQL query to create the table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );



        """
        #exceute the table creation query
        postgres_hook.run(create_table_query)


    ## step2 : Extract the NASA Api data (APOD - Astronomy Picture of the Day)[Extract pipeline]
    ## https://api.nasa.gov/planetary/apod?api_key=SkmA0znEeMzJ3PjEjmBVFiMaUFTQJyyyZPHOUmJc
    extract_apod = SimpleHttpOperator(
        task_id = "extract_apod",
        http_conn_id = 'nasa_api', # Connection ID defined in Airflow for NASA API
        endpoint = 'planetary/apod', # NASA API endpoint for APOD
        method = 'GET',
        data={"api_key":"{{ conn.nasa_api.extra_dejson.api_key}}"}, # Use the API key from the connection
        response_filter = lambda response: response.json() # Convert response to json
    )


    ## step3 : Transform the data [Transform pipeline](pick the information i need to save)
    @task
    def transform_apod_data(response):
        apod_data = {
            'title': response.get('title', ''),
            'explanation': response.get('explanation', ''),
            'url': response.get('url', ''),
            'date': response.get('date', ''),
            'media_type': response.get('media_type', '')
        }
        return apod_data


    #step4 : Load the data into the Postgres database [Load pipeline]
    @task
    def load_data_to_postgres(apod_data):
        #intilize the PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id = 'my_postgres_connection')

        #SQL query to insert the data
        insert_query = """

        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES(%s, %s, %s, %s, %s);
        """
        ## excute the insert query
        postgres_hook.run(insert_query, parameters = (apod_data['title'], apod_data['explanation'], apod_data['url'], apod_data['date'], apod_data['media_type']))

    #step5 : verify the data DBViewer

    #step6: Define the task dependencies
    ## Extract
    create_table() >> extract_apod  ## Ensure the table is create befor extraction
    api_response=extract_apod.output
    ## Transform
    transformed_data=transform_apod_data(api_response)
    ## Load
    load_data_to_postgres(transformed_data)


