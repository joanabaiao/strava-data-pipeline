from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from extract import extract
from transform import transform
from load import load

##############################################
# Task functions
##############################################
def extract_task_function(**kwargs):
    df_activities = extract()
    kwargs['ti'].xcom_push(key='dataframe', value=df_activities) # Push the DataFrame to XCom
    
    print(f"Activities DataFrame:\n{df_activities.head()}")

def transform_task_function(**kwargs):
    ti = kwargs['ti']
    
    # Pull the DataFrame from XCom
    df_activities = ti.xcom_pull(task_ids='extract_strava_data', key='dataframe')
    
    # Transform step
    df_activities_transformed = transform(df_activities)
    print(f"Transformed DataFrame:\n{df_activities_transformed.head()}")
        
    # Push the transformed DataFrame to XCom
    ti.xcom_push(key='dataframe', value=df_activities_transformed)
    
def load_task_function(**kwargs):
    ti = kwargs['ti']
    
    # Pull the transformed DataFrame from XCom
    df_activities_transformed = ti.xcom_pull(task_ids='transform_strava_data', key='dataframe')
    
    if df_activities_transformed is not None:
        load(df_activities_transformed)
        print("LOADED")
    else:
        print("No data retrieved from XCom")
     

##############################################
# Define DAG
##############################################

# Define the default arguments
default_args = {
    'owner': 'joana',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='strava_pipeline_DAG',
    default_args=default_args,
    schedule_interval='@weekly',  # Adjust the schedule interval as needed
    start_date=datetime(2024, 8, 1),
) as dag:
    
    task_create_tables = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id='postgres_localhost',
        sql='sql/schema.sql'
    )
    
    task_extract = PythonOperator(
        task_id='extract_strava_data',
        python_callable=extract_task_function,
    )
    
    task_transform = PythonOperator(
        task_id='transform_strava_data',
        python_callable=transform_task_function,
    )
    
    task_load = PythonOperator(
        task_id='load_strava_data',
        python_callable=load_task_function,
    )

    task_create_tables >> task_extract >> task_transform >> task_load
  