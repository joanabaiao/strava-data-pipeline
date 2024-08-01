from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from extract import extract

##############################################
# Task functions
##############################################
def extract_task_function(**kwargs):
    df_activities = extract()
    kwargs['ti'].xcom_push(key='dataframe', value=df_activities) # Push the DataFrame to XCom
    
    print(f"Activities DataFrame:\n{df_activities.head()}")
    
    
    
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
    schedule_interval=None,  # Adjust the schedule interval as needed
    start_date=days_ago(1),
) as dag:
    
    # Define the PythonOperator task
    task_extract = PythonOperator(
        task_id='extract_strava_data',
        python_callable=extract_task_function,
    )
