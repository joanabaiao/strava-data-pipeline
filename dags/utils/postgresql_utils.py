import psycopg2
from datetime import datetime

from airflow.hooks.postgres_hook import PostgresHook

def connect_postgresql(config):
    
    conn = psycopg2.connect(database = config['postgresql']['database'],
                            host = config['postgresql']['host'],
                            user = config['postgresql']['user'],
                            password = config['postgresql']['password'],
                            port = config['postgresql']['port'])
    
    if conn is None:
        print("Error connecting to the PostgreSQL database")
    else:
        print("PostgreSQL connection established!")
        
    return conn


def connect_postgresql_airflow():

    conn_id = 'postgres_localhost'
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()

    if conn is None:
        print("Error connecting to the PostgreSQL database")
    else:
        print("PostgreSQL connection established!")
        
    return conn

 
        
