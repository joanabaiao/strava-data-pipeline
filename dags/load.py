import pandas as pd
import numpy as np
import json
from datetime import datetime

from utils.postgresql_utils import * 
from utils.utils import *
from psycopg2.extras import execute_values

from airflow.hooks.base_hook import BaseHook
from airflow.hooks.postgres_hook import PostgresHook


def insert_dataframe_to_postgres(table_name, data, conn, cur):
    
    try:
        tuples = [tuple(x) for x in data.to_numpy()] 
        cols = ','.join(list(data.columns)) 
        query = "INSERT INTO %s(%s) VALUES %%s" % (table_name, cols) 
        
        # Execute the insert statement
        execute_values(cur, query, tuples) 
        conn.commit()
        print(f"Data inserted into {table_name} successfully.")
        
    except Exception as e:
        print(f"An error occurred while inserting data into {table_name}: {e}")
        conn.rollback()


    
def insert_current_extraction_date(conn, cur):
    
    try:
        current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        query = "INSERT INTO last_extracted (LastUpdated) VALUES (%s);"
        
        cur.execute(query, (current_date,))
        conn.commit()
        print(f"Inserted current date {current_date} into last_extracted table.")
        
    except Exception as e:
        print(f"An error occurred while inserting the date into last_extracted table: {e}")
        conn.rollback()
        
        
########################################################

def load(data):
    
    table_name = "activities"
    
    try: 
        # Establish database connection
        pg_hook = PostgresHook(postgres_conn_id='postgres_localhost')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # Insert DataFrame (activities) data
        insert_dataframe_to_postgres(table_name, data, conn, cursor)

        # Insert current date
        insert_current_extraction_date(conn, cursor)
        
    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback() 
        
    finally:
        cursor.close()
        conn.close()
        print("Database connection closed.")
    
    
    