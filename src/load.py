import pandas as pd
from datetime import datetime

from utils.postgresql_utils import * 
from utils.utils import *
import psycopg2.extras as extras 


def insert_dataframe_to_postgres(table_name, data, conn, cur):
    
    try:
        tuples = [tuple(x) for x in data.to_numpy()] 
        cols = ','.join(list(data.columns)) 
        query = "INSERT INTO %s(%s) VALUES %%s" % (table_name, cols) 
        
        # Execute the insert statement
        extras.execute_values(cur, query, tuples) 
        conn.commit()
        print(f"Data inserted into {table_name} successfully.")
        
    except Exception as e:
        print(f"An error occurred while inserting data into {table_name}: {e}")
        conn.rollback()


    
def insert_current_extraction_date(config, conn, cur):
    
    try:
        current_date = datetime.now().strftime('%Y-%m-%d')
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
        config = load_config_yaml()
        conn = connect_postgresql(config)
        cur = conn.cursor()
        
        # Insert DataFrame (activities) data
        insert_dataframe_to_postgres(table_name, data, conn, cur)

        # Insert current date
        insert_current_extraction_date(config, conn, cur)
        
    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()  # Rollback any changes if an error occurs
        
    finally:
        cur.close()
        conn.close()
        print("Database connection closed.")
    
    
    