import psycopg2
from datetime import datetime

def connect_postgresql(config):
    
    conn = psycopg2.connect(database = config['postgresql']['database'],
                            host = config['postgresql']['host'],
                            user = config['postgresql']['user'],
                            password = config['postgresql']['password'],
                            port = config['postgresql']['port'])
    
    if conn is None:
        print("Error connecting to the PostgreSQL database")
    else:
        print("MySQL connection established!")
        
    return conn

 
        
