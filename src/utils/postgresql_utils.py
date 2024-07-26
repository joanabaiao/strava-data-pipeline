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

 
def insert_current_extraction_date(config):
    
    current_date = datetime.now().strftime('%Y-%m-%d')
    query = """
        INSERT INTO last_extracted (LastUpdated)
        VALUES (%s);
    """
    
    try:
        conn = connect_postgresql(config)
        cur = conn.cursor()
        cur.execute(query, (current_date,))
        conn.commit()
        
        cur.close()
        conn.close()

        print(f"Inserted current date {current_date} into last_extracted table.")
        
    except Exception as e:
        print(f"An error occurred: {e}")
        
        
