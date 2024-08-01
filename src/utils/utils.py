import yaml
import pandas as pd
from datetime import datetime
import requests
import time
import psycopg2

def load_config_yaml(file_path="config/config.yaml"):

    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
        
    return config


def get_access_token(config):
    
    payload = {
    'client_id': config['strava']['client_id'],
    'client_secret': config['strava']['client_secret'],
    'refresh_token': config['strava']['refresh_token'],
    'grant_type': "refresh_token",
    'f': 'json'
    }
    
    res = requests.post(config['strava']['auth_url'], data=payload, verify=False)
    access_token = res.json()['access_token']

    return access_token


def save_data_to_csv(df):
    """Save extracted data to .csv file."""
    
    today = datetime.today().strftime("%d_%m_%Y")
    #export_file_path = f"data/strava_data_{today}.csv"
    export_file_path = f"strava_data_{today}.csv"
    
    df.to_csv(export_file_path, index=False)
    
    print(f"Data saved to {export_file_path}")
    

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
