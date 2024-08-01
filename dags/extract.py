import pandas as pd
from datetime import datetime
import requests
import time

from utils.postgresql_utils import * 
from utils.utils import *
from utils.strava_utils import * 

from sqlalchemy import create_engine, text
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.postgres_hook import PostgresHook

import yaml

def get_last_updated_date():
    
    query = """
        SELECT COALESCE(MAX(LastUpdated), '1900-01-01')
        FROM last_extracted;
    """
    
    try:
        conn_id = 'postgres_localhost'
        hook = PostgresHook(postgres_conn_id=conn_id)
        connection = hook.get_conn()
        cursor = connection.cursor()
        
        cursor.execute(query)
        result = cursor.fetchone()
        last_updated_date = result[0]  # Fetch the single value from the result
        last_updated_date = last_updated_date.strftime("%Y-%m-%d %H:%M:%S")

        cursor.close()
        connection.close()
        
        return last_updated_date

    except Exception as e:
        print(f"An error occurred: {e}")


def extract_strava_activities(activities_url, header, activities_per_page, last_updated_date):
    """Connect to Strava API and get all data."""
    
    all_activities = []
    page_number = 1
    
    while True:
        params = {'per_page': activities_per_page, 'page': page_number}
        
        # Strava has a rate limit of 100 requests every 15 mins
        if page_number % 75 == 0:
            print("Rate limit hit, sleeping for 15 minutes...")
            time.sleep(15 * 60)
        
        try:
            response = requests.get(activities_url, headers=header, params=params)
            response.raise_for_status()
            activities = response.json()
            
            if not activities:
                print("No more activities found, breaking out of the loop.")
                break   

            filtered_activities = [
                activity for activity in activities
                if datetime.fromisoformat(activity['start_date'].replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S') > last_updated_date
            ]     
                                 
            # Append the new activities to the all_activities list
            all_activities.extend(filtered_activities)
            print(f"Added {len(filtered_activities)} activities. Total activities: {len(all_activities)}")
            
            page_number += 1
        
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            break

    return all_activities


########################################################

def extract():

    config = load_config_yaml(file_path = f'config/config.yaml')
    access_token = get_access_token(config)
    last_updated_date = get_last_updated_date()
    
    header = {'Authorization': 'Bearer ' + access_token}
    activities_per_page = 200
    activities_url = config['strava']['activities_url']
    all_activities = extract_strava_activities(activities_url, header, activities_per_page, last_updated_date)
    
    if not all_activities:
        print("No new activities found.")
        return pd.DataFrame()  # Return an empty DataFrame if no activities are found
    
    # Convert to DataFrame
    df_activities = pd.json_normalize(all_activities)
    #print(f"Activities DataFrame:\n{df_activities.head()}")
    save_data_to_csv(df_activities)
    
    return df_activities


if __name__ == "__main__": 
    
    extract()
        