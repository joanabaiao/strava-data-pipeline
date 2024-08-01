import pandas as pd
from datetime import datetime

from utils.utils import *

def get_last_updated_date(config):
    
    query = """
        SELECT COALESCE(MAX(LastUpdated), '1900-01-01')
        FROM last_extracted;
    """
    
    try:
        conn = connect_postgresql(config)
        cur = conn.cursor()
        cur.execute(query)
        
        result = cur.fetchone()
        last_updated_date = result[0] if result else '1900-01-01'
        last_updated_date = last_updated_date.strftime("%Y-%m-%d %H:%M:%S")

        cur.close()
        conn.close()
        
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
    config = load_config_yaml()
    access_token = get_access_token(config)
    
    header = {'Authorization': 'Bearer ' + access_token}
    activities_per_page = 200
    activities_url = config['strava']['activities_url']
    
    last_updated_date = get_last_updated_date(config)
    all_activities = extract_strava_activities(activities_url, header, activities_per_page, last_updated_date)
    
    if not all_activities:
        print("No new activities found.")
        return pd.DataFrame() 
    
    df_activities = pd.json_normalize(all_activities)
    save_data_to_csv(df_activities)
    
    return df_activities
    

if __name__ == "__main__": 
    extract()
        
