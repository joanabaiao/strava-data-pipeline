import requests
import time


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


def extract_all_strava_activities(activities_url, header, activities_per_page):
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
            
            # Append the new activities to the all_activities list
            all_activities.extend(activities)
            print(f"Added {len(activities)} activities. Total activities: {len(all_activities)}")
            
            page_number += 1
        
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            break

    return all_activities