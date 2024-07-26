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
