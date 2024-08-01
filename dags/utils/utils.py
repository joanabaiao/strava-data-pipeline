import yaml
import pandas as pd
from datetime import datetime


def load_config_yaml(file_path):

    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
        
    return config


def save_data_to_csv(df_activities):
    """Save extracted data to .csv file."""
    
    today = datetime.today().strftime("%d_%m_%Y")
    export_file_path = f"dags/data/strava_data_{today}.csv"
    
    df_activities.to_csv(export_file_path, index=False)
    
    print(f"Data saved to {export_file_path}")