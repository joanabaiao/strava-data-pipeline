import yaml
import pandas as pd
import datetime


def load_config_yaml(filepath='config.yaml'):
    with open(filepath, 'r') as file:
        config = yaml.safe_load(file)
        
    return config


def save_data_to_csv(all_activities):
    """Save extracted data to .csv file."""
    
    today = datetime.today().strftime("%d_%m_%Y")
    export_file_path = f"data/strava_data_{today}.csv"
    
    df = pd.json_normalize(all_activities)
    df.to_csv(export_file_path, index=False)
    
    print(f"Data saved to {export_file_path}")