import pandas as pd 
import ast
import json
from extract import extract

def check_data_quality(data):
    
    # Check if DataFrame is empty
    if data.empty:
        print('No activities extracted!')
        return False

    # Check for uniqueness of the 'id' column
    if pd.Series(data['id']).is_unique:
        print("All values in 'id' are unique. Data integrity is maintained.")
        return True
    else:
        print("Primary Key Exception: Duplicate values found in 'id' column.")
        return False
  
    
def convert_to_list(element):
    if element is None:
        return []
    if isinstance(element, str):
        try:
            element = ast.literal_eval(element)
        except (ValueError, SyntaxError):
            return []
    return element

def array_to_json(coord_array):
    
    if len(coord_array) < 2:
        return json.dumps({"lat": None, "lng": None})
    else:
        try:
            lat = float(coord_array[0])
            lng = float(coord_array[1])
        except (ValueError, TypeError):
            return json.dumps({"lat": None, "lng": None})
    
    return json.dumps({"lat": lat, "lng": lng})
    

def process_data(data):
    
    # Convert array to JSON
    for col in ['start_latlng', 'end_latlng']:
        data[col] = data[col].apply(convert_to_list)
        data[col] = data[col].apply(array_to_json)
    
    # Convert dates to date format
    data['start_date'] = pd.to_datetime(data['start_date']).dt.tz_localize(None)
    data['start_date_local'] = pd.to_datetime(data['start_date_local']).dt.tz_localize(None)
    print("dates renamed")
    
    # Rename columns
    columns_to_rename = {
        'athlete.id': 'athlete_id',
        'athlete.resource_state': 'athlete_resource_state',
        'map.id': 'map_id',
        'map.summary_polyline': 'map_summary_polyline',
        'map.resource_state': 'map_resource_state'
    }
    data.rename(columns=columns_to_rename, inplace=True)
    print("Columns renamed")
    
    print(data.info())

    return data


########################################################

def transform(df_activities):
    
    print(f"DataFrame:\n{df_activities.head()}")
    
    if check_data_quality(df_activities):
        print("PASSED CHECK")
        df_activities_transformed = process_data(df_activities)
        print(f"Transformed DataFrame:\n{df_activities_transformed.head()}")
        
        return df_activities_transformed

    else:
        print("Data quality check failed.")
        return pd.DataFrame() 


if __name__ == "__main__":
    
    df_activities = extract()
    
    if check_data_quality(df_activities):
        data = process_data(df_activities)
        
