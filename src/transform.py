import pandas as pd 
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
    
    
def process_data(data):
    
    data = data.copy()
    
    # Drop duplicate rows
    data = data.drop_duplicates()
    
    # Convert dates to date format
    data['start_date'] = pd.to_datetime(data['start_date'])
    data['start_date_local'] = pd.to_datetime(data['start_date_local'])

    return data


########################################################

def transform(all_activities):
    
    data = pd.json_normalize(all_activities)
    
    if all_activities:
        data = process_data(data)
        
    return data

if __name__ == "__main__":
    
    all_activities = extract()
    data = pd.json_normalize(all_activities)
    
    if check_data_quality(data):
        data = process_data(data)
        
