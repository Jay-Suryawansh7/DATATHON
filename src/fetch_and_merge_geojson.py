
import pandas as pd
import requests
import json
import os

# --- Configuration ---
# Official India Districts GeoJSON (Alternative Source)
GEOJSON_URL = "https://raw.githubusercontent.com/geohacker/india/master/district/india_district.geojson"
INPUT_CSV_PATH = "infographic_data.csv"
OUTPUT_GEOJSON_PATH = "india_districts_final.geojson"

def fetch_and_merge():
    # 1. Load your local data
    print(f"Loading data from {INPUT_CSV_PATH}...")
    try:
        df = pd.read_csv(INPUT_CSV_PATH)
    except FileNotFoundError:
        print(f"Error: {INPUT_CSV_PATH} not found. Make sure you are in the project root.")
        return

    # 2. Download the GeoJSON
    print(f"Downloading India District Map from {GEOJSON_URL}...")
    try:
        response = requests.get(GEOJSON_URL)
        response.raise_for_status()
        geo_data = response.json()
    except Exception as e:
        print(f"Error downloading map: {e}")
        return

    print("Map downloaded successfully. Merging data...")

    # 3. Merge Data into GeoJSON Properties
    # We will try to match on District Name. 
    # Note: Names might differ slightly (e.g., "Bengaluru Urban" vs "Bangalore Urban").
    # This script does a basic exact match. You can enhance mapping if needed.
    
    # Create a lookup dictionary from the dataframe
    # Key: Lowercase district name for better matching
    data_lookup = {}
    for _, row in df.iterrows():
        # Standardize key
        key = str(row['District']).strip().lower()
        data_lookup[key] = row.to_dict()

    matched_count = 0
    total_features = len(geo_data['features'])

    for feature in geo_data['features']:
        props = feature['properties']
        # Debug: Print keys of first feature to ensure we have the right column
        if total_features > 0 and feature == geo_data['features'][0]:
            print(f"GeoJSON Properties Keys: {list(props.keys())}")
        
        # Try to find the name column dynamically
        possible_keys = ['DISTRICT', 'dtname', 'NAME_2', 'district', 'name']
        dist_name = ""
        for k in possible_keys:
            if k in props:
                dist_name = props[k]
                break
        
        # Debug: Print first 5 names found
        if matched_count < 5 and total_features > 0 and feature['properties'] == geo_data['features'][matched_count]['properties']: 
             # Just printing names from the loop roughly
             pass

        dist_name_clean = str(dist_name).strip().lower()
        
        if dist_name_clean in data_lookup:
            # Merge our data columns into the GeoJSON properties
            # This embeds the stats directly into the map shape
            feature['properties'].update(data_lookup[dist_name_clean])
            feature['properties']['has_data'] = True
            matched_count += 1
        else:
            feature['properties']['has_data'] = False
            # Optional: Add empties so tools don't complain
            for col in df.columns:
                if col not in feature['properties']:
                    feature['properties'][col] = None

    print(f"Merge Complete. Matched {matched_count} out of {total_features} map districts.")
    
    # 4. Save the Result
    with open(OUTPUT_GEOJSON_PATH, 'w') as f:
        json.dump(geo_data, f)
    
    print(f"Success! GeoJSON saved to: {OUTPUT_GEOJSON_PATH}")
    print("Upload this .geojson file to Power BI, Tableau, or Gemini to visualize your filled map.")

if __name__ == "__main__":
    fetch_and_merge()
