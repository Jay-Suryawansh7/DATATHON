
import pandas as pd
import glob
import os

def extract_for_infographic():
    # 1. Load the Final Ranked Data
    try:
        df_final = pd.read_csv("final_output_real/final_ranked_districts.csv")
    except FileNotFoundError:
        print("Error: Could not find final_output_real/final_ranked_districts.csv")
        return

    # 2. We need 'State' mapping. 
    # The raw data has 'district' and 'state' columns. Let's build a mapping.
    print("Building District-State mapping from raw data...")
    district_state_map = {}
    
    # Scan all CSVs in data/ folder to find district-state pairs
    all_files = glob.glob("data/*.csv")
    for f in all_files:
        try:
            temp_df = pd.read_csv(f, usecols=['district', 'state'])
            # Drop duplicates to speed up
            temp_df = temp_df.drop_duplicates()
            # Update map
            for _, row in temp_df.iterrows():
                # specific to our pipeline, district_id in output matches 'district' in raw
                district_state_map[str(row['district'])] = row['state']
        except ValueError:
            # File might not have 'state' column (e.g. some might be different)
            continue
        except Exception as e:
            print(f"Skipping {f}: {e}")
            continue

    # 3. Apply Mapping
    df_final['State'] = df_final['district_id'].map(district_state_map)
    
    # Fill missing states if any
    df_final['State'] = df_final['State'].fillna('Unknown')

    # 4. Select and Rename Columns for Infographic
    # Target: Rank, District, State, Biometric Staleness Index, Camp Priority, Recommended Camp Type, Deployment Frequency
    
    output_df = df_final[[
        'cps_rank',
        'district_id',
        'State',
        'bsi_score',
        'cps_score',
        'camp_type',
        'deployment_freq_days'
    ]].copy()
    
    output_df.rename(columns={
        'cps_rank': 'Rank',
        'district_id': 'District',
        'bsi_score': 'Biometric Staleness Index',
        'cps_score': 'Camp Priority Score',
        'camp_type': 'Recommended Camp Type',
        'deployment_freq_days': 'Deployment Frequency'
    }, inplace=True)
    
    # Sort just to be sure
    output_df = output_df.sort_values('Rank')
    
    # 5. Save
    output_path = "infographic_data.csv"
    output_df.to_csv(output_path, index=False)
    print(f"Successfully created {output_path} with {len(output_df)} rows.")
    print("Top 5 rows:")
    print(output_df.head().to_string(index=False))

if __name__ == "__main__":
    extract_for_infographic()
