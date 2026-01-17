
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

def generate_mock_dataset(filepath, num_districts=50, records_per_district=20):
    print(f"Generating mock dataset: {num_districts} districts, ~{records_per_district} records each...")
    
    data = []
    today = datetime.now()
    
    for i in range(1, num_districts + 1):
        d_id = f"D{i:03d}"
        
        # Randomly assign a profile to the district
        # 0: Good (Recent updates, high coverage)
        # 1: Average
        # 2: Bad (Old updates, low coverage)
        profile = random.choice([0, 1, 2])
        
        for j in range(records_per_district):
            u_id = f"UID_{d_id}_{j:03d}"
            
            # Update Flags & Dates based on profile
            if profile == 0:
                bio_flag = 1 if random.random() > 0.1 else 0
                days_ago = random.randint(0, 30)
            elif profile == 1:
                bio_flag = 1 if random.random() > 0.5 else 0
                days_ago = random.randint(30, 365)
            else:
                bio_flag = 0 # No biometric updates
                days_ago = random.randint(365, 1000)
            
            # Demographic updates
            demo_flag = 1 if random.random() > 0.7 else 0
            
            # Date
            if bio_flag == 1 or demo_flag == 1:
                date_val = (today - timedelta(days=days_ago)).strftime('%Y-%m-%d')
            else:
                date_val = None # No update ever
            
            # Add noise (Malformed data)
            if random.random() < 0.01:
                date_val = "invalid-date"
            
            row = {
                'district_id': d_id,
                'aadhaar_id': u_id,
                'biometric_update_flag': bio_flag,
                'demographic_update_flag': demo_flag,
                'last_update_date': date_val,
                'total_aadhaar_holders': records_per_district # simplified
            }
            data.append(row)
            
    df = pd.DataFrame(data)
    df.to_csv(filepath, index=False)
    print(f"Saved to {filepath}")

if __name__ == "__main__":
    generate_mock_dataset("full_mock_data.csv")
