
import pandas as pd
import os
from data_aggregation import aggregate_to_district_level

def run_verification():
    print("Creating mock data for aggregation test...")
    # Mock Data Setup
    # District 1: 3 users. 2 biometric updates. Dates: 2023-01-01 (bio), 2023-01-05 (demo), 2023-01-10 (bio)
    # Expected: Total holders=3, Bio updates=2, Last bio date=2023-01-10
    
    # District 2: 2 users. 0 biometric updates. Dates: 2023-02-01 (demo)
    # Expected: Total holders=2, Bio updates=0, Last bio date=NaT
    
    data = {
        'district_id': ['D1', 'D1', 'D1', 'D2', 'D2'],
        'aadhaar_id': ['U1', 'U2', 'U3', 'U4', 'U5'],
        'biometric_update_flag': [1, 0, 1, 0, 0],
        'demographic_update_flag': [0, 1, 0, 1, 0],
        'last_update_date': pd.to_datetime([
            '2023-01-01', '2023-01-05', '2023-01-10', 
            '2023-02-01', '2023-02-02'
        ])
    }
    df = pd.DataFrame(data)
    
    print("Input Data:")
    print(df)
    
    print("\nRunning aggregation...")
    agg_df = aggregate_to_district_level(df)
    
    print("Aggregated Data:")
    print(agg_df)
    
    # Assertions
    # Check D1
    row_d1 = agg_df[agg_df['district_id'] == 'D1'].iloc[0]
    assert row_d1['total_aadhaar_holders'] == 3, f"D1 holders mismatch: {row_d1['total_aadhaar_holders']}"
    assert row_d1['total_biometric_updates'] == 2, f"D1 bio updates mismatch: {row_d1['total_biometric_updates']}"
    assert row_d1['last_biometric_update_date'] == pd.Timestamp('2023-01-10'), f"D1 last bio date mismatch: {row_d1['last_biometric_update_date']}"
    
    # Check D2
    row_d2 = agg_df[agg_df['district_id'] == 'D2'].iloc[0]
    assert row_d2['total_biometric_updates'] == 0, "D2 bio updates should be 0"
    assert pd.isna(row_d2['last_biometric_update_date']), "D2 last bio date should be NaT"

    # Check Columns
    expected_cols = [
        'district_id', 'total_aadhaar_holders', 'total_biometric_updates', 
        'total_demographic_updates', 'biometric_coverage_count', 
        'last_biometric_update_date'
    ]
    for col in expected_cols:
        assert col in agg_df.columns, f"Missing column: {col}"
        
    print("\nVerification Passed!")

if __name__ == "__main__":
    run_verification()
