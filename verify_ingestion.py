
import pandas as pd
import os
from data_ingestion import load_raw_data

def create_sample_csv(filename):
    data = {
        'district_id': ['D001', 'D001', 'D002', None],
        'aadhaar_id': ['UID001', 'UID002', 'UID003', 'UID004'],
        'biometric_update_flag': [0, 1, 0, 1],
        'demographic_update_flag': [1, 0, 1, 0],
        'last_update_date': ['2023-01-01', '2023-01-02', 'invalid-date', '2023-01-04'],
        'total_aadhaar_holders': [100, 100, 200, 200]
    }
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    print(f"Created sample CSV: {filename}")

def run_verification():
    filename = "test_data.csv"
    create_sample_csv(filename)
    
    print("\n--- Running load_raw_data ---")
    df, metadata = load_raw_data(filename)
    
    print("\n--- Metadata ---")
    for k, v in metadata.items():
        print(f"{k}: {v}")
        
    print("\n--- DataFrame Head ---")
    print(df.head())
    
    print("\n--- Malformed Records ---")
    if 'is_malformed' in df.columns:
        malformed = df[df['is_malformed'] == True]
        print(malformed)
        assert len(malformed) > 0, "Expected malformed records but found none"
    else:
        print("Error: 'is_malformed' column missing")
        
    # Validation assertions
    assert not df.empty, "DataFrame should not be empty"
    assert metadata['row_count'] == 4, f"Expected 4 rows, got {metadata['row_count']}"
    assert metadata['malformed_records_count'] > 0, "Should have detected malformed records"
    
    print("\nVerification Passed!")
    
    # Cleanup
    if os.path.exists(filename):
        os.remove(filename)

if __name__ == "__main__":
    run_verification()
