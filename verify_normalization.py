
import pandas as pd
import numpy as np
from feature_normalization import normalize_features

def run_verification():
    print("Creating mock feature data for normalization test...")
    
    # Mock Data: 3 districts with simple values to verify scaling
    # Feature X: 0, 50, 100 -> Should become 0.0, 0.5, 1.0
    data = {
        'district_id': ['D1', 'D2', 'D3'],
        'days_since_last_update': [0, 50, 100],
        'years_since_last_update': [0, 1, 2], # Just dummy linear
        'biometric_coverage_ratio': [0.1, 0.5, 0.9],
        'population_impact_score': [10, 20, 30] # Random range
    }
    df = pd.DataFrame(data)
    
    print("\nInput Data:")
    print(df)
    
    print("\nRunning Normalization...")
    norm_df = normalize_features(df)
    
    print("\nNormalized Dataframe Head:")
    print(norm_df.head())
    
    # Assertions
    
    # Check Column Existence
    assert 'days_since_last_update_norm' in norm_df.columns, "Missing norm column"
    assert 'days_since_last_update' in norm_df.columns, "Original column removed"
    
    # Check Scaling Logic
    # D1 (0) -> 0.0
    # D2 (50) -> 0.5
    # D3 (100) -> 1.0
    
    col = 'days_since_last_update_norm'
    assert norm_df.loc[0, col] == 0.0, f"D1 {col} should be 0.0"
    assert norm_df.loc[1, col] == 0.5, f"D2 {col} should be 0.5"
    assert norm_df.loc[2, col] == 1.0, f"D3 {col} should be 1.0"
    
    # Check range constraints
    for col in norm_df.columns:
        if col.endswith('_norm'):
            assert norm_df[col].min() >= 0.0, f"{col} min < 0"
            assert norm_df[col].max() <= 1.0 + 1e-9, f"{col} max > 1" # float tolerance
            
    print("\nVerification Passed!")

if __name__ == "__main__":
    run_verification()
