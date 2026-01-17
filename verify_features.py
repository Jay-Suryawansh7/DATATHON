
import pandas as pd
import numpy as np
from feature_engineering import feature_engineer

def run_verification():
    print("Creating mock district data for feature test...")
    today = pd.Timestamp.now()
    
    # Mock District Data
    # D1: Perfect coverage, recent update.
    # D2: Zero coverage, never updated.
    # D3: 50% coverage, old update.
    
    data = {
        'district_id': ['D1', 'D2', 'D3'],
        'total_aadhaar_holders': [1000, 1000, 1000],
        'total_biometric_updates': [1000, 0, 500],
        'total_demographic_updates': [500, 100, 200],
        'last_biometric_update_date': [
            today - pd.Timedelta(days=10), # 10 days ago
            pd.NaT,                        # Never
            today - pd.Timedelta(days=365) # 1 year ago
        ]
    }
    district_df = pd.DataFrame(data)
    
    print("\nInput District Data:")
    print(district_df)
    
    print("\nRunning Feature Engineering...")
    feat_df = feature_engineer(district_df)
    
    print("\nFeature Dataframe Head:")
    print(feat_df.T) # Transpose for readability
    
    # Assertions
    
    # Check D1 (Good case)
    row_d1 = feat_df[feat_df['district_id'] == 'D1'].iloc[0]
    assert row_d1['biometric_coverage_ratio'] == 1.0, "D1 coverage should be 1.0"
    assert row_d1['days_since_last_update'] == 10, "D1 days since should be 10"
    assert row_d1['uncovered_population'] == 0, "D1 uncovered should be 0"
    
    # Check D2 (Bad case: No updates, NaT date)
    row_d2 = feat_df[feat_df['district_id'] == 'D2'].iloc[0]
    assert row_d2['biometric_coverage_ratio'] == 0.0, "D2 coverage should be 0.0"
    assert row_d2['days_since_last_update'] > 365, "D2 days since should be high (imputed)"
    assert row_d2['uncovered_population'] == 1000, "D2 uncovered should be 1000"
    assert row_d2['demographic_to_biometric_ratio'] == 0.0, "D2 demo/bio ratio should be 0 (handled edge case)"
    
    # Check D3 (Middle case)
    row_d3 = feat_df[feat_df['district_id'] == 'D3'].iloc[0]
    assert row_d3['biometric_coverage_ratio'] == 0.5, "D3 coverage should be 0.5"
    assert 364 <= row_d3['days_since_last_update'] <= 366, "D3 days since should be approx 365"
    
    # Check Ranking
    # D1 (10 days) should have rank 1. D3 (365 days) rank 2. D2 (imputed large) rank 3.
    assert row_d1['update_recency_rank'] == 1, "D1 should be rank 1"
    assert row_d2['update_recency_rank'] == 3, "D2 should be rank 3 (worst)"
    
    print("\nVerification Passed!")

if __name__ == "__main__":
    run_verification()
