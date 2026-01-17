
import pandas as pd
import numpy as np
from scoring_bsi import compute_bsi

def run_verification():
    print("Creating mock normalized data for BSI test...")
    
    # Create 4 districts covering the tiers
    # Formulas: 0.4*T + 0.35*F + 0.25*G
    # F = 1 - update_consistency_norm
    
    # D_Critical (Max Neglect): T=1.0, F=1.0 (cons=0), G=1.0 
    # BSI = 0.4 + 0.35 + 0.25 = 1.0 (Critical)
    
    # D_High: T=0.6, F=0.5 (cons=0.5), G=0.6
    # BSI = 0.24 + 0.175 + 0.15 = 0.565 (High)
    
    # D_Moderate: T=0.3, F=0.3 (cons=0.7), G=0.3
    # BSI = 0.12 + 0.105 + 0.075 = 0.3 (Moderate)
    
    # D_Low: T=0.0, F=0.0 (cons=1.0), G=0.0
    # BSI = 0.0 (Low)
    
    data = {
        'district_id': ['Crit', 'High', 'Mod', 'Low'],
        'days_since_last_update_norm': [1.0, 0.6, 0.3, 0.0],
        'update_consistency_norm': [0.0, 0.5, 0.7, 1.0], # Remember F = 1 - this
        'biometric_coverage_gap_norm': [1.0, 0.6, 0.3, 0.0]
    }
    df = pd.DataFrame(data)
    
    print("\nRunning BSI Computation...")
    bsi_df = compute_bsi(df)
    
    print("\nBSI Result:")
    cols = ['district_id', 'bsi_score', 'bsi_tier']
    print(bsi_df[cols])
    
    # Assertions
    # Check D_Critical
    row_crit = bsi_df[bsi_df['district_id'] == 'Crit'].iloc[0]
    assert row_crit['bsi_score'] >= 0.99, f"Critical BSI mismatch: {row_crit['bsi_score']}"
    assert row_crit['bsi_tier'] == 'Critical', "Tier mismatch: Critical"
    
    # Check D_High
    row_high = bsi_df[bsi_df['district_id'] == 'High'].iloc[0]
    # Expected: 0.565
    assert 0.56 <= row_high['bsi_score'] <= 0.57, f"High BSI mismatch: {row_high['bsi_score']}"
    assert row_high['bsi_tier'] == 'High', "Tier mismatch: High"
    
    # Check D_Mod (0.3)
    row_mod = bsi_df[bsi_df['district_id'] == 'Mod'].iloc[0]
    assert 0.29 <= row_mod['bsi_score'] <= 0.31, f"Mod BSI mismatch: {row_mod['bsi_score']}"
    assert row_mod['bsi_tier'] == 'Moderate', "Tier mismatch: Moderate"
    
    # Check D_Low
    row_low = bsi_df[bsi_df['district_id'] == 'Low'].iloc[0]
    assert row_low['bsi_score'] <= 0.01, f"Low BSI mismatch: {row_low['bsi_score']}"
    assert row_low['bsi_tier'] == 'Low', "Tier mismatch: Low"
    
    # Check Sorting (Should be Crit -> High -> Mod -> Low)
    assert bsi_df.iloc[0]['district_id'] == 'Crit', "Sorting failed"
    assert bsi_df.iloc[3]['district_id'] == 'Low', "Sorting failed"

    print("\nVerification Passed!")

if __name__ == "__main__":
    run_verification()
