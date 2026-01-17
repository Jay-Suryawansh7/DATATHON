
import pandas as pd
from scoring_cps import compute_camp_priority_score

def run_verification():
    print("Creating mock data for CPS test...")
    
    # Formula: 0.5*BSI + 0.3*Pop + 0.2*LowFreq
    # LowFreq = 1 - consistency
    
    # Case 1: Max Priority (Tier 1)
    # BSI=1.0, Pop=1.0, Cons=0.0 (LowFreq=1.0)
    # CPS = 0.5 + 0.3 + 0.2 = 1.0 -> 100
    
    # Case 2: Mid Priority (Tier 3)
    # BSI=0.6, Pop=0.5, Cons=0.5 (LowFreq=0.5)
    # CPS = 0.3 + 0.15 + 0.1 = 0.55 -> 55
    
    # Case 3: Low Priority (Tier 5)
    # BSI=0.1, Pop=0.1, Cons=0.9 (LowFreq=0.1)
    # CPS = 0.05 + 0.03 + 0.02 = 0.1 -> 10
    
    data = {
        'district_id': ['D_Max', 'D_Mid', 'D_Low'],
        'bsi_score': [1.0, 0.6, 0.1],
        'adult_population_proxy_norm': [1.0, 0.5, 0.1],
        'update_consistency_norm': [0.0, 0.5, 0.9]
    }
    df = pd.DataFrame(data)
    
    print("\nRunning CPS Computation...")
    cps_df = compute_camp_priority_score(df)
    
    # Assertions
    
    # Max Case
    row_max = cps_df[cps_df['district_id'] == 'D_Max'].iloc[0]
    assert row_max['cps_score'] >= 99.9, f"Max score error: {row_max['cps_score']}"
    assert row_max['cps_tier'] == 'Tier 1', "Tier 1 mismatch"
    assert row_max['is_top_20'] == True, "Flag mismatch"
    
    # Mid Case
    row_mid = cps_df[cps_df['district_id'] == 'D_Mid'].iloc[0]
    assert 54.9 <= row_mid['cps_score'] <= 55.1, f"Mid score error: {row_mid['cps_score']}"
    assert row_mid['cps_tier'] == 'Tier 3', f"Tier 3 mismatch: Got {row_mid['cps_tier']}"
    
    # Low Case
    row_low = cps_df[cps_df['district_id'] == 'D_Low'].iloc[0]
    assert row_low['cps_score'] <= 10.1, f"Low score error: {row_low['cps_score']}"
    assert row_low['cps_tier'] == 'Tier 5', "Tier 5 mismatch"
    
    # Sorting
    assert cps_df.iloc[0]['district_id'] == 'D_Max', "Sorting error"
    
    print("\nVerification Passed!")

if __name__ == "__main__":
    run_verification()
