
import pandas as pd
from strategy_recommendation import recommend_camp_strategy

def run_verification():
    print("Creating mock data for Strategy test...")
    
    # Case 1: Intensive / High Suitability
    # CPS=90 (>=85), Pop=0.8 (>0.6), Gap=0.4 (>0.3)
    
    # Case 2: Monthly / Medium Suitability
    # CPS=60 (55-69), Pop=0.5 (0.4-0.6), Gap=0.25 (0.2-0.3)
    
    # Case 3: Annual / Low Suitability
    # CPS=20 (<40), Pop=0.1 (<0.4), Gap=0.1 (<0.2)
    
    data = {
        'district_id': ['D1', 'D2', 'D3'],
        'cps_score': [90, 60, 20],
        'population_impact_score_norm': [0.8, 0.5, 0.1],
        'biometric_coverage_gap_norm': [0.4, 0.25, 0.1]
    }
    df = pd.DataFrame(data)
    
    print("\nRunning Strategy Recommendation...")
    strat_df = recommend_camp_strategy(df)
    
    # Assertions
    
    # Case 1
    row1 = strat_df[strat_df['district_id'] == 'D1'].iloc[0]
    assert row1['camp_type'] == 'INTENSIVE', f"D1 type mismatch: {row1['camp_type']}"
    assert row1['deployment_freq_days'] == '7-10 days', "D1 freq mismatch"
    assert row1['location_suitability'] == 'High Suitability', "D1 suit mismatch"
    assert "CPS 90" in row1['strategy_reasoning'], "Reasoning missing score"
    
    # Case 2
    row2 = strat_df[strat_df['district_id'] == 'D2'].iloc[0]
    assert row2['camp_type'] == 'MONTHLY_MOBILE', f"D2 type mismatch: {row2['camp_type']}"
    assert row2['location_suitability'] == 'Medium Suitability', "D2 suit mismatch"
    
    # Case 3
    row3 = strat_df[strat_df['district_id'] == 'D3'].iloc[0]
    assert row3['camp_type'] == 'ANNUAL_PREVENTIVE', f"D3 type mismatch: {row3['camp_type']}"
    assert row3['location_suitability'] == 'Low Suitability', "D3 suit mismatch"
    
    print("\nVerification Passed!")

if __name__ == "__main__":
    run_verification()
