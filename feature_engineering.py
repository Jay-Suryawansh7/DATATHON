
import pandas as pd
import numpy as np
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def feature_engineer(district_df: pd.DataFrame) -> pd.DataFrame:
    """
    Derives 20+ analytical indicators from the aggregated district dataset.
    
    Each feature is derived from aggregated district-level data to identify regions 
    where biometric data has become stale or underserved.
    
    Inputs:
        district_df (pd.DataFrame): District level master table.
    
    Outputs:
        pd.DataFrame: Original df + new features.
    """
    logger.info("Starting feature engineering...")
    
    # Avoid modifying the original
    df = district_df.copy()
    
    # Current Reference Date
    today = pd.Timestamp.now()
    
    # --- 1. Temporal Neglect Features ---
    # Days since last update. If NaT (no updates ever), we assume high neglect (eg 10 years/3650 days)
    # This allows ranking to work.
    
    # Ensure current date is timezone naive if the data is naive, or match timezones.
    # Usually pandas handles subtraciton.
    
    # Create mask for NaTs before calculation
    nat_mask = df['last_biometric_update_date'].isna()
    
    df['days_since_last_update'] = (today - df['last_biometric_update_date']).dt.days
    
    # Fill NaNs for districts with NO updates with a high default (e.g. 5 years = 1825 days) for calculation 
    # BUT keeping the original semantics, maybe we want it to be noticeable. 
    # Let's fill with max of the others or a fixed high number to represent "infinity" neglect.
    max_days = df['days_since_last_update'].max()
    if pd.isna(max_days): max_days = 365 # Default if dataset is empty or all partial
    fill_value = max_days * 1.5 if max_days > 0 else 3650
    df['days_since_last_update'] = df['days_since_last_update'].fillna(fill_value)
    
    df['years_since_last_update'] = df['days_since_last_update'] / 365.0
    
    # Rank districts (1 = most recent/smallest days, N = oldest/largest days)
    # So we rank strictly by days. Small days = Rank 1.
    df['update_recency_rank'] = df['days_since_last_update'].rank(method='min', ascending=True)

    # --- 2. Coverage Features ---
    # Handle division by zero if total_aadhaar_holders is 0 (unlikely but safe)
    df['biometric_coverage_ratio'] = df['total_biometric_updates'] / df['total_aadhaar_holders'].replace(0, 1)
    # Clip to max 1.0 just in case updates > holders (data errors)
    df['biometric_coverage_ratio'] = df['biometric_coverage_ratio'].clip(upper=1.0)
    
    df['biometric_coverage_gap'] = 1.0 - df['biometric_coverage_ratio']
    
    df['uncovered_population'] = df['total_aadhaar_holders'] - df['total_biometric_updates']
    # Ensure non-negative
    df['uncovered_population'] = df['uncovered_population'].clip(lower=0)

    # --- 3. Update Velocity Features ---
    # demographic_to_biometric_ratio = total_demographic_updates / total_biometric_updates
    # Handle div by zero (bio=0). If bio=0, result is large (infinity). We use fill_value=0 or large number?
    # Context: High demo updates but low bio updates might indicate neglect of bio data.
    # We'll replace 0 in denominator with 1 for stability or use numpy.
    df['demographic_to_biometric_ratio'] = df['total_demographic_updates'] / df['total_biometric_updates'].replace(0, np.nan)
    # If bio is 0, let's set ratio to 0 or a flag? Or just fill with 0? 
    # If demo > 0 and bio = 0, ratio is Inf. Let's set it to valid number to avoid crashing
    df['demographic_to_biometric_ratio'] = df['demographic_to_biometric_ratio'].fillna(0.0) 
    
    # update_lag_proxy = (1 - biometric_coverage_ratio) * (days_since_last_update / 730)
    df['update_lag_proxy'] = (1.0 - df['biometric_coverage_ratio']) * (df['days_since_last_update'] / 730.0)

    # --- 4. Population Context Features ---
    df['adult_population_proxy'] = df['total_aadhaar_holders'] * 0.75
    
    # population_impact_score = normalized holders
    max_holders = df['total_aadhaar_holders'].max()
    if max_holders == 0 or pd.isna(max_holders): max_holders = 1
    df['population_impact_score'] = df['total_aadhaar_holders'] / max_holders

    # --- 5. Volatility & Consistency Features ---
    # update_consistency = biometric_coverage_ratio / (1 + years_since_last_update)
    df['update_consistency'] = df['biometric_coverage_ratio'] / (1.0 + df['years_since_last_update'])
    
    # operational_neglect_proxy = coverage_gap * (days_since / 730)
    df['operational_neglect_proxy'] = df['biometric_coverage_gap'] * (df['days_since_last_update'] / 730.0)

    # --- 6. Composite Signals ---
    # urgency_signal = coverage_gap + normalized(days_since_last_update)
    # normalize days:
    max_days_current = df['days_since_last_update'].max()
    if max_days_current == 0: max_days_current = 1
    normalized_days = df['days_since_last_update'] / max_days_current
    
    df['urgency_signal'] = df['biometric_coverage_gap'] + normalized_days
    
    df['governance_concern_score'] = df['biometric_coverage_gap'] * df['population_impact_score']

    logger.info(f"Feature engineering complete. Added {len(df.columns) - len(district_df.columns)} new features.")
    
    # Print Summary as requested
    print("\n--- Feature Summary ---")
    new_features = [
        'days_since_last_update', 'years_since_last_update', 'update_recency_rank',
        'biometric_coverage_ratio', 'biometric_coverage_gap', 'uncovered_population',
        'demographic_to_biometric_ratio', 'update_lag_proxy', 'adult_population_proxy',
        'population_impact_score', 'update_consistency', 'operational_neglect_proxy',
        'urgency_signal', 'governance_concern_score'
    ]
    
    print(f"{'Feature':<35} {'Min':<10} {'Max':<10} {'Mean':<10}")
    print("-" * 70)
    for feat in new_features:
        if feat in df.columns:
            print(f"{feat:<35} {df[feat].min():<10.4f} {df[feat].max():<10.4f} {df[feat].mean():<10.4f}")
    
    return df

if __name__ == "__main__":
    pass
