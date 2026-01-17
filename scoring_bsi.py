
import pandas as pd
import logging
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def compute_bsi(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the Biometric Staleness Index (BSI) and assigns priority tiers.
    
    BSI Formula:
    BSI = (0.40 * Time since last update) 
        + (0.35 * Low update frequency) 
        + (0.25 * Coverage gap)
        
    Mapping:
    - Time since last update -> days_since_last_update_norm
    - Low update frequency -> (1 - update_consistency_norm)
    - Coverage gap -> biometric_coverage_gap_norm
    
    Inputs:
        df (pd.DataFrame): Normalized district dataframe.
        
    Outputs:
        pd.DataFrame: Dataframe with 'bsi_score' and 'bsi_tier'.
    """
    logger.info("Starting BSI computation...")
    
    # Validation
    required_cols = ['days_since_last_update_norm', 'update_consistency_norm', 'biometric_coverage_gap_norm']
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing normalized columns for BSI: {missing}")

    df_bsi = df.copy()
    
    # Weighted indices were computed programmatically to ensure consistency and reproducibility.
    # Biometric Staleness Index captures temporal neglect and coverage gaps in a single explainable score.
    
    w_time = 0.40
    w_freq = 0.35
    w_gap = 0.25
    
    # Calculate terms
    term_time = df_bsi['days_since_last_update_norm']
    
    # Low update frequency is the inverse of update consistency
    # If update_consistency_norm is high (1.0), low_freq_term is 0.0 (Good)
    # If update_consistency_norm is low (0.0), low_freq_term is 1.0 (Bad)
    term_freq = 1.0 - df_bsi['update_consistency_norm']
    
    term_gap = df_bsi['biometric_coverage_gap_norm']
    
    # Compute BSI
    df_bsi['bsi_score'] = (w_time * term_time) + (w_freq * term_freq) + (w_gap * term_gap)
    
    # Clip to ensure 0-1 range (due to float precision)
    df_bsi['bsi_score'] = df_bsi['bsi_score'].clip(0.0, 1.0)
    
    # Assign Tiers
    # BSI >= 0.75: Critical
    # BSI 0.50–0.74: High
    # BSI 0.25–0.49: Moderate
    # BSI < 0.25: Low
    
    conditions = [
        (df_bsi['bsi_score'] >= 0.75),
        (df_bsi['bsi_score'] >= 0.50) & (df_bsi['bsi_score'] < 0.75),
        (df_bsi['bsi_score'] >= 0.25) & (df_bsi['bsi_score'] < 0.50),
        (df_bsi['bsi_score'] < 0.25)
    ]
    tiers = ['Critical', 'High', 'Moderate', 'Low']
    
    df_bsi['bsi_tier'] = np.select(conditions, tiers, default='Unknown')
    
    # Sort descending by BSI (Highest urgency first)
    df_bsi = df_bsi.sort_values(by='bsi_score', ascending=False)
    
    logger.info("BSI computation complete.")
    
    # Print Summary
    print("\n--- BSI Summary ---")
    print(f"Min Score: {df_bsi['bsi_score'].min():.4f}")
    print(f"Max Score: {df_bsi['bsi_score'].max():.4f}")
    print(f"Mean Score: {df_bsi['bsi_score'].mean():.4f}")
    print("\nTier Distribution:")
    print(df_bsi['bsi_tier'].value_counts())
    
    return df_bsi

if __name__ == "__main__":
    pass
