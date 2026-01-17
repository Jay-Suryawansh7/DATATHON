
import pandas as pd
import numpy as np
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def compute_camp_priority_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the Camp Priority Score (CPS) and ranks districts.
    
    Formula:
    Priority (0-100) = [ 
        (0.5 * Staleness/BSI) 
        + (0.3 * Adult Population %) 
        + (0.2 * Low Update Frequency) 
    ] * 100
    
    Inputs:
        df: Dataframe with 'bsi_score', 'adult_population_proxy_norm', 'update_consistency_norm'
        
    Outputs:
        df: Sorted dataframe with 'cps_score', 'cps_tier', 'cps_rank', flags.
    """
    logger.info("Starting CPS computation...")
    
    df_cps = df.copy()
    
    # Validate required columns
    required = ['bsi_score', 'adult_population_proxy_norm', 'update_consistency_norm']
    missing = [col for col in required if col not in df_cps.columns]
    if missing:
        raise ValueError(f"Missing columns for CPS: {missing}")

    # Camp Priority Score combines biometric neglect (urgency) with population exposure (impact) 
    # to ensure resources are allocated where they matter most.
    # Score is deterministic, fully traceable, and designed for human review and override.

    w_stale = 0.5
    w_pop = 0.3
    w_freq = 0.2
    
    term_stale = df_cps['bsi_score']
    term_pop = df_cps['adult_population_proxy_norm']
    term_freq = 1.0 - df_cps['update_consistency_norm'] # Low frequency = High priority
    
    # Calculate raw 0-1 score
    raw_score = (w_stale * term_stale) + (w_pop * term_pop) + (w_freq * term_freq)
    
    # Scale to 0-100 and round to 2 decimals to avoid floating point issues at thresholds
    df_cps['cps_score'] = (raw_score * 100.0).round(2)
    
    # Assign Tiers
    # Tier 1 (85-100): Immediate
    # Tier 2 (70-84): High
    # Tier 3 (55-69): Medium
    # Tier 4 (40-54): Routine
    # Tier 5 (<40): Preventive
    
    conditions = [
        (df_cps['cps_score'] >= 85),
        (df_cps['cps_score'] >= 70) & (df_cps['cps_score'] < 85),
        (df_cps['cps_score'] >= 55) & (df_cps['cps_score'] < 70),
        (df_cps['cps_score'] >= 40) & (df_cps['cps_score'] < 55),
        (df_cps['cps_score'] < 40)
    ]
    tiers = ['Tier 1', 'Tier 2', 'Tier 3', 'Tier 4', 'Tier 5']
    
    df_cps['cps_tier'] = np.select(conditions, tiers, default='Unknown')
    
    # Sort descending
    df_cps = df_cps.sort_values(by='cps_score', ascending=False).reset_index(drop=True)
    
    # Rank (1 to N)
    df_cps['cps_rank'] = df_cps.index + 1
    
    # Flags
    df_cps['is_top_20'] = df_cps['cps_rank'] <= 20
    df_cps['is_top_100'] = df_cps['cps_rank'] <= 100
    
    logger.info("CPS computation complete.")
    
    # Summary
    print("\n--- CPS Summary ---")
    print(f"Top Score: {df_cps['cps_score'].max():.2f}")
    print(f"Low Score: {df_cps['cps_score'].min():.2f}")
    
    print("\nTier Distribution:")
    print(df_cps['cps_tier'].value_counts().sort_index())
    
    print("\nTop 20 Districts (Preview):")
    cols = ['district_id', 'cps_score', 'cps_tier', 'bsi_score', 'adult_population_proxy_norm']
    print(df_cps[cols].head(20))
    
    return df_cps

if __name__ == "__main__":
    pass
