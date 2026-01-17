
import pandas as pd
import numpy as np
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def recommend_camp_strategy(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generates rule-based camp strategy recommendations.
    
    Recommendations are rule-based and deterministic, ensuring explainability.
    Each recommendation is traceable to specific data signals and can be reviewed/overridden by UIDAI officials.
    
    Inputs:
        df: Dataframe with 'cps_score', 'population_impact_score_norm', 'biometric_coverage_gap_norm'
        
    Outputs:
        df: Dataframe with 'camp_type', 'deployment_freq_days', 'location_suitability', 'strategy_reasoning'
    """
    logger.info("Starting strategy recommendation...")
    
    df_strat = df.copy()
    
    required = ['cps_score', 'population_impact_score_norm', 'biometric_coverage_gap_norm']
    missing = [col for col in required if col not in df_strat.columns]
    if missing:
        raise ValueError(f"Missing columns for strategy: {missing}")

    # --- 1. Camp Type & Frequency (Based on CPS) ---
    
    # Rules:
    # CPS >= 85: INTENSIVE (7-10 days)
    # CPS 70–84: FREQUENT_MOBILE (14-21 days)
    # CPS 55–69: MONTHLY_MOBILE (28-35 days)
    # CPS 40–54: QUARTERLY_FIXED (90 days)
    # CPS < 40: ANNUAL_PREVENTIVE (365 days)
    
    conditions = [
        (df_strat['cps_score'] >= 85),
        (df_strat['cps_score'] >= 70) & (df_strat['cps_score'] < 85),
        (df_strat['cps_score'] >= 55) & (df_strat['cps_score'] < 70),
        (df_strat['cps_score'] >= 40) & (df_strat['cps_score'] < 55),
        (df_strat['cps_score'] < 40)
    ]
    
    camp_types = ['INTENSIVE', 'FREQUENT_MOBILE', 'MONTHLY_MOBILE', 'QUARTERLY_FIXED', 'ANNUAL_PREVENTIVE']
    freq_days = ['7-10 days', '14-21 days', '28-35 days', '90 days', '365 days']
    
    df_strat['camp_type'] = np.select(conditions, camp_types, default='UNKNOWN')
    df_strat['deployment_freq_days'] = np.select(conditions, freq_days, default='UNKNOWN')
    
    # --- 2. Location Suitability ---
    
    # High Suitability: Pop > 0.6 AND Gap > 0.3
    # Medium Suitability: Pop 0.4–0.6 AND Gap 0.2–0.3
    # Low Suitability: Pop < 0.4 AND Gap < 0.2
    # Note: User ranges have gaps (e.g., Pop 0.6 exactly, or Gap 0.2 exactly).
    # We will implement strict logic and a default "Standard" for cases falling in between.
    
    pop = df_strat['population_impact_score_norm']
    gap = df_strat['biometric_coverage_gap_norm']
    
    cond_suitability = [
        (pop > 0.6) & (gap > 0.3),
        (pop >= 0.4) & (pop <= 0.6) & (gap >= 0.2) & (gap <= 0.3),
        (pop < 0.4) & (gap < 0.2)
    ]
    suitabilities = ['High Suitability', 'Medium Suitability', 'Low Suitability']
    
    df_strat['location_suitability'] = np.select(cond_suitability, suitabilities, default='Standard Suitability')
    
    # --- 3. Strategy Reasoning ---
    
    def generate_reasoning(row):
        return (f"Assigned {row['camp_type']} due to CPS {row['cps_score']}. "
                f"Location is {row['location_suitability']} "
                f"(Pop Score: {row['population_impact_score_norm']:.2f}, Gap: {row['biometric_coverage_gap_norm']:.2f}).")

    df_strat['strategy_reasoning'] = df_strat.apply(generate_reasoning, axis=1)
    
    logger.info("Strategy recommendation complete.")
    
    # Summary
    print("\n--- Strategy Summary ---")
    print("\nCamp Type Distribution:")
    print(df_strat['camp_type'].value_counts())
    
    print("\nFrequency Distribution:")
    print(df_strat['deployment_freq_days'].value_counts())
    
    print("\nTop 20 Recommendations:")
    cols = ['district_id', 'camp_type', 'deployment_freq_days', 'location_suitability']
    print(df_strat[cols].head(20))

    return df_strat

if __name__ == "__main__":
    pass
