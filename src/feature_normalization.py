
import pandas as pd
import logging
from sklearn.preprocessing import MinMaxScaler

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def normalize_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies Min-Max scaling to selected features.
    
    Feature normalisation ensures that all indicators operate on a common 0–1 scale, 
    enabling fair composite scoring across districts with different populations 
    and update patterns.
    
    Inputs:
        df (pd.DataFrame): Dataframe with raw features.
        
    Outputs:
        pd.DataFrame: Dataframe with original columns + '_norm' columns.
    """
    logger.info("Starting feature normalization...")
    
    df_norm = df.copy()
    
    # List of features to normalize
    # We include the explicitly requested ones and other relevant continuous indicators
    features_to_scale = [
        'days_since_last_update',
        'years_since_last_update',
        'update_recency_rank',
        'biometric_coverage_ratio',
        'biometric_coverage_gap',
        'uncovered_population',
        'demographic_to_biometric_ratio',
        'update_lag_proxy',
        'adult_population_proxy',
        'population_impact_score',
        'update_consistency',
        'operational_neglect_proxy',
        'urgency_signal',
        'governance_concern_score'
    ]
    
    # Filter to only those present in df
    available_features = [f for f in features_to_scale if f in df.columns]
    
    if not available_features:
        logger.warning("No features found to normalize.")
        return df_norm

    scaler = MinMaxScaler()
    
    try:
        # Fit and transform
        scaled_values = scaler.fit_transform(df_norm[available_features])
        
        # Create new column names
        norm_col_names = [f"{col}_norm" for col in available_features]
        
        # Assign back
        df_norm[norm_col_names] = scaled_values
        
        logger.info(f"Normalized {len(available_features)} features.")
        
        # Print Summary
        print("\n--- Normalization Summary ---")
        print(f"{'Feature':<35} {'Orig Min':<10} {'Orig Max':<10} -> {'Norm Min':<10} {'Norm Max':<10}")
        print("-" * 90)
        
        for i, col in enumerate(available_features):
            orig_min = df_norm[col].min()
            orig_max = df_norm[col].max()
            norm_min = df_norm[norm_col_names[i]].min()
            norm_max = df_norm[norm_col_names[i]].max()
            print(f"{col:<35} {orig_min:<10.4f} {orig_max:<10.4f} -> {norm_min:<10.4f} {norm_max:<10.4f}")
            
    except Exception as e:
        logger.error(f"Normalization failed: {e}")
        raise e

    return df_norm

if __name__ == "__main__":
    pass
