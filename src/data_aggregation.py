
import pandas as pd
import logging
from typing import Dict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def aggregate_to_district_level(dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Aggregates multi-source data (Biometric, Demographic, Enrolment) to district level.
    
    Outputs a DataFrame with:
    - district_id
    - total_aadhaar_holders
    - total_biometric_updates
    - total_demographic_updates
    - biometric_coverage_count (proxied by total biometrics here)
    - last_biometric_update_date
    """
    logger.info("Aggregating multi-source data...")
    
    # 1. Enrolment (Base for Population)
    df_enrol = dfs.get('enrolment', pd.DataFrame())
    if not df_enrol.empty:
        # Sum age groups to get totals
        # Check columns existence
        req_cols = ['age_0_5', 'age_5_17', 'age_18_greater']
        for c in req_cols:
            if c not in df_enrol.columns:
                df_enrol[c] = 0
                
        df_enrol['total_holders'] = df_enrol['age_0_5'] + df_enrol['age_5_17'] + df_enrol['age_18_greater']
        
        agg_enrol = df_enrol.groupby('district')['total_holders'].sum().reset_index()
        agg_enrol.rename(columns={'district': 'district_id', 'total_holders': 'total_aadhaar_holders'}, inplace=True)
    else:
        agg_enrol = pd.DataFrame(columns=['district_id', 'total_aadhaar_holders'])

    # 2. Biometric
    df_bio = dfs.get('biometric', pd.DataFrame())
    if not df_bio.empty:
        df_bio['total_bio'] = df_bio.get('bio_age_5_17', 0) + df_bio.get('bio_age_17_', 0)
        
        # Updates Count
        agg_bio_count = df_bio.groupby('district')['total_bio'].sum().reset_index()
        agg_bio_count.rename(columns={'district': 'district_id', 'total_bio': 'total_biometric_updates'}, inplace=True)
        
        # Max Date
        agg_bio_date = df_bio.groupby('district')['date'].max().reset_index()
        agg_bio_date.rename(columns={'district': 'district_id', 'date': 'last_biometric_update_date'}, inplace=True)
    else:
        agg_bio_count = pd.DataFrame(columns=['district_id', 'total_biometric_updates'])
        agg_bio_date = pd.DataFrame(columns=['district_id', 'last_biometric_update_date'])

    # 3. Demographic
    df_demo = dfs.get('demographic', pd.DataFrame())
    if not df_demo.empty:
        df_demo['total_demo'] = df_demo.get('demo_age_5_17', 0) + df_demo.get('demo_age_17_', 0)
        agg_demo = df_demo.groupby('district')['total_demo'].sum().reset_index()
        agg_demo.rename(columns={'district': 'district_id', 'total_demo': 'total_demographic_updates'}, inplace=True)
    else:
        agg_demo = pd.DataFrame(columns=['district_id', 'total_demographic_updates'])

    # 4. Merge All
    # Start with enrolment (population base)
    if agg_enrol.empty and not agg_bio_count.empty:
        # Fallback if no enrolment file but bio exists
        base_df = agg_bio_count[['district_id']].drop_duplicates()
    elif not agg_enrol.empty:
        base_df = agg_enrol
    else:
        logger.error("No valid data to form district base.")
        return pd.DataFrame()

    # Merge Biometric
    base_df = pd.merge(base_df, agg_bio_count, on='district_id', how='left').fillna(0)
    base_df = pd.merge(base_df, agg_bio_date, on='district_id', how='left')
    
    # Merge Demographic
    base_df = pd.merge(base_df, agg_demo, on='district_id', how='left').fillna(0)
    
    # Biometric Coverage Count proxy
    # In this dataset, total_biometric_updates is our best proxy for coverage activity
    base_df['biometric_coverage_count'] = base_df['total_biometric_updates']
    
    # Ensure ID string
    base_df['district_id'] = base_df['district_id'].astype(str)
    
    logger.info(f"Aggregated data for {len(base_df)} districts.")
    return base_df

if __name__ == "__main__":
    pass
