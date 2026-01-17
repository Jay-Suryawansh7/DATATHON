
import pandas as pd
import logging
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def aggregate_to_district_level(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates individual transaction data to privacy-preserving district-level metrics.
    
    Operations:
    1. specific logic to find max date only for biometric updates.
    2. standard groupby for counts and sums.
    3. merges and cleanup.
    
    Privacy Note:
    - Individual 'aadhaar_id's are reduced to counts (nunique).
    - Output contains NO PII (Personal Identifiable Information), only aggregate statistics.
    """
    logger.info("Starting district-level aggregation...")
    
    # 1. Calculate Last Biometric Update Date (conditional max)
    # We filter only rows where biometric update occurred
    bio_updates_df = df[df['biometric_update_flag'] == 1]
    if not bio_updates_df.empty:
        last_bio_dates = bio_updates_df.groupby('district_id')['last_update_date'].max().rename('last_biometric_update_date')
    else:
        last_bio_dates = pd.Series(dtype='datetime64[ns]', name='last_biometric_update_date')

    # 2. Main Aggregation
    # We aggregate the full dataset
    agg_funcs = {
        'aadhaar_id': 'nunique',            # Count of unique holders
        'biometric_update_flag': 'sum',     # Total biometric updates
        'demographic_update_flag': 'sum',   # Total demographic updates
        # biometric_coverage_count is same as total_biometric_updates if flag is 1 for update
        # We will calculate it as sum just to be explicit per requirements
    }
    
    district_df = df.groupby('district_id').agg(agg_funcs)
    
    # Rename columns for clarity
    district_df = district_df.rename(columns={
        'aadhaar_id': 'total_aadhaar_holders',
        'biometric_update_flag': 'total_biometric_updates',
        'demographic_update_flag': 'total_demographic_updates'
    })
    
    # Add the Biometric Coverage Count (Requested explicitly)
    # Assuming the flag is 1 for an update, the sum is the count of records.
    district_df['biometric_coverage_count'] = district_df['total_biometric_updates']

    # 3. Merge conditional date
    district_df = district_df.merge(last_bio_dates, on='district_id', how='left')
    
    # 4. Final Cleanup
    district_df = district_df.reset_index()
    district_df = district_df.sort_values(by='district_id')
    
    # Log summary
    logger.info(f"Aggregation complete. Resulting district count: {len(district_df)}")
    logger.info("Columns in output: " + ", ".join(district_df.columns))
    
    # Print summary as requested
    print("\n--- Aggregation Summary ---")
    print(f"Row Count: {len(district_df)}")
    print("Columns:", district_df.columns.tolist())
    print("Null Values:\n", district_df.isnull().sum())
    print("---------------------------\n")

    return district_df

if __name__ == "__main__":
    pass
