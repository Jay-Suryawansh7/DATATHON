
import pandas as pd
import logging
import os
import glob
from typing import Dict, Any, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_raw_data(data_dir: str) -> Tuple[Dict[str, pd.DataFrame], Dict[str, Any]]:
    """
    Reads multiple CSV files from the data directory, separated by type:
    - Biometric
    - Demographic
    - Enrolment
    
    Returns:
        dfs: Dictionary {'biometric': df, 'demographic': df, 'enrolment': df}
        metadata: Summary stats
    """
    logger.info(f"Scanning data directory: {data_dir}")
    
    datasets = {
        'biometric': [],
        'demographic': [],
        'enrolment': []
    }
    
    # Identify files
    all_files = glob.glob(os.path.join(data_dir, "*.csv"))
    
    for f in all_files:
        filename = os.path.basename(f)
        try:
            df = pd.read_csv(f)
            # Standardize date format
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y', errors='coerce')
            
            if 'biometric' in filename:
                datasets['biometric'].append(df)
            elif 'demographic' in filename:
                datasets['demographic'].append(df)
            elif 'enrolment' in filename:
                datasets['enrolment'].append(df)
            else:
                logger.warning(f"Unknown file type: {filename}")
                
        except Exception as e:
            logger.error(f"Failed to read {f}: {e}")

    # Merge and Validate
    final_dfs = {}
    row_counts = {}
    
    for key, df_list in datasets.items():
        if df_list:
            merged = pd.concat(df_list, ignore_index=True)
            final_dfs[key] = merged
            row_counts[key] = len(merged)
            logger.info(f"Loaded {key}: {len(merged)} rows")
        else:
            final_dfs[key] = pd.DataFrame()
            row_counts[key] = 0
            logger.warning(f"No files found for {key}")

    metadata = {
        'row_counts': row_counts,
        'source_dir': data_dir
    }
    
    return final_dfs, metadata

if __name__ == "__main__":
    pass
