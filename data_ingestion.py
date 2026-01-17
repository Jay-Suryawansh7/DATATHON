
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, Any, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = [
    'district_id',
    'aadhaar_id',
    'biometric_update_flag',
    'demographic_update_flag',
    'last_update_date',
    'total_aadhaar_holders'
]

def _validate_schema(df: pd.DataFrame) -> Tuple[bool, list]:
    """
    Checks if all required columns differ in the dataframe.
    Returns (True, []) if valid, else (False, missing_count).
    """
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        logger.error(f"Missing required columns: {missing_cols}")
        return False, missing_cols
    return True, []

def _parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parses 'last_update_date' to datetime objects.
    Invalid dates are coerced to NaT (Not a Time).
    """
    if 'last_update_date' in df.columns:
        # Coerce errors to NaT so we don't crash, but we can flag them later
        df['last_update_date'] = pd.to_datetime(df['last_update_date'], errors='coerce')
    return df

def _generate_metadata(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Generates a summary dictionary of the dataset.
    """
    metadata = {
        'row_count': len(df),
        'columns': list(df.columns),
        'null_values': df.isnull().sum().to_dict(),
        'date_range': {}
    }

    if 'last_update_date' in df.columns:
        valid_dates = df['last_update_date'].dropna()
        if not valid_dates.empty:
            metadata['date_range'] = {
                'min': valid_dates.min().isoformat(),
                'max': valid_dates.max().isoformat()
            }
        else:
            metadata['date_range'] = 'No valid dates found'
            
    return metadata

def load_raw_data(filepath: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Reads a raw CSV file safely, validates the schema, identifies malformed records,
    and returns a validated dataframe and metadata dictionary.
    
    Privacy Note: Individual 'aadhaar_id's are processed here for validation 
    and deduplication only. They should not be exposed in downstream aggregate logs 
    except for specific debugging of malformed rows in a secure environment.
    """
    logger.info(f"Attempting to load data from {filepath}")
    
    try:
        # Read CSV
        df = pd.read_csv(filepath)
    except FileNotFoundError:
        logger.error(f"File not found: {filepath}")
        return pd.DataFrame(), {'error': 'File not found'}
    except Exception as e:
        logger.error(f"Failed to read CSV: {e}")
        return pd.DataFrame(), {'error': str(e)}

    # Validate Schema
    is_valid, missing = _validate_schema(df)
    if not is_valid:
        error_msg = f"Schema validation failed. Missing columns: {missing}"
        return pd.DataFrame(), {'error': error_msg}

    # Parse Dates
    df = _parse_dates(df)

    # Flag Malformed Records
    # Criteria: Null required fields or invalid dates (NaT)
    # We create a 'is_malformed' flag but keep the data for review
    
    # Check for NaNs in critical fields (excluding nullable stats if any)
    critical_fields = ['district_id', 'aadhaar_id']
    df['is_malformed'] = df[critical_fields].isnull().any(axis=1) | df['last_update_date'].isnull()
    
    malformed_count = df['is_malformed'].sum()
    if malformed_count > 0:
        logger.warning(f"Found {malformed_count} malformed records (missing IDs or invalid dates).")

    # Generate Metadata
    metadata = _generate_metadata(df)
    metadata['malformed_records_count'] = int(malformed_count)
    
    logger.info("Data loaded and validated successfully.")
    
    # Log summary mapping (Privacy-first: aggregate level only)
    logger.info(f"Summary: {metadata['row_count']} rows, Date Range: {metadata.get('date_range')}")

    return df, metadata

if __name__ == "__main__":
    # Internal test if run standalone
    pass
