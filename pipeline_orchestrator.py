
import pandas as pd
import logging
import os
import sys
import json
from datetime import datetime

# Import our modules
from data_ingestion import load_raw_data
from data_aggregation import aggregate_to_district_level
from feature_engineering import feature_engineer
from feature_normalization import normalize_features
from scoring_bsi import compute_bsi
from scoring_cps import compute_camp_priority_score
from strategy_recommendation import recommend_camp_strategy

def setup_logger(output_dir):
    """Sets up logging to both console and audit_log.txt"""
    log_filepath = os.path.join(output_dir, "audit_log.txt")
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Clear existing handlers to avoid duplicates
    if logger.hasHandlers():
        logger.handlers.clear()
        
    # File Handler
    file_handler = logging.FileHandler(log_filepath, mode='w')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

def run_aadhaar_netra_pipeline(input_csv_path: str, output_dir: str):
    """
    This orchestration integrates all Aadhaar Netra components into a single reproducible, 
    auditable pipeline. Each step logs its execution for transparency and human review.
    
    Pipeline Flow:
    1. Load & validate data
    2. Aggregate to district level
    3. Engineer features
    4. Normalise features
    5. Compute BSI
    6. Compute CPS
    7. Generate strategy recommendations
    8. Export final results
    
    Outputs created in output_dir:
    - final_ranked_districts.csv
    - top_20_priority_districts.csv
    - audit_log.txt
    - metadata.json
    """
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Setup logging
    logger = setup_logger(output_dir)
    logger.info("xxx STARTING AADHAAR NETRA PIPELINE xxx")
    logger.info(f"Input: {input_csv_path}")
    logger.info(f"Output Directory: {output_dir}")
    
    try:
        # 1. Ingestion
        logger.info("Step 1: Ingestion - Loading and Validating Data")
        df_raw, metadata = load_raw_data(input_csv_path)
        
        if df_raw.empty:
            msg = f"Ingestion failed: {metadata.get('error', 'Unknown Error')}"
            logger.error(msg)
            raise RuntimeError(msg)
            
        logger.info(f"Loaded {len(df_raw)} raw records. Malformed records flagged: {metadata.get('malformed_records_count', 0)}")
        
        # 2. Aggregation
        logger.info("Step 2: Aggregation - Grouping by District")
        df_dist = aggregate_to_district_level(df_raw)
        logger.info(f"Aggregated to {len(df_dist)} districts.")
        
        # 3. Feature Engineering
        logger.info("Step 3: Feature Engineering - Deriving Indicators")
        df_feat = feature_engineer(df_dist)
        
        # 4. Normalization
        logger.info("Step 4: Normalization - Scaling Features (0-1)")
        df_norm = normalize_features(df_feat)
        
        # 5. BSI Scoring
        logger.info("Step 5: BSI Scoring - Calculating Biometric Staleness Index")
        df_bsi = compute_bsi(df_norm)
        
        # 6. CPS Scoring
        logger.info("Step 6: CPS Scoring - Calculating Camp Priority Score")
        df_cps = compute_camp_priority_score(df_bsi)
        
        # 7. Strategy Recommendation
        logger.info("Step 7: Strategy - Generating Recommendations")
        df_final = recommend_camp_strategy(df_cps)
        
        # Validation checks
        if df_final['cps_score'].isnull().any():
            logger.warning("Critical Warning: NaN values detected in CPS scores!")
        
        if not ((df_final['cps_score'] >= 0) & (df_final['cps_score'] <= 100)).all():
            logger.warning("Critical Warning: CPS scores out of range (0-100) detected!")

        # 8. Export Artifacts
        timestamp = datetime.now().isoformat()
        
        # Final Ranked CSV
        final_csv_path = os.path.join(output_dir, "final_ranked_districts.csv")
        df_final.to_csv(final_csv_path, index=False)
        logger.info(f"Saved final ranked list to {final_csv_path}")
        
        # Top 20 CSV
        top20_csv_path = os.path.join(output_dir, "top_20_priority_districts.csv")
        df_top20 = df_final[df_final['is_top_20']].copy()
        df_top20.to_csv(top20_csv_path, index=False)
        logger.info(f"Saved Top 20 priority list to {top20_csv_path}")
        
        # Metadata JSON
        meta_content = {
            "pipeline_version": "1.0.0",
            "execution_timestamp": timestamp,
            "input_file": input_csv_path,
            "total_districts_processed": len(df_final),
            "data_date_range": metadata.get('date_range', 'Unknown'),
            "top_score": float(df_final['cps_score'].max()),
            "low_score": float(df_final['cps_score'].min())
        }
        meta_path = os.path.join(output_dir, "metadata.json")
        with open(meta_path, 'w') as f:
            json.dump(meta_content, f, indent=4)
        logger.info(f"Saved metadata to {meta_path}")
        
        # Console Summary
        print("\n" + "="*50)
        print("AADHAAR NETRA PIPELINE SUMMARY")
        print("="*50)
        print(f"Total Districts: {len(df_final)}")
        print("\nTier Distribution:")
        print(df_final['cps_tier'].value_counts().sort_index().to_string())
        
        print("\nMean CPS by Tier:")
        print(df_final.groupby('cps_tier')['cps_score'].mean().round(2).to_string())
        
        print("\nTop 5 Priority Districts:")
        cols = ['district_id', 'cps_score', 'cps_tier', 'camp_type']
        print(df_final[cols].head(5).to_string(index=False))
        print("="*50 + "\n")
        
        logger.info("xxx PIPELINE EXECUTION SUCCESSFUL xxx")
        
    except Exception as e:
        logger.error(f"Pipeline Execution Failed: {str(e)}")
        # Re-raise to ensure exit code is non-zero if needed elsewhere
        raise e

if __name__ == "__main__":
    if len(sys.argv) > 2:
        inp = sys.argv[1]
        out_d = sys.argv[2]
        run_aadhaar_netra_pipeline(inp, out_d)
    else:
        # Default behavior for verification
        print("Usage: python pipeline_orchestrator.py <input_csv> <output_dir>")
        print("Running verification with defaults...")
        # Create a temp output dir
        out_dir = "aadhaar_netra_output"
        if not os.path.exists("full_mock_data.csv"):
             print("Error: full_mock_data.csv missing. Run generate_full_mock_data.py first.")
        else:
             run_aadhaar_netra_pipeline("full_mock_data.csv", out_dir)
