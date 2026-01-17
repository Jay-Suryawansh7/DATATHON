
import pandas as pd
import logging
import os
import sys
import json
from datetime import datetime

# Import our modules
from src.data_ingestion import load_raw_data
from src.data_aggregation import aggregate_to_district_level
from src.feature_engineering import feature_engineer
from src.feature_normalization import normalize_features
from src.scoring_bsi import compute_bsi
from src.scoring_cps import compute_camp_priority_score
from src.strategy_recommendation import recommend_camp_strategy

def setup_logger(output_dir):
    """Sets up logging to both console and audit_log.txt"""
    os.makedirs(output_dir, exist_ok=True)
    log_filepath = os.path.join(output_dir, "audit_log.txt")
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    if logger.hasHandlers():
        logger.handlers.clear()
        
    file_handler = logging.FileHandler(log_filepath, mode='w')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

def run_aadhaar_netra_pipeline(input_path: str, output_dir: str):
    """
    Orchestrates the pipeline using the data folder path.
    """
    logger = setup_logger(output_dir)
    logger.info("xxx STARTING AADHAAR NETRA PIPELINE (REAL DATA) xxx")
    logger.info(f"Input Directory: {input_path}")
    logger.info(f"Output Directory: {output_dir}")
    
    try:
        # 1. Ingestion
        logger.info("Step 1: Ingestion - Loading Multi-Source Data")
        dfs, metadata = load_raw_data(input_path)
        
        if not dfs['biometric'].empty:
            logger.info("Biometric data loaded.")
        else:
            logger.warning("Biometric data missing/empty.")

        # 2. Aggregation
        logger.info("Step 2: Aggregation - Grouping by District")
        df_dist = aggregate_to_district_level(dfs)
        
        if df_dist.empty:
            raise RuntimeError("Aggregation resulted in empty dataframe.")
            
        logger.info(f"Aggregated to {len(df_dist)} districts.")
        
        # 3. Feature Engineering
        logger.info("Step 3: Feature Engineering")
        df_feat = feature_engineer(df_dist)
        
        # 4. Normalization
        logger.info("Step 4: Normalization")
        df_norm = normalize_features(df_feat)
        
        # 5. BSI Scoring
        logger.info("Step 5: BSI Scoring")
        df_bsi = compute_bsi(df_norm)
        
        # 6. CPS Scoring
        logger.info("Step 6: CPS Scoring")
        df_cps = compute_camp_priority_score(df_bsi)
        
        # 7. Strategy
        logger.info("Step 7: Strategy Recommendation")
        df_final = recommend_camp_strategy(df_cps)
        
        # 8. Export
        timestamp = datetime.now().isoformat()
        final_csv_path = os.path.join(output_dir, "final_ranked_districts.csv")
        df_final.to_csv(final_csv_path, index=False)
        logger.info(f"Saved final ranked list to {final_csv_path}")
        
        top20_csv_path = os.path.join(output_dir, "top_20_priority_districts.csv")
        df_top20 = df_final[df_final['is_top_20']].copy()
        df_top20.to_csv(top20_csv_path, index=False)
        
        # Console Summary
        print("\n" + "="*50)
        print("AADHAAR NETRA PIPELINE SUMMARY")
        print("="*50)
        print(f"Total Districts: {len(df_final)}")
        print("\nTier Distribution:")
        print(df_final['cps_tier'].value_counts().sort_index().to_string())
        print("\nTop 5 Priority Districts:")
        cols = ['district_id', 'cps_score', 'cps_tier', 'camp_type']
        print(df_final[cols].head(5).to_string(index=False))
        print("="*50 + "\n")
        
        logger.info("xxx PIPELINE EXECUTION SUCCESSFUL xxx")
        
    except Exception as e:
        logger.error(f"Pipeline Execution Failed: {str(e)}")
        raise e

if __name__ == "__main__":
    if len(sys.argv) > 2:
        inp = sys.argv[1]
        out_d = sys.argv[2]
        run_aadhaar_netra_pipeline(inp, out_d)
    else:
        # Default behavior: Assume 'data' folder in current dir
        print("Using default 'data' folder...")
        if os.path.exists("data"):
             run_aadhaar_netra_pipeline("data", "final_output_real")
        else:
             print("Error: 'data' folder not found.")
