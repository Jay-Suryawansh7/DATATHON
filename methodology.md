# Methodology: Aadhaar Netra Camp Priority System

This document outlines the end-to-end methodology adopted for the **Aadhaar Netra** system, detailing data cleaning, preprocessing, and analytical transformations used to generate deterministic Camp Strategies.

## 1. Data Ingestion & Cleaning
**Module**: `data_ingestion.py`
The raw dataset is ingested with strict schema validation to ensure integrity before processing.
*   **Schema Validation**: The system verifies the presence of critical primary keys: `district_id` and `aadhaar_id`, along with update flags.
*   **Date Parsing**: The `last_update_date` field is parsed into standard datetime objects. Invalid formats are coerced to `NaT` (Not a Time) to prevent pipeline failure, but are flagged for quality review.
*   **Quality Flagging**: A `is_malformed` flag is attached to records missing critical identifiers, allowing them to be excluded from sensitive counts without data loss.

## 2. Preprocessing & Aggregation (Privacy Preservation)
**Module**: `data_aggregation.py`
To comply with privacy standards, individual-level data is **never** used for analysis. The data is immediately aggregated to the district level.
*   **Grouping**: Records are grouped strictly by `district_id`.
*   **Deduplication**: `total_aadhaar_holders` is calculated as the count of *unique* `aadhaar_id`s, ensuring duplicates don't skew population estimates.
*   **Conditional Logic**: The `last_biometric_update_date` is determined by finding the maximum date *only* for records where a biometric update actually occurred (`biometric_update_flag == 1`).

## 3. Feature Engineering (Transformations)
**Module**: `feature_engineering.py`
Raw aggregates are transformed into 20+ analytical indicators to capture nuance.
*   **Temporal Neglect**:
    *   `days_since_last_update`: Measures the gap between today and the last biometric activity.
    *   *Imputation*: Districts with **zero** history are imputed with a high neglect value (1.5x max observed age) to ensure they are flagged as critical.
*   **Coverage Metrics**:
    *   `biometric_coverage_gap`: `1 - (updates / holders)`. Represents the percentage of the population potentially vulnerable due to slate data.
*   **Update Velocity**:
    *   `update_consistency`: A ratio combining coverage and recency to measure how "active" a district is.

## 4. Normalization
**Module**: `feature_normalization.py`
To ensure fair scoring across disparate metrics (e.g., "3000 days" vs "0.4 ratio"), all numeric indicators are scaled to a **0–1 range** using Min-Max Scaling.
*   Preserves the relative distribution of data.
*   Ensures no single feature dominates the final weighted index simply due to having larger magnitude numbers.

## 5. Scoring Algorithms
### Biometric Staleness Index (BSI)
**Module**: `scoring_bsi.py`
A weighted index measuring urgency.
> **Formula**: `(0.40 * Time) + (0.35 * Low Frequency) + (0.25 * Coverage Gap)`
*   **Low Frequency** is derived as `1 - Update Consistency`.
*   **Interpretation**: Higher score (closer to 1.0) indicates **Critical Neglect**.

### Camp Priority Score (CPS)
**Module**: `scoring_cps.py`
The final operational metric (0–100) used for resource allocation.
> **Formula**: `(0.5 * Staleness) + (0.3 * Adult Population) + (0.2 * Low Frequency)`
*   **Tier 1 (Critical)**: CPS 85–100
*   **Tier 5 (Preventive)**: CPS < 40

## 6. Strategy Recommendation
**Module**: `strategy_recommendation.py`
Deterministic rules translate scores into physical actions.
*   **Camp Type**: Directly mapped from CPS Tiers (e.g., Tier 1 -> **INTENSIVE**).
*   **Frequency**: Defined days between camps (e.g., Tier 1 -> **7–10 days**).
*   **Location Suitability**: Derived from `Population Impact` vs `Coverage Gap`. High population density + high gap triggers a "High Suitability" recommendation for multi-subcenter deployment.
