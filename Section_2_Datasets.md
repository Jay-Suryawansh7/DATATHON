# Section 2: Datasets Used

For this analysis, we utilized the official **Aadhaar Enrolment and Update Datasets** provided by UIDAI. The analytical pipeline ingested raw data from **12 distinct CSV files**, comprising millions of records across enrolment, biometric, and demographic categories. These disparate sources were harmonized into a single district-level master dataset covering **985 districts**, serving as the foundation for the Camp Priority Score (CPS).

## 2.1 Dataset Specifications

We integrated data from three primary functional domains. The selection of columns was driven strictly by the need to measure **Staleness** (Time) and **Coverage** (Volume).

### 1. Aadhaar Enrolment Data (Population Baseline)
*   **Source Files**: `api_data_aadhar_enrolment_*.csv` (3 files)
*   **Role in Analysis**: Established the "Total Aadhaar Holders" baseline. This serves as the **denominator** for all coverage ratios, acting as a proxy for the total addressable population in a district.
*   **Schema & Transformation**:
    *   **Aggregation Key**: `district` and `state`.
    *   **Metrics Used**: `age_0_5`, `age_5_17`, and `age_18_greater`.
    *   **Transformation**: These three age bands were summed to calculate the `total_aadhaar_holders` metric for each district.
    *   **Unused Columns**: `pincode` (aggregated up to district level), `gender` (not required for camp logistics).

### 2. Biometric Update Data (Staleness Signal)
*   **Source Files**: `api_data_aadhar_biometric_*.csv` (4 files)
*   **Role in Analysis**: The primary driver for the **Biometric Staleness Index (BSI)**. This dataset tracks mandatory update activity (e.g., at age 5 and 15) and voluntary biometric refreshes.
*   **Schema & Transformation**:
    *   **`date`**: The most critical field. We extracted the `MAX(date)` per district to determine the `last_biometric_update_date`. This allows us to compute `days_since_last_update` (Recency).
    *   **`bio_age_5_17` & `bio_age_17_`**: Summed to calculate `total_biometric_updates`.
    *   **Logic**: High update counts relative to enrolment indicate a healthy ecosystem. Low counts signal potential data rot.

### 3. Demographic Update Data (Operational Context)
*   **Source Files**: `api_data_aadhar_demographic_*.csv` (5 files)
*   **Role in Analysis**: Provided secondary signals for **Update Consistency**. It helps distinguish between "Dead Districts" (no activity at all) and "Biometric-Lagging Districts" (active centers but failing to capture biometrics).
*   **Schema & Transformation**:
    *   **Metrics Used**: `demo_age_5_17` and `demo_age_17_`.
    *   **Transformation**: Aggregated to `total_demographic_updates` to compute the `Demographic-to-Biometric Ratio`.

---

## 2.2 Data Preprocessing & Unification Methodology

To prepare this multi-source data for the **Camp Priority Algorithm**, we applied the following rigorous cleaning steps:

1.  **Temporal Standardization**:
    *   Raw dates appeared in mixed formats (e.g., `DD-MM-YYYY` vs `YYYY-MM-DD`). All date fields were parsed into standardized Python `datetime` objects to ensure accurate calculation of the `days_since_last_update` feature. "Invalid" or future dates were coerced to `NaT` and handled as missing values.

2.  **District-Level Aggregation Strategy**:
    *   The pipeline uses a **Left Join** strategy starting with the *Enrolment* dataset (Population Base). Biometric and Demographic updates were merged onto this base using the `district` name as the primary key.
    *   **Handling Missing Baselines**: In rare cases where Enrolment data was missing but Update data existed, the system preserved the district record to ensure no operational data was lost.

3.  **Missing Data Imputation (Edge Cases)**:
    *   **Scenario**: A district has Enrolment data but zero recorded Biometric updates (Null `last_update_date`).
    *   **Resolution**: Instead of dropping these rows, the system applies valid **Worst-Case Imputation**. The `days_since_last_update` is set to a high default (e.g., 3650 days), ensuring these "Invisible Districts" receive a critical **BSI Score of 1.0 (Tier 1)** rather than being excluded from the analysis.
