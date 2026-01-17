# Aadhaar Netra: Technical Governance & Auditability Manual

**System Version**: 1.0.0
**Target Audience**: UIDAI Officials, Policy Makers, External Auditors, Judiciary

---

## 1. System Overview
**Purpose**: Aadhaar Netra is a deterministic, privacy-preserving data pipeline aimed at identifying and prioritizing districts for biometric update camps based on strictly auditable metrics of neglect and population impact.

**Core Principles**:
1.  **Strict Determinism**: The system operates on fixed mathematical rules. It does **not** employ probabilistic Machine Learning (ML) or "black box" AI. Same inputs always yield identical outputs.
2.  **Privacy First**: No Personally Identifiable Information (PII) is processed at the analytical layer. All analysis occurs on aggregated district-level statistics.
3.  **Explainability**: Every scoring decision is traceable to a specific formula and root cause data point (e.g., "High Neglect" or "Low Coverage").

---

## 2. Deterministic Design
Unlike predictive AI models that may "hallucinate" or vary based on training data, Aadhaar Netra is **rule-based**.

*   **Mathematical Certainty**: Scores are calculated using standard weighted arithmetic equations.
*   **Reproducibility**: Any auditor running the pipeline on the same snapshot of raw data will generate the exact same ranking, down to the second decimal point.
*   **Parameter Stability**: Weights (e.g., 50% for Staleness) are policy decisions hard-coded into the logic, not learned parameters that drift over time.

---

## 3. Privacy Preservation
The system adheres to the principle of **Data Minimization**.

### Architecture of Privacy
1.  **Ingestion Layer**: Raw records (containing IDs) are read into memory mostly for validation.
2.  **Aggregation Firewall**: Immediate transformation groups data by `district_id`.
    *   *Input*: `UID_123, District_A, Update_Date`
    *   *Output*: `District_A, Total_Updates: 500`
3.  **Analytical Layer**: The pipeline processes **only** the aggregated counts. No individual Aadhaar number exists in the memory space during scoring or strategy generation.

---

## 4. Explainability & Auditability
Aadhaar Netra is designed to answer "Why?" for every recommendation.

**Traceability Example**:
*   **Recommendation**: *District D045 -> Intensive Camp (Tier 1)*
*   **Logic Chain**:
    1.  **Final Score (CPS)** is **87.5** (Threshold for Tier 1 is >85).
    2.  **Why 87.5?**
        *   **Staleness (BSI)** was **0.95** (Contribution: $0.95 \times 50 = 47.5$).
        *   **Population Impact** was **0.80** (Contribution: $0.80 \times 30 = 24.0$).
        *   **Low Frequency** was **0.80** (Contribution: $0.80 \times 20 = 16.0$).
    3.  **Why Staleness 0.95?** Raw data shows last update was >1000 days ago (normalized to ~1.0).

This chain allows officials to override granular components if real-world knowledge contradicts data (e.g., knowing a flood prevented updates).

---

## 5. Pipeline Architecture

The system follows a linear, modular flow:

`Input (CSV) -> [Ingestion] -> [Aggregation] -> [Feature Engineering] -> [Normalization] -> [Scoring] -> [Strategy] -> Output (CSV/Report)`

### Modules
1.  **Ingestion**: Validates schema, parses dates, flags malformed records.
2.  **Aggregation**: Reduces granular data to district vectors.
3.  **Feature Engineering**: Derives 20+ indicators from raw counts.
4.  **Normalization**: Min-Max scaling (0–1) for fair comparison.
5.  **BSI Scoring**: Computes urgency index.
6.  **CPS Scoring**: Computes final priority and tiers.
7.  **Strategy**: Maps tiers to physical deployment plans.

---

## 6. Feature Catalogue

All features are derived from aggregated data.

| Feature Name | Definition | Governance Intent |
| :--- | :--- | :--- |
| **Temporal Neglect** | | |
| `days_since_last_update` | Count of days since max date. | Identifying abandoned regions. |
| `years_since_last_update` | Days / 365. | Human-readable staleness. |
| `update_recency_rank` | Rank of district by date. | Relative standing check. |
| **Coverage** | | |
| `biometric_coverage_gap` | `1 - (Updates / Holders)` | estimating un-serviced population % |
| `uncovered_population` | Raw count of un-updated people. | Magnitude of the problem. |
| **Velocity** | | |
| `update_consistency` | Composite of frequency & coverage. | identifying operational regularity. |
| `update_lag_proxy` | Inverse of consistency. | "How slow are they moving?" |
| **Composite** | | |
| `urgency_signal` | Sum of normalized gap + lag. | Early warning indicator. |
| `governance_concern_score` | Weighted composite of neglect features. | High-level audit flag. |

---

## 7. Scoring Methodology

### A. Biometric Staleness Index (BSI)
**Purpose**: To measure pure **Urgency/Neglect**.
**Formula**:
$$ BSI = (0.40 \times Time) + (0.35 \times (1 - Consistency)) + (0.25 \times Gap) $$

| Weight | Factor | Rationale |
| :--- | :--- | :--- |
| **40%** | **Time Recency** | Old data is the primary failure mode. |
| **35%** | **Low Frequency** | Sporadic updates indicate operational failure. |
| **25%** | **Coverage Gap** | Large % of un-updated people is a risk. |

### B. Camp Priority Score (CPS)
**Purpose**: To guide **Resource Allocation** (Impact).
**Formula**:
$$ CPS = [(0.50 \times BSI) + (0.30 \times Population) + (0.20 \times Low Frequency)] \times 100 $$

| Weight | Factor | Rationale |
| :--- | :--- | :--- |
| **50%** | **Staleness (BSI)** | Urgency is the primary driver. |
| **30%** | **Population** | Impact: Prioritize high-population centers if neglected. |
| **20%** | **Low Frequency** | Operational boost: Target areas that struggle to self-sustain. |

**Tiers**:
*   **Tier 1 (85-100)**: Immediate/Intensive.
*   **Tier 2 (70-84)**: Frequent Mobile.
*   **Tier 3 (55-69)**: Monthly Mobile.
*   **Tier 4 (40-54)**: Quarterly Fixed.
*   **Tier 5 (<40)**: Annual Preventive.

---

## 8. How to Run

### Dependencies
Python 3.8+
*   `pandas`
*   `numpy`
*   `scikit-learn` (for normalization)

### Execution
Command line orchestration ensures simplicity in production.

```bash
# General Syntax
python pipeline_orchestrator.py <input_csv_path> <output_directory>

# Example
python pipeline_orchestrator.py raw_uidai_data_2024.csv ./outputs/jan_2024
```

### Outputs
1.  `final_ranked_districts.csv`: Complete audit trail of all scores.
2.  `top_20_priority_districts.csv`: Action list for operations teams.
3.  `audit_log.txt`: Technical execution log for debugging.
4.  `metadata.json`: Provenance data (timestamp, input file hash).

---

## 9. Reproducibility & Assurance

To ensure this system stands up to judicial or external scrutiny:
1.  **Version Control**: This codebase is versioned. Scores generated today can be regenerated 5 years from now using the same code commit (v1.0.0).
2.  **Schema Locking**: The input schema is validated strictly. Unexpected columns or data types cause the pipeline to halt rather than produce erroneous guess-work.
3.  **No Random Seeds**: Unlike Monte Carlo or Stochastic systems, `random` is never used in the analytical scoring logic.

---

*This README serves as the complete auditability and governance foundation for Aadhaar Netra, ensuring judges, officials, and external auditors can understand, verify, and trust the system.*
