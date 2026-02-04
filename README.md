# Aadhaar Netra
**Governance & Auditability System for Targeted Biometric Updates**

> **Created for UIDAI Data Hackathon 2026**

![Status](https://img.shields.io/badge/Status-Active-success)
![Version](https://img.shields.io/badge/Version-1.0.0-blue)
![Privacy](https://img.shields.io/badge/Privacy-Preserving-green)

---

## 📖 Overview

**Aadhaar Netra** is a deterministic, privacy-preserving data pipeline designed to identifying and prioritize districts for biometric update camps. It addresses the challenge of resource allocation by using strictly auditable metrics of neglect ("staleness") and population impact, ensuring that limited operational resources are deployed where they are needed most.

The system operates on **fixed mathematical rules**—not probabilistic AI—to guarantee explainability and reproducibility, which is critical for government auditing and judicial scrutiny.

## 🔑 Key Principles

1.  **Strict Determinism**: Inputs always yield identical outputs. No black-box ML or random seeds.
2.  **Privacy First**: Analysis is performed strictly on **aggregated district-level data**. No PII (Personally Identifiable Information) is processed in the scoring layer.
3.  **Explainability**: Every recommendation (e.g., "Tier 1 Camp") allows a drill-down to specific root causes (e.g., "High Neglect" or "Low Coverage").

## 🏗️ System Architecture

The pipeline follows a linear, modular architecture to ensure data integrity and traceability:

```mermaid
graph LR
    A[Ingestion] --> B[Aggregation]
    B --> C[Feature Engineering]
    C --> D[Normalization]
    D --> E[Scoring]
    E --> F[Strategy]
    F --> G[Output Reports]
```

1.  **Ingestion**: Validates raw CSV schemas, parses dates, and flags malformed records (`src/data_ingestion.py`).
2.  **Aggregation**: Groups data by district ID to create a privacy firewall (`src/data_aggregation.py`).
3.  **Feature Engineering**: Derives analytical indicators like `days_since_last_update` and `biometric_coverage_gap` (`src/feature_engineering.py`).
4.  **Normalization**: Scales all metrics to a 0-1 range for fair comparison (`src/feature_normalization.py`).
5.  **Scoring**: Computes BSI and CPS indices (`src/scoring_bsi.py`, `src/scoring_cps.py`).
6.  **Strategy**: Maps scores to physical camp deployment plans (`src/strategy_recommendation.py`).

## 🧠 Scoring Methodology

The system uses two core indices to rank districts:

### 1. Biometric Staleness Index (BSI)
Measures the **Urgency** of intervention based on neglect.
$$ BSI = (0.40 \times \text{Recency}) + (0.35 \times \text{Low Frequency}) + (0.25 \times \text{Gap}) $$

### 2. Camp Priority Score (CPS)
Measures the **Operational Priority** by combining urgency with **Impact**.
$$ CPS = \left[ (0.50 \times BSI) + (0.30 \times \text{Population}) + (0.20 \times \text{Low Frequency}) \right] \times 100 $$

**Tiers:**
-   **Tier 1 (85-100)**: Intensive Camps (Immediate)
-   **Tier 2 (70-84)**: Frequent Mobile Camps
-   **Tier 3 (55-69)**: Monthly Mobile Camps
-   **Tier 4 (40-54)**: Quarterly Fixed Camps
-   **Tier 5 (<40)**: Annual Preventive

## 🚀 Getting Started

### Prerequisites
-   Python 3.8+
-   `pip`

### Installation
1.  Clone the repository:
    ```bash
    git clone https://github.com/CongneoVerse/Aadhaar-Netra.git
    cd Aadhaar-Netra
    ```
2.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

### Execution
Run the pipeline orchestrator pointing to your data directory:

```bash
# Syntax: python pipeline_orchestrator.py <input_dir> <output_dir>
python pipeline_orchestrator.py data final_output_real
```

## 📂 Outputs

The pipeline generates the following in the output directory:

| File | Description |
| :--- | :--- |
| `final_ranked_districts.csv` | Complete audit trail with all scores, tiers, and reasoning. |
| `top_20_priority_districts.csv` | Actionable list for immediate deployment. |
| `audit_log.txt` | Technical log of the execution flow. |
| `metadata.json` | Provenance data for the run. |

## 📚 Documentation
For deeper technical details, please refer to:
-   [Technical Governance Manual](TECHNICAL_README.md)
-   [Methodology](methodology.md)
-   [Post-MVP Architecture](Post_MVP_Architecture.md)

---
*Built with ❤️ for a stronger digital foundation.*
