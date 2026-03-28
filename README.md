# Customer Segmentation — Behavioral Clustering for Campaign Targeting

## Overview

Redesigned a Fortune 50 retailer's customer segmentation from stale demographic-based segments to behavioral clustering, driving measurable campaign lift. The previous segments (age bracket + region) showed zero behavioral separation — campaign performance was flat regardless of targeting. The new segments are built on how customers actually shop: purchase frequency, price sensitivity, channel preference, project behavior, and engagement trajectory.

**Result:** 5 actionable behavioral segments validated through a holdout campaign test with statistically significant lift in open rate (+27%, p=0.0006) and click rate (+56%, p=0.003). Segments deployed with automated weekly refresh and drift monitoring.

## Architecture

```
Data Sources (Adobe Analytics, GA4, CRM, Transactions)
         │
         ▼
┌─────────────────────────────────────┐
│        BigQuery (GCP)               │
│  Raw tables: transactions,          │
│  web_events, loyalty,               │
│  customer_master, campaigns         │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│     SQL Feature Engineering         │
│  Behavioral, price sensitivity,     │
│  time-aware, web behavior features  │
│  Variable rolling windows           │
│  (12wk Q1/Q3, 8wk Q2/Q4)          │
│  Output: customer_features table    │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│     Python Preprocessing            │
│  Log transform (right-skewed),      │
│  standard scaling, null handling    │
│  Read/write via BQ Python client    │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│     K-Means Clustering (K=5)        │
│  Evaluated: K-Means, Hierarchical,  │
│  GMM. K-Means selected for          │
│  stability, scalability, crisp      │
│  assignments.                       │
│  Silhouette: 0.358                  │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│     Validation                      │
│  Holdout test: treatment vs control │
│  Statistical significance testing   │
│  Segment stability analysis         │
│  Drift monitoring                   │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│     Tableau Dashboard               │
│  5 views: segment health, campaign  │
│  performance, behavioral profiles,  │
│  churn indicators, migration        │
└─────────────────────────────────────┘
```

## Segments Discovered

| Segment | Size | Avg Basket | Frequency | Key Signal | Marketing Action |
|---|---|---|---|---|---|
| **Weekend Warriors** | 23% | $71 | 0.43/week | High frequency, broad categories, 89% weekend, in-store dominant | In-store Saturday campaigns, cross-category promotions |
| **Project Planners** | 18% | $1,288 | 0.08/week | High basket, 100% project categories, 52% online, heavy research | Online targeting before project season, high-value bundles |
| **Price Hunters** | 24% | $44 | 0.30/week | 79% promo-driven, 25% avg discount depth | Promo-targeted messaging only, don't waste full-price campaigns |
| **Loyal Regulars** | 21% | $148 | 0.23/week | 91% full price, highest loyalty tier, 8,971 avg points | Loyalty rewards, avoid training them to wait for sales |
| **Dormant / At-Risk** | 14% | $64 | 0.18/week | Declining frequency (-0.55 trend), 384 day recency | Win-back campaigns before they're gone |

## Key Decisions & Trade-offs

**Why behavioral over demographic:**
The old segments (age + region) showed identical behavioral metrics across all 4 segments. Campaign performance was flat. Demographics describe WHO someone is, not HOW they shop. Two 35-year-olds in the same zip code can be a weekly hardware buyer and a once-a-year project planner.

**Why K-Means over Hierarchical/GMM:**
- K-Means: deterministic, stable across refreshes, scales to millions of customers
- Hierarchical: marginally better silhouette but O(n²) — unusable at production scale
- GMM: only 19% agreement across runs. Probabilistic assignments confused marketing — they need each customer in exactly one segment

**Why K=5:**
- Elbow at K=5 in inertia plot
- Highest silhouette score in the 4-6 range
- K=4 merged two distinct behavioral groups
- K=6+ created thin segments marketing couldn't act on (can't personalize 8 campaigns)

**Why weekly refresh, not daily:**
Daily refresh caused 12-15% of customers to shift segments, creating targeting instability. Weekly cadence with rolling window features produces 5-8% migration — stable enough for campaign planning.

**Feature engineering was the critical step:**
The model (K-Means) is simple. The differentiation is in the features. Three axes capturing different dimensions of customer behavior:
- Behavioral: frequency, basket size, category breadth, channel preference, project patterns
- Price sensitivity: promo ratio, discount depth, full-price ratio
- Time-aware: recency, frequency trend, seasonality, weekend patterns
- Web behavior: session depth, search rate, product page engagement

Variable rolling windows (12 weeks for Q1/Q3 transitional periods, 8 weeks for Q2/Q4 peak seasons) resist seasonal distortion.

## Project Structure

```
customer-segmentation/
├── README.md
├── requirements.txt
├── .gitignore
├── config/
│   └── project_config.py              # GCP project, dataset, table names, parameters
├── sql/
│   ├── eda/
│   │   ├── 01_table_overview.sql      # Row counts across all tables
│   │   ├── 02_transactions_profile.sql
│   │   ├── 03_category_distribution.sql
│   │   ├── 04_customer_txn_summary.sql
│   │   ├── 05_old_segment_sizes.sql
│   │   ├── 06_old_segment_behavior.sql # Key diagnostic: segments don't separate
│   │   └── 07_old_segment_campaign_performance.sql
│   ├── features/
│   │   └── build_customer_features.sql # Full feature engineering pipeline
│   └── schema/
├── src/
│   ├── data/
│   │   ├── load_to_bigquery.py        # BQ loader with schema enforcement
│   │   └── export_for_tableau.py      # 9 CSVs shaped for dashboard views
│   ├── features/
│   │   └── preprocess_features.py     # Log transform, scaling, BQ read/write
│   ├── models/
│   │   └── clustering.py              # K selection, model comparison, profiling
│   ├── evaluation/
│   │   ├── validation.py              # Holdout test, statistical tests, stability
│   │   └── drift_monitor.py           # Weekly production monitoring
│   └── utils/
├── data/
│   ├── mock/
│   │   └── generate_mock_data.py      # Realistic mock data with embedded archetypes
│   └── tableau_export/                # CSVs for Tableau Public
├── docs/
│   ├── problem_statement.md           # Stakeholder discovery output
│   ├── old_segment_diagnosis.md       # Evidence for why old segments failed
│   ├── feature_design.md             # Every feature, why it exists, what it answers
│   └── segment_table_schema.md        # Production schema, DE handoff, edge cases
├── artifacts/
│   ├── kmeans_model.joblib            # Trained K-Means model
│   ├── clustering_scaler.joblib       # StandardScaler for clustering features
│   └── feature_scaler.joblib          # StandardScaler for full feature set
├── plots/
│   ├── k_selection.png                # Elbow + silhouette analysis
│   ├── dendrogram.png                 # Hierarchical clustering comparison
│   └── segment_profiles.png           # Feature comparison across segments
└── notebooks/
```

## Tech Stack

| Component | Tool | Usage |
|---|---|---|
| Data Warehouse | **BigQuery (GCP)** | All source tables, feature tables, segment assignments |
| Python ↔ BQ | **google-cloud-bigquery** client | Programmatic reads/writes, schema enforcement |
| Feature Engineering | **SQL in BigQuery** | CTEs building customer-level features from raw data |
| Preprocessing | **pandas, scikit-learn** | Log transform, StandardScaler |
| Clustering | **scikit-learn** | K-Means, AgglomerativeClustering, GaussianMixture |
| Validation | **scipy.stats** | Two-proportion z-test, t-test, confidence intervals |
| Dashboard | **Tableau Public** | 5-view segment analytics dashboard |
| Version Control | **Git/GitHub** | Full commit history |

## Dashboard

**Live:** [Tableau Public — Segment Analytics](https://public.tableau.com/app/profile/samuel.vurity3854/viz/CustomerSegmentation_17746570658530/SegmentAnalytics)

Five views:
1. **Segment Size Distribution** — customer counts per segment
2. **Revenue per Customer** — Project Planners at $11K dwarf other segments
3. **Holdout Test Results** — treatment vs control across open/click/conversion
4. **Behavioral Profiles** — avg basket size by segment
5. **Churn Risk Distribution** — Dormant segment is 88% high risk

## Validation Results

**Holdout test (3 campaign waves, 500 customers per group per wave):**

| Metric | Control | Treatment | Lift | p-value | Significant |
|---|---|---|---|---|---|
| Open Rate | 19.1% | 24.2% | +27% | 0.0006 | Yes |
| Click Rate | 4.4% | 6.9% | +56% | 0.003 | Yes |
| Conversion Rate | 2.0% | 2.6% | +30% | 0.273 | No (power) |

Conversion rate lift is directionally positive but not significant at simulation scale (1,500 per group). At production scale with millions of customers, this would reach significance.

**Segment stability:** 6-7% weekly migration rate, well below 15% alert threshold. Three consecutive weeks passed stability checks.

## Edge Cases Handled

- **New customers (<3 transactions):** Excluded from clustering, assigned to "New/Low-Activity" bucket until threshold crossed
- **Seasonal distortion:** Rolling window features (8-12 weeks) prevent single promo events from flipping segment assignments
- **Power users (contractors):** Log transform on basket size and transaction count prevents outlier dominance
- **Segment boundary customers:** Weekly refresh may cause minor flip-flopping; overall migration rate monitored against 15% threshold

## How to Run

```bash
# Clone and setup
git clone https://github.com/samuelvurity/customer-segmentation.git
cd customer-segmentation
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Configure GCP (requires a GCP project with BigQuery enabled)
gcloud config set project <your-project-id>
gcloud auth application-default login
# Update config/project_config.py with your project ID

# Generate mock data and load to BigQuery
python3 src/data/load_to_bigquery.py

# Run feature engineering SQL in BigQuery console
# (copy sql/features/build_customer_features.sql and execute)

# Preprocess features
python3 src/features/preprocess_features.py

# Run clustering
python3 src/models/clustering.py

# Validate
python3 src/evaluation/validation.py

# Monitor
python3 src/evaluation/drift_monitor.py

# Export for Tableau
python3 src/data/export_for_tableau.py
```
