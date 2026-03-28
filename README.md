# Customer Segmentation — Behavioral Clustering for Campaign Targeting

## Business Context

### The Problem

A Fortune 50 home improvement retailer's marketing team was spending against customer segments that produced zero measurable campaign lift. They were targeting "Young Urban (18-30, Metro)" differently from "Senior Established (60+, Rural/Suburban)" — but response rates, click rates, and conversion rates were statistically identical across all four segments. Marketing was doing the work of personalization without getting any of the benefit.

The segments had been built years earlier by a previous analyst using demographic attributes (age bracket and region) and had not been refreshed since January 2023. Nobody knew why they weren't working — marketing just knew "the segments don't do anything for us."

### Why This Matters

This wasn't an academic exercise. Marketing was allocating campaign spend, designing creative, and making channel decisions based on segments that had no predictive power. Every campaign sent to "Suburban Family" was wasted personalization effort — the segment contained an equal mix of weekly hardware buyers, once-a-year project planners, promo-only bargain hunters, and customers who had already stopped shopping. Sending them all the same campaign was no better than sending everyone a generic email.

At the scale of a Fortune 50 retailer with millions of customers and tens of millions in marketing spend, even small improvements in targeting efficiency translate to significant revenue impact.

### The Approach

**Phase 1 — Diagnose, don't assume.** Marketing said the segments didn't work. The first step was proving WHY, with evidence, before proposing a replacement. Profiled all four existing segments against transaction data, web behavior, and campaign history. Found that behavioral metrics (purchase frequency, basket size, channel preference, promo sensitivity) were nearly identical across all demographic segments. Root cause: demographics don't predict shopping behavior.

**Phase 2 — Design features that capture behavior, not identity.** Built a customer feature set across three axes: behavioral (how they shop), price sensitivity (what motivates them to buy), and time-aware (what direction they're heading). Incorporated web behavior signals and loyalty data. Used variable rolling windows (12 weeks during transitional seasons Q1/Q3, 8 weeks during peak seasons Q2/Q4) calibrated through testing. Adjusted regional boundaries after discovering different regions exhibited different seasonal timing patterns. Feature engineering consumed more time than modeling — the model is simple, the differentiation is in the features.

**Phase 3 — Cluster, validate, deploy.** Evaluated K-Means, Hierarchical Clustering, and Gaussian Mixture Models. Selected K-Means for production requirements: deterministic results, stability across weekly refreshes, scalability to millions, and crisp assignments marketing could act on. Selected K=5 through elbow method, silhouette analysis, and business validation.

**Phase 4 — Prove it works, don't just ship it.** Designed a holdout campaign test with treatment (new segments) vs. control (old targeting) across three campaign waves.

### The Challenges

**Rebuilding trust after the old system failed.** Marketing had stopped believing in segments entirely. The diagnosis presentation — hard numbers showing exactly why the old segments failed — was as important as the new solution. Silhouette scores mean nothing to a marketing lead. The 12% campaign lift proved the approach worked.

**Feature design required domain immersion.** Understanding that a customer buying Lumber + Flooring + Plumbing in the same quarter is a renovation project (not three separate needs) required learning the product catalog and working with the business to define project categories and product chains. Web behavior signals supplemented transaction-based project detection.

**Actionability vs. granularity trade-off.** K=8 produced more statistically pure segments. But marketing can't design 8 campaign strategies. K=5 balanced statistical separation with operational reality. Each segment needed a clear, intuitive description — if a marketer can't explain it in one sentence, they won't use it.

**Weekly vs. daily refresh.** Daily refresh created targeting instability — 12-15% of customers shifted daily. Moved to weekly with rolling window features, producing 5-8% migration — stable enough for campaign cycles.

**Seasonal distortion.** During Black Friday, everyone looks price-sensitive. Rolling window features dilute single-event spikes. Regional variation in seasonal timing required adjusting boundaries based on observed data rather than calendar assumptions.

**Holdout test politics.** Marketing didn't want to withhold customers from "better" targeting. Made the case that 2-3 weeks of holdout was worth years of validated targeting.

### The Implementation

**Data pipeline:** Raw data sources (Adobe Analytics, GA4, CRM/loyalty, transactional) → BigQuery → SQL feature engineering (28 features across 4 axes) → Python preprocessing (log transform, scaling) → K-Means clustering → segment assignment table in BigQuery → weekly refresh → Tableau dashboards.

**Production deployment:** K-Means on Vertex AI with scheduled weekly batch scoring. Segment assignment table with full replace each refresh. Drift monitoring checking distribution, migration rates, and feature drift after each refresh. New customer handling with 3-transaction minimum threshold.

**Self-service dashboard:** 5-view Tableau dashboard serving marketing and leadership — segment health, campaign performance, behavioral profiles, migration monitoring, and churn risk indicators.

### Business Impact

**Measurable campaign lift.** 12% improvement in campaign performance validated through holdout test with statistical significance (p < 0.001 on open rate, p < 0.01 on click rate).

**Lifecycle marketing activation.** Segments fed directly into email, push, and SMS workflows. Marketing designed segment-specific campaigns for the first time: promo messaging for Price Hunters, in-store weekend campaigns for Weekend Warriors, online project bundles for Project Planners, win-back campaigns for Dormant customers, loyalty rewards for Loyal Regulars.

**Downstream system integration.** Segment assignments consumed as features by the forecasting system and Marketing Mix Model — enabling segment-level budget allocation and attribution. Segments became a foundational data asset, not a standalone analysis.

**Churn detection.** The dashboard's churn risk view revealed 88% of the Dormant segment showed high churn risk. This directly triggered proactive cohort analysis — retention curves by acquisition channel, segment, and category, identifying the 45-day win-back window, contributing to 8% churn reduction.

**Self-service analytics.** Marketing and leadership independently monitored segment health and campaign performance without ad-hoc analysis requests. Reduced reporting turnaround and enabled faster campaign iteration.

### Who Benefits

| Stakeholder | What They Got |
|---|---|
| **Marketing team** | Actionable segments for campaign targeting — WHO to send WHAT through WHICH channel. Segment-specific strategies replaced one-size-fits-all messaging. |
| **Marketing leadership** | Visibility into segment health, campaign lift, and customer trajectory. Data-informed budget allocation. |
| **Campaign operations** | Automated weekly segment refresh feeding directly into lifecycle platforms (email, push, SMS). No manual segment pulls. |
| **Forecasting system** | Segment assignments as input features improving forecast accuracy at the segment level. |
| **MMM** | Segment-level attribution enabling per-segment channel budget decisions. |
| **Executive leadership** | Single source of truth for customer behavior. KPI dashboards showing segment-level trends. |
| **Data science team** | Reusable feature engineering pipeline and clustering framework. Drift monitoring infrastructure applicable to future models. |

---

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
The old segments (age + region) showed identical behavioral metrics across all 4 segments. Campaign performance was flat. Demographics describe WHO someone is, not HOW they shop.

**Why K-Means over Hierarchical/GMM:**
- K-Means: deterministic, stable across refreshes, scales to millions
- Hierarchical: marginally better silhouette but O(n²) at production scale
- GMM: only 19% agreement across runs, probabilistic assignments confused marketing

**Why K=5:**
- Elbow + best silhouette in the 4-6 range
- K=4 merged distinct groups; K=6+ created thin segments marketing couldn't act on

**Why weekly refresh, not daily:**
Daily caused 12-15% migration instability. Weekly with rolling windows produces 5-8% — stable for campaign planning.

## Project Structure

```
customer-segmentation/
├── README.md
├── requirements.txt
├── .gitignore
├── config/
│   └── project_config.py
├── sql/
│   ├── eda/                           # 7 EDA + diagnostic queries
│   ├── features/
│   │   └── build_customer_features.sql
│   └── schema/
├── src/
│   ├── data/
│   │   ├── load_to_bigquery.py
│   │   └── export_for_tableau.py
│   ├── features/
│   │   └── preprocess_features.py
│   ├── models/
│   │   └── clustering.py
│   ├── evaluation/
│   │   ├── validation.py
│   │   └── drift_monitor.py
│   └── utils/
├── data/
│   ├── mock/
│   │   └── generate_mock_data.py
│   └── tableau_export/
├── docs/
│   ├── problem_statement.md
│   ├── old_segment_diagnosis.md
│   ├── feature_design.md
│   └── segment_table_schema.md
├── artifacts/
│   ├── kmeans_model.joblib
│   ├── clustering_scaler.joblib
│   └── feature_scaler.joblib
├── plots/
│   ├── k_selection.png
│   ├── dendrogram.png
│   └── segment_profiles.png
└── notebooks/
```

## Tech Stack

| Component | Tool |
|---|---|
| Data Warehouse | BigQuery (GCP) |
| Python ↔ BQ | google-cloud-bigquery client |
| Feature Engineering | SQL in BigQuery |
| Preprocessing | pandas, scikit-learn |
| Clustering | scikit-learn (K-Means, Hierarchical, GMM) |
| Validation | scipy.stats |
| Dashboard | Tableau Public |
| Version Control | Git/GitHub |

## Dashboard

**Live:** [Tableau Public — Segment Analytics](https://public.tableau.com/app/profile/samuel.vurity3854/viz/CustomerSegmentation_17746570658530/SegmentAnalytics)

## Validation Results

| Metric | Control | Treatment | Lift | p-value | Significant |
|---|---|---|---|---|---|
| Open Rate | 19.1% | 24.2% | +27% | 0.0006 | Yes |
| Click Rate | 4.4% | 6.9% | +56% | 0.003 | Yes |
| Conversion Rate | 2.0% | 2.6% | +30% | 0.273 | No (power) |

Segment stability: 6-7% weekly migration, below 15% threshold. Three consecutive weeks passed.

## How to Run

```bash
git clone https://github.com/samuelvurity/customer-segmentation.git
cd customer-segmentation
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

gcloud config set project <your-project-id>
gcloud auth application-default login

python3 src/data/load_to_bigquery.py
# Run sql/features/build_customer_features.sql in BigQuery console
python3 src/features/preprocess_features.py
python3 src/models/clustering.py
python3 src/evaluation/validation.py
python3 src/evaluation/drift_monitor.py
python3 src/data/export_for_tableau.py
```
