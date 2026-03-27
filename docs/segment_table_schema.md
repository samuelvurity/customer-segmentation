# Segment Assignment Table — Production Schema

## Table: `hd_segmentation.segment_assignments`

### Purpose
Stores the current segment assignment for each eligible customer.
Consumed by lifecycle marketing (email, push, SMS), forecasting system, MMM, and Tableau dashboards.

### Schema

| Column | Type | Description |
|---|---|---|
| customer_id | STRING | Primary key. Maps to customer_master. |
| segment_id | INT64 | Numeric segment identifier (0-4). |
| segment_name | STRING | Human-readable segment label. |
| assignment_date | DATE | Date of latest assignment/refresh. |

### Refresh Cadence
**Weekly.** Scheduled batch job runs every Monday at 6:00 AM CT.

Pipeline: `customer_features` rebuild → preprocessing → K-Means scoring → write to `segment_assignments` → Tableau extract refresh.

### Why Weekly, Not Daily
Daily refresh was tested and rejected:
- 12-15% of customers shifted segments daily due to minor behavioral fluctuations
- Marketing campaigns targeting Segment X on Monday would find different customers in Segment X on Tuesday
- Weekly cadence with 8-12 week rolling features produces 5-8% migration per week — stable enough for campaign targeting

### Downstream Consumers

| Consumer | How They Use It | Update Dependency |
|---|---|---|
| Email/Push/SMS platform | Segment-based campaign targeting | Reads after Monday refresh |
| Forecasting system | Segment as input feature | Reads on forecast run (weekly) |
| MMM | Segment-level attribution | Reads on model refresh (weekly) |
| Tableau dashboards | Segment health, campaign performance | Extract refreshes after Monday AM |
| Cohort analysis | Segment-level retention curves | Ad-hoc reads |

### Edge Cases

#### New Customers (< 3 transactions)
- Not included in `segment_assignments` table
- Assigned to implicit "New/Low-Activity" bucket
- After crossing 3-transaction threshold, picked up on next weekly refresh
- DE should monitor count of eligible-but-unassigned customers (drift_monitor.py does this)

#### Seasonal Distortion
- Features use 8-12 week rolling windows depending on quarter
- Q2/Q4 (peak seasons): 8-week window to capture current behavior
- Q1/Q3 (transitional): 12-week window for stability
- Black Friday: a single promo weekend does NOT flip a customer from "Loyal Regular" to "Price Hunter" because the rolling window dilutes it

#### Power Users (Contractors/Property Managers)
- Log transform on basket size and transaction count prevents outlier dominance in clustering
- These customers typically land in "Weekend Warriors" (high frequency, broad category) or get their own micro-cluster if K is increased
- At K=5, log transform keeps them within the Weekend Warriors segment without distortion

#### Segment Boundary Customers
- Some customers sit near the boundary between two segments
- Weekly refresh may cause them to flip back and forth
- Stability threshold: >15% overall migration in one week triggers investigation
- Individual flip-flopping is acceptable if overall distribution is stable

#### Regional Differences
- Region is NOT a clustering feature (would recreate demographic segments)
- Seasonal differences across regions are captured through time-aware features
- Southeast spring starts earlier than Midwest — the rolling window adapts to actual purchase patterns, not calendar-defined seasons

### Monitoring
`drift_monitor.py` runs after each refresh and checks:
1. Segment size distribution (no segment <5% or >40%)
2. New customer queue (eligible but unassigned)
3. Migration rate vs 15% threshold
4. Feature distribution drift

### DE Handoff Requirements
1. BigQuery table with WRITE_TRUNCATE on each refresh (full replace, not append)
2. Vertex AI batch job triggered on schedule (or Cloud Scheduler + Cloud Functions)
3. Feature table must be rebuilt BEFORE scoring (stale features = wrong assignments)
4. Tableau extract refresh must trigger AFTER assignment table is updated
5. Alerting: if migration rate > 15% OR feature drift detected, notify DS team before marketing acts on new segments
