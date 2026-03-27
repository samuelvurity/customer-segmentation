# Feature Design Document — Customer Segmentation

## Approach

Three feature axes capturing HOW customers shop, not WHO they are.
Variable rolling windows: 12 weeks (Q1/Q3 transitional periods), 8 weeks (Q2/Q4 peak seasons).
Minimum 3 transactions required for inclusion — below that, customer assigned to "New/Low-Activity" bucket.

## Feature Inventory

### Behavioral Axis — "How does this customer shop?"

| Feature | Definition | Why It Matters |
|---|---|---|
| weekly_purchase_frequency | Transactions per week within rolling window | Separates regulars from occasional buyers |
| total_transactions | Lifetime transaction count | Scale indicator |
| category_breadth | Count of distinct categories purchased | Broad shoppers vs single-department buyers |
| avg_basket_size | Mean transaction amount | Small maintenance runs vs big project buys |
| median_basket_size | Median transaction amount | Robust to outlier purchases |
| online_ratio | Share of transactions made online | Channel preference for campaign routing |
| project_ratio | Share of transactions in project categories | Project buyer signal |
| is_project_buyer | Binary: 2+ project categories purchased in window | Flags active project behavior |

Project categories: Lumber, Flooring, Kitchen & Bath, Plumbing, Appliances.
Maintenance categories: Hardware, Paint, Lighting, Electrical, Garden & Outdoor.

Project detection: customer purchased from 2+ project categories within the rolling window. At HD, this was supplemented with web navigation signals (browsing project pages), customer service interactions, and pre-defined product chain detection (e.g., deck boards + screws + stain). Simulation uses category combination proxy.

### Price Sensitivity Axis — "What motivates this customer to buy?"

| Feature | Definition | Why It Matters |
|---|---|---|
| promo_purchase_ratio | Share of transactions during a promo | Promo-driven vs organic buyer |
| avg_discount_depth | Average discount % when promo applied | Shallow coupon responder vs deep discount chaser |
| full_price_ratio | Share of transactions at full price | Willingness to pay full price |

Marketing application: don't waste 5% off on someone who only responds to 30% off. Don't send promos to full-price buyers — it trains them to wait for sales.

### Time-Aware Axis — "What's this customer's trajectory?"

| Feature | Definition | Why It Matters |
|---|---|---|
| days_since_last_purchase | Days between reference date and last transaction | Churn signal — higher = more at risk |
| monthly_frequency | Transactions per month over full history | Long-term shopping cadence |
| frequency_trend | Second-half txn count / first-half txn count - 1 | Positive = growing, negative = declining |
| q2_concentration | Share of transactions in Q2 (Apr-Jun) | Spring/summer seasonal buyer signal |
| weekend_ratio | Share of transactions on Saturday/Sunday | Weekend warrior detection |

Frequency trend is critical: a customer with 20 transactions sounds good, but if 18 were in the first year and 2 were in the last year, they're churning.

### Web Behavior Axis — "How does this customer research?"

| Feature | Definition | Why It Matters |
|---|---|---|
| total_sessions | Count of distinct web sessions | Digital engagement level |
| avg_pages_per_session | Pages viewed per session | Browse depth |
| search_rate | Search events per session | Active search vs browsing |
| product_page_ratio | Share of page views on product pages | Purchase intent signal |
| cart_page_ratio | Share of page views on cart page | Conversion proximity |
| mobile_ratio | Share of sessions on mobile | Device preference |
| deep_browse_ratio | Share of sessions with 5+ pages | Research behavior indicator |

Web features feed the project buyer detection: high search rate + deep browse sessions + product page concentration = research-heavy buyer, likely planning a project.

### Loyalty Features

| Feature | Definition | Why It Matters |
|---|---|---|
| loyalty_tier_numeric | Gold=3, Silver=2, Bronze=1, None=0 | Program engagement level |
| points_balance | Current loyalty points | Reward proximity signal |

## Regional Adjustment

Region is included as context for downstream analysis but NOT as a clustering feature. Regional differences in seasonality (Southeast spring starts earlier than Midwest) are captured through the time-aware features — the variable window absorbs seasonal distortion rather than requiring explicit regional encoding.

At HD, started with standard geographic zones and adjusted boundaries based on observed seasonal behavior patterns in the data.

## Features NOT Included (and why)

| Excluded | Reason |
|---|---|
| Age bracket | Demographic — proven not predictive of behavior |
| Gender | Demographic — same reason |
| Zip code / region as clustering feature | Would re-create demographic segments. Region is context, not input. |
| Absolute spend | Captured by basket size. Absolute lifetime spend conflates frequency with ticket size. |
| Customer service interactions | Not available in simulation data. At HD, fed into project detection. |
| Product chain detection | Requires product catalog with chain definitions. At HD, pre-defined by business. |

## Preprocessing Plan

1. Log transform: avg_basket_size, median_basket_size, total_transactions, total_sessions, points_balance (right-skewed)
2. Standard scaling: all features to zero mean, unit variance (K-Means uses distance — scale matters)
3. Missing value handling: web features COALESCE to 0 for customers with no web activity. Loyalty COALESCE to 0 for non-members.
