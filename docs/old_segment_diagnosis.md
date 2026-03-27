# Old Segment Diagnosis

## Summary

The current customer segments are not driving campaign performance because they are based on demographics (age bracket + region), not behavior. All four segments show nearly identical behavioral metrics and campaign response rates.

## Evidence

### 1. Segments are demographic-only

Four segments based on age and geography:
- Young Urban (18-30, Metro): 12.1% of customers
- Suburban Family (31-45, Suburban): 31.1%
- Mature Homeowner (46-60, Mixed): 34.5%
- Senior Established (60+, Rural/Suburban): 22.3%

These labels describe WHO the customer is, not HOW they shop.

### 2. Segments are stale

All assignments dated 2023-01-15 — over two years old with no refresh. Customer behavior has changed; segments have not.

### 3. Behavioral metrics are flat across segments

| Segment | Avg Basket | Median Basket | Txns/Cust | Online % | Promo % |
|---|---|---|---|---|---|
| Young Urban | $203 | $62 | 15.0 | 29.5% | 39.3% |
| Suburban Family | $181 | $57 | 16.4 | 28.1% | 42.5% |
| Mature Homeowner | $171 | $57 | 16.7 | 28.8% | 43.5% |
| Senior Established | $192 | $61 | 15.8 | 27.5% | 39.4% |

No meaningful behavioral separation. Every segment shops the same way.

### 4. Campaign performance is flat

| Segment | Open Rate | Click Rate | Conversion | Rev/Send |
|---|---|---|---|---|
| Young Urban | 20.6% | 6.1% | 2.5% | $4.25 |
| Suburban Family | 17.5% | 3.8% | 1.5% | $2.40 |
| Mature Homeowner | 19.5% | 6.3% | 2.2% | $3.63 |
| Senior Established | 18.8% | 4.3% | 1.4% | $2.72 |

Targeting by segment produces no meaningful lift over untargeted sends.

### 5. Root cause

Each demographic segment contains a mix of every behavioral type — frequent buyers, project buyers, promo hunters, and dormant customers all exist in every segment in roughly equal proportions. Demographics don't predict purchase behavior.

## Recommendation

Redesign segmentation using behavioral signals: purchase frequency, price sensitivity, channel preference, recency, and seasonal patterns. These features capture HOW customers shop, which is what determines campaign response.

## Next Steps

1. Design behavioral feature set
2. Engineer features from transaction, web, and loyalty data
3. Cluster using unsupervised methods (K-Means)
4. Validate via holdout campaign test
5. Deploy with automated weekly refresh
