# Customer Segmentation — Problem Statement & Initial Approach

## Date: Week 1
## Stakeholder: Marketing Lead
## Author: Samuel Vurity

---

## Problem as Stated

Marketing reports that current customer segments are "not working." Campaign performance shows no meaningful difference between targeted and untargeted sends. Marketing is spending against segments but seeing no lift.

## Discovery Questions & Answers

| Question | Answer |
|---|---|
| What does "not working" mean specifically? | No measurable campaign lift from segment-based targeting. Same performance whether they target or not. |
| Who built the current segments and when? | Built by a previous analyst, unknown timeline. Based on demographics (age, location). No documentation on logic. No evidence of refresh. |
| Are segments being used for differentiation? | Yes — different campaigns sent to different segments, but segments may not reflect actual behavioral differences. |
| What channels are campaigns running through? | Email, push, SMS. All consume segment assignments. |
| What does success look like? | Segments that produce measurable campaign lift. Marketing wants to know WHO to target with WHAT. |
| What data is available? | BigQuery access to transactions, web events (Adobe Analytics + GA4), loyalty program, customer master, campaign history. |

## Initial Hypothesis

Current segments are demographic-only and stale. Demographics (age, location) don't predict purchase behavior or campaign response. Two customers in the same age bracket and zip code can have completely different shopping patterns.

## Proposed Approach

1. Profile existing segments — quantify exactly why they're failing (behavioral overlap, flat campaign performance, staleness)
2. Design behavioral feature set — capture how customers actually shop (frequency, price sensitivity, channel preference, recency, seasonality)
3. Build new segments using unsupervised clustering on behavioral features
4. Validate via holdout test — prove lift with statistical rigor
5. Deploy with automated refresh — segments stay current
6. Build dashboard — marketing can self-serve segment insights

## What I Need

- BigQuery access (granted)
- Campaign performance data broken down by current segment
- Confirmation of downstream systems consuming segments (email, push, SMS platforms)
- Stakeholder time for segment profile review once new segments are ready

## Success Metric

Measurable campaign lift (response rate, conversion, revenue per customer) from segment-based targeting vs. untargeted baseline.
