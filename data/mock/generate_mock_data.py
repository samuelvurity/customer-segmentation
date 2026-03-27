"""
Mock data generator for HD Customer Segmentation simulation.

Generates realistic Home Depot-scale customer data with embedded behavioral
archetypes that demographic segments can't capture but behavioral clustering will.

Customer archetypes (hidden — these are what K-Means should discover):
  1. Weekend Warriors  — high frequency, broad category, in-store, moderate spend
  2. Project Planners  — seasonal bursts, high basket, online research + in-store buy
  3. Price Hunters     — promo-driven, coupon-heavy, narrow categories
  4. Loyal Regulars    — steady mid-frequency, full-price, consistent patterns
  5. Dormant/At-Risk   — declining frequency, increasing recency, fading engagement

Old segments are purely demographic (age + region) and intentionally cross-cut
the behavioral archetypes — a "Young Urban" segment contains Weekend Warriors,
Price Hunters, AND Dormant customers in roughly equal measure.
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config.project_config import N_CUSTOMERS, N_TRANSACTIONS, N_WEB_EVENTS, N_CAMPAIGNS

np.random.seed(42)

CATEGORIES = [
    "Lumber", "Plumbing", "Electrical", "Paint",
    "Garden & Outdoor", "Appliances", "Hardware",
    "Flooring", "Kitchen & Bath", "Lighting"
]

CHANNELS = ["in_store", "online"]
CAMPAIGN_CHANNELS = ["email", "push", "sms"]
DEVICES = ["desktop", "mobile", "tablet"]
PAGE_TYPES = ["home", "category", "product", "search", "cart", "checkout"]

OLD_SEGMENT_NAMES = {
    1: "Young Urban (18-30, Metro)",
    2: "Suburban Family (31-45, Suburban)",
    3: "Mature Homeowner (46-60, Mixed)",
    4: "Senior Established (60+, Rural/Suburban)",
}

REGIONS = ["Northeast", "Southeast", "Midwest", "Southwest", "West"]

DATE_START = datetime(2023, 6, 1)
DATE_END = datetime(2025, 2, 28)
TOTAL_DAYS = (DATE_END - DATE_START).days


def _random_dates(start, end, n):
    delta = (end - start).days
    random_days = np.random.randint(0, delta, size=n)
    return [start + timedelta(days=int(d)) for d in random_days]


def _assign_archetype(n):
    archetypes = np.random.choice(
        [1, 2, 3, 4, 5],
        size=n,
        p=[0.22, 0.18, 0.25, 0.20, 0.15]
    )
    return archetypes


def _assign_old_segment(age_bracket, region):
    if age_bracket in ["18-25", "26-35"] and region in ["Northeast", "West"]:
        return 1
    elif age_bracket in ["26-35", "36-45"] and region in ["Midwest", "Southeast", "Southwest"]:
        return 2
    elif age_bracket in ["46-55", "56-65"]:
        return 3
    else:
        return 4


def generate_customer_master(n=N_CUSTOMERS):
    age_brackets = ["18-25", "26-35", "36-45", "46-55", "56-65", "65+"]
    age_probs = [0.08, 0.22, 0.28, 0.22, 0.13, 0.07]

    customers = pd.DataFrame({
        "customer_id": [f"CUST_{i:05d}" for i in range(1, n + 1)],
        "age_bracket": np.random.choice(age_brackets, size=n, p=age_probs),
        "region": np.random.choice(REGIONS, size=n, p=[0.18, 0.22, 0.25, 0.17, 0.18]),
        "gender": np.random.choice(["M", "F", "Unknown"], size=n, p=[0.52, 0.38, 0.10]),
    })

    signup_dates = _random_dates(
        datetime(2018, 1, 1),
        datetime(2024, 6, 1),
        n
    )
    customers["signup_date"] = signup_dates

    archetypes = _assign_archetype(n)
    customers["_archetype"] = archetypes

    customers["old_segment_id"] = customers.apply(
        lambda r: _assign_old_segment(r["age_bracket"], r["region"]), axis=1
    )

    return customers


def generate_transactions(customers, n_target=N_TRANSACTIONS):
    rows = []
    archetype_configs = {
        1: {"freq_range": (15, 40), "basket_low": (30, 80), "basket_high": (100, 400),
            "high_basket_prob": 0.15, "online_ratio": 0.25, "promo_prob": 0.30,
            "top_categories": ["Hardware", "Paint", "Garden & Outdoor", "Lumber", "Electrical"],
            "cat_breadth": (4, 8), "weekend_prob": 0.70},
        2: {"freq_range": (4, 12), "basket_low": (80, 200), "basket_high": (500, 4500),
            "high_basket_prob": 0.45, "online_ratio": 0.40, "promo_prob": 0.15,
            "top_categories": ["Flooring", "Kitchen & Bath", "Lumber", "Appliances", "Plumbing"],
            "cat_breadth": (2, 5), "weekend_prob": 0.50},
        3: {"freq_range": (10, 30), "basket_low": (25, 60), "basket_high": (60, 150),
            "high_basket_prob": 0.08, "online_ratio": 0.35, "promo_prob": 0.75,
            "top_categories": ["Paint", "Hardware", "Lighting", "Garden & Outdoor"],
            "cat_breadth": (2, 5), "weekend_prob": 0.45},
        4: {"freq_range": (8, 22), "basket_low": (40, 120), "basket_high": (120, 500),
            "high_basket_prob": 0.20, "online_ratio": 0.20, "promo_prob": 0.20,
            "top_categories": CATEGORIES,
            "cat_breadth": (3, 7), "weekend_prob": 0.55},
        5: {"freq_range": (2, 8), "basket_low": (30, 70), "basket_high": (70, 200),
            "high_basket_prob": 0.10, "online_ratio": 0.30, "promo_prob": 0.35,
            "top_categories": ["Hardware", "Paint", "Lighting"],
            "cat_breadth": (1, 3), "weekend_prob": 0.50},
    }

    for _, cust in customers.iterrows():
        arch = cust["_archetype"]
        cfg = archetype_configs[arch]

        n_txns = np.random.randint(cfg["freq_range"][0], cfg["freq_range"][1] + 1)
        cat_count = np.random.randint(cfg["cat_breadth"][0], cfg["cat_breadth"][1] + 1)
        cust_categories = list(np.random.choice(
            cfg["top_categories"],
            size=min(cat_count, len(cfg["top_categories"])),
            replace=False
        ))

        txn_dates = _random_dates(DATE_START, DATE_END, n_txns)

        if arch == 2:
            seasonal_dates = []
            for d in txn_dates:
                if np.random.random() < 0.6:
                    spring_start = datetime(d.year, 3, 15)
                    spring_end = datetime(d.year, 6, 15)
                    fall_start = datetime(d.year, 9, 1)
                    fall_end = datetime(d.year, 11, 15)
                    if np.random.random() < 0.5:
                        days_in_range = (spring_end - spring_start).days
                        seasonal_dates.append(spring_start + timedelta(days=np.random.randint(0, days_in_range)))
                    else:
                        days_in_range = (fall_end - fall_start).days
                        seasonal_dates.append(fall_start + timedelta(days=np.random.randint(0, days_in_range)))
                else:
                    seasonal_dates.append(d)
            txn_dates = seasonal_dates

        if arch == 5:
            txn_dates_sorted = sorted(txn_dates)
            midpoint = len(txn_dates_sorted) // 2
            early_dates = txn_dates_sorted[:midpoint]
            late_count = len(txn_dates_sorted) - midpoint
            if late_count > 0:
                late_start = datetime(2024, 8, 1)
                late_dates = _random_dates(late_start, DATE_END, late_count)
                late_dates = sorted(late_dates)
                sparse_late = []
                for i, d in enumerate(late_dates):
                    if np.random.random() < 0.4:
                        sparse_late.append(d)
                txn_dates = early_dates + sparse_late
                if len(txn_dates) < 2:
                    txn_dates = early_dates[:2]

        for txn_date in txn_dates:
            is_high = np.random.random() < cfg["high_basket_prob"]
            if is_high:
                amount = round(np.random.uniform(cfg["basket_high"][0], cfg["basket_high"][1]), 2)
            else:
                amount = round(np.random.uniform(cfg["basket_low"][0], cfg["basket_low"][1]), 2)

            is_promo = np.random.random() < cfg["promo_prob"]
            is_bf = txn_date.month == 11 and txn_date.day >= 20 and txn_date.day <= 30
            if is_bf:
                is_promo = True if np.random.random() < 0.8 else is_promo

            discount = 0.0
            if is_promo:
                if arch == 3:
                    discount = round(np.random.uniform(0.10, 0.35), 2)
                else:
                    discount = round(np.random.uniform(0.05, 0.20), 2)

            is_weekend = txn_date.weekday() >= 5
            if not is_weekend and np.random.random() < cfg["weekend_prob"]:
                days_to_sat = 5 - txn_date.weekday()
                if days_to_sat <= 0:
                    days_to_sat += 7
                txn_date = txn_date + timedelta(days=days_to_sat)

            channel = "online" if np.random.random() < cfg["online_ratio"] else "in_store"
            category = np.random.choice(cust_categories)

            rows.append({
                "customer_id": cust["customer_id"],
                "transaction_date": txn_date.strftime("%Y-%m-%d"),
                "amount": amount,
                "category": category,
                "channel": channel,
                "promo_flag": is_promo,
                "discount_pct": discount,
            })

    df = pd.DataFrame(rows)
    df["transaction_id"] = [f"TXN_{i:07d}" for i in range(1, len(df) + 1)]
    df = df[["transaction_id", "customer_id", "transaction_date", "amount",
             "category", "channel", "promo_flag", "discount_pct"]]

    if len(df) > n_target * 1.2:
        df = df.sample(n=n_target, random_state=42).reset_index(drop=True)
        df["transaction_id"] = [f"TXN_{i:07d}" for i in range(1, len(df) + 1)]

    return df


def generate_web_events(customers, transactions, n_target=N_WEB_EVENTS):
    rows = []
    cust_txn_dates = transactions.groupby("customer_id")["transaction_date"].apply(list).to_dict()

    for _, cust in customers.iterrows():
        arch = cust["_archetype"]
        cid = cust["customer_id"]

        if arch == 1:
            n_sessions = np.random.randint(20, 60)
            pages_per_session = (2, 6)
        elif arch == 2:
            n_sessions = np.random.randint(30, 80)
            pages_per_session = (4, 12)
        elif arch == 3:
            n_sessions = np.random.randint(25, 55)
            pages_per_session = (3, 8)
        elif arch == 4:
            n_sessions = np.random.randint(10, 35)
            pages_per_session = (2, 5)
        else:
            n_sessions = np.random.randint(5, 20)
            pages_per_session = (1, 4)

        session_dates = _random_dates(DATE_START, DATE_END, n_sessions)

        for i, sess_date in enumerate(session_dates):
            session_id = f"SESS_{cid}_{i:04d}"
            n_pages = np.random.randint(pages_per_session[0], pages_per_session[1] + 1)
            device = np.random.choice(DEVICES, p=[0.45, 0.45, 0.10])

            if arch == 2:
                device = np.random.choice(DEVICES, p=[0.55, 0.35, 0.10])

            search_flag = 1 if np.random.random() < (0.6 if arch == 2 else 0.3) else 0

            for page_num in range(n_pages):
                if page_num == 0:
                    page_type = np.random.choice(["home", "search", "category"], p=[0.4, 0.3, 0.3])
                elif page_num == n_pages - 1 and np.random.random() < 0.2:
                    page_type = "cart"
                else:
                    page_type = np.random.choice(PAGE_TYPES, p=[0.10, 0.25, 0.35, 0.10, 0.10, 0.10])

                rows.append({
                    "customer_id": cid,
                    "session_id": session_id,
                    "event_date": sess_date.strftime("%Y-%m-%d"),
                    "page_type": page_type,
                    "device": device,
                    "search_flag": search_flag if page_num == 0 else 0,
                })

    df = pd.DataFrame(rows)
    if len(df) > n_target * 1.2:
        df = df.sample(n=n_target, random_state=42).reset_index(drop=True)

    df["event_id"] = [f"EVT_{i:07d}" for i in range(1, len(df) + 1)]
    df = df[["event_id", "customer_id", "session_id", "event_date",
             "page_type", "device", "search_flag"]]

    return df


def generate_loyalty(customers):
    rows = []
    tier_by_archetype = {
        1: {"Gold": 0.30, "Silver": 0.45, "Bronze": 0.20, "None": 0.05},
        2: {"Gold": 0.20, "Silver": 0.30, "Bronze": 0.25, "None": 0.25},
        3: {"Gold": 0.10, "Silver": 0.35, "Bronze": 0.40, "None": 0.15},
        4: {"Gold": 0.40, "Silver": 0.35, "Bronze": 0.20, "None": 0.05},
        5: {"Gold": 0.05, "Silver": 0.15, "Bronze": 0.30, "None": 0.50},
    }

    for _, cust in customers.iterrows():
        arch = cust["_archetype"]
        tiers = tier_by_archetype[arch]
        tier = np.random.choice(list(tiers.keys()), p=list(tiers.values()))

        if tier == "None":
            points = 0
            enrollment_date = None
            last_activity = None
        else:
            points_range = {"Gold": (5000, 25000), "Silver": (1000, 5000), "Bronze": (100, 1000)}
            points = np.random.randint(points_range[tier][0], points_range[tier][1])
            enrollment_date = cust["signup_date"] + timedelta(days=np.random.randint(0, 180))

            if arch == 5:
                last_activity = enrollment_date + timedelta(days=np.random.randint(30, 300))
            else:
                last_activity = DATE_END - timedelta(days=np.random.randint(1, 90))

        rows.append({
            "customer_id": cust["customer_id"],
            "loyalty_tier": tier,
            "points_balance": points,
            "enrollment_date": enrollment_date.strftime("%Y-%m-%d") if enrollment_date else None,
            "last_activity_date": last_activity.strftime("%Y-%m-%d") if last_activity else None,
        })

    return pd.DataFrame(rows)


def generate_old_segments(customers):
    df = customers[["customer_id", "old_segment_id"]].copy()
    df["segment_name"] = df["old_segment_id"].map(OLD_SEGMENT_NAMES)
    df["assignment_date"] = "2023-01-15"
    return df


def generate_campaigns(customers, n_target=N_CAMPAIGNS):
    rows = []
    campaign_dates = _random_dates(datetime(2023, 9, 1), datetime(2025, 2, 1), 30)
    campaign_dates = sorted(campaign_dates)

    campaigns_meta = []
    for i, cd in enumerate(campaign_dates):
        campaigns_meta.append({
            "campaign_id": f"CAMP_{i+1:04d}",
            "campaign_name": f"Campaign_{cd.strftime('%Y%m%d')}",
            "send_date": cd.strftime("%Y-%m-%d"),
            "channel": np.random.choice(CAMPAIGN_CHANNELS, p=[0.50, 0.30, 0.20]),
        })

    for camp in campaigns_meta:
        segment_ids = [1, 2, 3, 4]
        targeted_segment = np.random.choice(segment_ids)

        eligible = customers[customers["old_segment_id"] == targeted_segment]
        sample_size = min(len(eligible), np.random.randint(80, 200))
        recipients = eligible.sample(n=sample_size, random_state=hash(camp["campaign_id"]) % 2**31)

        for _, cust in recipients.iterrows():
            base_open = np.random.uniform(0.15, 0.25)
            base_click = np.random.uniform(0.03, 0.08)
            base_convert = np.random.uniform(0.01, 0.03)

            opened = 1 if np.random.random() < base_open else 0
            clicked = 1 if opened and np.random.random() < (base_click / base_open) else 0
            converted = 1 if clicked and np.random.random() < (base_convert / base_click) else 0
            revenue = round(np.random.uniform(30, 300), 2) if converted else 0.0

            rows.append({
                "campaign_id": camp["campaign_id"],
                "campaign_name": camp["campaign_name"],
                "channel": camp["channel"],
                "send_date": camp["send_date"],
                "segment_targeted": targeted_segment,
                "customer_id": cust["customer_id"],
                "opened": opened,
                "clicked": clicked,
                "converted": converted,
                "revenue": revenue,
            })

    df = pd.DataFrame(rows)

    if len(df) > n_target:
        df = df.head(n_target)

    return df


def generate_all():
    print("Generating customer master...")
    customers = generate_customer_master()

    print("Generating transactions...")
    transactions = generate_transactions(customers)

    print("Generating web events...")
    web_events = generate_web_events(customers, transactions)

    print("Generating loyalty data...")
    loyalty = generate_loyalty(customers)

    print("Generating old segments...")
    old_segments = generate_old_segments(customers)

    print("Generating campaign data...")
    campaigns = generate_campaigns(customers)

    customers_out = customers.drop(columns=["_archetype", "old_segment_id"])

    print(f"\nGeneration complete:")
    print(f"  Customers:    {len(customers_out):,}")
    print(f"  Transactions: {len(transactions):,}")
    print(f"  Web events:   {len(web_events):,}")
    print(f"  Loyalty:      {len(loyalty):,}")
    print(f"  Old segments: {len(old_segments):,}")
    print(f"  Campaigns:    {len(campaigns):,}")

    return {
        "customer_master": customers_out,
        "transactions": transactions,
        "web_events": web_events,
        "loyalty": loyalty,
        "old_segments": old_segments,
        "campaigns": campaigns,
    }


if __name__ == "__main__":
    data = generate_all()
    for name, df in data.items():
        print(f"\n{name} sample:")
        print(df.head(3).to_string())
