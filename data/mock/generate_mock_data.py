"""
Mock data generator V2 — sharper archetype separation.

Key changes from V1:
- More distinct purchase frequency ranges between archetypes
- Sharper price sensitivity differences
- Clearer channel preference separation
- More pronounced seasonal patterns for Project Planners
- Stronger decline signal for Dormant customers
- Basket size distributions less overlapping
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

PROJECT_CATEGORIES = ["Lumber", "Flooring", "Kitchen & Bath", "Plumbing", "Appliances"]
MAINTENANCE_CATEGORIES = ["Hardware", "Paint", "Lighting", "Electrical", "Garden & Outdoor"]

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


def _random_dates(start, end, n):
    delta = (end - start).days
    if delta <= 0:
        return [start] * n
    random_days = np.random.randint(0, delta, size=n)
    return [start + timedelta(days=int(d)) for d in random_days]


def _assign_archetype(n):
    return np.random.choice(
        [1, 2, 3, 4, 5],
        size=n,
        p=[0.22, 0.18, 0.25, 0.20, 0.15]
    )


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

    customers["signup_date"] = _random_dates(datetime(2018, 1, 1), datetime(2024, 6, 1), n)
    customers["_archetype"] = _assign_archetype(n)
    customers["old_segment_id"] = customers.apply(
        lambda r: _assign_old_segment(r["age_bracket"], r["region"]), axis=1
    )

    return customers


def generate_transactions(customers):
    rows = []

    # Archetype configs with SHARP separation
    configs = {
        # Weekend Warriors: HIGH frequency, LOW-MID basket, broad categories, IN-STORE, moderate promo
        1: {
            "freq": (25, 50),
            "basket_base": (30, 90),
            "basket_high": (90, 250),
            "high_prob": 0.10,
            "online_pct": 0.15,
            "promo_pct": 0.25,
            "discount_range": (0.05, 0.15),
            "categories": MAINTENANCE_CATEGORIES + ["Lumber"],
            "cat_breadth": (5, 8),
            "weekend_pct": 0.85,
        },
        # Project Planners: LOW frequency, HIGH basket, project categories, ONLINE research, seasonal
        2: {
            "freq": (5, 12),
            "basket_base": (200, 600),
            "basket_high": (800, 4500),
            "high_prob": 0.40,
            "online_pct": 0.55,
            "promo_pct": 0.12,
            "discount_range": (0.03, 0.10),
            "categories": PROJECT_CATEGORIES,
            "cat_breadth": (2, 4),
            "weekend_pct": 0.45,
        },
        # Price Hunters: MID frequency, LOW basket, promo-HEAVY, deep discounts, narrow categories
        3: {
            "freq": (15, 35),
            "basket_base": (25, 55),
            "basket_high": (55, 120),
            "high_prob": 0.08,
            "online_pct": 0.40,
            "promo_pct": 0.80,
            "discount_range": (0.15, 0.35),
            "categories": ["Paint", "Hardware", "Lighting", "Garden & Outdoor"],
            "cat_breadth": (2, 4),
            "weekend_pct": 0.40,
        },
        # Loyal Regulars: MID frequency, MID basket, FULL PRICE, consistent, in-store
        4: {
            "freq": (12, 25),
            "basket_base": (50, 150),
            "basket_high": (150, 500),
            "high_prob": 0.20,
            "online_pct": 0.18,
            "promo_pct": 0.10,
            "discount_range": (0.03, 0.08),
            "categories": CATEGORIES,
            "cat_breadth": (4, 8),
            "weekend_pct": 0.60,
        },
        # Dormant/At-Risk: transactions concentrated in EARLY period, then drop off sharply
        5: {
            "freq": (8, 18),
            "basket_base": (30, 80),
            "basket_high": (80, 200),
            "high_prob": 0.10,
            "online_pct": 0.30,
            "promo_pct": 0.35,
            "discount_range": (0.05, 0.20),
            "categories": ["Hardware", "Paint", "Lighting"],
            "cat_breadth": (1, 3),
            "weekend_pct": 0.50,
        },
    }

    for _, cust in customers.iterrows():
        arch = cust["_archetype"]
        cfg = configs[arch]
        cid = cust["customer_id"]

        n_txns = np.random.randint(cfg["freq"][0], cfg["freq"][1] + 1)
        cat_count = np.random.randint(cfg["cat_breadth"][0], min(cfg["cat_breadth"][1] + 1, len(cfg["categories"]) + 1))
        cust_cats = list(np.random.choice(cfg["categories"], size=min(cat_count, len(cfg["categories"])), replace=False))

        # Generate transaction dates
        if arch == 2:
            # Project Planners: clustered in spring and fall
            txn_dates = []
            for _ in range(n_txns):
                year = np.random.choice([2023, 2024, 2025], p=[0.3, 0.5, 0.2])
                if np.random.random() < 0.70:
                    if np.random.random() < 0.5:
                        month = np.random.choice([3, 4, 5, 6])
                    else:
                        month = np.random.choice([9, 10, 11])
                else:
                    month = np.random.randint(1, 13)
                day = np.random.randint(1, 28)
                try:
                    txn_dates.append(datetime(year, month, day))
                except ValueError:
                    txn_dates.append(datetime(year, month, 15))

        elif arch == 5:
            # Dormant: 80% of transactions in first half of date range, 20% sparse in second half
            midpoint = DATE_START + (DATE_END - DATE_START) / 2
            early_count = int(n_txns * 0.80)
            late_count = n_txns - early_count
            early_dates = _random_dates(DATE_START, midpoint, early_count)
            late_dates = _random_dates(midpoint, DATE_END, max(late_count, 1))
            txn_dates = early_dates + late_dates

        else:
            txn_dates = _random_dates(DATE_START, DATE_END, n_txns)

        # Weekend adjustment
        adjusted_dates = []
        for d in txn_dates:
            if np.random.random() < cfg["weekend_pct"]:
                days_to_sat = (5 - d.weekday()) % 7
                if days_to_sat == 0:
                    days_to_sat = 7 if d.weekday() != 5 else 0
                d = d + timedelta(days=days_to_sat)
            adjusted_dates.append(d)

        for txn_date in adjusted_dates:
            # Basket size
            if np.random.random() < cfg["high_prob"]:
                amount = round(np.random.uniform(cfg["basket_high"][0], cfg["basket_high"][1]), 2)
            else:
                amount = round(np.random.uniform(cfg["basket_base"][0], cfg["basket_base"][1]), 2)

            # Promo and discount
            is_promo = np.random.random() < cfg["promo_pct"]
            discount = 0.0
            if is_promo:
                discount = round(np.random.uniform(cfg["discount_range"][0], cfg["discount_range"][1]), 2)

            channel = "online" if np.random.random() < cfg["online_pct"] else "in_store"
            category = np.random.choice(cust_cats)

            rows.append({
                "customer_id": cid,
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
    return df


def generate_web_events(customers):
    rows = []

    session_configs = {
        1: {"sessions": (15, 30), "pages": (2, 4), "search_prob": 0.15, "device_probs": [0.25, 0.60, 0.15]},
        2: {"sessions": (40, 90), "pages": (5, 12), "search_prob": 0.65, "device_probs": [0.60, 0.30, 0.10]},
        3: {"sessions": (25, 50), "pages": (3, 7), "search_prob": 0.45, "device_probs": [0.35, 0.55, 0.10]},
        4: {"sessions": (10, 25), "pages": (2, 4), "search_prob": 0.15, "device_probs": [0.30, 0.50, 0.20]},
        5: {"sessions": (5, 15), "pages": (1, 3), "search_prob": 0.10, "device_probs": [0.40, 0.45, 0.15]},
    }

    for _, cust in customers.iterrows():
        arch = cust["_archetype"]
        cfg = session_configs[arch]
        cid = cust["customer_id"]

        n_sessions = np.random.randint(cfg["sessions"][0], cfg["sessions"][1] + 1)
        session_dates = _random_dates(DATE_START, DATE_END, n_sessions)

        for i, sess_date in enumerate(session_dates):
            session_id = f"SESS_{cid}_{i:04d}"
            n_pages = np.random.randint(cfg["pages"][0], cfg["pages"][1] + 1)
            device = np.random.choice(DEVICES, p=cfg["device_probs"])
            search_flag = 1 if np.random.random() < cfg["search_prob"] else 0

            for page_num in range(n_pages):
                if arch == 2:
                    page_type = np.random.choice(PAGE_TYPES, p=[0.05, 0.15, 0.45, 0.15, 0.10, 0.10])
                elif arch == 3:
                    page_type = np.random.choice(PAGE_TYPES, p=[0.10, 0.30, 0.25, 0.15, 0.15, 0.05])
                else:
                    page_type = np.random.choice(PAGE_TYPES, p=[0.15, 0.25, 0.30, 0.10, 0.10, 0.10])

                rows.append({
                    "customer_id": cid,
                    "session_id": session_id,
                    "event_date": sess_date.strftime("%Y-%m-%d"),
                    "page_type": page_type,
                    "device": device,
                    "search_flag": search_flag if page_num == 0 else 0,
                })

    df = pd.DataFrame(rows)
    if len(df) > N_WEB_EVENTS * 1.5:
        df = df.sample(n=N_WEB_EVENTS, random_state=42).reset_index(drop=True)

    df["event_id"] = [f"EVT_{i:07d}" for i in range(1, len(df) + 1)]
    df = df[["event_id", "customer_id", "session_id", "event_date",
             "page_type", "device", "search_flag"]]
    return df


def generate_loyalty(customers):
    rows = []
    tier_configs = {
        1: {"Gold": 0.35, "Silver": 0.40, "Bronze": 0.20, "None": 0.05},
        2: {"Gold": 0.15, "Silver": 0.25, "Bronze": 0.25, "None": 0.35},
        3: {"Gold": 0.05, "Silver": 0.25, "Bronze": 0.45, "None": 0.25},
        4: {"Gold": 0.50, "Silver": 0.30, "Bronze": 0.15, "None": 0.05},
        5: {"Gold": 0.03, "Silver": 0.10, "Bronze": 0.27, "None": 0.60},
    }

    for _, cust in customers.iterrows():
        arch = cust["_archetype"]
        tiers = tier_configs[arch]
        tier = np.random.choice(list(tiers.keys()), p=list(tiers.values()))

        if tier == "None":
            points, enrollment_date, last_activity = 0, None, None
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


def generate_campaigns(customers):
    rows = []
    campaign_dates = sorted(_random_dates(datetime(2023, 9, 1), datetime(2025, 2, 1), 30))

    campaigns_meta = []
    for i, cd in enumerate(campaign_dates):
        campaigns_meta.append({
            "campaign_id": f"CAMP_{i+1:04d}",
            "campaign_name": f"Campaign_{cd.strftime('%Y%m%d')}",
            "send_date": cd.strftime("%Y-%m-%d"),
            "channel": np.random.choice(CAMPAIGN_CHANNELS, p=[0.50, 0.30, 0.20]),
        })

    for camp in campaigns_meta:
        targeted_segment = np.random.choice([1, 2, 3, 4])
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

    return pd.DataFrame(rows)


def generate_all():
    print("Generating customer master...")
    customers = generate_customer_master()

    print("Generating transactions...")
    transactions = generate_transactions(customers)

    print("Generating web events...")
    web_events = generate_web_events(customers)

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
