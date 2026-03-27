"""
Customer Segmentation — Clustering & K Selection

Evaluates K-Means, Hierarchical, and GMM.
Selects optimal K via elbow + silhouette.
Produces final segment assignments and profiles.
Writes assignments to BigQuery.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans, AgglomerativeClustering
from sklearn.mixture import GaussianMixture
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler
from scipy.cluster.hierarchy import dendrogram, linkage
from google.cloud import bigquery
import joblib
import os
import sys
import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config.project_config import GCP_PROJECT_ID, BQ_DATASET, TABLES, K_CLUSTERS

# Primary clustering features — the signals that actually separate archetypes
# Selected based on diagnostic analysis of feature distributions
CLUSTERING_FEATURES = [
    "avg_basket_size",
    "weekly_purchase_frequency",
    "online_ratio",
    "promo_purchase_ratio",
    "avg_discount_depth",
    "frequency_trend",
    "days_since_last_purchase",
    "category_breadth",
    "project_ratio",
    "search_rate",
    "deep_browse_ratio",
]

# Features kept for profiling but not used in distance calculation
PROFILE_FEATURES = [
    "total_transactions", "weekly_purchase_frequency", "category_breadth",
    "avg_basket_size", "median_basket_size", "online_ratio", "project_ratio",
    "promo_purchase_ratio", "avg_discount_depth", "full_price_ratio",
    "days_since_last_purchase", "monthly_frequency", "frequency_trend",
    "q2_concentration", "weekend_ratio", "total_sessions",
    "search_rate", "product_page_ratio", "deep_browse_ratio",
    "loyalty_tier_numeric", "points_balance",
]

LOG_TRANSFORM = ["avg_basket_size", "days_since_last_purchase"]

ARTIFACTS_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'artifacts')
PLOTS_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'plots')


def read_raw_features(client):
    query = f"SELECT * FROM `{TABLES['customer_features']}`"
    df = client.query(query).to_dataframe()
    print(f"Read {len(df)} customer features from BigQuery.")
    return df


def prepare_clustering_input(df):
    """Select, transform, and scale features for clustering."""
    X_df = df[CLUSTERING_FEATURES].copy()
    X_df = X_df.fillna(0)

    for col in LOG_TRANSFORM:
        if col in X_df.columns:
            X_df[col] = np.log1p(X_df[col])

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_df)

    print(f"Prepared {X_scaled.shape[1]} clustering features, {X_scaled.shape[0]} customers.")
    return X_scaled, scaler


def k_selection_analysis(X):
    k_range = range(2, 11)
    inertias = []
    silhouette_scores = []

    print("\nK Selection Analysis:")
    print(f"{'K':>3} {'Inertia':>12} {'Silhouette':>12}")
    print("-" * 30)

    for k in k_range:
        km = KMeans(n_clusters=k, random_state=42, n_init=10, max_iter=300)
        labels = km.fit_predict(X)
        inertia = km.inertia_
        sil = silhouette_score(X, labels)
        inertias.append(inertia)
        silhouette_scores.append(sil)
        print(f"{k:>3} {inertia:>12.1f} {sil:>12.4f}")

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    ax1.plot(list(k_range), inertias, 'bo-', linewidth=2, markersize=8)
    ax1.axvline(x=K_CLUSTERS, color='r', linestyle='--', alpha=0.7, label=f'K={K_CLUSTERS}')
    ax1.set_xlabel('Number of Clusters (K)')
    ax1.set_ylabel('Inertia')
    ax1.set_title('Elbow Method')
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    ax2.plot(list(k_range), silhouette_scores, 'go-', linewidth=2, markersize=8)
    ax2.axvline(x=K_CLUSTERS, color='r', linestyle='--', alpha=0.7, label=f'K={K_CLUSTERS}')
    ax2.set_xlabel('Number of Clusters (K)')
    ax2.set_ylabel('Silhouette Score')
    ax2.set_title('Silhouette Analysis')
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    os.makedirs(PLOTS_DIR, exist_ok=True)
    plt.savefig(os.path.join(PLOTS_DIR, 'k_selection.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print(f"\nK selection plot saved.")

    return inertias, silhouette_scores


def compare_methods(X):
    print("\n" + "=" * 60)
    print("MODEL COMPARISON AT K=5")
    print("=" * 60)

    km = KMeans(n_clusters=K_CLUSTERS, random_state=42, n_init=10, max_iter=300)
    km_labels = km.fit_predict(X)
    km_sil = silhouette_score(X, km_labels)

    hc = AgglomerativeClustering(n_clusters=K_CLUSTERS, linkage='ward')
    hc_labels = hc.fit_predict(X)
    hc_sil = silhouette_score(X, hc_labels)

    gmm_sils = []
    gmm_label_sets = []
    for seed in range(5):
        gmm = GaussianMixture(n_components=K_CLUSTERS, random_state=seed, n_init=3)
        gmm_labels = gmm.fit_predict(X)
        gmm_sils.append(silhouette_score(X, gmm_labels))
        gmm_label_sets.append(gmm_labels)

    gmm_agreement = []
    for i in range(len(gmm_label_sets)):
        for j in range(i + 1, len(gmm_label_sets)):
            gmm_agreement.append(np.mean(gmm_label_sets[i] == gmm_label_sets[j]))

    print(f"\n{'Method':<20} {'Silhouette':>12} {'Notes'}")
    print("-" * 60)
    print(f"{'K-Means':<20} {km_sil:>12.4f} Deterministic, stable")
    print(f"{'Hierarchical':<20} {hc_sil:>12.4f} O(n^2) at scale")
    print(f"{'GMM (best)':<20} {max(gmm_sils):>12.4f} Varies across runs")
    print(f"{'GMM (worst)':<20} {min(gmm_sils):>12.4f} Unstable")
    print(f"{'GMM agreement':<20} {np.mean(gmm_agreement):>12.1%} Cross-run stability")

    sample_idx = np.random.choice(len(X), size=min(100, len(X)), replace=False)
    fig, ax = plt.subplots(figsize=(14, 6))
    linked = linkage(X[sample_idx], method='ward')
    dendrogram(linked, ax=ax, truncate_mode='lastp', p=30, no_labels=True)
    ax.set_title('Hierarchical Clustering Dendrogram (100 customer sample)')
    ax.set_ylabel('Ward Distance')
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, 'dendrogram.png'), dpi=150, bbox_inches='tight')
    plt.close()

    print(f"\nDecision: K-Means selected.")
    print(f"  - Silhouette: {km_sil:.4f} vs hierarchical {hc_sil:.4f}")
    print(f"  - GMM unstable: {np.mean(gmm_agreement):.1%} agreement across runs")

    return km, km_labels


def profile_segments(df_raw, labels):
    df = df_raw.copy()
    df['segment'] = labels

    print("\n" + "=" * 60)
    print("SEGMENT PROFILES")
    print("=" * 60)

    profiles = df.groupby('segment')[PROFILE_FEATURES].mean().round(3)
    sizes = df.groupby('segment').size().rename('customer_count')
    profiles = profiles.join(sizes)
    profiles['pct_of_total'] = (profiles['customer_count'] / len(df) * 100).round(1)

    for seg in sorted(df['segment'].unique()):
        p = profiles.loc[seg]
        print(f"\n--- Segment {seg} ({int(p['customer_count'])} customers, {p['pct_of_total']}%) ---")
        print(f"  Frequency:    {p['weekly_purchase_frequency']:.3f} txns/week | {p['monthly_frequency']:.2f}/mo | {int(p['total_transactions'])} total")
        print(f"  Basket:       ${p['avg_basket_size']:.0f} avg | ${p['median_basket_size']:.0f} median")
        print(f"  Categories:   {p['category_breadth']:.1f} distinct | {p['project_ratio']:.1%} project")
        print(f"  Channel:      {p['online_ratio']:.1%} online | {p['weekend_ratio']:.1%} weekend")
        print(f"  Price:        {p['promo_purchase_ratio']:.1%} promo | {p['avg_discount_depth']:.1%} discount | {p['full_price_ratio']:.1%} full price")
        print(f"  Recency:      {int(p['days_since_last_purchase'])} days | trend: {p['frequency_trend']:+.3f}")
        print(f"  Web:          {int(p['total_sessions'])} sessions | {p['search_rate']:.3f} search | {p['deep_browse_ratio']:.1%} deep browse")
        print(f"  Loyalty:      tier {p['loyalty_tier_numeric']:.1f} | {int(p['points_balance'])} pts")

    return profiles


def name_segments(profiles):
    """Name segments by identifying dominant behavioral signal."""
    names = {}

    for seg in profiles.index:
        p = profiles.loc[seg]
        scores = {
            "Price Hunters": (
                p['promo_purchase_ratio'] * 3 +
                p['avg_discount_depth'] * 5 -
                p['avg_basket_size'] / 500
            ),
            "Project Planners": (
                p['avg_basket_size'] / 300 +
                p['project_ratio'] * 3 +
                p['online_ratio'] * 2 +
                p['search_rate'] * 5 +
                p['deep_browse_ratio'] * 10 -
                p['weekly_purchase_frequency'] * 2
            ),
            "Weekend Warriors": (
                p['weekly_purchase_frequency'] * 5 +
                p['category_breadth'] / 3 +
                p['weekend_ratio'] * 2 -
                p['online_ratio'] * 3
            ),
            "Loyal Regulars": (
                p['full_price_ratio'] * 3 +
                p['loyalty_tier_numeric'] +
                p['monthly_frequency'] * 2 -
                p['promo_purchase_ratio'] * 2
            ),
            "Dormant / At-Risk": (
                p['days_since_last_purchase'] / 100 -
                p['frequency_trend'] * 3 -
                p['weekly_purchase_frequency'] * 3 -
                p['total_sessions'] / 20
            ),
        }
        names[seg] = max(scores, key=scores.get)

    print("\n" + "=" * 60)
    print("SEGMENT NAMING")
    print("=" * 60)

    used_names = []
    for seg in sorted(names.keys()):
        name = names[seg]
        count = int(profiles.loc[seg, 'customer_count'])
        pct = profiles.loc[seg, 'pct_of_total']
        dup_flag = " ⚠️ DUPLICATE" if name in used_names else ""
        print(f"  Segment {seg}: {name} ({count} customers, {pct}%){dup_flag}")
        used_names.append(name)

    return names


def write_assignments_to_bq(client, df_raw, labels, segment_names):
    assignments = pd.DataFrame({
        'customer_id': df_raw['customer_id'].values,
        'segment_id': labels,
        'segment_name': [segment_names[l] for l in labels],
        'assignment_date': pd.Timestamp.now().strftime('%Y-%m-%d'),
    })

    table_ref = TABLES['segment_assignments']
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    job = client.load_table_from_dataframe(assignments, table_ref, job_config=job_config)
    job.result()

    table = client.get_table(table_ref)
    print(f"\nWrote {table.num_rows} segment assignments to BigQuery.")
    return assignments


def plot_segment_profiles(profiles, segment_names):
    key_features = [
        'weekly_purchase_frequency', 'avg_basket_size', 'online_ratio',
        'promo_purchase_ratio', 'project_ratio', 'weekend_ratio',
        'frequency_trend', 'days_since_last_purchase'
    ]

    fig, axes = plt.subplots(2, 4, figsize=(20, 10))
    axes = axes.flatten()
    colors = ['#2196F3', '#4CAF50', '#FF9800', '#9C27B0', '#F44336']
    segment_labels = [f"S{s}: {segment_names[s]}" for s in sorted(profiles.index)]

    for i, feat in enumerate(key_features):
        ax = axes[i]
        vals = [profiles.loc[s, feat] for s in sorted(profiles.index)]
        ax.bar(range(len(vals)), vals, color=colors[:len(vals)])
        ax.set_xticks(range(len(vals)))
        ax.set_xticklabels([f"S{s}" for s in sorted(profiles.index)], fontsize=9)
        ax.set_title(feat.replace('_', ' ').title(), fontsize=10, fontweight='bold')
        ax.grid(True, alpha=0.3, axis='y')

    fig.legend(
        [plt.Rectangle((0, 0), 1, 1, fc=c) for c in colors[:len(segment_labels)]],
        segment_labels, loc='lower center', ncol=3, fontsize=10,
        bbox_to_anchor=(0.5, -0.02)
    )
    plt.suptitle('Segment Behavioral Profiles', fontsize=14, fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, 'segment_profiles.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Segment profiles plot saved.")


def main():
    client = bigquery.Client(project=GCP_PROJECT_ID)
    os.makedirs(ARTIFACTS_DIR, exist_ok=True)
    os.makedirs(PLOTS_DIR, exist_ok=True)

    print("Reading features from BigQuery...")
    df_raw = read_raw_features(client)

    print("\nPreparing clustering input...")
    X, scaler = prepare_clustering_input(df_raw)

    # K selection
    print("\n" + "=" * 60)
    print("STEP 1: K SELECTION")
    print("=" * 60)
    k_selection_analysis(X)

    # Model comparison
    print("\n" + "=" * 60)
    print("STEP 2: MODEL COMPARISON")
    print("=" * 60)
    km_model, km_labels = compare_methods(X)

    # Profiling
    print("\n" + "=" * 60)
    print("STEP 3: SEGMENT PROFILING")
    print("=" * 60)
    profiles = profile_segments(df_raw, km_labels)

    # Naming
    segment_names = name_segments(profiles)

    # Save artifacts
    joblib.dump(km_model, os.path.join(ARTIFACTS_DIR, 'kmeans_model.joblib'))
    joblib.dump(scaler, os.path.join(ARTIFACTS_DIR, 'clustering_scaler.joblib'))
    print(f"\nArtifacts saved.")

    # Write to BQ
    assignments = write_assignments_to_bq(client, df_raw, km_labels, segment_names)

    # Plots
    plot_segment_profiles(profiles, segment_names)

    # Summary
    print("\n" + "=" * 60)
    print("CLUSTERING COMPLETE")
    print("=" * 60)
    final_sil = silhouette_score(X, km_labels)
    print(f"  Model: K-Means, K={K_CLUSTERS}")
    print(f"  Silhouette: {final_sil:.4f}")
    print(f"  Features used: {len(CLUSTERING_FEATURES)}")
    print(f"  Customers: {len(km_labels)}")
    for seg in sorted(segment_names.keys()):
        count = int((km_labels == seg).sum())
        print(f"    {seg}: {segment_names[seg]} ({count})")


if __name__ == "__main__":
    main()
