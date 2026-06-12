"""
24_visualize_clusters.py (v2 - TOKEN EMBEDDING)

UMAP 2D 좌표 별도 계산 후, K-means 베스트 K와 HDBSCAN 결과를 시각화.

입력:
  data/processed/li_token_embeddings.npy
  data/processed/phase1/clusters/li_token_kmeans.parquet
  data/processed/phase1/clusters/li_token_kmeans_metrics.csv
  data/processed/phase1/clusters/li_token_hdbscan.parquet
  data/processed/phase1/clusters/li_token_hdbscan_metrics.csv

출력:
  figures/
    fig_umap_kmeans.png
    fig_umap_hdbscan.png
    fig_silhouette_curve.png
"""

from pathlib import Path
import numpy as np
import pandas as pd
import umap
import matplotlib.pyplot as plt
from sklearn.preprocessing import normalize

# ============================================================
PROJECT_ROOT = Path("/Users/vairocana/projects/sadan-chiljeong-dh")
INPUT_VECTORS = PROJECT_ROOT / "data" / "processed" / "li_token_embeddings.npy"
CLUSTER_DIR = PROJECT_ROOT / "data" / "processed" / "phase1" / "clusters"
OUTPUT_DIR = PROJECT_ROOT / "figures"

UMAP_2D_NEIGHBORS = 30
UMAP_2D_MIN_DIST = 0.1
RANDOM_STATE = 42

plt.rcParams["font.family"] = ["AppleGothic", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False



# ── 군집 정체 레이블 (보고서 5.2.2의 분석에 따른 각 군집의 지배적 표현) ──
# 군집 ID 대신 크기 내림차순 순위로 매핑: random_state 고정 하에서도 ID가 바뀌어도 안전.
KMEANS_LABELS_BY_SIZE_RANK = ["之理·此理", "道理·義理", "理會", "天理"]
HDBSCAN_LABELS_BY_SIZE_RANK = ["之理·此理", "道理·義理", "理會", "天理", "窮理"]


def annotate_clusters(ax, X2, labels, names_by_rank, exclude=(-1,)):
    """군집 중심 위에 정체 레이블을 단다 (크기 내림차순 순위 → 이름)."""
    from collections import Counter
    sizes = Counter(int(l) for l in labels if l not in exclude)
    for rank, (c, _) in enumerate(sizes.most_common()):
        if rank >= len(names_by_rank):
            break
        m = labels == c
        cx = X2[m, 0].mean()
        top = X2[m, 1].max()
        ax.annotate(
            names_by_rank[rank], (cx, top),
            xytext=(cx, top + 0.7), ha="center", va="bottom",
            fontsize=13, fontweight="bold",
            bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="gray", alpha=0.88),
        )


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading vectors and cluster results...")
    vectors = np.load(INPUT_VECTORS)
    vectors_norm = normalize(vectors, norm="l2", axis=1)

    kmeans_df = pd.read_parquet(CLUSTER_DIR / "li_token_kmeans.parquet")
    kmeans_metrics = pd.read_csv(CLUSTER_DIR / "li_token_kmeans_metrics.csv")
    hdbscan_df = pd.read_parquet(CLUSTER_DIR / "li_token_hdbscan.parquet")
    hdbscan_metrics = pd.read_csv(CLUSTER_DIR / "li_token_hdbscan_metrics.csv")

    # 1. UMAP 2D
    print("\nUMAP -> 2D (시각화용)...")
    reducer = umap.UMAP(
        n_components=2,
        n_neighbors=UMAP_2D_NEIGHBORS,
        min_dist=UMAP_2D_MIN_DIST,
        metric="cosine",
        random_state=RANDOM_STATE,
        verbose=True,
    )
    X2 = reducer.fit_transform(vectors_norm)
    np.save(CLUSTER_DIR / "li_token_umap2.npy", X2.astype(np.float32))

    # 2. Silhouette curve
    print("\nFig 1: Silhouette curve...")
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(kmeans_metrics["k"], kmeans_metrics["silhouette"], "o-", label="Token embedding (new)")
    ax.set_xlabel("K (number of clusters)")
    ax.set_ylabel("Silhouette score")
    ax.set_title("K-means silhouette by K")
    ax.legend()
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "fig_silhouette_curve.png", dpi=150)
    plt.close(fig)

    # 3. K-means 베스트 K 시각화
    best_k = int(kmeans_metrics.loc[kmeans_metrics["silhouette"].idxmax(), "k"])
    print(f"\nFig 2: K-means K={best_k} on UMAP 2D...")
    labels_k = kmeans_df[f"k{best_k}"].values

    fig, axes = plt.subplots(1, 2, figsize=(16, 7))
    # K=2 (가설 검증용)
    labels2 = kmeans_df["k2"].values
    scatter = axes[0].scatter(X2[:, 0], X2[:, 1], c=labels2, s=3, cmap="tab10", alpha=0.6)
    axes[0].set_title(f"K-means K=2 (silhouette={kmeans_metrics.loc[0, 'silhouette']:.4f})")
    axes[0].set_xlabel("UMAP-1"); axes[0].set_ylabel("UMAP-2")

    # 베스트 K
    scatter = axes[1].scatter(X2[:, 0], X2[:, 1], c=labels_k, s=3, cmap="tab10", alpha=0.6)
    best_sil = kmeans_metrics.loc[kmeans_metrics["k"] == best_k, "silhouette"].values[0]
    axes[1].set_title(f"K-means K={best_k} (silhouette={best_sil:.4f}, BEST)")
    axes[1].set_xlabel("UMAP-1"); axes[1].set_ylabel("UMAP-2")

    annotate_clusters(axes[1], X2, labels_k, KMEANS_LABELS_BY_SIZE_RANK)

    fig.suptitle("UMAP 2D + K-means clusters (理 token embeddings)", fontsize=14)
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "fig_umap_kmeans.png", dpi=150)
    plt.close(fig)

    # 4. HDBSCAN 베스트 시각화
    valid = hdbscan_metrics[
        (hdbscan_metrics["noise_pct"] >= 0) &
        (hdbscan_metrics["noise_pct"] <= 40) &
        (hdbscan_metrics["n_clusters"] >= 3)
    ]
    if len(valid) > 0:
        best_mcs = int(valid.loc[valid["silhouette_excl_noise"].idxmax(), "min_cluster_size"])
    else:
        best_mcs = int(hdbscan_metrics.loc[hdbscan_metrics["n_clusters"].idxmax(), "min_cluster_size"])

    print(f"\nFig 3: HDBSCAN mcs={best_mcs} on UMAP 2D...")
    labels_h = hdbscan_df[f"mcs{best_mcs}"].values

    fig, ax = plt.subplots(figsize=(10, 8))
    # noise는 회색
    mask_noise = labels_h == -1
    ax.scatter(X2[mask_noise, 0], X2[mask_noise, 1],
               c="lightgray", s=2, alpha=0.4, label=f"Noise ({mask_noise.sum():,})")
    # 클러스터는 색
    unique_clusters = sorted([c for c in np.unique(labels_h) if c != -1])
    cmap = plt.cm.get_cmap("tab20", len(unique_clusters))
    for i, c in enumerate(unique_clusters):
        m = labels_h == c
        ax.scatter(X2[m, 0], X2[m, 1], c=[cmap(i)], s=3, alpha=0.7,
                   label=f"C{c} ({m.sum():,})")

    h_metric_row = hdbscan_metrics[hdbscan_metrics["min_cluster_size"] == best_mcs].iloc[0]
    ax.set_title(
        f"HDBSCAN min_cluster_size={best_mcs}  "
        f"({int(h_metric_row['n_clusters'])} clusters, "
        f"{h_metric_row['noise_pct']:.1f}% noise, "
        f"sil={h_metric_row['silhouette_excl_noise']:.4f})"
    )
    ax.set_xlabel("UMAP-1"); ax.set_ylabel("UMAP-2")
    if len(unique_clusters) <= 12:
        ax.legend(loc="best", fontsize=8, markerscale=2)
    annotate_clusters(ax, X2, labels_h, HDBSCAN_LABELS_BY_SIZE_RANK)
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "fig_umap_hdbscan.png", dpi=150)
    plt.close(fig)

    print("\n" + "=" * 60)
    print(f"Saved 3 figures to {OUTPUT_DIR}")
    print(f"  fig_silhouette_curve.png")
    print(f"  fig_umap_kmeans.png      (K=2 vs best K={best_k})")
    print(f"  fig_umap_hdbscan.png     (mcs={best_mcs})")
    print("=" * 60)


if __name__ == "__main__":
    main()
