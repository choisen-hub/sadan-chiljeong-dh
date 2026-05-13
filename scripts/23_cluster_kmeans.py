"""
23_cluster_kmeans.py (v2 - TOKEN EMBEDDING)

입력:
  data/processed/li_token_embeddings.npy       (10474, 768)
  data/processed/li_token_mapping.parquet

처리:
  1. L2 normalize
  2. UMAP 50차원 축소 (cosine metric, 차원의 저주 회피)
  3. K-means K=2~10 grid
  4. silhouette + Davies-Bouldin + Calinski-Harabasz 평가

출력:
  data/processed/phase1/clusters/li_token_umap50.npy
  data/processed/phase1/clusters/li_token_kmeans.parquet
  data/processed/phase1/clusters/li_token_kmeans_metrics.csv
"""

from pathlib import Path
import numpy as np
import pandas as pd
import umap
from sklearn.cluster import KMeans
from sklearn.metrics import (
    silhouette_score,
    davies_bouldin_score,
    calinski_harabasz_score,
)
from sklearn.preprocessing import normalize

# ============================================================
PROJECT_ROOT = Path("/Users/vairocana/projects/sadan-chiljeong-dh")
INPUT_VECTORS = PROJECT_ROOT / "data" / "processed" / "li_token_embeddings.npy"
INPUT_MAPPING = PROJECT_ROOT / "data" / "processed" / "li_token_mapping.parquet"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "phase1" / "clusters"

UMAP_N_COMPONENTS = 50
UMAP_N_NEIGHBORS = 30
UMAP_MIN_DIST = 0.0
UMAP_METRIC = "cosine"
RANDOM_STATE = 42

K_RANGE = list(range(2, 11))


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Load
    print("Loading...")
    vectors = np.load(INPUT_VECTORS)
    mapping = pd.read_parquet(INPUT_MAPPING)
    print(f"  vectors: {vectors.shape}")
    print(f"  mapping: {mapping.shape}")
    assert len(vectors) == len(mapping)

    # 2. L2 normalize (cosine == euclidean on unit sphere)
    print("\nL2 normalizing...")
    vectors_norm = normalize(vectors, norm="l2", axis=1)

    # 3. UMAP 50차원 축소
    print(f"\nUMAP -> {UMAP_N_COMPONENTS}D (this takes ~1-2 min)...")
    reducer = umap.UMAP(
        n_components=UMAP_N_COMPONENTS,
        n_neighbors=UMAP_N_NEIGHBORS,
        min_dist=UMAP_MIN_DIST,
        metric=UMAP_METRIC,
        random_state=RANDOM_STATE,
        verbose=True,
    )
    X = reducer.fit_transform(vectors_norm)
    print(f"  reduced: {X.shape}")

    np.save(OUTPUT_DIR / "li_token_umap50.npy", X.astype(np.float32))

    # 4. K-means grid
    print(f"\nK-means K={K_RANGE}...")
    labels_dict = {"token_id": mapping["token_id"].values}
    metrics = []

    for k in K_RANGE:
        km = KMeans(n_clusters=k, random_state=RANDOM_STATE, n_init=10)
        labels = km.fit_predict(X)
        labels_dict[f"k{k}"] = labels

        # silhouette은 샘플링해서 빠르게 (전체 돌리면 너무 오래)
        sil = silhouette_score(X, labels, sample_size=3000, random_state=RANDOM_STATE)
        db = davies_bouldin_score(X, labels)
        ch = calinski_harabasz_score(X, labels)

        metrics.append({
            "k": k,
            "silhouette": sil,
            "davies_bouldin": db,
            "calinski_harabasz": ch,
            "inertia": km.inertia_,
        })
        print(f"  k={k:2d}: silhouette={sil:.4f}  DB={db:.4f}  CH={ch:.1f}")

    # 5. 저장
    cluster_df = pd.DataFrame(labels_dict)
    cluster_df.to_parquet(OUTPUT_DIR / "li_token_kmeans.parquet", index=False)

    metrics_df = pd.DataFrame(metrics)
    metrics_df.to_csv(OUTPUT_DIR / "li_token_kmeans_metrics.csv", index=False)

    # 6. 베스트 K 보고
    print("\n" + "=" * 60)
    best_sil = metrics_df.loc[metrics_df["silhouette"].idxmax()]
    best_db = metrics_df.loc[metrics_df["davies_bouldin"].idxmin()]
    print(f"Best by silhouette: k={int(best_sil['k'])}  ({best_sil['silhouette']:.4f})")
    print(f"Best by DB index:   k={int(best_db['k'])}  ({best_db['davies_bouldin']:.4f})")
    print(f"\n기존 (sentence emb, K=2): silhouette 0.058")
    print(f"신규 best silhouette:     {best_sil['silhouette']:.4f}")
    delta = best_sil["silhouette"] - 0.058
    print(f"개선 폭: {delta:+.4f}")
    print("=" * 60)


if __name__ == "__main__":
    main()
