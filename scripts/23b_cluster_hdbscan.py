"""
23b_cluster_hdbscan.py

23번이 만든 UMAP 50차원 좌표에 HDBSCAN 적용.

입력:
  data/processed/phase1/clusters/li_token_umap50.npy
  data/processed/li_token_mapping.parquet

처리:
  min_cluster_size grid = [30, 50, 100, 200]
  HDBSCAN -> 클러스터 수, noise 비율, silhouette (noise 제외)

출력:
  data/processed/phase1/clusters/li_token_hdbscan.parquet
  data/processed/phase1/clusters/li_token_hdbscan_metrics.csv
"""

from pathlib import Path
import numpy as np
import pandas as pd
import hdbscan
from sklearn.metrics import silhouette_score

# ============================================================
PROJECT_ROOT = Path("/Users/vairocana/projects/sadan-chiljeong-dh")
INPUT_UMAP = PROJECT_ROOT / "data" / "processed" / "phase1" / "clusters" / "li_token_umap50.npy"
INPUT_MAPPING = PROJECT_ROOT / "data" / "processed" / "li_token_mapping.parquet"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "phase1" / "clusters"

MIN_CLUSTER_SIZES = [30, 50, 100, 200]
MIN_SAMPLES = 5
METRIC = "euclidean"   # UMAP 결과는 normalized space에서의 euclidean이 적합
CLUSTER_SELECTION_METHOD = "eom"
RANDOM_STATE = 42


def main():
    print("Loading UMAP coordinates...")
    X = np.load(INPUT_UMAP)
    mapping = pd.read_parquet(INPUT_MAPPING)
    print(f"  X: {X.shape}")
    assert len(X) == len(mapping)

    labels_dict = {"token_id": mapping["token_id"].values}
    metrics = []

    print(f"\nHDBSCAN grid: min_cluster_size = {MIN_CLUSTER_SIZES}\n")
    for mcs in MIN_CLUSTER_SIZES:
        clusterer = hdbscan.HDBSCAN(
            min_cluster_size=mcs,
            min_samples=MIN_SAMPLES,
            metric=METRIC,
            cluster_selection_method=CLUSTER_SELECTION_METHOD,
        )
        labels = clusterer.fit_predict(X)

        n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
        n_noise = (labels == -1).sum()
        noise_pct = n_noise / len(labels) * 100

        # silhouette: noise 제외하고 계산
        mask = labels != -1
        if mask.sum() > 100 and n_clusters >= 2:
            sil = silhouette_score(
                X[mask], labels[mask],
                sample_size=min(3000, mask.sum()),
                random_state=RANDOM_STATE,
            )
        else:
            sil = np.nan

        labels_dict[f"mcs{mcs}"] = labels
        labels_dict[f"mcs{mcs}_prob"] = clusterer.probabilities_

        metrics.append({
            "min_cluster_size": mcs,
            "n_clusters": n_clusters,
            "n_noise": int(n_noise),
            "noise_pct": noise_pct,
            "silhouette_excl_noise": sil,
        })
        print(f"  mcs={mcs:3d}: n_clusters={n_clusters:3d}  noise={noise_pct:5.1f}%  sil={sil if np.isnan(sil) else f'{sil:.4f}'}")

    # 저장
    cluster_df = pd.DataFrame(labels_dict)
    cluster_df.to_parquet(OUTPUT_DIR / "li_token_hdbscan.parquet", index=False)

    metrics_df = pd.DataFrame(metrics)
    metrics_df.to_csv(OUTPUT_DIR / "li_token_hdbscan_metrics.csv", index=False)

    print("\n" + "=" * 60)
    print("Saved:")
    print(f"  {OUTPUT_DIR / 'li_token_hdbscan.parquet'}")
    print(f"  {OUTPUT_DIR / 'li_token_hdbscan_metrics.csv'}")
    print("=" * 60)

    # 추천 mcs 자동 선정 (noise 5~30%, silhouette 최고)
    valid = metrics_df[
        (metrics_df["noise_pct"] >= 0) &
        (metrics_df["noise_pct"] <= 40) &
        (metrics_df["n_clusters"] >= 3)
    ]
    if len(valid) > 0:
        best = valid.loc[valid["silhouette_excl_noise"].idxmax()]
        print(f"\n추천 설정: mcs={int(best['min_cluster_size'])}")
        print(f"  클러스터 {int(best['n_clusters'])}개, noise {best['noise_pct']:.1f}%, sil={best['silhouette_excl_noise']:.4f}")
    else:
        print("\n⚠️  적절한 설정을 자동 추천할 수 없음. 결과 직접 검토 필요")


if __name__ == "__main__":
    main()
