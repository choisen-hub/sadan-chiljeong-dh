"""
23_cluster_kmeans.py
====================

Phase 1 Step 1·3: k-means 클러스터링.

4개 글자(理/心/性/天) 각각에 k=2~10 적용,
silhouette · Davies-Bouldin 평가로 best k 선정.

설계 결정 (2026-05-06):
- L2 normalized embedding (sentence embedding 클러스터링 표준)
- 768-dim 그대로 (차원축소는 24번 시각화용으로만)
- k=2~10 (rubric)
- silhouette 메인, DB index 보조
- random_state 고정 (재현성)

입력:
  data/processed/phase1/embeddings/{char}_embeddings.parquet

산출:
  data/processed/phase1/clusters/
    ├── {char}_clusters.parquet     # 모든 k의 cluster_label 저장
    ├── {char}_metrics.csv          # k별 silhouette, DB
    └── silhouette_summary.csv      # 4글자 합본

사용법:
  python scripts/23_cluster_kmeans.py
  python scripts/23_cluster_kmeans.py --silhouette_sample 2000
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import davies_bouldin_score, silhouette_score
from sklearn.preprocessing import normalize

# ---------- config ----------
TARGET_CHARS = ["理", "心", "性", "天"]
DEFAULT_K_MIN = 2
DEFAULT_K_MAX = 10
DEFAULT_RANDOM_STATE = 42
DEFAULT_SILHOUETTE_SAMPLE = 2000  # silhouette 계산 속도 ↑
# ---------------------------


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


def load_embeddings(path: Path) -> tuple[pd.DataFrame, np.ndarray]:
    """parquet 로드 + embedding을 ndarray로 변환."""
    df = pd.read_parquet(path)
    if "embedding" not in df.columns:
        raise KeyError(f"'embedding' column not found in {path}")
    emb = np.vstack(df["embedding"].values).astype(np.float32)
    return df, emb


def cluster_one_k(
    emb: np.ndarray,
    k: int,
    random_state: int,
    silhouette_sample: int | None,
) -> tuple[np.ndarray, float, float]:
    """단일 k에 대한 k-means. 반환: labels, silhouette, davies_bouldin."""
    km = KMeans(n_clusters=k, random_state=random_state, n_init=10)
    labels = km.fit_predict(emb)

    # silhouette은 큰 데이터에서 느림 → sample 옵션 활용
    sil = silhouette_score(
        emb, labels,
        sample_size=silhouette_sample if silhouette_sample else None,
        random_state=random_state,
    )
    db = davies_bouldin_score(emb, labels)
    return labels, float(sil), float(db)


def cluster_one_char(
    emb: np.ndarray,
    k_range: range,
    random_state: int,
    silhouette_sample: int | None,
) -> tuple[pd.DataFrame, dict[int, np.ndarray], dict]:
    """글자별 k 스윕."""
    metrics_rows = []
    labels_by_k: dict[int, np.ndarray] = {}

    for k in k_range:
        labels, sil, db = cluster_one_k(emb, k, random_state, silhouette_sample)
        labels_by_k[k] = labels
        metrics_rows.append({"k": k, "silhouette": sil, "davies_bouldin": db})
        logging.info(f"  k={k:2d}: silhouette={sil:.4f}, DB={db:.4f}")

    metrics = pd.DataFrame(metrics_rows)
    best_sil_k = int(metrics.loc[metrics["silhouette"].idxmax(), "k"])
    best_db_k = int(metrics.loc[metrics["davies_bouldin"].idxmin(), "k"])

    summary = {
        "best_silhouette_k": best_sil_k,
        "best_silhouette_score": float(metrics["silhouette"].max()),
        "best_davies_bouldin_k": best_db_k,
        "best_davies_bouldin_score": float(metrics["davies_bouldin"].min()),
    }
    return metrics, labels_by_k, summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--input_dir", type=Path,
                        default=Path("data/processed/phase1/embeddings"),
                        help="22번 산출 임베딩 디렉토리")
    parser.add_argument("--output_dir", type=Path,
                        default=Path("data/processed/phase1/clusters"),
                        help="클러스터링 결과 저장 디렉토리")
    parser.add_argument("--target_chars", type=str, nargs="+",
                        default=TARGET_CHARS)
    parser.add_argument("--k_min", type=int, default=DEFAULT_K_MIN)
    parser.add_argument("--k_max", type=int, default=DEFAULT_K_MAX)
    parser.add_argument("--random_state", type=int, default=DEFAULT_RANDOM_STATE)
    parser.add_argument("--silhouette_sample", type=int,
                        default=DEFAULT_SILHOUETTE_SAMPLE,
                        help="silhouette 계산 시 sample 크기 (0이면 전체)")
    args = parser.parse_args()

    setup_logging()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    sil_sample = args.silhouette_sample if args.silhouette_sample > 0 else None
    k_range = range(args.k_min, args.k_max + 1)

    summary_rows = []
    all_metrics = []

    for char in args.target_chars:
        emb_path = args.input_dir / f"{char}_embeddings.parquet"
        if not emb_path.exists():
            logging.warning(f"missing: {emb_path}, skip")
            continue

        logging.info(f"\n=== {char} ===")
        df, emb = load_embeddings(emb_path)
        logging.info(f"loaded: {len(df):,} sentences, dim={emb.shape[1]}")

        # L2 normalize → cosine 의미공간에서 k-means
        emb_norm = normalize(emb, norm="l2", axis=1)

        metrics, labels_by_k, char_summary = cluster_one_char(
            emb_norm, k_range, args.random_state, sil_sample,
        )

        # 모든 k의 label을 컬럼으로 추가 (df는 embedding 포함된 채로 두고 새 cols 추가)
        for k, labels in labels_by_k.items():
            df[f"cluster_k{k}"] = labels
        df.to_parquet(args.output_dir / f"{char}_clusters.parquet", index=False)

        metrics["char"] = char
        metrics.to_csv(
            args.output_dir / f"{char}_metrics.csv",
            index=False, encoding="utf-8-sig",
        )
        all_metrics.append(metrics)

        char_summary["char"] = char
        char_summary["n_sentences"] = len(df)
        summary_rows.append(char_summary)

        logging.info(
            f"  → best k by silhouette: {char_summary['best_silhouette_k']} "
            f"(score={char_summary['best_silhouette_score']:.4f})"
        )

    # 합본 metrics
    if all_metrics:
        all_metrics_df = pd.concat(all_metrics, ignore_index=True)
        all_metrics_df.to_csv(
            args.output_dir / "all_metrics.csv",
            index=False, encoding="utf-8-sig",
        )

    # 합본 summary
    summary_df = pd.DataFrame(summary_rows)
    summary_df = summary_df[[
        "char", "n_sentences",
        "best_silhouette_k", "best_silhouette_score",
        "best_davies_bouldin_k", "best_davies_bouldin_score",
    ]]
    summary_df.to_csv(
        args.output_dir / "silhouette_summary.csv",
        index=False, encoding="utf-8-sig",
    )

    logging.info("\n=== Summary ===")
    logging.info(f"\n{summary_df.to_string(index=False)}")

    logging.info("done.")


if __name__ == "__main__":
    main()
