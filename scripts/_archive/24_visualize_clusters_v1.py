"""
24_visualize_clusters.py
========================

Phase 1 Step 1·3 시각화 + BERT anisotropy 진단.

이번 단계 목적:
1. UMAP 2D projection — 임베딩 공간이 실제로 어떻게 생겼는지 정성 확인
2. PCA explained variance — BERT anisotropy 진단 (첫 PC dominance 여부)
3. 클러스터별 random sample → 정성 검증용 csv

배경: 23번에서 4글자 모두 silhouette < 0.06로 매우 낮음.
- 진짜 동질성인지(RQ1 No)
- 아니면 BERT-like 이등방성(anisotropy) artifact인지
판단하려면 시각화·진단 필요.

입력:
  data/processed/phase1/clusters/{char}_clusters.parquet  (23번 산출)
  data/processed/phase1/clusters/silhouette_summary.csv

산출:
  data/processed/phase1/figures/
    ├── {char}_umap.png
    ├── pca_variance.png        # 4글자 비교
    └── overview_4chars.png     # 4글자 UMAP 한 figure
  data/processed/phase1/cluster_samples/
    └── {char}_cluster_samples.csv
  data/processed/phase1/clusters/
    └── anisotropy_diagnostics.csv

사용법:
  pip3 install umap-learn
  python scripts/24_visualize_clusters.py
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.preprocessing import normalize

# ---------- config ----------
TARGET_CHARS = ["理", "心", "性", "天"]
DEFAULT_RANDOM_STATE = 42
DEFAULT_UMAP_N_NEIGHBORS = 15
DEFAULT_UMAP_MIN_DIST = 0.1
DEFAULT_SAMPLES_PER_CLUSTER = 30
DEFAULT_PCA_COMPONENTS = 50
# ---------------------------


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


def setup_font() -> None:
    """macOS 한자 폰트."""
    try:
        plt.rcParams["font.family"] = [
            "Heiti SC", "STHeiti", "PingFang SC",
            "AppleGothic", "DejaVu Sans",
        ]
        plt.rcParams["axes.unicode_minus"] = False
    except Exception:
        pass


def load_clusters(path: Path) -> tuple[pd.DataFrame, np.ndarray]:
    df = pd.read_parquet(path)
    emb = np.vstack(df["embedding"].values).astype(np.float32)
    return df, emb


def compute_pca_variance(emb: np.ndarray, n_components: int) -> np.ndarray:
    """PCA explained variance ratio. anisotropy 진단용."""
    n = min(n_components, emb.shape[1], emb.shape[0])
    pca = PCA(n_components=n, random_state=42)
    pca.fit(emb)
    return pca.explained_variance_ratio_


def compute_umap(
    emb: np.ndarray,
    n_neighbors: int,
    min_dist: float,
    random_state: int,
) -> np.ndarray:
    """UMAP 2D projection."""
    import umap  # lazy import (무거움)

    reducer = umap.UMAP(
        n_neighbors=n_neighbors,
        min_dist=min_dist,
        random_state=random_state,
        n_components=2,
        metric="cosine",
    )
    return reducer.fit_transform(emb)


def extract_cluster_samples(
    df: pd.DataFrame,
    cluster_col: str,
    text_col: str,
    id_col: str,
    n_per_cluster: int,
    random_state: int,
) -> pd.DataFrame:
    """클러스터별 random sample. 정성 검증용."""
    rows = []
    for c in sorted(df[cluster_col].unique()):
        sub = df[df[cluster_col] == c]
        n = min(len(sub), n_per_cluster)
        sample = sub.sample(n=n, random_state=random_state)
        for _, row in sample.iterrows():
            rows.append({
                "cluster": int(c),
                "sentence_id": row[id_col],
                "text": row[text_col],
            })
    return pd.DataFrame(rows)


def plot_umap_single(
    coords: np.ndarray,
    labels: np.ndarray,
    char: str,
    best_k: int,
    silhouette: float,
    output_path: Path,
) -> None:
    fig, ax = plt.subplots(figsize=(8, 7))
    cmap = plt.get_cmap("tab10")

    for c in sorted(np.unique(labels)):
        mask = labels == c
        ax.scatter(
            coords[mask, 0], coords[mask, 1],
            s=8, alpha=0.5, color=cmap(c),
            label=f"c{c} (n={mask.sum():,})",
        )

    ax.set_title(f"{char}  |  best k={best_k}, silhouette={silhouette:.3f}, n={len(coords):,}")
    ax.set_xlabel("UMAP-1")
    ax.set_ylabel("UMAP-2")
    ax.legend(loc="best", fontsize=9)
    ax.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
    logging.info(f"  saved → {output_path.name}")


def plot_pca_variance(
    variance_data: dict[str, np.ndarray],
    output_path: Path,
) -> None:
    """4글자 PCA explained variance 비교."""
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    ax = axes[0]
    for char, vr in variance_data.items():
        ax.plot(range(1, len(vr) + 1), vr, marker="o", label=char,
                alpha=0.7, markersize=4)
    ax.set_xlabel("Principal component")
    ax.set_ylabel("Explained variance ratio")
    ax.set_title("Per-component variance ratio")
    ax.legend()
    ax.grid(True, alpha=0.3)

    ax = axes[1]
    for char, vr in variance_data.items():
        ax.plot(range(1, len(vr) + 1), np.cumsum(vr), marker="o", label=char,
                alpha=0.7, markersize=4)
    ax.set_xlabel("Principal component")
    ax.set_ylabel("Cumulative variance ratio")
    ax.set_title("Cumulative explained variance")
    ax.axhline(0.5, linestyle="--", color="gray", alpha=0.5)
    ax.legend()
    ax.grid(True, alpha=0.3)

    fig.suptitle("PCA variance — anisotropy diagnostic", fontsize=12)
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
    logging.info(f"  saved → {output_path.name}")


def plot_overview_4chars(
    char_data: dict[str, dict],
    output_path: Path,
) -> None:
    """4글자 UMAP 2x2 grid."""
    n = len(char_data)
    fig, axes = plt.subplots(2, 2, figsize=(14, 12))
    axes = axes.flatten()

    cmap = plt.get_cmap("tab10")
    for idx, (char, data) in enumerate(char_data.items()):
        if idx >= 4:
            break
        ax = axes[idx]
        coords = data["coords"]
        labels = data["labels"]
        best_k = data["best_k"]
        sil = data["silhouette"]

        for c in sorted(np.unique(labels)):
            mask = labels == c
            ax.scatter(
                coords[mask, 0], coords[mask, 1],
                s=4, alpha=0.4, color=cmap(c),
            )

        ax.set_title(f"{char}  k={best_k}, sil={sil:.3f}, n={len(coords):,}")
        ax.set_xlabel("UMAP-1")
        ax.set_ylabel("UMAP-2")
        ax.grid(True, alpha=0.3)

    # unused subplots
    for idx in range(n, 4):
        axes[idx].set_visible(False)

    fig.suptitle("Phase 1: UMAP 2D (cosine, n_neighbors=15, min_dist=0.1)",
                 fontsize=14)
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
    logging.info(f"  saved → {output_path.name}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--clusters_dir", type=Path,
                        default=Path("data/processed/phase1/clusters"))
    parser.add_argument("--figures_dir", type=Path,
                        default=Path("data/processed/phase1/figures"))
    parser.add_argument("--samples_dir", type=Path,
                        default=Path("data/processed/phase1/cluster_samples"))
    parser.add_argument("--target_chars", type=str, nargs="+",
                        default=TARGET_CHARS)
    parser.add_argument("--text_col", type=str, default="text_plain")
    parser.add_argument("--id_col", type=str, default="sentence_id")
    parser.add_argument("--samples_per_cluster", type=int,
                        default=DEFAULT_SAMPLES_PER_CLUSTER)
    parser.add_argument("--umap_n_neighbors", type=int,
                        default=DEFAULT_UMAP_N_NEIGHBORS)
    parser.add_argument("--umap_min_dist", type=float,
                        default=DEFAULT_UMAP_MIN_DIST)
    parser.add_argument("--pca_components", type=int,
                        default=DEFAULT_PCA_COMPONENTS)
    parser.add_argument("--random_state", type=int,
                        default=DEFAULT_RANDOM_STATE)
    args = parser.parse_args()

    setup_logging()
    setup_font()

    args.figures_dir.mkdir(parents=True, exist_ok=True)
    args.samples_dir.mkdir(parents=True, exist_ok=True)

    # silhouette summary
    summary_path = args.clusters_dir / "silhouette_summary.csv"
    if not summary_path.exists():
        raise FileNotFoundError(f"run 23번 first: {summary_path}")
    summary = pd.read_csv(summary_path)
    summary_dict = {row["char"]: row for _, row in summary.iterrows()}

    variance_data: dict[str, np.ndarray] = {}
    char_data: dict[str, dict] = {}
    diagnostics = []

    for char in args.target_chars:
        path = args.clusters_dir / f"{char}_clusters.parquet"
        if not path.exists():
            logging.warning(f"missing: {path}, skip")
            continue
        if char not in summary_dict:
            logging.warning(f"no summary for {char}, skip")
            continue

        logging.info(f"\n=== {char} ===")
        df, emb = load_clusters(path)
        logging.info(f"loaded: {len(df):,} sentences")

        emb_norm = normalize(emb, norm="l2", axis=1)

        # 1. PCA variance
        logging.info("computing PCA variance...")
        vr = compute_pca_variance(emb_norm, args.pca_components)
        variance_data[char] = vr
        logging.info(
            f"  PC1: {vr[0]:.4f}, "
            f"top-10 cum: {vr[:10].sum():.4f}, "
            f"top-50 cum: {vr.sum():.4f}"
        )

        # 2. UMAP
        logging.info("computing UMAP (cosine)...")
        coords = compute_umap(
            emb_norm,
            args.umap_n_neighbors,
            args.umap_min_dist,
            args.random_state,
        )

        # 3. best k labels
        best_k = int(summary_dict[char]["best_silhouette_k"])
        sil = float(summary_dict[char]["best_silhouette_score"])
        cluster_col = f"cluster_k{best_k}"
        labels = df[cluster_col].values

        # 4. UMAP plot
        plot_umap_single(
            coords, labels, char, best_k, sil,
            args.figures_dir / f"{char}_umap.png",
        )

        # 5. cluster samples
        samples = extract_cluster_samples(
            df, cluster_col, args.text_col, args.id_col,
            args.samples_per_cluster, args.random_state,
        )
        samples.to_csv(
            args.samples_dir / f"{char}_cluster_samples.csv",
            index=False, encoding="utf-8-sig",
        )
        logging.info(
            f"  saved {len(samples)} samples ({args.samples_per_cluster}/cluster) "
            f"→ {char}_cluster_samples.csv"
        )

        # collect for overview + diagnostics
        char_data[char] = {
            "coords": coords,
            "labels": labels,
            "best_k": best_k,
            "silhouette": sil,
        }
        diagnostics.append({
            "char": char,
            "n_sentences": len(df),
            "pc1_ratio": float(vr[0]),
            "top10_cumulative": float(vr[:10].sum()),
            "top50_cumulative": float(vr.sum()),
            "best_k": best_k,
            "silhouette": sil,
        })

    # 합본 plot
    if variance_data:
        plot_pca_variance(
            variance_data, args.figures_dir / "pca_variance.png"
        )
    if char_data:
        plot_overview_4chars(
            char_data, args.figures_dir / "overview_4chars.png"
        )

    # diagnostics
    diag_df = pd.DataFrame(diagnostics)
    diag_df.to_csv(
        args.clusters_dir / "anisotropy_diagnostics.csv",
        index=False, encoding="utf-8-sig",
    )
    logging.info(f"\n=== Diagnostics ===\n{diag_df.to_string(index=False)}")

    logging.info("\ndone.")


if __name__ == "__main__":
    main()
