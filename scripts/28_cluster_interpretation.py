"""
28_cluster_interpretation.py

각 클러스터에 의미 부여 + 검증.

입력:
  data/processed/li_token_embeddings.npy
  data/processed/li_token_mapping.parquet
  data/final/zhuzi_sentences.xlsx (li_sentences 시트: 원문 가져오기)
  data/processed/phase1/clusters/li_token_kmeans.parquet
  data/processed/phase1/clusters/li_token_hdbscan.parquet
  /mnt/project/理_cluster_samples_classified.xlsx (60개 골든 라벨)

처리:
  1. 베스트 K-means 클러스터별 대표 문장 30개 추출 + 빈출 bigram
  2. 60개 골든 라벨 (M/E/F/D/V) 새 클러스터링에 투영
  3. 능동/수동 어구 문장이 어느 클러스터에 떨어지는지 분석

출력:
  data/final/li_clustering_results.xlsx (다중 시트)
"""

from pathlib import Path
from collections import Counter
import numpy as np
import pandas as pd

# ============================================================
PROJECT_ROOT = Path("/Users/vairocana/projects/sadan-chiljeong-dh")
INPUT_MAPPING = PROJECT_ROOT / "data" / "processed" / "li_token_mapping.parquet"
INPUT_ZHUZI = PROJECT_ROOT / "data" / "final" / "zhuzi_sentences.xlsx"
CLUSTER_DIR = PROJECT_ROOT / "data" / "processed" / "phase1" / "clusters"
GOLDEN_PATH = Path("/Users/vairocana/projects/sadan-chiljeong-dh/data") / "raw" / "理_cluster_samples_classified.xlsx"
# 만약 경로 다르면 본인이 두는 곳으로 조정
# 예: ~/Downloads/理_cluster_samples_classified.xlsx

OUTPUT_PATH = PROJECT_ROOT / "data" / "final" / "li_clustering_results.xlsx"

N_SAMPLES_PER_CLUSTER = 30
N_TOP_BIGRAMS = 20

# 능동/수동 어구 (본인 docs 기반)
ACTIVE_ANCHORS = ["理發", "理流", "理動", "理生", "理能", "理自", "理主", "理運", "理做", "理之發"]
PASSIVE_ANCHORS = ["理無", "理只", "理寓", "理依", "理乘", "理待", "理不能", "理無爲", "理無形", "理靜"]


def get_bigrams(text):
    """文 안에서 理 주변 ±1자 bigram 추출"""
    bigrams = []
    for i, ch in enumerate(text):
        if ch == "理":
            if i > 0:
                bigrams.append(text[i-1] + ch)   # X理
            if i < len(text) - 1:
                bigrams.append(ch + text[i+1])   # 理X
    return bigrams


def main():
    print("Loading...")
    mapping = pd.read_parquet(INPUT_MAPPING)
    zhuzi = pd.read_excel(INPUT_ZHUZI, sheet_name="li_sentences")
    # sentence_id -> text 매핑
    sent_text = zhuzi.set_index("sentence_id")["text_plain"].to_dict()
    sent_text_p = zhuzi.set_index("sentence_id")["text_punctuated"].to_dict()

    kmeans_df = pd.read_parquet(CLUSTER_DIR / "li_token_kmeans.parquet")
    kmeans_metrics = pd.read_csv(CLUSTER_DIR / "li_token_kmeans_metrics.csv")
    hdbscan_df = pd.read_parquet(CLUSTER_DIR / "li_token_hdbscan.parquet")
    hdbscan_metrics = pd.read_csv(CLUSTER_DIR / "li_token_hdbscan_metrics.csv")

    # token_id 기준으로 mapping에 클러스터 라벨 병합
    df = mapping.merge(kmeans_df, on="token_id").merge(hdbscan_df, on="token_id")
    df["text_plain"] = df["sentence_id"].map(sent_text)
    df["text_punctuated"] = df["sentence_id"].map(sent_text_p)

    # 베스트 K 선정
    best_k = int(kmeans_metrics.loc[kmeans_metrics["silhouette"].idxmax(), "k"])
    print(f"  K-means best k = {best_k}")
    df["best_kmeans"] = df[f"k{best_k}"]

    # HDBSCAN 베스트 mcs
    valid = hdbscan_metrics[
        (hdbscan_metrics["noise_pct"] >= 0) &
        (hdbscan_metrics["noise_pct"] <= 40) &
        (hdbscan_metrics["n_clusters"] >= 3)
    ]
    if len(valid) > 0:
        best_mcs = int(valid.loc[valid["silhouette_excl_noise"].idxmax(), "min_cluster_size"])
    else:
        best_mcs = int(hdbscan_metrics.iloc[0]["min_cluster_size"])
    df["best_hdbscan"] = df[f"mcs{best_mcs}"]
    print(f"  HDBSCAN best mcs = {best_mcs}")

    # ============================================================
    # 시트 1: summary
    # ============================================================
    summary_rows = [
        ["분석 대상", "8,443문장에서 추출된 理 토큰", 10474],
        ["방법론 변경", "문장 임베딩 (CLS) → 理 토큰 임베딩 (last hidden state)", ""],
        ["", "", ""],
        ["기존 (sentence emb, K=2)", "silhouette", 0.058],
        ["신규 (token emb)", f"K-means best K={best_k} silhouette", kmeans_metrics["silhouette"].max()],
        ["", "K=2 silhouette (비교용)", kmeans_metrics.loc[kmeans_metrics["k"] == 2, "silhouette"].values[0]],
        ["", "", ""],
        ["HDBSCAN", f"best mcs={best_mcs}, n_clusters", int(hdbscan_metrics.loc[hdbscan_metrics["min_cluster_size"] == best_mcs, "n_clusters"].values[0])],
        ["", "noise %", float(hdbscan_metrics.loc[hdbscan_metrics["min_cluster_size"] == best_mcs, "noise_pct"].values[0])],
        ["", "silhouette (noise 제외)", float(hdbscan_metrics.loc[hdbscan_metrics["min_cluster_size"] == best_mcs, "silhouette_excl_noise"].values[0])],
    ]
    summary_df = pd.DataFrame(summary_rows, columns=["항목", "지표", "값"])

    # ============================================================
    # 시트 2: kmeans_metrics
    # ============================================================
    # (already loaded as kmeans_metrics)

    # ============================================================
    # 시트 3: hdbscan_metrics
    # ============================================================
    # (already loaded as hdbscan_metrics)

    # ============================================================
    # 시트 4: kmeans_clusters_overview - 클러스터별 요약
    # ============================================================
    print("\n클러스터별 빈출 bigram 추출 (K-means)...")
    rows = []
    for c in sorted(df["best_kmeans"].unique()):
        sub = df[df["best_kmeans"] == c]
        bigrams = []
        for txt in sub["text_plain"].dropna():
            bigrams.extend(get_bigrams(txt))
        top = Counter(bigrams).most_common(N_TOP_BIGRAMS)
        rows.append({
            "cluster": c,
            "size": len(sub),
            "pct": f"{len(sub) / len(df) * 100:.1f}%",
            "top_bigrams": ", ".join([f"{b}({n})" for b, n in top]),
        })
    kmeans_overview = pd.DataFrame(rows)

    # ============================================================
    # 시트 5: hdbscan_clusters_overview
    # ============================================================
    print("클러스터별 빈출 bigram 추출 (HDBSCAN)...")
    rows = []
    for c in sorted(df["best_hdbscan"].unique()):
        sub = df[df["best_hdbscan"] == c]
        bigrams = []
        for txt in sub["text_plain"].dropna():
            bigrams.extend(get_bigrams(txt))
        top = Counter(bigrams).most_common(N_TOP_BIGRAMS)
        label = f"C{c}" if c != -1 else "NOISE"
        rows.append({
            "cluster": label,
            "size": len(sub),
            "pct": f"{len(sub) / len(df) * 100:.1f}%",
            "top_bigrams": ", ".join([f"{b}({n})" for b, n in top]),
        })
    hdbscan_overview = pd.DataFrame(rows)

    # ============================================================
    # 시트 6: kmeans_samples - 클러스터별 대표 문장 30개씩
    # ============================================================
    print(f"클러스터별 대표 문장 추출 (각 {N_SAMPLES_PER_CLUSTER}개)...")
    samples_rows = []
    for c in sorted(df["best_kmeans"].unique()):
        sub = df[df["best_kmeans"] == c]
        # 랜덤 샘플
        n = min(N_SAMPLES_PER_CLUSTER, len(sub))
        sample = sub.sample(n=n, random_state=42)
        for _, row in sample.iterrows():
            samples_rows.append({
                "cluster": c,
                "token_id": row["token_id"],
                "sentence_id": row["sentence_id"],
                "li_idx_in_sent": row["li_idx_in_sent"],
                "char_left_right": f"...{row['char_left']}[理]{row['char_right']}...",
                "text_plain": row["text_plain"],
                "text_punctuated": row["text_punctuated"],
                "본인_해석": "",  # 검토용 빈칸
                "理_분류_M_E_F_D_V": "",  # 검토용 빈칸
            })
    kmeans_samples = pd.DataFrame(samples_rows)

    # ============================================================
    # 시트 7: golden_60_projection - 60개 골든 라벨 투영
    # ============================================================
    print("\n60개 골든 샘플 투영...")
    if GOLDEN_PATH.exists():
        golden = pd.read_excel(GOLDEN_PATH, sheet_name="분류")
        # sentence_id 기준으로 새 클러스터 라벨 가져오기
        golden_proj = golden.merge(
            df.groupby("sentence_id").first().reset_index()[["sentence_id", "best_kmeans", "best_hdbscan"]],
            on="sentence_id",
            how="left",
        )

        # M이 어느 클러스터에 떨어지는지 cross-tab
        print("\n  M/E/F/D/V × K-means 클러스터:")
        ct_kmeans = pd.crosstab(golden_proj["理_분류"], golden_proj["best_kmeans"], margins=True)
        print(ct_kmeans)

        print("\n  M/E/F/D/V × HDBSCAN 클러스터:")
        ct_hdbscan = pd.crosstab(golden_proj["理_분류"], golden_proj["best_hdbscan"], margins=True)
        print(ct_hdbscan)

        # 시트용 데이터
        golden_for_xlsx = golden_proj[[
            "cluster",  # 기존 클러스터링의 라벨
            "sentence_id", "text", "解釋", "理_분류",
            "best_kmeans", "best_hdbscan", "비고",
        ]].rename(columns={"cluster": "old_kmeans_k2"})
    else:
        print(f"  ⚠️ 골든 파일을 찾을 수 없음: {GOLDEN_PATH}")
        print(f"  이 시트는 비어있게 저장됨. 본인이 경로 수정 후 재실행.")
        golden_for_xlsx = pd.DataFrame()
        ct_kmeans = pd.DataFrame()
        ct_hdbscan = pd.DataFrame()

    # ============================================================
    # 시트 8: anchor_projection - 능동/수동 어구 투영
    # ============================================================
    print("\n능동/수동 어구 투영...")
    anchor_rows = []
    for anchor_type, anchors in [("ACTIVE", ACTIVE_ANCHORS), ("PASSIVE", PASSIVE_ANCHORS)]:
        for anchor in anchors:
            # 원문에 anchor가 포함된 문장
            matching = df[df["text_plain"].fillna("").str.contains(anchor, regex=False)]
            if len(matching) == 0:
                anchor_rows.append({
                    "anchor_type": anchor_type,
                    "anchor": anchor,
                    "n_tokens": 0,
                    "kmeans_dist": "",
                    "hdbscan_dist": "",
                })
                continue
            km_dist = matching["best_kmeans"].value_counts().sort_index()
            hd_dist = matching["best_hdbscan"].value_counts().sort_index()
            anchor_rows.append({
                "anchor_type": anchor_type,
                "anchor": anchor,
                "n_tokens": len(matching),
                "kmeans_dist": ", ".join([f"C{k}:{v}" for k, v in km_dist.items()]),
                "hdbscan_dist": ", ".join([f"C{k}:{v}" for k, v in hd_dist.items()]),
            })
    anchor_df = pd.DataFrame(anchor_rows)

    # 능동 vs 수동 어구의 클러스터 분포 비교
    print("\n  ACTIVE 어구 → K-means 클러스터:")
    active_mask = df["text_plain"].fillna("").apply(
        lambda t: any(a in t for a in ACTIVE_ANCHORS)
    )
    passive_mask = df["text_plain"].fillna("").apply(
        lambda t: any(a in t for a in PASSIVE_ANCHORS)
    )
    print(f"    ACTIVE matching tokens: {active_mask.sum():,}")
    print(f"    PASSIVE matching tokens: {passive_mask.sum():,}")
    print(f"    BOTH:                    {(active_mask & passive_mask).sum():,}")

    active_dist = df[active_mask]["best_kmeans"].value_counts().sort_index()
    passive_dist = df[passive_mask]["best_kmeans"].value_counts().sort_index()
    print(f"    ACTIVE  -> {dict(active_dist)}")
    print(f"    PASSIVE -> {dict(passive_dist)}")

    # ============================================================
    # 엑셀 저장
    # ============================================================
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    print(f"\n저장: {OUTPUT_PATH}")
    with pd.ExcelWriter(OUTPUT_PATH, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="summary", index=False)
        kmeans_metrics.to_excel(writer, sheet_name="kmeans_metrics", index=False)
        hdbscan_metrics.to_excel(writer, sheet_name="hdbscan_metrics", index=False)
        kmeans_overview.to_excel(writer, sheet_name="kmeans_overview", index=False)
        hdbscan_overview.to_excel(writer, sheet_name="hdbscan_overview", index=False)
        kmeans_samples.to_excel(writer, sheet_name="kmeans_samples", index=False)
        if len(golden_for_xlsx) > 0:
            golden_for_xlsx.to_excel(writer, sheet_name="golden_60", index=False)
            ct_kmeans.to_excel(writer, sheet_name="golden_x_kmeans")
            ct_hdbscan.to_excel(writer, sheet_name="golden_x_hdbscan")
        anchor_df.to_excel(writer, sheet_name="anchor_projection", index=False)

    print("\n" + "=" * 60)
    print("DONE")
    print(f"  {OUTPUT_PATH}")
    print(f"  시트 수: {7 if len(golden_for_xlsx) == 0 else 9}")
    print("=" * 60)


if __name__ == "__main__":
    main()
