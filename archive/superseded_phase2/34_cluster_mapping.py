"""
34_cluster_mapping.py

35건 인용 (score >= 47.43)을 Phase 1 클러스터에 매핑.

입력:
  data/processed/phase2/citation_candidates.parquet (1,504 매칭)
  data/processed/li_token_mapping.parquet (10,474 理 토큰 + sentence_id)
  data/processed/phase1/clusters/li_token_kmeans.parquet (각 토큰의 클러스터)
  data/processed/phase1/clusters/li_token_hdbscan.parquet
  data/final/zhuzi_sentences.xlsx (li_sentences 시트)

처리:
  1. score >= 47.43 컷 적용 → 35건
  2. 각 인용의 zhuzi_sent_id → 그 문장 안의 理 토큰들의 클러스터 라벨 매핑
  3. 학자별 × 클러스터별 분포 cross-tab
  4. unique 인용 / 중복 매칭 구분 (A형 중복: 같은 letter_sent_id + lcs)

출력:
  data/final/citation_cluster_mapping.xlsx
    - summary
    - top35_with_clusters (35건 + 클러스터 라벨)
    - cluster_distribution (학자 × 클러스터 cross-tab)
"""

from pathlib import Path
import pandas as pd
import numpy as np

PROJECT_ROOT = Path("/Users/vairocana/projects/sadan-chiljeong-dh")
CAND_PATH = PROJECT_ROOT / "data" / "processed" / "phase2" / "citation_candidates.parquet"
MAPPING_PATH = PROJECT_ROOT / "data" / "processed" / "li_token_mapping.parquet"
KMEANS_PATH = PROJECT_ROOT / "data" / "processed" / "phase1" / "clusters" / "li_token_kmeans.parquet"
HDBSCAN_PATH = PROJECT_ROOT / "data" / "processed" / "phase1" / "clusters" / "li_token_hdbscan.parquet"
ZHUZI_PATH = PROJECT_ROOT / "data" / "final" / "zhuzi_sentences.xlsx"
OUTPUT_PATH = PROJECT_ROOT / "data" / "final" / "citation_cluster_mapping.xlsx"

SCORE_CUTOFF = 47.43   # 35위 컷
BEST_KMEANS_K = 4      # Phase 1 베스트 K
BEST_HDBSCAN_MCS = 200 # Phase 1 베스트 mcs


def main():
    print("Loading...")
    cand = pd.read_parquet(CAND_PATH)
    li_mapping = pd.read_parquet(MAPPING_PATH)
    kmeans = pd.read_parquet(KMEANS_PATH)
    hdbscan = pd.read_parquet(HDBSCAN_PATH)
    zhuzi = pd.read_excel(ZHUZI_PATH, sheet_name="li_sentences")

    # 1. 35건 컷
    print(f"\nApplying score cutoff >= {SCORE_CUTOFF}...")
    top = cand[cand["score"] >= SCORE_CUTOFF].sort_values("score", ascending=False).reset_index(drop=True)
    print(f"  {len(top)}건 (퇴계 {(top['sender']=='T').sum()} / 율곡 {(top['sender']=='Y').sum()})")

    # 2. 토큰 -> 문장 단위 클러스터 매핑
    # li_mapping: token_id, sentence_id, li_idx_in_sent
    # kmeans/hdbscan: token_id, k2/k3/k4/.../mcs30/mcs50/.../mcs200
    print("\nMerging token -> cluster...")
    token_cluster = li_mapping[["token_id", "sentence_id", "li_idx_in_sent"]].copy()
    token_cluster["kmeans_k4"] = kmeans[f"k{BEST_KMEANS_K}"].values
    token_cluster["hdbscan_mcs200"] = hdbscan[f"mcs{BEST_HDBSCAN_MCS}"].values

    # 문장 단위 집계: 한 문장에 理 여러 번이면 클러스터 정보를 모두 모음
    # 가장 빈출하는 클러스터 (mode)와 모든 클러스터 (list) 둘 다 보고
    def agg_clusters(s):
        vals = s.tolist()
        if not vals:
            return None
        # 최빈값
        return max(set(vals), key=vals.count)

    sent_cluster = token_cluster.groupby("sentence_id").agg(
        kmeans_cluster=("kmeans_k4", agg_clusters),
        hdbscan_cluster=("hdbscan_mcs200", agg_clusters),
        n_li_tokens=("li_idx_in_sent", "count"),
        kmeans_all=("kmeans_k4", lambda s: ",".join(map(str, s))),
        hdbscan_all=("hdbscan_mcs200", lambda s: ",".join(map(str, s))),
    ).reset_index()
    sent_cluster.columns = ["zhuzi_sent_id", "kmeans_cluster", "hdbscan_cluster",
                            "n_li_tokens", "kmeans_all", "hdbscan_all"]

    # 3. 인용에 클러스터 정보 + 주자/서신 메타 병합
    print("\nMerging with citation candidates and meta...")

    # 주자 메타
    zhuzi_meta = zhuzi[["sentence_id", "juan_num", "juan_label", "text_punctuated", "text_plain"]].rename(
        columns={"sentence_id": "zhuzi_sent_id",
                 "juan_num": "zhuzi_juan_num",
                 "juan_label": "zhuzi_juan",
                 "text_punctuated": "zhuzi_text",
                 "text_plain": "zhuzi_text_plain"}
    )

    df = top.merge(sent_cluster, on="zhuzi_sent_id", how="left")
    df = df.merge(zhuzi_meta, on="zhuzi_sent_id", how="left")

    # 서신 본문도
    letters = pd.read_json(PROJECT_ROOT / "data" / "processed" / "sentences_annotated.jsonl", lines=True)
    letter_meta = letters[["sentence_id", "letter_title", "letter_year", "kwon",
                           "sent_text", "sent_text_plain"]].rename(
        columns={"sentence_id": "letter_sent_id",
                 "kwon": "letter_kwon",
                 "sent_text": "letter_text",
                 "sent_text_plain": "letter_text_plain"}
    )
    df = df.merge(letter_meta, on="letter_sent_id", how="left")

    # 4. A형 중복 표시: 같은 (letter_sent_id, lcs) 조합
    df["dup_group"] = df.groupby(["letter_sent_id", "lcs"]).ngroup()
    df["is_primary"] = df.groupby("dup_group")["score"].transform(lambda s: s == s.max())

    # 컬럼 순서 정리
    col_order = [
        "score", "lcs_len", "lcs", "is_primary", "dup_group",
        "kmeans_cluster", "hdbscan_cluster", "n_li_tokens",
        "sender", "letter_sent_id", "letter_title", "letter_kwon",
        "zhuzi_sent_id", "zhuzi_juan_num", "zhuzi_juan",
        "letter_text", "zhuzi_text",
        "kmeans_all", "hdbscan_all",
    ]
    df = df[col_order]

    # 5. Cross-tab: 학자 × kmeans 클러스터
    print("\n--- 학자 × K-means 클러스터 (전체 35건) ---")
    ct_all = pd.crosstab(df["sender"], df["kmeans_cluster"], margins=True)
    print(ct_all)

    print("\n--- 학자 × K-means 클러스터 (unique 인용, is_primary=True만) ---")
    df_prim = df[df["is_primary"]]
    ct_prim = pd.crosstab(df_prim["sender"], df_prim["kmeans_cluster"], margins=True)
    print(ct_prim)

    print(f"\n  Unique 인용: 퇴계 {(df_prim['sender']=='T').sum()} / 율곡 {(df_prim['sender']=='Y').sum()}")

    print("\n--- HDBSCAN 클러스터 ---")
    ct_hdbscan = pd.crosstab(df_prim["sender"], df_prim["hdbscan_cluster"], margins=True)
    print(ct_hdbscan)

    # 6. 저장
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    print(f"\nSaving to {OUTPUT_PATH}...")

    summary_rows = [
        ["컷오프 점수", SCORE_CUTOFF],
        ["전체 35건", len(df)],
        ["퇴계 매칭 수", (df["sender"]=="T").sum()],
        ["율곡 매칭 수", (df["sender"]=="Y").sum()],
        ["Unique 인용 (is_primary)", len(df_prim)],
        ["  퇴계 unique", (df_prim["sender"]=="T").sum()],
        ["  율곡 unique", (df_prim["sender"]=="Y").sum()],
        ["", ""],
        ["베스트 K-means", f"K={BEST_KMEANS_K}"],
        ["베스트 HDBSCAN", f"mcs={BEST_HDBSCAN_MCS}"],
    ]
    summary_df = pd.DataFrame(summary_rows, columns=["항목", "값"])

    with pd.ExcelWriter(OUTPUT_PATH, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="summary", index=False)
        df.to_excel(writer, sheet_name="top35_full", index=False)
        df_prim.to_excel(writer, sheet_name="top35_unique", index=False)
        ct_all.to_excel(writer, sheet_name="ct_all")
        ct_prim.to_excel(writer, sheet_name="ct_unique")
        ct_hdbscan.to_excel(writer, sheet_name="ct_hdbscan")

    print("\n" + "=" * 60)
    print("DONE")
    print(f"  {OUTPUT_PATH}")
    print("=" * 60)


if __name__ == "__main__":
    main()
