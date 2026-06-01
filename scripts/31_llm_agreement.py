"""
31_llm_agreement.py
理 문장별 다중 LLM 라벨의 모델 간 일치도 + 다의성 지표 (2차 설계)

라벨 A/B/C/U/N 기준. 입력: 30_llm_judgment.py 의 merged_labels.csv
핵심 산출 (지도교수 5/25 MAGI 논리 대응):
  - 모델별 분포 / 평균 자기일치도
  - 전체 만장일치율 / 쌍별 일치율 / Fleiss kappa
  - [핵심] A·B만으로 답한 문장 중 모델 간 불일치율 = 능동/수동 다의성 proxy
  - 만장일치 문장(=강한 신호) → consensus.csv  (A합의 / B합의 분리)
  - 논쟁 문장(A·B 동시 출현) → contested_AB.csv  (정성 예시용)
  - (옵션) 클러스터 / 인용 교차표

사용 예:
  python 31_llm_agreement.py --merged data/llm_judgment/merged_labels.csv
  python 31_llm_agreement.py --merged data/llm_judgment/merged_labels.csv \
      --clusters 理_cluster_samples_classified.xlsx --cluster-sheet 분류
"""

import argparse
from pathlib import Path
from itertools import combinations
from collections import Counter

import pandas as pd

LABELS = ["A", "B", "C", "U", "N"]


def fleiss_kappa(rows_counts, categories):
    N = len(rows_counts)
    if N == 0:
        return float("nan")
    n = sum(rows_counts[0].values())
    if n < 2:
        return float("nan")
    P_is, cat_totals = [], {c: 0 for c in categories}
    for rc in rows_counts:
        s = sum(rc.get(c, 0) ** 2 for c in categories)
        P_is.append((s - n) / (n * (n - 1)))
        for c in categories:
            cat_totals[c] += rc.get(c, 0)
    P_bar = sum(P_is) / N
    p_j = {c: cat_totals[c] / (N * n) for c in categories}
    P_e = sum(v ** 2 for v in p_j.values())
    if abs(1 - P_e) < 1e-12:
        return float("nan")
    return (P_bar - P_e) / (1 - P_e)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--merged", required=True)
    ap.add_argument("--clusters", default=None)
    ap.add_argument("--cluster-sheet", default=None)
    ap.add_argument("--citations", default=None)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    df = pd.read_csv(args.merged, dtype=str)
    label_cols = [c for c in df.columns if c.startswith("label_")]
    models = [c[len("label_"):] for c in label_cols]

    lines = []
    def emit(s=""):
        print(s); lines.append(s)

    emit("=" * 64)
    emit(f"입력: {args.merged}")
    emit(f"모델: {models}  | 전체 문장: {len(df)}")
    emit("=" * 64)

    emit("\n[1] 모델별 라벨 분포")
    for c in label_cols:
        vc = df[c].value_counts(dropna=False)
        emit(f"  {c}: " + ", ".join(f"{k}={int(v)}" for k, v in vc.items()))
    for m in models:
        sc = f"selfcon_{m}"
        if sc in df.columns:
            emit(f"  selfcon_{m} 평균: {df[sc].astype(float).mean():.3f}")

    valid = df.copy()
    for c in label_cols:
        valid = valid[valid[c].isin(LABELS)]
    emit(f"\n[2] 유효(모든 모델 정상 라벨) 문장: {len(valid)}  (제외 {len(df)-len(valid)})")

    if len(models) >= 2 and len(valid) > 0:
        emit(f"\n[3] 전체 만장일치율: {(valid[label_cols].nunique(axis=1)==1).mean():.3f}")
        emit("\n[4] 쌍별 일치율")
        for a, b in combinations(models, 2):
            emit(f"  {a} ~ {b}: {(valid[f'label_{a}']==valid[f'label_{b}']).mean():.3f}")
        rc = [dict(Counter(row[c] for c in label_cols)) for _, row in valid.iterrows()]
        emit(f"\n[5] Fleiss kappa (5범주, {len(models)}rater): {fleiss_kappa(rc, LABELS):.3f}")
        emit("    (참고: 다국어 LLM-as-judge 선행연구 평균 κ≈0.3 — Fu & Liu 2025)")

        ab = valid[(valid[label_cols].isin(['A', 'B'])).all(axis=1)]
        emit(f"\n[6] ** 핵심 ** A/B만으로 답한 문장: {len(ab)}")
        if len(ab) > 0:
            emit(f"    그 중 능동/수동 모델 간 불일치율: {(ab[label_cols].nunique(axis=1)>1).mean():.3f}  <- 다의성 proxy")

        c_share = (valid[label_cols] == 'C').mean().mean()
        emit(f"\n[7] 전체 라벨 중 C(공존) 비율(모델 평균): {c_share:.3f}")

        outdir = Path(args.merged).parent
        def has_both(row):
            s = set(row[c] for c in label_cols)
            return ('A' in s) and ('B' in s)
        contested = valid[valid.apply(has_both, axis=1)].copy()
        emit(f"\n[8] 논쟁 문장(A·B 동시): {len(contested)}")
        if len(contested) > 0:
            keep = ["sentence_id", "text"] + label_cols
            keep = [k for k in keep if k in contested.columns]
            contested[keep].to_csv(outdir / "contested_AB.csv", index=False, encoding="utf-8-sig")
            emit(f"    저장: {outdir/'contested_AB.csv'}")
            for _, r in contested[keep].head(5).iterrows():
                labs = "/".join(f"{m}:{r[f'label_{m}']}" for m in models)
                emit(f"      [{r['sentence_id']}] {str(r['text'])[:40]} | {labs}")

        consensus = valid[valid[label_cols].nunique(axis=1) == 1].copy()
        consensus["consensus_label"] = consensus[label_cols[0]]
        keepc = ["sentence_id", "text", "consensus_label"]
        keepc = [k for k in keepc if k in consensus.columns]
        consensus[keepc].to_csv(outdir / "consensus.csv", index=False, encoding="utf-8-sig")
        emit(f"\n[9] 만장일치 문장(강한 신호): {len(consensus)}  → {outdir/'consensus.csv'}")
        emit("    합의 라벨 분포: " + str(consensus["consensus_label"].value_counts().to_dict()))
        for lab in ["A", "B"]:
            sub = consensus[consensus["consensus_label"] == lab]
            if len(sub):
                emit(f"    [{lab} 만장일치 예시]")
                for _, r in sub.head(3).iterrows():
                    emit(f"      [{r['sentence_id']}] {str(r['text'])[:42]}")

    if args.clusters:
        try:
            cdf = pd.read_excel(args.clusters, sheet_name=args.cluster_sheet or 0, dtype=str)
            cid = _pc(cdf, ["sentence_id", "id"]); ccl = next((c for c in cdf.columns if "cluster" in str(c).lower()), None)
            if cid and ccl and len(models) >= 1:
                merged = valid.merge(cdf[[cid, ccl]].rename(columns={cid: "sentence_id", ccl: "cluster"}), on="sentence_id", how="inner")
                if len(merged):
                    merged["maj"] = merged[label_cols].apply(lambda r: r.value_counts().idxmax(), axis=1)
                    emit("\n[10] 클러스터 × 다수결 라벨 교차표")
                    emit(pd.crosstab(merged["cluster"], merged["maj"]).to_string())
        except Exception as e:
            emit(f"\n[10] 클러스터 교차표 실패: {e}")

    if args.citations:
        try:
            qdf = pd.read_csv(args.citations, dtype=str)
            qid = _pc(qdf, ["sentence_id", "id"]); qby = next((c for c in qdf.columns if "cited" in str(c).lower() or "인용" in str(c)), None)
            if qid and qby:
                merged = valid.merge(qdf[[qid, qby]].rename(columns={qid: "sentence_id", qby: "cited_by"}), on="sentence_id", how="inner")
                if len(merged):
                    merged["maj"] = merged[label_cols].apply(lambda r: r.value_counts().idxmax(), axis=1)
                    emit("\n[11] 인용자 × 다수결 라벨 교차표")
                    emit(pd.crosstab(merged["cited_by"], merged["maj"]).to_string())
        except Exception as e:
            emit(f"\n[11] 인용 교차표 실패: {e}")

    out_txt = Path(args.out) if args.out else Path(args.merged).parent / "agreement_summary.txt"
    out_txt.write_text("\n".join(lines), encoding="utf-8")
    print(f"\n요약 저장: {out_txt}")


def _pc(df, prefer):
    lower = {str(c).lower(): c for c in df.columns}
    for p in prefer:
        if p in lower:
            return lower[p]
    return None


if __name__ == "__main__":
    main()
