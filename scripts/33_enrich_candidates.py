"""
33_enrich_candidates.py

31번 매칭 결과에 noise 거름용 보조 컬럼 추가:
  방법 1: LCS 양옆 ±3자 자리 무관 일치율
  방법 2: letter 문장 전체에 인용표지 (曰/云/謂 등) 있는지

입력:
  data/processed/phase2/citation_candidates.parquet
  data/final/zhuzi_sentences.xlsx
  data/processed/sentences_annotated.jsonl

출력:
  data/final/citation_candidates_enriched.xlsx
    - summary
    - all_sorted (점수 내림차순)
    - toegye_only
    - yulgok_only
"""

from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path("/Users/vairocana/projects/sadan-chiljeong-dh")
INPUT_CANDIDATES = PROJECT_ROOT / "data" / "processed" / "phase2" / "citation_candidates.parquet"
INPUT_ZHUZI = PROJECT_ROOT / "data" / "final" / "zhuzi_sentences.xlsx"
INPUT_LETTERS = PROJECT_ROOT / "data" / "processed" / "sentences_annotated.jsonl"
OUTPUT_PATH = PROJECT_ROOT / "data" / "final" / "citation_candidates_enriched.xlsx"

WINDOW = 3

# 인용표지 — 본인 한문 직관으로 추후 조정 가능
QUOTE_MARKERS = [
    "曰", "云", "謂", "言", "稱",
    "以爲",
    "嘗",
    "先儒",
    "朱子", "晦菴", "晦庵", "晦翁",
    "程子", "伊川", "明道",
    "橫渠", "張子",
    "濂溪", "周子",
]


def get_context_around_lcs(text: str, lcs: str, window: int) -> tuple:
    """
    text 안에서 lcs의 첫 번째 출현 위치를 찾고, 그 양옆 window 자를 반환.
    
    Returns:
        (left_context, right_context)
        없으면 ("", "")
    """
    if not lcs or lcs not in text:
        return "", ""
    pos = text.find(lcs)
    left_start = max(0, pos - window)
    left = text[left_start:pos]
    right_end = min(len(text), pos + len(lcs) + window)
    right = text[pos + len(lcs):right_end]
    return left, right


def positionless_match_rate(s1: str, s2: str) -> float:
    """
    두 문자열 간 자리 무관 글자 일치율.
    공통 글자 수 / max(len(s1), len(s2))
    중복은 multiset으로 처리.
    """
    if not s1 or not s2:
        return 0.0
    from collections import Counter
    c1 = Counter(s1)
    c2 = Counter(s2)
    common = sum((c1 & c2).values())  # multiset intersection
    return common / max(len(s1), len(s2))


def detect_quote_markers(text: str, markers: list) -> tuple:
    """
    text 안에 있는 인용표지를 찾아 (개수, 발견된 표지 리스트) 반환.
    """
    if not text:
        return 0, ""
    found = []
    for m in markers:
        if m in text:
            found.append(m)
    return len(found), ",".join(found)


def main():
    print("Loading...")
    cand = pd.read_parquet(INPUT_CANDIDATES)
    zhuzi = pd.read_excel(INPUT_ZHUZI, sheet_name="li_sentences")
    letters = pd.read_json(INPUT_LETTERS, lines=True)
    print(f"  Candidates: {len(cand):,}")

    # 메타 + 본문
    zhuzi_meta = zhuzi[[
        "sentence_id", "juan_num", "juan_label", "text_plain", "text_punctuated"
    ]].rename(columns={
        "sentence_id": "zhuzi_sent_id",
        "juan_num": "zhuzi_juan_num",
        "juan_label": "zhuzi_juan",
        "text_plain": "zhuzi_text_plain",
        "text_punctuated": "zhuzi_text",
    })

    letter_meta = letters[[
        "sentence_id", "letter_title", "letter_year", "kwon",
        "sent_text", "sent_text_plain"
    ]].rename(columns={
        "sentence_id": "letter_sent_id",
        "letter_title": "letter_title",
        "letter_year": "letter_year",
        "kwon": "letter_kwon",
        "sent_text": "letter_text",
        "sent_text_plain": "letter_text_plain",
    })

    df = cand.merge(letter_meta, on="letter_sent_id", how="left")
    df = df.merge(zhuzi_meta, on="zhuzi_sent_id", how="left")

    # 방법 1: 양옆 ±3자 일치율
    print(f"\n방법 1: ±{WINDOW}자 양옆 일치율 계산...")
    left_letter_list = []
    right_letter_list = []
    left_zhuzi_list = []
    right_zhuzi_list = []
    left_match_list = []
    right_match_list = []
    total_match_list = []

    for _, row in df.iterrows():
        ll, lr = get_context_around_lcs(row["letter_text_plain"], row["lcs"], WINDOW)
        zl, zr = get_context_around_lcs(row["zhuzi_text_plain"], row["lcs"], WINDOW)
        left_match = positionless_match_rate(ll, zl)
        right_match = positionless_match_rate(lr, zr)
        # 가중 평균 (좌우 글자 수 가중)
        total_len = max(len(ll), len(zl)) + max(len(lr), len(zr))
        if total_len > 0:
            weighted = (left_match * max(len(ll), len(zl)) + right_match * max(len(lr), len(zr))) / total_len
        else:
            weighted = 0.0

        left_letter_list.append(ll)
        right_letter_list.append(lr)
        left_zhuzi_list.append(zl)
        right_zhuzi_list.append(zr)
        left_match_list.append(round(left_match, 3))
        right_match_list.append(round(right_match, 3))
        total_match_list.append(round(weighted, 3))

    df["ctx_left_letter"] = left_letter_list
    df["ctx_right_letter"] = right_letter_list
    df["ctx_left_zhuzi"] = left_zhuzi_list
    df["ctx_right_zhuzi"] = right_zhuzi_list
    df["ctx_left_match"] = left_match_list
    df["ctx_right_match"] = right_match_list
    df["ctx_total_match"] = total_match_list

    # 방법 2: letter 문장 전체에 인용표지
    print("방법 2: 인용표지 검출...")
    df["quote_markers_found"] = df["letter_text_plain"].apply(
        lambda t: detect_quote_markers(t, QUOTE_MARKERS)[1]
    )
    df["quote_marker_count"] = df["letter_text_plain"].apply(
        lambda t: detect_quote_markers(t, QUOTE_MARKERS)[0]
    )
    df["has_quote_marker"] = df["quote_marker_count"] > 0

    # 컬럼 순서 (검토 효율 우선)
    col_order = [
        # 점수/매칭 핵심
        "score", "lcs_len", "lcs",
        # noise 거름용
        "ctx_total_match", "ctx_left_match", "ctx_right_match",
        "has_quote_marker", "quote_markers_found",
        # 양옆 문맥 (눈으로 확인용)
        "ctx_left_letter", "ctx_right_letter",
        "ctx_left_zhuzi", "ctx_right_zhuzi",
        # 메타
        "sender", "letter_sent_id", "letter_title", "letter_year", "letter_kwon",
        "zhuzi_sent_id", "zhuzi_juan_num", "zhuzi_juan",
        # 원문
        "letter_text", "zhuzi_text",
        # 백문
        "letter_text_plain", "zhuzi_text_plain",
        # 기타
        "mean_idf", "quote_marker_count",
    ]
    df = df[col_order]
    df = df.sort_values("score", ascending=False).reset_index(drop=True)

    toegye = df[df["sender"] == "T"].reset_index(drop=True)
    yulgok = df[df["sender"] == "Y"].reset_index(drop=True)

    # Summary
    summary_rows = [
        ["총 매칭", len(df)],
        ["퇴계 매칭", len(toegye)],
        ["율곡 매칭", len(yulgok)],
        ["", ""],
        ["LCS 길이별", ""],
    ]
    for length, count in df["lcs_len"].value_counts().sort_index().items():
        summary_rows.append([f"  {length}자", count])
    summary_rows.extend([
        ["", ""],
        ["양옆 일치율 분포 (ctx_total_match)", ""],
        ["  0.0 (양옆 완전 다름)", (df["ctx_total_match"] == 0).sum()],
        ["  0.0 ~ 0.3 (거의 다름)", ((df["ctx_total_match"] > 0) & (df["ctx_total_match"] < 0.3)).sum()],
        ["  0.3 ~ 0.5", ((df["ctx_total_match"] >= 0.3) & (df["ctx_total_match"] < 0.5)).sum()],
        ["  0.5 ~ 0.7", ((df["ctx_total_match"] >= 0.5) & (df["ctx_total_match"] < 0.7)).sum()],
        ["  0.7 이상 (양옆도 거의 일치, 강한 인용)", (df["ctx_total_match"] >= 0.7).sum()],
        ["", ""],
        ["인용표지 통계", ""],
        ["  표지 있는 letter 문장 매칭 수", df["has_quote_marker"].sum()],
        ["  퇴계 중 표지 있는 매칭", toegye["has_quote_marker"].sum()],
        ["  율곡 중 표지 있는 매칭", yulgok["has_quote_marker"].sum()],
        ["", ""],
        ["조합 신뢰도", ""],
        ["  양옆 ≥0.5 + 표지 있음 (가장 강한 인용)",
            ((df["ctx_total_match"] >= 0.5) & df["has_quote_marker"]).sum()],
        ["  양옆 ≥0.5 (표지 무관)",
            (df["ctx_total_match"] >= 0.5).sum()],
        ["  양옆 <0.3 + 표지 없음 (거의 확실한 noise)",
            ((df["ctx_total_match"] < 0.3) & ~df["has_quote_marker"]).sum()],
    ])
    summary = pd.DataFrame(summary_rows, columns=["항목", "값"])

    # 저장
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    print(f"\nSaving to {OUTPUT_PATH}...")
    with pd.ExcelWriter(OUTPUT_PATH, engine="openpyxl") as writer:
        summary.to_excel(writer, sheet_name="summary", index=False)
        df.to_excel(writer, sheet_name="all_sorted", index=False)
        toegye.to_excel(writer, sheet_name="toegye_only", index=False)
        yulgok.to_excel(writer, sheet_name="yulgok_only", index=False)

    print("\n" + "=" * 60)
    print("DONE")
    print(f"  {OUTPUT_PATH}")
    print("=" * 60)

    # 콘솔 빠른 요약
    print("\n--- 양옆 일치율 분포 ---")
    print(f"  ≥0.7 (강한 인용 후보): {(df['ctx_total_match'] >= 0.7).sum()}")
    print(f"  ≥0.5: {(df['ctx_total_match'] >= 0.5).sum()}")
    print(f"  ≥0.3: {(df['ctx_total_match'] >= 0.3).sum()}")
    print(f"  =0.0: {(df['ctx_total_match'] == 0).sum()}")

    print("\n--- 인용표지 ---")
    print(f"  표지 있는 매칭: 퇴계 {toegye['has_quote_marker'].sum()} / 율곡 {yulgok['has_quote_marker'].sum()}")

    print("\n--- 양옆 ≥0.5 매칭의 sender별 분포 ---")
    strong = df[df["ctx_total_match"] >= 0.5]
    print(strong["sender"].value_counts())


if __name__ == "__main__":
    main()
