"""
32_export_candidates_xlsx.py

31번이 만든 citation_candidates.parquet (1,504행)을 검토용 엑셀로 변환.
컷오프/필터링/분석 일절 없음. 원문 텍스트를 같이 넣어 본인이 눈으로 검토 가능하게.

입력:
  data/processed/phase2/citation_candidates.parquet
  data/final/zhuzi_sentences.xlsx (주자어류 원문)
  data/processed/sentences_annotated.jsonl (서신 원문)

출력:
  data/final/citation_candidates_review.xlsx
    - all_sorted: 점수 내림차순 전체 1,504행
    - toegye_only: 퇴계 매칭만
    - yulgok_only: 율곡 매칭만
    - summary: 요약 통계
"""

from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path("/Users/vairocana/projects/sadan-chiljeong-dh")
INPUT_CANDIDATES = PROJECT_ROOT / "data" / "processed" / "phase2" / "citation_candidates.parquet"
INPUT_ZHUZI = PROJECT_ROOT / "data" / "final" / "zhuzi_sentences.xlsx"
INPUT_LETTERS = PROJECT_ROOT / "data" / "processed" / "sentences_annotated.jsonl"
OUTPUT_PATH = PROJECT_ROOT / "data" / "final" / "citation_candidates_review.xlsx"


def main():
    print("Loading...")
    cand = pd.read_parquet(INPUT_CANDIDATES)
    print(f"  Candidates: {len(cand):,}")

    zhuzi = pd.read_excel(INPUT_ZHUZI, sheet_name="li_sentences")
    letters = pd.read_json(INPUT_LETTERS, lines=True)
    print(f"  Zhuzi: {len(zhuzi):,}")
    print(f"  Letters: {len(letters):,}")

    # 1. 주자어류 메타 + 본문 병합
    zhuzi_meta = zhuzi[[
        "sentence_id", "juan_num", "juan_label",
        "text_plain", "text_punctuated"
    ]].rename(columns={
        "sentence_id": "zhuzi_sent_id",
        "juan_num": "zhuzi_juan_num",
        "juan_label": "zhuzi_juan",
        "text_plain": "zhuzi_text_plain",
        "text_punctuated": "zhuzi_text",
    })

    # 2. 서신 메타 + 본문 병합
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

    # 3. 병합
    df = cand.merge(letter_meta, on="letter_sent_id", how="left")
    df = df.merge(zhuzi_meta, on="zhuzi_sent_id", how="left")

    # 4. 컬럼 순서 정리 (검토 효율 최우선)
    col_order = [
        "score", "lcs_len", "mean_idf", "lcs",
        "sender", "letter_sent_id", "letter_title", "letter_year", "letter_kwon",
        "letter_text",
        "zhuzi_sent_id", "zhuzi_juan_num", "zhuzi_juan",
        "zhuzi_text",
        "letter_text_plain", "zhuzi_text_plain",
    ]
    df = df[col_order]
    df = df.sort_values("score", ascending=False).reset_index(drop=True)

    # 5. 시트별 분리
    toegye = df[df["sender"] == "T"].reset_index(drop=True)
    yulgok = df[df["sender"] == "Y"].reset_index(drop=True)

    # 6. 요약
    summary_rows = [
        ["총 매칭", len(df)],
        ["퇴계 (T) 매칭", len(toegye)],
        ["율곡 (Y) 매칭", len(yulgok)],
        ["", ""],
        ["퇴계 점수 평균", round(toegye["score"].mean(), 2)],
        ["퇴계 점수 최대", round(toegye["score"].max(), 2)],
        ["퇴계 LCS 길이 최대", int(toegye["lcs_len"].max())],
        ["", ""],
        ["율곡 점수 평균", round(yulgok["score"].mean(), 2)],
        ["율곡 점수 최대", round(yulgok["score"].max(), 2)],
        ["율곡 LCS 길이 최대", int(yulgok["lcs_len"].max())],
        ["", ""],
        ["LCS 길이별 분포", ""],
    ]
    for length, count in df["lcs_len"].value_counts().sort_index().items():
        summary_rows.append([f"  {length}자", count])

    summary = pd.DataFrame(summary_rows, columns=["항목", "값"])

    # 7. 저장
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    print(f"\nSaving to {OUTPUT_PATH}...")
    with pd.ExcelWriter(OUTPUT_PATH, engine="openpyxl") as writer:
        summary.to_excel(writer, sheet_name="summary", index=False)
        df.to_excel(writer, sheet_name="all_sorted", index=False)
        toegye.to_excel(writer, sheet_name="toegye_only", index=False)
        yulgok.to_excel(writer, sheet_name="yulgok_only", index=False)

    print("\n" + "=" * 60)
    print(f"DONE")
    print(f"  {OUTPUT_PATH}")
    print(f"  시트 4개: summary / all_sorted / toegye_only / yulgok_only")
    print("=" * 60)


if __name__ == "__main__":
    main()
