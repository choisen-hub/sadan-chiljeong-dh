"""
31_citation_matching.py

Phase 2 Step 1: 정확 부분 문자열 기반 인용 매칭.

전제:
  - 주자어류 理 문장 (8,443) vs 서신 理 문장 (T+Y)
  - 문장 쌍별 가장 긴 공통 부분 문자열(LCS) 추출
  - LCS 길이 ≥ 4자 인 경우만 인용 후보
  - 점수 = LCS 길이 × 평균 IDF (LCS 안의 모든 4-gram의 평균 IDF)
  - IDF는 주자어류 + 서신을 합친 결합 코퍼스 기준

입력:
  data/final/zhuzi_sentences.xlsx (li_sentences 시트)
  data/processed/sentences_annotated.jsonl (has_li=True만 사용)

출력:
  data/processed/phase2/citation_candidates.parquet
    columns: letter_sent_id, sender, zhuzi_sent_id, lcs, lcs_len, mean_idf, score
"""

from pathlib import Path
from collections import Counter
import math

import numpy as np
import pandas as pd
from tqdm import tqdm

# ============================================================
PROJECT_ROOT = Path("/Users/vairocana/projects/sadan-chiljeong-dh")
ZHUZI_PATH = PROJECT_ROOT / "data" / "final" / "zhuzi_sentences.xlsx"
LETTERS_PATH = PROJECT_ROOT / "data" / "processed" / "sentences_annotated.jsonl"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "phase2"
OUTPUT_PATH = OUTPUT_DIR / "citation_candidates.parquet"

MIN_LCS_LEN = 4
NGRAM_SIZE = 4   # IDF 계산용 n-gram 크기


def longest_common_substring(s1: str, s2: str) -> tuple:
    """
    Return (lcs_string, lcs_length).
    Dynamic programming O(n*m). Both strings are short (avg ~20 chars).
    """
    if not s1 or not s2:
        return "", 0
    n, m = len(s1), len(s2)
    # dp[i][j] = length of LCS ending at s1[i-1], s2[j-1]
    dp = [[0] * (m + 1) for _ in range(n + 1)]
    best_len = 0
    best_end = 0
    for i in range(1, n + 1):
        for j in range(1, m + 1):
            if s1[i-1] == s2[j-1]:
                dp[i][j] = dp[i-1][j-1] + 1
                if dp[i][j] > best_len:
                    best_len = dp[i][j]
                    best_end = i
    return s1[best_end - best_len:best_end], best_len


def build_idf_table(sentences: list, n: int) -> dict:
    """
    Combined corpus n-gram IDF.
    IDF = log(N / df) where N = 전체 문장 수, df = 그 n-gram을 포함한 문장 수.
    """
    N = len(sentences)
    df_counter = Counter()

    for s in sentences:
        # 한 문장 안의 unique n-grams만 카운트 (document frequency)
        ngrams_in_sent = set()
        for i in range(len(s) - n + 1):
            ngrams_in_sent.add(s[i:i+n])
        for ng in ngrams_in_sent:
            df_counter[ng] += 1

    idf = {}
    for ng, df in df_counter.items():
        idf[ng] = math.log(N / df)
    return idf


def lcs_mean_idf(lcs: str, idf: dict, n: int) -> float:
    """LCS 안의 모든 n-gram의 IDF 평균. n-gram이 없으면 (LCS가 너무 짧으면) 0."""
    if len(lcs) < n:
        return 0.0
    ngrams = [lcs[i:i+n] for i in range(len(lcs) - n + 1)]
    idfs = [idf.get(ng, 0.0) for ng in ngrams]
    return sum(idfs) / len(idfs) if idfs else 0.0


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Load 주자어류 (理 문장)
    print("Loading 주자어류 (理 문장)...")
    zhuzi = pd.read_excel(ZHUZI_PATH, sheet_name="li_sentences")
    print(f"  {len(zhuzi):,} sentences")

    # 2. Load 서신 (理 문장만)
    print("\nLoading 서신 (理 문장만)...")
    letters = pd.read_json(LETTERS_PATH, lines=True)
    letters = letters[letters["has_li"] == True].copy()
    print(f"  Total: {len(letters):,}")
    print(f"  퇴계 (T): {(letters['sender_label'] == 'T').sum():,}")
    print(f"  율곡 (Y): {(letters['sender_label'] == 'Y').sum():,}")

    # 3. 결합 IDF 계산
    print(f"\nBuilding combined IDF (n={NGRAM_SIZE})...")
    combined_texts = zhuzi["text_plain"].tolist() + letters["sent_text_plain"].tolist()
    idf = build_idf_table(combined_texts, NGRAM_SIZE)
    print(f"  Unique {NGRAM_SIZE}-grams: {len(idf):,}")
    print(f"  IDF range: {min(idf.values()):.3f} ~ {max(idf.values()):.3f}")

    # 4. LCS 기반 매칭
    print(f"\nLCS matching (min_lcs={MIN_LCS_LEN})...")
    zhuzi_ids = zhuzi["sentence_id"].tolist()
    zhuzi_texts = zhuzi["text_plain"].tolist()
    letter_ids = letters["sentence_id"].tolist()
    letter_senders = letters["sender_label"].tolist()
    letter_texts = letters["sent_text_plain"].tolist()

    # 총 매칭 쌍 수
    total_pairs = len(letter_ids) * len(zhuzi_ids)
    print(f"  Total pairs to check: {total_pairs:,}")

    candidates = []
    for li, (lid, lsender, ltext) in enumerate(tqdm(
        zip(letter_ids, letter_senders, letter_texts),
        total=len(letter_ids),
        desc="서신 문장 진행",
    )):
        for zid, ztext in zip(zhuzi_ids, zhuzi_texts):
            lcs, lcs_len = longest_common_substring(ltext, ztext)
            if lcs_len >= MIN_LCS_LEN:
                m_idf = lcs_mean_idf(lcs, idf, NGRAM_SIZE)
                score = lcs_len * m_idf
                candidates.append({
                    "letter_sent_id": lid,
                    "sender": lsender,
                    "zhuzi_sent_id": zid,
                    "lcs": lcs,
                    "lcs_len": lcs_len,
                    "mean_idf": m_idf,
                    "score": score,
                })

    # 5. 저장
    df = pd.DataFrame(candidates)
    df = df.sort_values("score", ascending=False).reset_index(drop=True)
    df.to_parquet(OUTPUT_PATH, index=False)

    print("\n" + "=" * 60)
    print(f"DONE")
    print(f"  Total candidates: {len(df):,}")
    print(f"  Saved: {OUTPUT_PATH}")
    print("=" * 60)

    # 6. 분포 요약
    if len(df) > 0:
        print("\n--- Score Distribution ---")
        print(df["score"].describe())
        print("\n--- LCS Length Distribution ---")
        print(df["lcs_len"].value_counts().sort_index())
        print("\n--- By Sender ---")
        print(df["sender"].value_counts())
        print("\n--- Top 20 by score ---")
        print(df.head(20)[["sender", "letter_sent_id", "zhuzi_sent_id", "lcs", "lcs_len", "mean_idf", "score"]].to_string())


if __name__ == "__main__":
    main()
