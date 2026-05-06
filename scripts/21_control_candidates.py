"""
21_control_candidates.py
========================

Phase 1 Step 3 대조군 후보 도출.

미팅 5 (2026-04-23) 김바로 교수님 코멘트 반영:
- 대조군 선정 기준 = 퇴계/율곡 서신 코퍼스 출현 빈도
- 메인 (a): 양쪽 모두 자주 쓰는 글자
- 보조 (b): 양쪽 출현 편차 극심 (논거 부족 시 사용)
- 어조사/허사 제외

입력:
    data/processed/sentences_annotated.jsonl
    필요 컬럼: sentence_id (T0001~/Y0001~), sent_text_plain (표점 제거 백문)

출력:
    data/processed/phase1/control_candidates/
    ├── char_freq.csv
    ├── top_balanced.csv
    ├── top_divergent.csv
    ├── core_terms_check.csv
    └── figures/freq_comparison.png

사용법:
    python scripts/21_control_candidates.py
    python scripts/21_control_candidates.py --top_n 50 --min_count 30
"""

from __future__ import annotations

import argparse
import logging
from collections import Counter
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

# ---------------------------------------------------------------------------
# 한문 stopword (허사·기능어)
# ---------------------------------------------------------------------------
# NOTE: 도메인 특화 리스트. 결과 보면서 조정 필요.
# 曰/云/謂/言은 인용 패턴 분석 핵심이지만 RQ1 대조군 선정에는 의미 없어 포함.
HANJA_STOPWORDS: frozenset[str] = frozenset(
    "之而以於于也乎矣焉哉耳爾與且夫兮"      # 조사·어조사
    "不無非未莫否勿弗毋"                       # 부정사
    "其此是彼斯該"                             # 지시사
    "則故又亦復但唯惟雖若如猶蓋凡或乃因然所即"  # 접속·부사
    "何誰孰奚胡曷安"                           # 의문사
    "甚太至已皆悉俱共每"                       # 양태·범위
    "曰云謂言"                                 # 인용·발화
    "一二三四五六七八九十百千萬"               # 숫자
    "者所有為使令"                             # 일반 기능어
)


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------

def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


def load_data(path: Path) -> pd.DataFrame:
    """확장자별 자동 로드."""
    suffix = path.suffix.lower()
    if suffix == ".jsonl":
        return pd.read_json(path, lines=True)
    if suffix == ".json":
        return pd.read_json(path)
    if suffix == ".parquet":
        return pd.read_parquet(path)
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in (".xlsx", ".xls"):
        return pd.read_excel(path)
    raise ValueError(f"Unsupported format: {suffix}")


# ---------------------------------------------------------------------------
# 핵심 로직
# ---------------------------------------------------------------------------

def split_by_author(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """sentence_id prefix (T/Y)로 퇴계/율곡 분리."""
    if "sentence_id" not in df.columns:
        raise KeyError("'sentence_id' column not found")

    toegye = df[df["sentence_id"].str.startswith("T")].copy()
    yulgok = df[df["sentence_id"].str.startswith("Y")].copy()

    logging.info(f"퇴계: {len(toegye):,} sentence")
    logging.info(f"율곡: {len(yulgok):,} sentence")

    if len(toegye) == 0 or len(yulgok) == 0:
        raise ValueError("Empty subset detected; check sentence_id prefixes")

    return toegye, yulgok


def count_chars(df: pd.DataFrame, text_col: str) -> Counter[str]:
    """한자 글자 빈도 (stopword·비-한자 제외)."""
    counter: Counter[str] = Counter()
    for text in df[text_col].dropna():
        for ch in text:
            if "\u4e00" <= ch <= "\u9fff" and ch not in HANJA_STOPWORDS:
                counter[ch] += 1
    return counter


def build_freq_table(t_counts: Counter[str], y_counts: Counter[str]) -> pd.DataFrame:
    """양 코퍼스 빈도 통합 테이블.

    balance_ratio = min(t,y) / max(t,y)   1.0=완벽 균형, 0=한쪽만
    divergence    = |t-y| / (t+y)         0=균형, 1=한쪽만
    """
    chars = set(t_counts) | set(y_counts)
    rows = []
    for ch in chars:
        t = t_counts.get(ch, 0)
        y = y_counts.get(ch, 0)
        total = t + y
        balance = (min(t, y) / max(t, y)) if max(t, y) > 0 else 0.0
        divergence = abs(t - y) / total if total > 0 else 0.0
        rows.append({
            "char": ch,
            "toegye_count": t,
            "yulgok_count": y,
            "total": total,
            "balance_ratio": round(balance, 4),
            "divergence": round(divergence, 4),
        })
    return (
        pd.DataFrame(rows)
        .sort_values("total", ascending=False)
        .reset_index(drop=True)
    )


def select_balanced(freq: pd.DataFrame, top_n: int, min_count: int) -> pd.DataFrame:
    """메인 후보: 양쪽 모두 자주 쓰는 글자."""
    eligible = freq[
        (freq["toegye_count"] >= min_count)
        & (freq["yulgok_count"] >= min_count)
        & (freq["balance_ratio"] >= 0.5)
    ].copy()
    return eligible.sort_values("total", ascending=False).head(top_n)


def select_divergent(freq: pd.DataFrame, top_n: int, min_total: int) -> pd.DataFrame:
    """보조 후보: 한쪽에서 두드러지게 많이 출현하는 글자."""
    eligible = freq[freq["total"] >= min_total].copy()
    return eligible.sort_values("divergence", ascending=False).head(top_n)


def check_core_terms(freq: pd.DataFrame) -> pd.DataFrame:
    """사단칠정 도메인 핵심 글자 빈도 점검."""
    targets = ["理", "氣", "心", "性", "天", "情", "道", "德", "仁", "義",
               "發", "中", "和", "敬", "誠", "知", "行", "善", "惡"]
    check = freq[freq["char"].isin(targets)].copy()
    return check.sort_values("total", ascending=False).reset_index(drop=True)


def plot_comparison(freq: pd.DataFrame, balanced: pd.DataFrame, output_path: Path) -> None:
    """퇴계 vs 율곡 빈도 산점도. 메인 후보 강조."""
    plt.rcParams["font.family"] = ["AppleGothic", "Malgun Gothic", "Noto Sans CJK KR", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False

    fig, ax = plt.subplots(figsize=(9, 9))

    # 전체 점 (회색)
    ax.scatter(
        freq["toegye_count"], freq["yulgok_count"],
        s=8, alpha=0.25, color="gray", label="all chars",
    )
    # 메인 후보 강조
    ax.scatter(
        balanced["toegye_count"], balanced["yulgok_count"],
        s=50, color="crimson", label=f"balanced top-{len(balanced)}", zorder=3,
    )
    for _, row in balanced.iterrows():
        ax.annotate(
            row["char"],
            (row["toegye_count"], row["yulgok_count"]),
            fontsize=11, ha="left", va="bottom",
        )

    lim = max(freq["toegye_count"].max(), freq["yulgok_count"].max()) * 1.1
    ax.plot([1, lim], [1, lim], "--", color="lightgray", linewidth=0.8, label="y=x")

    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("Toegye character count (log)")
    ax.set_ylabel("Yulgok character count (log)")
    ax.set_title("Character frequency: Toegye vs Yulgok\n(stopwords excluded)")
    ax.legend(loc="lower right")
    ax.grid(True, which="both", alpha=0.3)

    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
    logging.info(f"saved plot → {output_path}")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--input_path", type=Path,
                        default=Path("data/processed/sentences_annotated.jsonl"),
                        help="서신 sentence-level 데이터 (jsonl/csv/parquet/xlsx)")
    parser.add_argument("--text_col", type=str, default="sent_text_plain",
                        help="분석에 쓸 텍스트 컬럼 (sent_text 또는 sent_text_plain)")
    parser.add_argument("--output_dir", type=Path,
                        default=Path("data/processed/phase1/control_candidates"),
                        help="결과 저장 디렉토리")
    parser.add_argument("--top_n", type=int, default=30)
    parser.add_argument("--min_count", type=int, default=20,
                        help="balanced 후보 양쪽 각각 최소 빈도")
    parser.add_argument("--min_total_divergent", type=int, default=30,
                        help="divergent 후보 최소 합계 빈도")
    args = parser.parse_args()

    setup_logging()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    figures_dir = args.output_dir / "figures"
    figures_dir.mkdir(exist_ok=True)

    # 1) 데이터 로드
    logging.info(f"load: {args.input_path}")
    df = load_data(args.input_path)
    logging.info(f"total sentences: {len(df):,}")

    if args.text_col not in df.columns:
        raise KeyError(f"'{args.text_col}' column not found. available: {df.columns.tolist()}")

    # 2) 저자 분리
    toegye_df, yulgok_df = split_by_author(df)

    # 3) 글자 빈도
    t_counts = count_chars(toegye_df, args.text_col)
    y_counts = count_chars(yulgok_df, args.text_col)
    logging.info(f"퇴계 unique chars: {len(t_counts):,}")
    logging.info(f"율곡 unique chars: {len(y_counts):,}")

    # 4) 통합 테이블
    freq = build_freq_table(t_counts, y_counts)
    freq.to_csv(args.output_dir / "char_freq.csv", index=False, encoding="utf-8-sig")
    logging.info(f"saved char_freq.csv ({len(freq):,} rows)")

    # 5) 메인 후보
    balanced = select_balanced(freq, args.top_n, args.min_count)
    balanced.to_csv(args.output_dir / "top_balanced.csv", index=False, encoding="utf-8-sig")
    logging.info(f"\n=== top_balanced (N={len(balanced)}) ===\n{balanced.to_string(index=False)}")

    # 6) 보조 후보
    divergent = select_divergent(freq, args.top_n, args.min_total_divergent)
    divergent.to_csv(args.output_dir / "top_divergent.csv", index=False, encoding="utf-8-sig")
    logging.info(f"\n=== top_divergent (N={len(divergent)}) ===\n{divergent.to_string(index=False)}")

    # 7) 핵심 글자 점검
    core = check_core_terms(freq)
    core.to_csv(args.output_dir / "core_terms_check.csv", index=False, encoding="utf-8-sig")
    logging.info(f"\n=== core terms ===\n{core.to_string(index=False)}")

    # 8) 시각화
    plot_comparison(freq, balanced, figures_dir / "freq_comparison.png")

    logging.info("done.")


if __name__ == "__main__":
    main()
