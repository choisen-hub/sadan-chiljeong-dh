"""
07_annotate.py

zhuzi_sentences.jsonl (06)에 분석용 플래그를 부착한다.

추가 필드:
  - has_li: bool          # 理 포함 (text_plain 기준)
  - has_qi: bool          # 氣 포함
  - li_qi_category: str   # "both" | "li_only" | "qi_only" | "neither"

판단 기준은 text_plain (한자만, 표점 제외)이므로 표점에 우연히 동일 글자가
들어 있어도 영향 없음.

입력:  data/intermediate/zhuzi_sentences.jsonl
출력:  data/intermediate/zhuzi_sentences_annotated.jsonl

사용법:
  python3 scripts/07_annotate.py
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT = PROJECT_ROOT / "data" / "intermediate" / "zhuzi_sentences.jsonl"
OUT = PROJECT_ROOT / "data" / "intermediate" / "zhuzi_sentences_annotated.jsonl"

LI = "理"
QI = "氣"


def categorize(has_li: bool, has_qi: bool) -> str:
    if has_li and has_qi:
        return "both"
    if has_li:
        return "li_only"
    if has_qi:
        return "qi_only"
    return "neither"


def main() -> None:
    if not INPUT.exists():
        print(f"[ERROR] 입력 없음: {INPUT}", file=sys.stderr)
        print("        먼저 06_segment.py 실행 필요", file=sys.stderr)
        sys.exit(1)

    sentences = [json.loads(l) for l in INPUT.open(encoding="utf-8")]
    print(f"[LOAD] {len(sentences):,} sentences")

    cat_cnt: Counter = Counter()
    for s in sentences:
        plain = s["text_plain"]
        has_li = LI in plain
        has_qi = QI in plain
        s["has_li"] = has_li
        s["has_qi"] = has_qi
        s["li_qi_category"] = categorize(has_li, has_qi)
        cat_cnt[s["li_qi_category"]] += 1

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", encoding="utf-8") as f:
        for s in sentences:
            f.write(json.dumps(s, ensure_ascii=False) + "\n")

    print(f"\n[DONE] 저장: {OUT}")

    total = len(sentences)
    print(f"\n  분류 분포 ({total:,}건):")
    for k in ["both", "li_only", "qi_only", "neither"]:
        n = cat_cnt[k]
        bar = "█" * int(n / total * 50)
        print(f"    {k:>8s}: {n:>6,}건 ({n / total * 100:5.1f}%) {bar}")


if __name__ == "__main__":
    main()
