#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
14_annotate_letters.py — Annotate letter sentences with 理/氣 flags.

Step 14 of the Phase 0 데이터 정제 파이프라인 (서신 측).

13_segment_letters.py 가 산출한 sentences.jsonl 의 각 sentence 에 분석용
플래그를 부착한다. 주자어류 측 05_annotate.py 와 대칭적으로 동작하여,
두 corpus 의 통계 비교가 일관된 기준으로 가능하도록 한다.

추가 필드:
  - has_li: bool          # 理 포함 (sent_text_plain 기준)
  - has_qi: bool          # 氣 포함
  - li_qi_category: str   # "both" | "li_only" | "qi_only" | "neither"

판단 기준은 sent_text_plain (한자만, 표점 제외)이므로 표점 부호에 우연히
동일 글자가 포함되어 있어도 영향 없음.

용도:
  (1) 메타분석: 퇴계·율곡 서신의 理·氣 언급 비율 정량 비교
  (2) citation matching pre-filter: 주자어류 sentence 와 매칭 시
      理/氣 카테고리 기반 사전 필터링

입력:  data/processed/sentences.jsonl
출력:  data/processed/sentences_annotated.jsonl

사용법:
  python3 scripts/14_annotate_letters.py
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT = PROJECT_ROOT / "data" / "processed" / "sentences.jsonl"
OUT = PROJECT_ROOT / "data" / "processed" / "sentences_annotated.jsonl"

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
        print("        먼저 13_segment_letters.py 실행 필요", file=sys.stderr)
        sys.exit(1)

    sentences = [json.loads(l) for l in INPUT.open(encoding="utf-8")]
    print(f"[LOAD] {len(sentences):,} sentences")

    cat_cnt: Counter = Counter()
    cat_by_target: dict[str, Counter] = {}

    for s in sentences:
        plain = s["sent_text_plain"]
        has_li = LI in plain
        has_qi = QI in plain
        s["has_li"] = has_li
        s["has_qi"] = has_qi
        s["li_qi_category"] = categorize(has_li, has_qi)
        cat_cnt[s["li_qi_category"]] += 1

        # target_name 별 통계 (퇴계 / 율곡 분리)
        target = s.get("target_name", "unknown")
        if target not in cat_by_target:
            cat_by_target[target] = Counter()
        cat_by_target[target][s["li_qi_category"]] += 1

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", encoding="utf-8") as f:
        for s in sentences:
            f.write(json.dumps(s, ensure_ascii=False) + "\n")

    print(f"\n[DONE] 저장: {OUT}")

    # 전체 분포
    total = len(sentences)
    print(f"\n  전체 분류 분포 ({total:,}건):")
    for k in ["both", "li_only", "qi_only", "neither"]:
        n = cat_cnt[k]
        bar = "█" * int(n / total * 50)
        print(f"    {k:>8s}: {n:>6,}건 ({n / total * 100:5.1f}%) {bar}")

    # target 별 분포 (퇴계 vs 율곡 비교용)
    print(f"\n  Target별 분포:")
    for target in sorted(cat_by_target.keys()):
        cnt = cat_by_target[target]
        sub_total = sum(cnt.values())
        print(f"\n    {target} ({sub_total:,}건):")
        for k in ["both", "li_only", "qi_only", "neither"]:
            n = cnt[k]
            print(f"      {k:>8s}: {n:>5,}건 ({n / sub_total * 100:5.1f}%)")


if __name__ == "__main__":
    main()
