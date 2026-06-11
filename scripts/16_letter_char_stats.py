#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
16_letter_char_stats.py
서신 코퍼스의 핵심 글자 분포 집계 — 최종보고서 §5.1 (표 5-1) 재현.

산출 항목 (발신측 T=퇴계 / Y=율곡 각각):
  - 문장 수
  - 理 또는 氣 등장 문장률 (has_li | has_qi, %)
  - 互·情 총출현 횟수 (글자 단위, 백문 기준) 및 포함 문장 수

입력: data/final/corpus_review.xlsx 의 "Sentences" 시트
      (scripts/15_export_letters_xlsx.py 산출물)

사용 예:
  python3 scripts/16_letter_char_stats.py
  python3 scripts/16_letter_char_stats.py --chars 互 情 發 心
"""

import argparse
from pathlib import Path

import pandas as pd

DEFAULT_INPUT = Path("data/final/corpus_review.xlsx")
DEFAULT_CHARS = ["互", "情"]
SIDE_LABEL = {"T": "퇴계", "Y": "율곡"}


def main():
    ap = argparse.ArgumentParser(description="서신 코퍼스 핵심 글자 분포 집계 (보고서 표 5-1)")
    ap.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    ap.add_argument("--sheet", default="Sentences")
    ap.add_argument("--chars", nargs="+", default=DEFAULT_CHARS,
                    help="집계할 글자 목록 (기본: 互 情)")
    args = ap.parse_args()

    df = pd.read_excel(args.input, sheet_name=args.sheet)
    text = df["백문"].astype(str)

    print("=" * 64)
    print(f"입력: {args.input}  |  전체 문장: {len(df):,}")
    print("=" * 64)

    rows = []
    for side in ["T", "Y"]:
        mask = df["발신측"] == side
        sub, st = df[mask], text[mask]
        n = len(sub)
        liqi_n = int((sub["has_li"] | sub["has_qi"]).sum())
        row = {
            "발신측": f"{SIDE_LABEL[side]}({side})",
            "문장 수": n,
            "理|氣 문장": liqi_n,
            "理|氣 문장률(%)": round(liqi_n / n * 100, 1),
        }
        for ch in args.chars:
            row[f"{ch} 총출현"] = int(st.str.count(ch).sum())
            row[f"{ch} 포함문장"] = int(st.str.contains(ch).sum())
        rows.append(row)

    out = pd.DataFrame(rows).set_index("발신측")
    print(out.to_string())

    t, y = rows[0], rows[1]
    print()
    print(f"理|氣 문장률 비율 (율곡/퇴계): "
          f"{y['理|氣 문장률(%)'] / t['理|氣 문장률(%)']:.2f}배")
    for ch in args.chars:
        tk, yk = t[f"{ch} 총출현"], y[f"{ch} 총출현"]
        if tk and yk:
            print(f"{ch} 총출현 비율 (율곡/퇴계): {yk / tk:.2f}배")


if __name__ == "__main__":
    main()
