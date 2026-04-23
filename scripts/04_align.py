"""
04_align.py

칸리포 원본(02번 산출물)과 祝平次 편집본(03번 산출물)을 권 단위로 정렬·비교한다.

핵심 아이디어:
  - 두 버전은 표점 유무가 달라 직접 비교 불가
    · 칸리포: 한자만, 표점 없음
    · 祝平次: 「」、。，？ 등 표점 풍부
  - → 祝平次를 "백문(표점 제거)"으로 만든 뒤 칸리포와 문자 단위 비교
  - 표점 제거: CJK 한자 블록만 남기고 나머지 전부 제거
    (이체자 확장 블록 포함: Ext A/B~, Compatibility)
  - 정렬: difflib.SequenceMatcher (문자 단위)
  - 불일치 구간 추출: get_opcodes()에서 equal 이외의 항목

입력:
  data/intermediate/kanripo_parsed.jsonl   (02번)
  data/intermediate/zhuzi_parsed.jsonl     (03번)

출력:
  data/final/alignment_summary.csv   권별 요약 (GitHub 공개)
  data/final/zhuzi_mismatch.csv      불일치 상세 (GitHub 공개)

사용법:
  python scripts/04_align.py
"""

from __future__ import annotations

import csv
import json
import re
import sys
from difflib import SequenceMatcher
from pathlib import Path

# ---------- 경로 ----------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
KANRIPO_JSONL = PROJECT_ROOT / "data" / "intermediate" / "kanripo_parsed.jsonl"
ZHUZI_JSONL = PROJECT_ROOT / "data" / "intermediate" / "zhuzi_parsed.jsonl"

OUT_DIR = PROJECT_ROOT / "data" / "final"
SUMMARY_CSV = OUT_DIR / "alignment_summary.csv"
MISMATCH_CSV = OUT_DIR / "zhuzi_mismatch.csv"

# ---------- 설정 ----------

# 불일치 구간 앞뒤로 보여줄 맥락 길이
CONTEXT_LEN = 20

# CJK 한자 블록만 남기는 정규식
# - \u3400-\u4dbf : CJK Extension A
# - \u4e00-\u9fff : CJK Unified Ideographs (일반 한자)
# - \uf900-\ufaff : CJK Compatibility Ideographs
# - \U00020000-\U0002ffff : Extensions B~H (이체자 등)
HANZI_RE = re.compile(
    r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff\U00020000-\U0002ffff]"
)


# ---------- 유틸 ----------

def to_plain(text: str) -> str:
    """한자 외 모든 문자 제거 (표점, 공백, 영숫자 등)."""
    return "".join(HANZI_RE.findall(text))


def load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        print(f"[ERROR] 없음: {path}", file=sys.stderr)
        sys.exit(1)
    with path.open(encoding="utf-8") as f:
        return [json.loads(line) for line in f]


def context_slice(text: str, pos: int, after: bool = False) -> str:
    """pos 앞쪽(기본) 또는 뒤쪽(after=True)으로 CONTEXT_LEN만큼 자른 문자열."""
    if after:
        return text[pos:pos + CONTEXT_LEN]
    return text[max(0, pos - CONTEXT_LEN):pos]


# ---------- 정렬 ----------

def align_one(kanripo_text: str, zhuzi_plain: str) -> tuple[float, list[dict]]:
    """
    두 텍스트를 정렬하고 (ratio, mismatch 리스트) 반환.
    위치는 칸리포 원본 기준.
    autojunk=True: 200자 이상 반복 요소를 junk로 취급해 속도 개선.
    이 텍스트에서는 '理氣' 같은 단어가 반복되지만 긴 유일 시퀀스가 대부분이라 OK.
    """
    matcher = SequenceMatcher(None, kanripo_text, zhuzi_plain)
    ratio = matcher.ratio()
    mismatches: list[dict] = []

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            continue
        mismatches.append({
            "type": tag,                       # replace / delete / insert
            "kanripo_pos": f"{i1}-{i2}",
            "kanripo_text": kanripo_text[i1:i2],
            "zhuzi_text": zhuzi_plain[j1:j2],
            "context_before": context_slice(kanripo_text, i1),
            "context_after": context_slice(kanripo_text, i2, after=True),
        })

    return ratio, mismatches


# ---------- 메인 ----------

def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print("[LOAD] 02번, 03번 산출물 읽는 중...")
    kanripo = {r["juan_label"]: r for r in load_jsonl(KANRIPO_JSONL)}
    zhuzi = {r["juan_label"]: r for r in load_jsonl(ZHUZI_JSONL)}
    print(f"       칸리포 레코드: {len(kanripo)}")
    print(f"       祝平次 레코드: {len(zhuzi)}")

    # 두 소스 모두에 있는 juan_label만 처리. 한쪽에만 있는 건 경고.
    common = sorted(set(kanripo) & set(zhuzi),
                    key=lambda lbl: (kanripo[lbl]["juan_num"], lbl))
    only_k = set(kanripo) - set(zhuzi)
    only_z = set(zhuzi) - set(kanripo)
    if only_k:
        print(f"[WARN] 칸리포에만 있음: {sorted(only_k)}", file=sys.stderr)
    if only_z:
        print(f"[WARN] 祝平次에만 있음: {sorted(only_z)}", file=sys.stderr)

    print(f"\n[ALIGN] {len(common)}개 섹션 정렬 중...")

    summary_rows: list[dict] = []
    mismatch_rows: list[dict] = []

    for idx, label in enumerate(common, 1):
        k_text = kanripo[label]["raw_concat"]          # 이미 한자만 (표점 없음)
        z_raw = zhuzi[label]["raw_concat"]             # 표점 포함
        z_plain = to_plain(z_raw)                      # 한자만 남김

        # 추가 안전장치: 칸리포 쪽도 한자만 남기기
        # (이론상 이미 그렇지만 혹시 모를 노이즈 방어)
        k_plain = to_plain(k_text)

        ratio, mismatches = align_one(k_plain, z_plain)

        if idx % 20 == 0 or idx == len(common):
            print(f"  ... {idx}/{len(common)}  ({label}, ratio={ratio:.4f})")

        summary_rows.append({
            "juan_label": label,
            "juan_num": kanripo[label]["juan_num"],
            "kanripo_chars": len(k_plain),
            "zhuzi_plain_chars": len(z_plain),
            "zhuzi_with_punct_chars": len(z_raw),
            "ratio": f"{ratio:.4f}",
            "mismatch_count": len(mismatches),
        })

        for m in mismatches:
            mismatch_rows.append({"juan_label": label, **m})

    # CSV 저장 (권 번호 순으로 정렬)
    summary_rows.sort(key=lambda r: (r["juan_num"], r["juan_label"]))

    with SUMMARY_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(summary_rows[0].keys()))
        w.writeheader()
        w.writerows(summary_rows)

    with MISMATCH_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        fieldnames = ["juan_label", "type", "kanripo_pos",
                      "kanripo_text", "zhuzi_text",
                      "context_before", "context_after"]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(mismatch_rows)

    # 전체 지표
    total_k = sum(r["kanripo_chars"] for r in summary_rows)
    total_z = sum(r["zhuzi_plain_chars"] for r in summary_rows)
    total_mismatches = len(mismatch_rows)

    # 전체 ratio: 권별 평균 말고, 전권을 하나로 이어서 계산한 지표가 더 의미 있음
    # 권별 ratio의 가중 평균 (길이 기준)으로 근사
    weighted = sum(
        float(r["ratio"]) * (r["kanripo_chars"] + r["zhuzi_plain_chars"])
        for r in summary_rows
    )
    denom = sum(r["kanripo_chars"] + r["zhuzi_plain_chars"] for r in summary_rows)
    overall_ratio = weighted / denom if denom else 0.0

    print(f"\n[DONE]")
    print(f"  요약:   {SUMMARY_CSV}")
    print(f"  불일치: {MISMATCH_CSV}")
    print(f"\n  전체 일치율 (길이 가중 평균): {overall_ratio:.4%}")
    print(f"  칸리포 총 글자:  {total_k:,}")
    print(f"  祝平次 총 글자:  {total_z:,}  (백문, 표점 제거)")
    print(f"  불일치 레코드:   {total_mismatches:,}")

    # 최악·최상 권 몇 개
    sorted_by_ratio = sorted(summary_rows, key=lambda r: float(r["ratio"]))
    print(f"\n  일치율 낮은 권 Top 5:")
    for r in sorted_by_ratio[:5]:
        print(f"    {r['juan_label']:10s}  {r['ratio']}  (불일치 {r['mismatch_count']}건)")
    print(f"  일치율 높은 권 Top 3:")
    for r in sorted_by_ratio[-3:]:
        print(f"    {r['juan_label']:10s}  {r['ratio']}  (불일치 {r['mismatch_count']}건)")


if __name__ == "__main__":
    main()
