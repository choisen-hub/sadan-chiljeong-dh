"""
04_align.py

칸리포 원본(02)과 祝平次 편집본(03)을 권 단위로 정렬·비교한다.

두 일치율을 병기한다:
  - strict_ratio  : 이체자(𫝊↔傳 등)도 불일치로 카운트. 순수 문자 일치율.
  - relaxed_ratio : 이체자(길이 일치 replace)를 일치로 재분류한 "판본 일치율".
                    실질 판본 차이만 불일치로 본다. 논문 보고용 주 지표.

불일치를 두 종류로 분리 저장:
  - variant       : 길이 일치 replace (이체자 치환, 판본학적 부록 자료)
  - substantive   : 나머지 (길이 불일치 replace, delete, insert — 실질 차이)

입력:
  data/intermediate/kanripo_parsed.jsonl   (02번)
  data/intermediate/zhuzi_parsed.jsonl     (03번)

출력 (data/final/):
  alignment_summary.csv              권별 요약 (strict + relaxed)
  zhuzi_mismatch.csv                 불일치 전체 (기존 유지, is_variant 플래그 추가)
  zhuzi_mismatch_variant.csv         이체자 치환만 (판본학 부록)
  zhuzi_mismatch_substantive.csv     실질 불일치 (05의 hanja.dev 대상 후보)

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
VARIANT_CSV = OUT_DIR / "zhuzi_mismatch_variant.csv"
SUBSTANTIVE_CSV = OUT_DIR / "zhuzi_mismatch_substantive.csv"

# ---------- 설정 ----------

CONTEXT_LEN = 20

# CJK 한자 블록 (이체자 포함)
HANZI_RE = re.compile(
    r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff\U00020000-\U0002ffff]"
)


# ---------- 유틸 ----------

def to_plain(text: str) -> str:
    return "".join(HANZI_RE.findall(text))


def load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        print(f"[ERROR] 없음: {path}", file=sys.stderr)
        sys.exit(1)
    with path.open(encoding="utf-8") as f:
        return [json.loads(line) for line in f]


def context_slice(text: str, pos: int, after: bool = False) -> str:
    if after:
        return text[pos:pos + CONTEXT_LEN]
    return text[max(0, pos - CONTEXT_LEN):pos]


# ---------- 정렬 ----------

def align_one(
    kanripo_text: str, zhuzi_plain: str
) -> tuple[float, float, list[dict]]:
    """
    두 텍스트를 정렬하고 (strict_ratio, relaxed_ratio, 불일치 리스트) 반환.

    불일치 각 항목에 is_variant (이체자 치환 여부) 플래그를 붙인다.
      - is_variant = True : replace 이면서 len(k) == len(z) (이체자 1:1 치환)
      - is_variant = False: 나머지 (실질 차이)

    relaxed_ratio는 이체자를 equal로 재분류한 후 SequenceMatcher.ratio()와
    동일 공식(2*M/T)으로 재계산한다.
    """
    matcher = SequenceMatcher(None, kanripo_text, zhuzi_plain)
    strict_ratio = matcher.ratio()

    mismatches: list[dict] = []
    equal_chars = 0      # equal span의 k 글자 수 (이게 matcher의 matches와 동일)
    variant_chars = 0    # 이체자 span의 k 글자 수 (len 일치라 z도 같음)

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            equal_chars += i2 - i1
            continue

        is_variant = (tag == "replace" and (i2 - i1) == (j2 - j1))
        if is_variant:
            variant_chars += i2 - i1

        mismatches.append({
            "type": tag,
            "kanripo_pos": f"{i1}-{i2}",
            "kanripo_text": kanripo_text[i1:i2],
            "zhuzi_text": zhuzi_plain[j1:j2],
            "context_before": context_slice(kanripo_text, i1),
            "context_after": context_slice(kanripo_text, i2, after=True),
            "is_variant": is_variant,
        })

    # relaxed ratio: 이체자를 equal로 취급. 이체자 span은 len(k)==len(z)이므로
    # matches에 2*variant_chars가 추가되는 것과 같은 효과.
    total = len(kanripo_text) + len(zhuzi_plain)
    relaxed_match = equal_chars + variant_chars
    relaxed_ratio = (2.0 * relaxed_match / total) if total else 0.0

    return strict_ratio, relaxed_ratio, mismatches


# ---------- 메인 ----------

def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print("[LOAD] 02번, 03번 산출물 읽는 중...")
    kanripo = {r["juan_label"]: r for r in load_jsonl(KANRIPO_JSONL)}
    zhuzi = {r["juan_label"]: r for r in load_jsonl(ZHUZI_JSONL)}
    print(f"       칸리포 레코드: {len(kanripo)}")
    print(f"       祝平次 레코드: {len(zhuzi)}")

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
    all_mismatches: list[dict] = []
    variant_rows: list[dict] = []
    substantive_rows: list[dict] = []

    for idx, label in enumerate(common, 1):
        k_raw = kanripo[label]["raw_concat"]
        z_raw = zhuzi[label]["raw_concat"]
        k_plain = to_plain(k_raw)
        z_plain = to_plain(z_raw)

        strict_r, relaxed_r, mismatches = align_one(k_plain, z_plain)

        if idx % 20 == 0 or idx == len(common):
            print(f"  ... {idx}/{len(common)}  ({label}, "
                  f"strict={strict_r:.4f}, relaxed={relaxed_r:.4f})")

        variant_count = sum(1 for m in mismatches if m["is_variant"])
        substantive_count = len(mismatches) - variant_count

        summary_rows.append({
            "juan_label": label,
            "juan_num": kanripo[label]["juan_num"],
            "kanripo_chars": len(k_plain),
            "zhuzi_plain_chars": len(z_plain),
            "zhuzi_with_punct_chars": len(z_raw),
            "strict_ratio": f"{strict_r:.4f}",
            "relaxed_ratio": f"{relaxed_r:.4f}",
            "mismatch_count": len(mismatches),
            "variant_count": variant_count,
            "substantive_count": substantive_count,
        })

        for m in mismatches:
            row = {"juan_label": label, **m}
            all_mismatches.append(row)
            if m["is_variant"]:
                variant_rows.append(row)
            else:
                substantive_rows.append(row)

    summary_rows.sort(key=lambda r: (r["juan_num"], r["juan_label"]))

    # ---- CSV 저장 ----
    with SUMMARY_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(summary_rows[0].keys()))
        w.writeheader()
        w.writerows(summary_rows)

    mismatch_fields = [
        "juan_label", "type", "kanripo_pos",
        "kanripo_text", "zhuzi_text",
        "context_before", "context_after", "is_variant",
    ]

    def write_mismatches(path: Path, rows: list[dict]) -> None:
        with path.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=mismatch_fields)
            w.writeheader()
            for r in rows:
                w.writerow({k: r.get(k, "") for k in mismatch_fields})

    write_mismatches(MISMATCH_CSV, all_mismatches)
    write_mismatches(VARIANT_CSV, variant_rows)
    write_mismatches(SUBSTANTIVE_CSV, substantive_rows)

    # ---- 전체 지표 ----
    total_k = sum(r["kanripo_chars"] for r in summary_rows)
    total_z = sum(r["zhuzi_plain_chars"] for r in summary_rows)

    def weighted_avg(key: str) -> float:
        num = sum(
            float(r[key]) * (r["kanripo_chars"] + r["zhuzi_plain_chars"])
            for r in summary_rows
        )
        denom = sum(r["kanripo_chars"] + r["zhuzi_plain_chars"]
                    for r in summary_rows)
        return num / denom if denom else 0.0

    overall_strict = weighted_avg("strict_ratio")
    overall_relaxed = weighted_avg("relaxed_ratio")

    print(f"\n[DONE]")
    print(f"  요약:       {SUMMARY_CSV}")
    print(f"  불일치 전체: {MISMATCH_CSV}")
    print(f"  이체자만:   {VARIANT_CSV}       ({len(variant_rows):,}건)")
    print(f"  실질 불일치: {SUBSTANTIVE_CSV}   ({len(substantive_rows):,}건)")
    print()
    print(f"  칸리포 총 글자:  {total_k:,}")
    print(f"  祝平次 총 글자:  {total_z:,}  (백문, 표점 제거)")
    print()
    print(f"  전체 strict 일치율  (길이 가중):  {overall_strict:.4%}")
    print(f"  전체 relaxed 일치율 (이체자 제외): {overall_relaxed:.4%}")
    print(f"  불일치 전체:     {len(all_mismatches):,}건")
    print(f"    ├ 이체자:     {len(variant_rows):,}건  "
          f"({len(variant_rows) / len(all_mismatches) * 100:.2f}%)")
    print(f"    └ 실질 불일치: {len(substantive_rows):,}건  "
          f"({len(substantive_rows) / len(all_mismatches) * 100:.2f}%)")

    # 실질 불일치 type 분포
    from collections import Counter
    sub_types = Counter(r["type"] for r in substantive_rows)
    print(f"\n  실질 불일치 type 분포:")
    for t, n in sub_types.most_common():
        print(f"    {t:10s}  {n:,}건")

    # 최악·최상 권
    sorted_by_relaxed = sorted(summary_rows,
                                key=lambda r: float(r["relaxed_ratio"]))
    print(f"\n  relaxed 일치율 낮은 권 Top 5:")
    for r in sorted_by_relaxed[:5]:
        print(f"    {r['juan_label']:10s}  strict={r['strict_ratio']}  "
              f"relaxed={r['relaxed_ratio']}  "
              f"(실질 불일치 {r['substantive_count']}건)")


if __name__ == "__main__":
    main()
