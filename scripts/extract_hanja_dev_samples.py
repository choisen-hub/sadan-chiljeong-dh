"""
extract_hanja_dev_samples.py

hanja.dev 검증용 샘플을 로컬 parsed jsonl에서 추출.

각 샘플은 권 내 긴 equal span (祝平次와 칸리포가 완전히 일치하는 구간)에서
뽑혔다. 칸리포 백문은 hanja.dev 입력으로, 祝平次 표점본은 "정답"으로 사용한다.

입력:
  data/intermediate/kanripo_parsed.jsonl
  data/intermediate/zhuzi_parsed.jsonl

출력:
  data/eval/samples_for_hanja_dev.txt     hanja.dev 웹 UI 입력용 (칸리포 백문)
  data/eval/samples_with_zhuzi_punct.txt  祝平次 표점 정답
  data/eval/samples_meta.json             샘플 메타 (권, 위치, 길이)

사용법:
  python scripts/extract_hanja_dev_samples.py

그 다음 절차:
  1. samples_for_hanja_dev.txt 를 열어서 각 샘플(### 구분)을 hanja.dev/punc 에
     하나씩 복사·붙여넣기.
  2. hanja.dev 결과를 samples_hanja_dev_output.txt 로 저장.
  3. 별도 비교 스크립트 (compare_hanja_dev.py, 추후 제공)로 祝平次와 일치율 측정.
"""

from __future__ import annotations

import json
import re
import sys
from difflib import SequenceMatcher
from pathlib import Path

# ---------- 경로 ----------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
KANRIPO_JSONL = PROJECT_ROOT / "data" / "intermediate" / "kanripo_parsed.jsonl"
ZHUZI_JSONL = PROJECT_ROOT / "data" / "intermediate" / "zhuzi_parsed.jsonl"

OUT_DIR = PROJECT_ROOT / "data" / "eval"
OUT_KANRIPO = OUT_DIR / "samples_for_hanja_dev.txt"
OUT_ZHUZI = OUT_DIR / "samples_with_zhuzi_punct.txt"
OUT_META = OUT_DIR / "samples_meta.json"

# 샘플 앵커 (04_align 결과의 zhuzi_mismatch.csv 에서 긴 equal span 5개 선정)
# i1, i2는 칸리포 raw_concat 기준 (hanzi-only이므로 k_plain과 동일)
ANCHORS = [
    {
        "name": "sample1",
        "juan": "卷一百一",
        "i1": 16432, "i2": 17310,
        "topic": "性情論 (張載 논평)",
    },
    {
        "name": "sample2",
        "juan": "卷十六",
        "i1": 13142, "i2": 13997,
        "topic": "中庸 慎獨",
    },
    {
        "name": "sample3",
        "juan": "卷九十九",
        "i1": 1742, "i2": 2578,
        "topic": "陰陽 天氣地氣",
    },
    {
        "name": "sample4",
        "juan": "卷四十三",
        "i1": 1590, "i2": 2357,
        "topic": "論語·春秋",
    },
    {
        "name": "sample5",
        "juan": "卷九十八",
        "i1": 7811, "i2": 8543,
        "topic": "中庸 父母之命",
    },
]

HANZI_RE = re.compile(
    r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff\U00020000-\U0002ffff]"
)


def to_plain(s: str) -> str:
    return "".join(HANZI_RE.findall(s))


def load_jsonl(path: Path) -> dict[str, dict]:
    if not path.exists():
        print(f"[ERROR] 없음: {path}", file=sys.stderr)
        sys.exit(1)
    result = {}
    with path.open(encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            result[r["juan_label"]] = r
    return result


def find_zhuzi_span(
    k_plain: str, z_plain: str, z_raw: str, i1: int, i2: int
) -> tuple[int, int, str, str]:
    """
    칸리포 plain 의 [i1, i2] 구간에 대응하는 祝平次 부분을 찾는다.

    Returns:
        (j1, j2, z_plain_slice, z_raw_slice)
        - j1, j2: z_plain 기준 대응 위치
        - z_plain_slice: 한자만
        - z_raw_slice: 표점 포함
    """
    # 1) 권 전체 align으로 i1~i2가 equal 범위 안에 온전히 들어가는지 확인
    matcher = SequenceMatcher(None, k_plain, z_plain)
    opcodes = matcher.get_opcodes()

    j1 = None
    j2 = None
    for tag, a1, a2, b1, b2 in opcodes:
        if tag != "equal":
            continue
        # i1이 이 equal 구간 안이면 대응 j1 계산
        if a1 <= i1 < a2 and j1 is None:
            j1 = b1 + (i1 - a1)
        if a1 < i2 <= a2 and j2 is None:
            j2 = b1 + (i2 - a1)
        if j1 is not None and j2 is not None:
            break

    if j1 is None or j2 is None:
        # fallback: 스니펫 문자열 검색
        head = k_plain[i1:i1 + 10]
        tail = k_plain[i2 - 10:i2]
        j1 = z_plain.find(head)
        tail_pos = z_plain.find(tail)
        if j1 == -1 or tail_pos == -1:
            raise RuntimeError(f"샘플 위치 매핑 실패 (i1={i1}, i2={i2})")
        j2 = tail_pos + 10

    z_plain_slice = z_plain[j1:j2]

    # 2) z_plain의 j1:j2 → z_raw 내 대응 범위
    hanzi_count = 0
    start_raw = None
    end_raw = None
    for idx, ch in enumerate(z_raw):
        if HANZI_RE.match(ch):
            if hanzi_count == j1 and start_raw is None:
                start_raw = idx
            hanzi_count += 1
            if hanzi_count == j2:
                end_raw = idx + 1
                break

    if start_raw is None or end_raw is None:
        raise RuntimeError(f"z_raw 매핑 실패 (j1={j1}, j2={j2})")

    z_raw_slice = z_raw[start_raw:end_raw]
    return j1, j2, z_plain_slice, z_raw_slice


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print("[LOAD] parsed jsonl 읽는 중...")
    kdata = load_jsonl(KANRIPO_JSONL)
    zdata = load_jsonl(ZHUZI_JSONL)

    k_blocks: list[str] = []
    z_blocks: list[str] = []
    meta: list[dict] = []

    for anc in ANCHORS:
        label = anc["juan"]
        if label not in kdata or label not in zdata:
            print(f"[SKIP] {label} 없음", file=sys.stderr)
            continue

        k_raw = kdata[label]["raw_concat"]
        z_raw = zdata[label]["raw_concat"]
        k_plain = to_plain(k_raw)
        z_plain = to_plain(z_raw)

        i1, i2 = anc["i1"], anc["i2"]
        if i2 > len(k_plain):
            print(f"[SKIP] {anc['name']}: i2({i2}) > len(k_plain)({len(k_plain)})",
                  file=sys.stderr)
            continue

        k_slice = k_plain[i1:i2]

        try:
            j1, j2, z_plain_slice, z_raw_slice = find_zhuzi_span(
                k_plain, z_plain, z_raw, i1, i2
            )
        except RuntimeError as e:
            print(f"[SKIP] {anc['name']}: {e}", file=sys.stderr)
            continue

        # 안전장치: 길이 일치 확인
        if len(k_slice) != len(z_plain_slice):
            print(f"[WARN] {anc['name']}: 길이 불일치 "
                  f"(k={len(k_slice)}, z_plain={len(z_plain_slice)}). "
                  f"equal span 가정 깨짐 — 그래도 계속")

        header_k = (f"### {anc['name']} — {label} ({anc['topic']}), "
                    f"칸리포 i1={i1} i2={i2}, {len(k_slice)}자\n")
        header_z = (f"### {anc['name']} — {label} ({anc['topic']}), "
                    f"祝平次 j1={j1} j2={j2}, 백문 {len(z_plain_slice)}자 "
                    f"/ 표점포함 {len(z_raw_slice)}자\n")

        k_blocks.append(header_k + k_slice + "\n")
        z_blocks.append(header_z + z_raw_slice + "\n")

        meta.append({
            "name": anc["name"],
            "juan": label,
            "topic": anc["topic"],
            "kanripo_i1": i1,
            "kanripo_i2": i2,
            "zhuzi_j1": j1,
            "zhuzi_j2": j2,
            "kanripo_plain_len": len(k_slice),
            "zhuzi_plain_len": len(z_plain_slice),
            "zhuzi_raw_len": len(z_raw_slice),
            "length_equal": len(k_slice) == len(z_plain_slice),
        })

        print(f"  [OK] {anc['name']}: {label}, k={len(k_slice)}자, "
              f"z_raw={len(z_raw_slice)}자")

    OUT_KANRIPO.write_text("\n".join(k_blocks), encoding="utf-8")
    OUT_ZHUZI.write_text("\n".join(z_blocks), encoding="utf-8")
    OUT_META.write_text(json.dumps(meta, ensure_ascii=False, indent=2),
                        encoding="utf-8")

    print()
    print(f"[DONE]")
    print(f"  {OUT_KANRIPO} — hanja.dev 입력용 (백문)")
    print(f"  {OUT_ZHUZI} — 祝平次 표점 정답")
    print(f"  {OUT_META} — 샘플 메타")
    print()
    print(f"다음 절차:")
    print(f"  1. {OUT_KANRIPO.name} 의 각 샘플을 hanja.dev/punc 에 넣기")
    print(f"  2. 결과를 {OUT_DIR}/samples_hanja_dev_output.txt 로 저장")
    print(f"     (각 샘플을 '### sampleN' 헤더로 구분)")
    print(f"  3. Claude에게 비교 스크립트 요청 → 祝平次와의 일치율 측정")


if __name__ == "__main__":
    main()
