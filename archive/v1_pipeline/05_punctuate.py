"""
05_punctuate.py

칸리포 백문(02)에 표점을 부여한다. 祝平次(03)의 표점을 차용하되, 판본 차이가
큰 구간만 별도로 떼어내 hanja.dev (HERITAGE, Song et al. 2025)에 위임한다.

정렬 원리:
  - 권 단위로 칸리포(백문)와 祝平次(표점 제거본)를 difflib.SequenceMatcher로 문자
    단위 정렬 (04_align.py와 동일 로직).
  - get_opcodes() 결과를 다섯 경우로 분기 처리:

      경우 1: equal
          → 祝平次 표점 그대로 차용.
      경우 2: replace, len(k) == len(z)  (이체자 계열, 실측 99.71%)
          → 칸리포 글자 + 祝平次 표점 (위치 보존, 문자만 이체자로 치환됨)
      경우 3: replace(len 불일치) / delete(len > THRESHOLD)
          → fallback_spans에 기록. 출력엔 칸리포 원문자만 넣고 표점은 비움.
             추후 06 이전에 hanja.dev 결과로 채워넣는 단계 필요.
      경우 4: delete, len <= THRESHOLD
          → 칸리포 고유 단편 글자. 표점 없이 통과 (인접 equal span의 祝平次
             경계 표점이 이 구간을 감싸서 흡수함).
      경우 5: insert
          → 祝平次에만 있고 칸리포엔 없음. 출력에 포함하지 않음.

Provenance:
  각 출력 문자마다 1바이트 태그를 기록한다 (paragraphs_provenance).
    ' ' (공백)  : 한자 (출처 개념 없음)
    'Z'        : 祝平次 표점 차용 (경우 1, 2)
    '?'        : hanja.dev 예정 자리 (경우 3 구간의 한자는 ' ', 표점은 현재 없음)
  경우 3 구간은 별도 fallback_spans.csv로 관리한다.

입력:
  data/intermediate/kanripo_parsed.jsonl    (02)
  data/intermediate/zhuzi_parsed.jsonl      (03)

출력:
  data/intermediate/kanripo_punctuated.jsonl
  data/final/fallback_spans.csv             (hanja.dev 요청 목록)

사용법:
  python scripts/05_punctuate.py
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

OUT_JSONL = PROJECT_ROOT / "data" / "intermediate" / "kanripo_punctuated.jsonl"
FALLBACK_CSV = PROJECT_ROOT / "data" / "final" / "fallback_spans.csv"

# ---------- 설정 ----------

# delete 구간을 "짧은 결자(경우 4)"로 볼 최대 길이. 이 이하면 표점 없이 통과,
# 초과하면 경우 3으로 분류되어 hanja.dev 대상이 됨.
SMALL_DELETE_THRESHOLD = 3

# fallback span의 hanja.dev 요청 context로 붙일 앞뒤 글자 수
FALLBACK_CONTEXT = 50

# CJK 한자 블록 (04와 동일)
HANZI_RE = re.compile(
    r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff\U00020000-\U0002ffff]"
)


# ---------- 유틸 ----------

def load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        print(f"[ERROR] 없음: {path}", file=sys.stderr)
        sys.exit(1)
    with path.open(encoding="utf-8") as f:
        return [json.loads(line) for line in f]


def build_zhuzi_lookup(z_full_with_punct: str) -> tuple[str, list[str]]:
    """
    祝平次의 표점 포함 문자열에서 (z_plain, z_punct_after)를 만든다.

    Returns:
        z_plain: 표점 제거된 한자열 (길이 N)
        z_punct_after: z_plain[j] 뒤에 오는 표점 문자열 리스트 (길이 N)
                       z_punct_after[j]는 z_plain[j]와 z_plain[j+1] 사이의
                       비한자 문자들을 이어붙인 것 ("" 가능).

    참고:
        첫 한자 앞에 있는 표점은 무시한다 (실제로는 거의 없음).
        마지막 한자 뒤의 표점은 z_punct_after[N-1]에 포함된다.
    """
    z_plain_chars: list[str] = []
    z_punct_after: list[str] = []
    buf: list[str] = []

    for ch in z_full_with_punct:
        if HANZI_RE.match(ch):
            if z_plain_chars:
                # 직전 한자의 후속 표점으로 등록
                z_punct_after[-1] = "".join(buf)
                buf = []
            else:
                # 첫 한자 이전의 비한자는 버림
                buf = []
            z_plain_chars.append(ch)
            z_punct_after.append("")
        else:
            buf.append(ch)

    # 마지막 한자 뒤의 표점
    if z_plain_chars and buf:
        z_punct_after[-1] = "".join(buf)

    return "".join(z_plain_chars), z_punct_after


def punctuate_juan(
    k_full: str,
    z_full_with_punct: str,
    juan_label: str,
) -> tuple[str, str, list[int], list[dict]]:
    """
    한 권의 칸리포 백문에 祝平次 표점을 부여한다.

    Returns:
        out_text: 표점 부여된 문자열 (경우 3 구간은 표점 없이 한자만)
        out_prov: 각 문자 위치의 provenance 태그 (길이 == len(out_text))
        k_to_out: 원본 k_full의 문자 위치 i → out_text 내 위치 매핑
                  (k_to_out[len(k_full)] = len(out_text) 센티널 포함)
        fallback_spans: 경우 3 구간 레코드 리스트
    """
    z_plain, z_punct_after = build_zhuzi_lookup(z_full_with_punct)

    matcher = SequenceMatcher(a=k_full, b=z_plain, autojunk=False)
    opcodes = matcher.get_opcodes()

    out_text: list[str] = []
    out_prov: list[str] = []
    k_to_out: list[int] = [0] * (len(k_full) + 1)
    fallback_spans: list[dict] = []

    def emit_kchar_with_zhuzi_punct(k_idx: int, z_idx: int) -> None:
        """k_full[k_idx]를 출력하고 그 뒤에 z_plain[z_idx]의 후속 표점을 삽입."""
        k_to_out[k_idx] = len(out_text)
        out_text.append(k_full[k_idx])
        out_prov.append(" ")
        punct = z_punct_after[z_idx]
        for p in punct:
            out_text.append(p)
            out_prov.append("Z")

    def emit_kchar_no_punct(k_idx: int) -> None:
        k_to_out[k_idx] = len(out_text)
        out_text.append(k_full[k_idx])
        out_prov.append(" ")

    for tag, i1, i2, j1, j2 in opcodes:
        k_len = i2 - i1
        z_len = j2 - j1

        # ----- 경우 1: equal -----
        if tag == "equal":
            for k in range(i1, i2):
                j = j1 + (k - i1)
                emit_kchar_with_zhuzi_punct(k, j)

        # ----- 경우 2: 이체자 계열 (len 일치 replace) -----
        elif tag == "replace" and k_len == z_len:
            for k in range(i1, i2):
                j = j1 + (k - i1)
                emit_kchar_with_zhuzi_punct(k, j)

        # ----- 경우 3a: len 불일치 replace -----
        elif tag == "replace":  # and k_len != z_len 자동
            span_out_start = len(out_text)
            for k in range(i1, i2):
                emit_kchar_no_punct(k)
            fallback_spans.append({
                "juan_label": juan_label,
                "type": "replace_lendiff",
                "k_start": i1,
                "k_end": i2,
                "out_start": span_out_start,
                "out_end": len(out_text),
                "k_text": k_full[i1:i2],
                "z_text": z_plain[j1:j2],
            })

        # ----- delete 분기 -----
        elif tag == "delete":
            if k_len <= SMALL_DELETE_THRESHOLD:
                # 경우 4: 짧은 결자. 표점 없이 통과.
                for k in range(i1, i2):
                    emit_kchar_no_punct(k)
            else:
                # 경우 3b: 큰 덩어리 결자. hanja.dev 대상.
                span_out_start = len(out_text)
                for k in range(i1, i2):
                    emit_kchar_no_punct(k)
                fallback_spans.append({
                    "juan_label": juan_label,
                    "type": "delete_large",
                    "k_start": i1,
                    "k_end": i2,
                    "out_start": span_out_start,
                    "out_end": len(out_text),
                    "k_text": k_full[i1:i2],
                    "z_text": "",
                })

        # ----- 경우 5: insert (칸리포에 없는 祝平次 고유 글자) -----
        elif tag == "insert":
            # 출력에 반영 안 함. 단, 이 구간 직전까지의 z_punct_after는 이미
            # 이전 equal/replace 구간 처리에서 붙였으므로 여기서 할 일 없음.
            # (insert 구간 내부의 표점은 칸리포에 대응 글자가 없으므로 버림)
            pass

    # 센티널
    k_to_out[len(k_full)] = len(out_text)

    return "".join(out_text), "".join(out_prov), k_to_out, fallback_spans


def split_to_paragraphs(
    out_text: str,
    out_prov: str,
    k_to_out: list[int],
    k_paragraphs: list[str],
) -> tuple[list[str], list[str]]:
    """
    권 단위 punctuate 결과를 칸리포 원본 문단 경계에 맞춰 split.

    문단 끝 글자 뒤의 표점은 그 문단에 귀속된다 (다음 문단 첫 글자의
    k_to_out 값이 "그 표점 다음" 을 가리키기 때문).
    """
    k_boundaries = [0]
    pos = 0
    for p in k_paragraphs:
        pos += len(p)
        k_boundaries.append(pos)

    paragraphs_punct: list[str] = []
    paragraphs_prov: list[str] = []
    for i in range(len(k_paragraphs)):
        k_start = k_boundaries[i]
        k_end = k_boundaries[i + 1]
        out_start = k_to_out[k_start]
        out_end = k_to_out[k_end]
        paragraphs_punct.append(out_text[out_start:out_end])
        paragraphs_prov.append(out_prov[out_start:out_end])

    return paragraphs_punct, paragraphs_prov


def extract_fallback_context(
    out_text: str,
    span: dict,
    context_len: int = FALLBACK_CONTEXT,
) -> tuple[str, str, str]:
    """
    fallback span 주변의 out_text 기준 앞뒤 context 추출.
    hanja.dev 요청 시 context 포함 전송, 결과에서 context 제외하고 중앙만 취할 때 사용.

    반환: (context_before, span_text, context_after)
    """
    s, e = span["out_start"], span["out_end"]
    before = out_text[max(0, s - context_len):s]
    span_text = out_text[s:e]
    after = out_text[e:e + context_len]
    return before, span_text, after


# ---------- 메인 ----------

def main() -> None:
    kanripo = load_jsonl(KANRIPO_JSONL)
    zhuzi = load_jsonl(ZHUZI_JSONL)

    # juan_label 키 매칭
    z_by_label = {r["juan_label"]: r for r in zhuzi}

    OUT_JSONL.parent.mkdir(parents=True, exist_ok=True)
    FALLBACK_CSV.parent.mkdir(parents=True, exist_ok=True)

    out_records: list[dict] = []
    all_fallbacks: list[dict] = []

    # 통계
    total_paras = 0
    total_out_chars = 0
    total_punct_Z = 0
    total_punct_unresolved = 0  # 경우 3 구간 안에서 아직 표점 없이 비워진 위치는
                                # 현재 0 (한자만 들어있으므로). 추후 hanja.dev 채워지면 증가.

    print(f"[PUNCT] 권 {len(kanripo)}개 처리 중...")

    for idx, krec in enumerate(kanripo, 1):
        label = krec["juan_label"]
        zrec = z_by_label.get(label)
        if zrec is None:
            print(f"  [WARN] 祝平次에 {label} 없음 — 표점 없이 pass-through",
                  file=sys.stderr)
            out_records.append({
                "juan_num": krec["juan_num"],
                "juan_label": label,
                "file_name": krec.get("file_name", ""),
                "headings": krec.get("headings", []),
                "paragraphs_plain": krec["paragraphs"],
                "paragraphs_punctuated": list(krec["paragraphs"]),  # 백문 그대로
                "paragraphs_provenance": [" " * len(p) for p in krec["paragraphs"]],
                "paragraphs_byline": krec.get("paragraphs_byline", [None] * len(krec["paragraphs"])),
                "fallback_span_count": 0,
                "zhuzi_matched": False,
            })
            continue

        k_full = "".join(krec["paragraphs"])
        z_full = "".join(zrec["paragraphs"])

        out_text, out_prov, k_to_out, fallbacks = punctuate_juan(
            k_full, z_full, juan_label=label
        )

        paras_punct, paras_prov = split_to_paragraphs(
            out_text, out_prov, k_to_out, krec["paragraphs"]
        )

        # 통계
        total_paras += len(paras_punct)
        total_out_chars += len(out_text)
        total_punct_Z += out_prov.count("Z")

        # fallback span에 context 첨부
        for sp in fallbacks:
            before, span_text, after = extract_fallback_context(out_text, sp)
            sp["context_before"] = before
            sp["context_after"] = after
        all_fallbacks.extend(fallbacks)

        out_records.append({
            "juan_num": krec["juan_num"],
            "juan_label": label,
            "file_name": krec.get("file_name", ""),
            "headings": krec.get("headings", []),
            "paragraphs_plain": krec["paragraphs"],
            "paragraphs_punctuated": paras_punct,
            "paragraphs_provenance": paras_prov,
            "paragraphs_byline": krec.get("paragraphs_byline", [None] * len(krec["paragraphs"])),
            "fallback_span_count": len(fallbacks),
            "zhuzi_matched": True,
        })

        if idx % 20 == 0 or idx == len(kanripo):
            print(f"  ... {idx}/{len(kanripo)}  ({label}, "
                  f"out {len(out_text):,}자, Z표점 {out_prov.count('Z'):,}, "
                  f"fallback {len(fallbacks)}건)")

    # ---- JSONL 저장 ----
    with OUT_JSONL.open("w", encoding="utf-8") as f:
        for r in out_records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    # ---- fallback CSV 저장 ----
    fieldnames = [
        "juan_label", "type", "k_start", "k_end",
        "k_text", "z_text", "context_before", "context_after",
        "out_start", "out_end",
    ]
    with FALLBACK_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for sp in all_fallbacks:
            w.writerow({k: sp.get(k, "") for k in fieldnames})

    # ---- 요약 ----
    fallback_by_type = {}
    fallback_chars = 0
    for sp in all_fallbacks:
        fallback_by_type[sp["type"]] = fallback_by_type.get(sp["type"], 0) + 1
        fallback_chars += sp["k_end"] - sp["k_start"]

    print()
    print(f"[DONE] JSONL:   {OUT_JSONL}")
    print(f"       CSV:     {FALLBACK_CSV}")
    print(f"       레코드:  {len(out_records)} 권")
    print(f"       문단:    {total_paras:,}")
    print(f"       출력 글자: {total_out_chars:,} (한자 + 표점)")
    print(f"       祝平次 표점: {total_punct_Z:,}")
    print(f"       fallback: {len(all_fallbacks)}건, {fallback_chars:,}자")
    for t, n in sorted(fallback_by_type.items()):
        print(f"           {t}: {n}건")
    print()
    print(f"       ※ fallback 구간은 현재 칸리포 원문자만 들어있고 표점이 비어 있음.")
    print(f"         hanja.dev 처리 단계 (05b_fill_fallback.py) 필요.")


if __name__ == "__main__":
    main()
