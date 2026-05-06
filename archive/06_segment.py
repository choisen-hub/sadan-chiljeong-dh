"""
06_segment.py

표점 부여된 주자어류(05 산출물)를 sentence 단위로 분절한다.

분절 단위 (3-level):
  - 권 (juan)              : 100여 개
  - 발화 (utterance)        : paragraph_idx (사용자 결정으로 paragraph = utterance)
  - 문장 (sentence)         : 본 단계의 출력 단위

문장 분절 표점 (사용자 결정):
  「。？！；」  — sentence break
  「，」「、」「：」 등은 절(clause) 분리이므로 break 아님 (sentence 안에 유지)
  「」『』《》 인용부호는 sentence 경계 아님

전처리:
  본문에 잔존 가능성이 있는 byline-like 패턴 「(X/Y)」 (8자 이하 + slash 포함)을
  정규식으로 한 번 더 제거. (02/03이 처리했어야 하지만 누락 케이스 대비 안전망)

입력:
  data/intermediate/kanripo_punctuated.jsonl   (05)

출력:
  data/intermediate/zhuzi_sentences.jsonl

스키마 (한 줄에 한 sentence):
  {
    "sentence_id":      "S000001",                # 전역 순번 (분석 식별자)
    "juan_num":         1,
    "juan_label":       "卷一",
    "headings":         [...],                    # 권/편/절 헤더
    "paragraph_idx":    1,                        # 권 안에서의 paragraph 번호
    "sentence_idx":     3,                        # paragraph 안에서의 sentence 번호
    "utterance_id":     "卷一#p001",              # 같은 발화 묶기용 키
    "text_punctuated":  "問太極…？",              # 표점 포함 본문
    "text_plain":       "問太極…",                # 한자만 (표점 제거)
    "char_count":       25,                       # 한자 글자 수
    "byline":           {"raw": "(僴/)"} | null   # utterance level 메타
  }

사용법:
  python3 scripts/06_segment.py
"""

from __future__ import annotations

import json
import re
import sys
from collections import Counter
from pathlib import Path

# ---------- 경로 ----------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT = PROJECT_ROOT / "data" / "intermediate" / "kanripo_punctuated.jsonl"
OUT = PROJECT_ROOT / "data" / "intermediate" / "zhuzi_sentences.jsonl"

# ---------- 설정 ----------
SENT_BREAKS = set("。？！；")

HANZI_RE = re.compile(
    r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff\U00020000-\U0002ffff]"
)

# 본문 잔존 byline-like 패턴 (안전망: 02/03이 못 잡은 거 추가 정제)
RE_INLINE_BYLINE = re.compile(r"\(([^()]{1,8})\)")


# ---------- 유틸 ----------
def is_hanzi(ch: str) -> bool:
    return bool(HANZI_RE.match(ch))


def hanzi_only(text: str) -> str:
    return "".join(ch for ch in text if is_hanzi(ch))


def cleanup_residual_byline(text: str) -> str:
    """본문에 (X/Y) 패턴 (8자 이하 + slash) 잔존 시 제거."""
    def _replace(m: re.Match) -> str:
        return "" if "/" in m.group(1) else m.group(0)
    return RE_INLINE_BYLINE.sub(_replace, text)


def split_sentences(text: str) -> list[str]:
    """SENT_BREAKS 만나면 sentence 종결. 표점은 sentence 끝에 포함."""
    out: list[str] = []
    cur: list[str] = []
    for ch in text:
        cur.append(ch)
        if ch in SENT_BREAKS:
            s = "".join(cur).strip()
            if s:
                out.append(s)
            cur = []
    s = "".join(cur).strip()
    if s:
        out.append(s)
    return out


# ---------- 메인 ----------
def main() -> None:
    if not INPUT.exists():
        print(f"[ERROR] 입력 없음: {INPUT}", file=sys.stderr)
        print("        먼저 05_punctuate.py 실행 필요", file=sys.stderr)
        sys.exit(1)

    records = [json.loads(l) for l in INPUT.open(encoding="utf-8")]
    print(f"[LOAD] {len(records)} 권")

    sentences: list[dict] = []
    skipped_empty = 0
    sent_seq = 0

    for rec in records:
        juan_num = rec["juan_num"]
        juan_label = rec["juan_label"]
        headings = rec.get("headings", [])
        paragraphs = rec["paragraphs_punctuated"]
        bylines = rec.get("paragraphs_byline", [None] * len(paragraphs))

        for p_idx, p_text in enumerate(paragraphs, start=1):
            byline = bylines[p_idx - 1] if p_idx - 1 < len(bylines) else None
            # 안전망: paragraph 본문에 byline 잔존 시 추가 정제
            cleaned_text = cleanup_residual_byline(p_text)
            sents = split_sentences(cleaned_text)
            sent_idx = 0
            for s in sents:
                plain = hanzi_only(s)
                if not plain:
                    skipped_empty += 1
                    continue
                sent_idx += 1
                sent_seq += 1
                sentences.append({
                    "sentence_id": f"S{sent_seq:06d}",
                    "juan_num": juan_num,
                    "juan_label": juan_label,
                    "headings": headings,
                    "paragraph_idx": p_idx,
                    "sentence_idx": sent_idx,
                    "utterance_id": f"{juan_label}#p{p_idx:03d}",
                    "text_punctuated": s,
                    "text_plain": plain,
                    "char_count": len(plain),
                    "byline": byline,
                })

    # ---- 저장 ----
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", encoding="utf-8") as f:
        for s in sentences:
            f.write(json.dumps(s, ensure_ascii=False) + "\n")

    # ---- 통계 ----
    lens = [s["char_count"] for s in sentences]
    sorted_lens = sorted(lens)

    print(f"\n[DONE] 저장: {OUT}")
    print(f"       sentence 수:        {len(sentences):,}")
    print(f"       skip된 빈 sentence: {skipped_empty:,}")
    print(f"       평균 길이:          {sum(lens) / max(1, len(lens)):.1f}자")
    print(f"       중위 길이:          {sorted_lens[len(sorted_lens) // 2]}자")
    print(f"       최댓값:             {max(lens)}자")
    print(f"       최솟값:             {min(lens)}자")

    # 길이 분포
    buckets = Counter()
    for L in lens:
        if L <= 3:
            buckets["1-3자"] += 1
        elif L <= 10:
            buckets["4-10자"] += 1
        elif L <= 30:
            buckets["11-30자"] += 1
        elif L <= 60:
            buckets["31-60자"] += 1
        elif L <= 100:
            buckets["61-100자"] += 1
        else:
            buckets["101자+"] += 1

    print(f"\n       길이 분포:")
    for k in ["1-3자", "4-10자", "11-30자", "31-60자", "61-100자", "101자+"]:
        n = buckets[k]
        bar = "█" * int(n / max(1, len(lens)) * 50)
        print(f"         {k:>10s}: {n:>6,}건  ({n / max(1, len(lens)) * 100:5.1f}%) {bar}")

    # 권별 sentence 수 top 10
    juan_cnt = Counter(s["juan_label"] for s in sentences)
    print(f"\n       권별 sentence 수 (Top 10):")
    for lbl, n in juan_cnt.most_common(10):
        print(f"         {lbl:>10s}: {n:>6,}")


if __name__ == "__main__":
    main()
