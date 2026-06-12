#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
12_segment_letters.py — Segment 한문 letters into sentences using 표점.

Step 12 of the Phase 0 data preprocessing pipeline.
Reads the output of 11_crawl_itkc.py (data/raw/letters.jsonl) and
segments each letter's raw_text into sentence-level units, following
김서윤(2024)의 표점 기준 분절 방식.


SEGMENTATION RULES
==================

한국문집총간本 표점체계 분석 (실제 본문 카운트):
  。 (U+3002 句點)  : 7778 회  ← 사실상 단일 분절자
  ， (U+FF0C 讀點)  :   37 회  ← 매우 드뭄 (분절자 아님)
  \\n              :  268 회  ← 단락 boundary (11_crawl이 \\n\\n로 삽입)

따라서 분절 규칙은 매우 단순:

  Rule 1.  단락 boundary (\\n\\n) 는 sentence를 가르는 strong boundary.
  Rule 2.  단락 내에서는 句點 `。`를 만나면 sentence 종료.
  Rule 3.  句點 자체는 sentence text에 포함시킨다 (raw 보존).
  Rule 4.  問號 `？`, 嘆號 `！` 도 만일 등장하면 sentence 종료자로 처리
           (현재 corpus에는 없음).

Sentence 안에 등장하는 讀點 `，` 는 그대로 유지한다 (분절자가 아님).

분절 후 빈 sentence (단순 화이트스페이스 또는 표점만 있는 것)는 제거한다.


OUTPUTS
=======

  data/processed/sentences.jsonl     한 줄당 한 sentence
  data/processed/sentences.csv       CSV (수동 검토 / Excel 호환)

각 sentence row schema:
  sentence_id          ITKC_MO_xxxA_xxxx_xxx_xxxx_p<para>_s<sent>
  letter_data_id       원본 letter id (11_crawl_itkc.py output과 join key)
  munjip               toegye / yulgok
  kwon                 권 number
  letter_seq           letter 순서 within 권
  letter_title         letter 제목
  letter_year          letter inferred_year (干支 기반)
  sender_label         T / Y
  target_name          11번 config의 target name
  para_idx             단락 index within letter (1-indexed)
  sent_idx_in_para     sentence index within 단락 (1-indexed)
  sent_idx_in_letter   sentence index within letter (1-indexed)
  sent_text            표점 포함 sentence text
  sent_text_plain      백문 (표점 제거)
  char_count_raw       sent_text 글자 수
  char_count_plain     sent_text_plain 글자 수


USAGE
=====

  python3 scripts/12_segment_letters.py

  # CSV 만 출력
  python3 scripts/12_segment_letters.py --no-jsonl

  # 분절 결과 미리보기 (파일 안 만듦)
  python3 scripts/12_segment_letters.py --dry-run | head -30


DEPENDENCIES
============

  Python ≥ 3.9 표준 라이브러리만 (csv, json, re).
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_REPO_ROOT = Path(__file__).resolve().parent.parent

# Sentence-terminating characters
SENT_TERMINATORS = "。？！"

# Punctuation pattern for stripping (백문 생성 시)
_PUNCT_PATTERN = re.compile(
    r"[，。、；：？！「」『』《》〈〉（）()\[\]【】〔〕,.;:!?\"'’‘“”—\-·…\s]+"
)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Sentence:
    sentence_id: str
    letter_data_id: str
    munjip: str
    kwon: int
    letter_seq: int
    letter_title: str
    letter_year: int | None
    sender_label: str
    target_name: str
    para_idx: int
    sent_idx_in_para: int
    sent_idx_in_letter: int
    sent_text: str
    sent_text_plain: str
    char_count_raw: int
    char_count_plain: int


# ---------------------------------------------------------------------------
# Segmentation
# ---------------------------------------------------------------------------

def _strip_punct(text: str) -> str:
    """표점/공백 제거 → 백문."""
    return _PUNCT_PATTERN.sub("", text)


def _split_paragraph(para_text: str) -> list[str]:
    """단락 한 개를 sentence 리스트로 분할.

    句點(。問號?嘆號!)을 만나면 sentence 종료. 종결자는 sentence에 포함.
    빈 sentence (단순 표점/공백) 는 제거.
    """
    if not para_text.strip():
        return []

    sentences: list[str] = []
    buf: list[str] = []
    for ch in para_text:
        buf.append(ch)
        if ch in SENT_TERMINATORS:
            sent = "".join(buf).strip()
            if sent and _strip_punct(sent):
                # 빈 백문 sentence (단순 표점만) 제외
                sentences.append(sent)
            buf = []

    # Trailing fragment without terminator (drop unless has content)
    tail = "".join(buf).strip()
    if tail and _strip_punct(tail):
        sentences.append(tail)
        # logging은 main에서 letter context와 함께
    return sentences


def segment_letter(letter: dict) -> list[Sentence]:
    """letter dict (from letters.jsonl) → list of Sentence."""
    raw_text = letter.get("raw_text", "")
    paragraphs = raw_text.split("\n\n") if raw_text else []

    out: list[Sentence] = []
    sent_idx_global = 0
    for para_idx, para in enumerate(paragraphs, start=1):
        sentences = _split_paragraph(para)
        for sent_idx_in_para, sent in enumerate(sentences, start=1):
            sent_idx_global += 1
            sent_plain = _strip_punct(sent)
            out.append(Sentence(
                sentence_id=(
                    f"{letter['data_id']}_p{para_idx}_s{sent_idx_in_para}"
                ),
                letter_data_id=letter["data_id"],
                munjip=letter["munjip"],
                kwon=letter["kwon"],
                letter_seq=letter["seq"],
                letter_title=letter["title"],
                letter_year=letter.get("inferred_year"),
                sender_label=letter.get("sender_label", ""),
                target_name=letter.get("target_name", ""),
                para_idx=para_idx,
                sent_idx_in_para=sent_idx_in_para,
                sent_idx_in_letter=sent_idx_global,
                sent_text=sent,
                sent_text_plain=sent_plain,
                char_count_raw=len(sent),
                char_count_plain=len(sent_plain),
            ))
    return out


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------

def write_jsonl(sentences: list[Sentence], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for s in sentences:
            f.write(json.dumps(asdict(s), ensure_ascii=False) + "\n")
    logging.info("Wrote %d sentences → %s", len(sentences), path)


def write_csv(sentences: list[Sentence], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "sentence_id", "letter_data_id", "munjip", "kwon",
        "letter_seq", "letter_title", "letter_year", "sender_label",
        "target_name", "para_idx", "sent_idx_in_para", "sent_idx_in_letter",
        "char_count_raw", "char_count_plain",
        "sent_text", "sent_text_plain",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for s in sentences:
            w.writerow({k: getattr(s, k) for k in fieldnames})
    logging.info("Wrote CSV → %s", path)


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

def summarize(sentences: list[Sentence]) -> None:
    """Print summary statistics."""
    if not sentences:
        logging.info("No sentences.")
        return

    by_target: dict[str, list[Sentence]] = {}
    for s in sentences:
        by_target.setdefault(s.target_name, []).append(s)

    logging.info("─" * 70)
    logging.info("Segmentation summary")
    logging.info("─" * 70)
    for target, sents in sorted(by_target.items()):
        n_sent = len(sents)
        n_letters = len({s.letter_data_id for s in sents})
        total_raw = sum(s.char_count_raw for s in sents)
        total_plain = sum(s.char_count_plain for s in sents)
        avg_len = total_plain / n_sent if n_sent else 0
        logging.info(
            "%-30s  %3d편 → %5d 문장  (%5.1f자/문장 백문, %s 자)",
            target, n_letters, n_sent, avg_len, f"{total_plain:,}"
        )

    n_total = len(sentences)
    logging.info("─" * 70)
    logging.info("총 %d 문장 (백문 평균 %.1f 자)",
                 n_total,
                 sum(s.char_count_plain for s in sentences) / n_total)
    logging.info("─" * 70)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--repo-root", type=Path, default=DEFAULT_REPO_ROOT,
                    help="Repo root (default: %(default)s)")
    ap.add_argument("--input", type=Path, default=None,
                    help="Override letters.jsonl path "
                         "(default: <repo>/data/raw/letters.jsonl)")
    ap.add_argument("--no-jsonl", action="store_true",
                    help="Skip JSONL output (CSV only)")
    ap.add_argument("--no-csv", action="store_true",
                    help="Skip CSV output (JSONL only)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print first 30 sentences to stdout, no files")
    ap.add_argument("--debug", action="store_true",
                    help="Verbose logging")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    repo_root = args.repo_root.resolve()
    in_path = args.input or (repo_root / "data/raw/letters.jsonl")
    if not in_path.exists():
        sys.exit(
            f"Input not found: {in_path}\n"
            "Run scripts/11_crawl_itkc.py first."
        )

    # Load letters
    letters = []
    with in_path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                letters.append(json.loads(line))
    logging.info("Loaded %d letters from %s", len(letters), in_path)

    # Segment
    all_sents: list[Sentence] = []
    for L in letters:
        sents = segment_letter(L)
        all_sents.extend(sents)
        logging.debug("  %s — %d sentences", L["data_id"], len(sents))

    # Summary
    summarize(all_sents)

    if args.dry_run:
        for s in all_sents[:30]:
            print(json.dumps({
                "id": s.sentence_id,
                "year": s.letter_year,
                "sender": s.sender_label,
                "len": s.char_count_plain,
                "text": s.sent_text,
            }, ensure_ascii=False))
        if len(all_sents) > 30:
            print(f"... ({len(all_sents)-30} more)")
        return

    proc_dir = repo_root / "data/processed"
    if not args.no_jsonl:
        write_jsonl(all_sents, proc_dir / "sentences.jsonl")
    if not args.no_csv:
        write_csv(all_sents, proc_dir / "sentences.csv")


if __name__ == "__main__":
    main()
