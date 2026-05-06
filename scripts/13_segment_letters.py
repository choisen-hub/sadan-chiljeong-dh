#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
13_segment_letters.py — Segment hanja.dev-punctuated letters into sentences.

Step 13 of the Phase 0 data preprocessing pipeline.

12_punctuate.py 가 산출한 letters_punctuated.jsonl 의 punctuated_text
(SikuRoBERTa-PUNC-AJD-KLC 가 부여한 표점) 를 sentence 단위로 분절.

이전 12_segment_letters.py 는 한국문집총간 정본의 raw_text(전통식 표점,
평균 6자) 를 분절했으나 의미 단위로 부적합. 본 스크립트는 hanja.dev 가
부여한 새 표점 기준으로 분절하여 평균 ~30자대의 의미 단위 문장을 산출.


SEGMENTATION RULES
==================

  종결자: 。？！ (전각) + ?! (반각)
    - hanja.dev 출력에 전각/반각이 섞여 등장하므로 둘 다 인식.
  종결자를 만나면 sentence 종료. 종결자는 sentence text 에 포함.
  빈 sentence (단순 표점·공백만) 는 제거.

  hanja.dev 출력은 단락 구분자를 포함하지 않음 → 한 letter 전체를 1개
  paragraph 로 처리. para_idx 는 1 로 고정 (기존 schema 호환용).


OUTPUTS
=======

  data/processed/sentences.jsonl     한 줄당 한 sentence
  data/processed/sentences.csv       CSV (수동 검토용)


SCHEMA (출력 jsonl 한 줄)
=========================

  sentence_id          T0001 (퇴계) 또는 Y0001 (율곡), sender_label 별 전역 순번
  source_id            {letter_data_id}_p1_s{idx}  (합성 출처 ID)
  letter_data_id       원본 letter id
  munjip               toegye / yulgok
  kwon                 권 number
  letter_seq           letter 순서 within 권
  letter_title         letter 제목
  letter_year          letter inferred_year
  sender_label         T / Y
  target_name          11번 config 의 target name
  para_idx             항상 1 (단락 정보 부재 — schema 호환용)
  sent_idx_in_para     sent_idx_in_letter 와 동일
  sent_idx_in_letter   sentence index within letter (1-indexed)
  sent_text            표점 포함 sentence text (hanja.dev 결과 기반)
  sent_text_plain      백문 (표점 제거)
  char_count_raw       sent_text 글자 수
  char_count_plain     sent_text_plain 글자 수


USAGE
=====

  python3 scripts/13_segment_letters.py

  # CSV 만 출력
  python3 scripts/13_segment_letters.py --no-jsonl

  # 분절 결과 미리보기 (파일 안 만듦)
  python3 scripts/13_segment_letters.py --dry-run | head -30


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


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_REPO_ROOT = Path(__file__).resolve().parent.parent

# Sentence-terminating characters (전각 + 반각, hanja.dev 출력 모두 커버)
SENT_TERMINATORS = set("。？！?!")

_PUNCT_PATTERN = re.compile(
    r"[，。、；：？！「」『』《》〈〉（）()\[\]【】〔〕,.;:!?\"'’‘“”—\-·…\s]+"
)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Sentence:
    sentence_id: str
    source_id: str
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
    return _PUNCT_PATTERN.sub("", text)


def _split_text(text: str) -> list[str]:
    """종결자 기준 sentence 분할. 종결자는 sentence 에 포함."""
    if not text or not text.strip():
        return []

    sentences: list[str] = []
    buf: list[str] = []
    for ch in text:
        buf.append(ch)
        if ch in SENT_TERMINATORS:
            sent = "".join(buf).strip()
            if sent and _strip_punct(sent):
                sentences.append(sent)
            buf = []

    # Trailing fragment without terminator
    tail = "".join(buf).strip()
    if tail and _strip_punct(tail):
        sentences.append(tail)
    return sentences


def segment_letter(letter: dict) -> list[Sentence]:
    """letter dict (from letters_punctuated.jsonl) → list of Sentence.

    sentence_id 는 letter 단위 함수에서 전역 순번 부여 불가 → 빈 문자열로 두고
    main() 에서 sender_label 별로 'T0001' / 'Y0001' 형식 일괄 할당.
    """
    text = letter.get("punctuated_text", "")
    sentences_text = _split_text(text)

    out: list[Sentence] = []
    for idx, sent in enumerate(sentences_text, start=1):
        sent_plain = _strip_punct(sent)
        out.append(Sentence(
            sentence_id="",  # main() 에서 전역 순번 부여
            source_id=f"{letter['data_id']}_p1_s{idx}",
            letter_data_id=letter["data_id"],
            munjip=letter["munjip"],
            kwon=letter["kwon"],
            letter_seq=letter["seq"],
            letter_title=letter["title"],
            letter_year=letter.get("inferred_year"),
            sender_label=letter.get("sender_label", ""),
            target_name=letter.get("target_name", ""),
            para_idx=1,
            sent_idx_in_para=idx,
            sent_idx_in_letter=idx,
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
        "sentence_id", "source_id", "letter_data_id", "munjip", "kwon",
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
        total_plain = sum(s.char_count_plain for s in sents)
        avg_len = total_plain / n_sent if n_sent else 0
        logging.info(
            "%-30s  %3d편 → %5d 문장  (%5.1f자/문장, 백문 %s 자)",
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
                    help="Override input path "
                         "(default: <repo>/data/processed/letters_punctuated.jsonl)")
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
    in_path = args.input or (repo_root / "data/processed/letters_punctuated.jsonl")
    if not in_path.exists():
        sys.exit(
            f"Input not found: {in_path}\n"
            "Run scripts/12_punctuate.py first."
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

    # 전역 순번 sentence_id 부여 — sender_label 별로 카운터 분리
    #   퇴계 (T): T0001 ~ T{퇴계 sentence 수}
    #   율곡 (Y): Y0001 ~ Y{율곡 sentence 수}
    # 4자리 zero-padding: 각 corpus 모두 ~2,000개 미만 규모이므로 충분
    counters: dict[str, int] = {}
    for s in all_sents:
        prefix = s.sender_label or "L"  # fallback: 라벨 없으면 L
        counters[prefix] = counters.get(prefix, 0) + 1
        s.sentence_id = f"{prefix}{counters[prefix]:04d}"

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
