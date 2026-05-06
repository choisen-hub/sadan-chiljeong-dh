#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
03_punctuate.py — Apply hanja.dev model to 주자어류 paragraphs.

Phase 0 주자어류 파이프라인의 표점 부여 단계.
02_parse_kanripo.py 가 산출한 kanripo_parsed.jsonl 의 paragraphs[] (백문)
를 SikuRoBERTa-PUNC-AJD-KLC 모델에 통과시켜 paragraphs_punctuated[] 를
추가한 jsonl 산출.


WHY hanja.dev 일괄적용
======================

  이전 05_punctuate.py 는 1~7권은 祝平次 표점을 차용하고 8권 이후는
  hanja.dev 로 fallback 하는 hybrid 방식. 이를 폐기하고 1~141권 전체를
  hanja.dev 로 일원화한다.
    - 표점 부여 규칙을 corpus 전체에 동일하게 적용 (서신 corpus와 일치)
    - 100번대 이후 권의 표점 부재 문제 해결
    - 祝平次 차용에 따른 코퍼스 내 이질성 제거


표점 부여 단위
==============

  권 단위 X — 권당 평균 ~1만자, 청킹 의미상 부적절.
  paragraph 단위 O — 02 가 정의한 paragraph 가 자연스러운 의미 단위.
  paragraph 가 480자 초과 시 punctuate_hanja 가 내부에서 청킹.


SCHEMA (출력 jsonl, 권당 한 줄)
================================

  02 출력 필드 모두 유지 + 추가:
    paragraphs_punctuated : list[str]
        paragraphs[i] 를 표점 부여한 결과. 길이는 paragraphs 와 동일.
    punctuation_meta       : dict
        - model_id, model_revision
        - timestamp (권 단위 시작/종료, 마지막 paragraph 처리 시각 기준)
        - n_paragraphs
        - total_chars_in    (paragraphs 합계 글자수)
        - total_chars_out   (paragraphs_punctuated 합계 글자수)


USAGE
=====

  # 전체 141권
  python3 scripts/03_punctuate.py

  # 권1만 (sanity check)
  python3 scripts/03_punctuate.py --limit 1

  # 권1~5 (안정성 확인)
  python3 scripts/03_punctuate.py --limit 5

  # 모델 revision pinning (재현성)
  python3 scripts/03_punctuate.py --revision <commit_hash>


OUTPUTS
=======

  data/intermediate/kanripo_punctuated.jsonl
  docs/zhuzi_punctuation_provenance.md


DEPENDENCIES
============

  Python ≥ 3.10
  + torch, transformers, huggingface_hub
  + common.punctuate_hanja
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

# common 모듈 import
DEFAULT_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(DEFAULT_REPO_ROOT))

from common.punctuate_hanja import (  # noqa: E402
    DEFAULT_MODEL_ID,
    HanjaPunctuator,
)


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def punctuate_juan(juan_rec: dict, pun: HanjaPunctuator) -> dict:
    """권 한 개 처리: paragraphs[] 각각 표점 부여 → paragraphs_punctuated 추가.

    in-place 수정 + 반환. 02 의 다른 필드는 그대로 유지.
    """
    paragraphs = juan_rec.get("paragraphs", [])
    juan_num = juan_rec.get("juan_num")

    punctuated_list: list[str] = []
    total_chars_in = 0
    total_chars_out = 0

    for p_idx, para in enumerate(paragraphs):
        if not para:
            punctuated_list.append("")
            continue

        # paragraph 단위 ID — 캐시 키 + 디버깅 용
        text_id = f"juan{juan_num}_p{p_idx}"
        result = pun.punctuate(para, text_id, use_cache=True)
        punctuated_list.append(result.punctuated_text)
        total_chars_in += len(para)
        total_chars_out += len(result.punctuated_text)

    juan_rec["paragraphs_punctuated"] = punctuated_list
    juan_rec["punctuation_meta"] = {
        "model_id": pun.model_id,
        "model_revision": pun.revision,
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "n_paragraphs": len(paragraphs),
        "total_chars_in": total_chars_in,
        "total_chars_out": total_chars_out,
    }
    return juan_rec


def punctuate_corpus(
    input_path: Path,
    output_path: Path,
    pun: HanjaPunctuator,
    limit: int | None = None,
) -> dict:
    """jsonl 일괄 처리 + 통계 반환."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    n_juan = 0
    n_paragraphs = 0
    total_in = 0
    total_out = 0

    with input_path.open(encoding="utf-8") as f_in, \
         output_path.open("w", encoding="utf-8") as f_out:
        for line in f_in:
            line = line.strip()
            if not line:
                continue
            if limit is not None and n_juan >= limit:
                break

            juan = json.loads(line)
            juan_num = juan.get("juan_num")
            juan_label = juan.get("juan_label", "")
            n_para = len(juan.get("paragraphs", []))

            logging.info(
                "  권 %3s (%s) — %d paragraphs 처리 시작",
                str(juan_num), juan_label, n_para,
            )
            juan = punctuate_juan(juan, pun)
            f_out.write(json.dumps(juan, ensure_ascii=False) + "\n")

            meta = juan["punctuation_meta"]
            n_juan += 1
            n_paragraphs += meta["n_paragraphs"]
            total_in += meta["total_chars_in"]
            total_out += meta["total_chars_out"]

            logging.info(
                "  ✓ 권 %3s — %d paragraphs, %d자 → %d자 (+%.1f%%)",
                str(juan_num),
                meta["n_paragraphs"],
                meta["total_chars_in"],
                meta["total_chars_out"],
                (meta["total_chars_out"] - meta["total_chars_in"]) /
                max(meta["total_chars_in"], 1) * 100,
            )

    return {
        "n_juan": n_juan,
        "n_paragraphs": n_paragraphs,
        "total_chars_in": total_in,
        "total_chars_out": total_out,
    }


# ---------------------------------------------------------------------------
# Provenance
# ---------------------------------------------------------------------------

def write_provenance_md(
    path: Path,
    stats: dict,
    model_id: str,
    revision: str | None,
    device: str,
    started_at: str,
    finished_at: str,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    added = stats["total_chars_out"] - stats["total_chars_in"]
    rate = added / max(stats["total_chars_in"], 1) * 100
    lines = [
        "# 주자어류 punctuation — provenance",
        "",
        f"Auto-generated by `scripts/03_punctuate.py` at `{finished_at}`.",
        "",
        "이 파일은 매 실행 시 덮어씁니다. 수동 편집 금지.",
        "",
        "## Model",
        "",
        f"- Model ID    : `{model_id}`",
        f"- Revision    : `{revision or '(latest — unpinned)'}`",
        f"- Device      : `{device}`",
        "- Architecture: BertForTokenClassification (NER 스타일 토큰 분류)",
        "- 표점 체계  : 한국고전번역원 (AJD/KLC)",
        "",
        "## 표점 부여 단위",
        "",
        "- 02_parse_kanripo.py 가 정의한 paragraph 단위로 모델 호출.",
        "- paragraph 가 480자(모델 max_length 안전 마진) 초과 시 내부 청킹.",
        "- 권 단위 통계는 paragraph 단위 합계.",
        "",
        "## Stats",
        "",
        f"- 권 처리 수             : {stats['n_juan']}",
        f"- paragraph 처리 수     : {stats['n_paragraphs']:,}",
        f"- 입력 백문 총 글자수    : {stats['total_chars_in']:,}",
        f"- 출력 표점 총 글자수    : {stats['total_chars_out']:,}",
        f"- 표점 추가 글자 수      : {added:,} ({rate:.1f}%)",
        "",
        "## Run",
        "",
        f"- Started : {started_at}",
        f"- Finished: {finished_at}",
        "",
    ]
    if revision is None:
        lines += [
            "## ⚠ Reproducibility note",
            "",
            "Revision 이 pinned 되지 않음. 정확한 재현을 위해서는 모델",
            "커밋 해시를 고정해 `--revision <hash>` 로 실행 권장.",
            "",
        ]
    path.write_text("\n".join(lines), encoding="utf-8")
    logging.info("Wrote provenance log → %s", path)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--repo-root", type=Path, default=DEFAULT_REPO_ROOT)
    ap.add_argument("--input", type=Path, default=None,
                    help="입력 jsonl (default: <repo>/data/intermediate/kanripo_parsed.jsonl)")
    ap.add_argument("--output", type=Path, default=None,
                    help="출력 jsonl (default: <repo>/data/intermediate/kanripo_punctuated.jsonl)")
    ap.add_argument("--model", default=DEFAULT_MODEL_ID,
                    help="HuggingFace 모델 ID (default: %(default)s)")
    ap.add_argument("--revision", default=None,
                    help="모델 revision — 커밋 해시 권장")
    ap.add_argument("--device", default=None,
                    help="cpu / cuda / mps (default: 자동)")
    ap.add_argument("--limit", type=int, default=None,
                    help="처음 N권만 처리 (테스트용)")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    repo_root = args.repo_root.resolve()
    input_path = (
        args.input or (repo_root / "data/intermediate/kanripo_parsed.jsonl")
    ).resolve()
    output_path = (
        args.output or (repo_root / "data/intermediate/kanripo_punctuated.jsonl")
    ).resolve()

    if not input_path.exists():
        sys.exit(
            f"입력 파일 없음: {input_path}\n"
            "먼저 `python3 scripts/02_parse_kanripo.py` 실행 필요."
        )

    started_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    logging.info("=== 03_punctuate.py ===")
    logging.info("Input : %s", input_path)
    logging.info("Output: %s", output_path)
    logging.info("Model : %s (revision=%s)", args.model, args.revision)

    pun = HanjaPunctuator(
        model_id=args.model,
        revision=args.revision,
        device=args.device,
    )

    if args.limit:
        logging.info("Limit set: 처음 %d권만 처리", args.limit)

    stats = punctuate_corpus(input_path, output_path, pun, limit=args.limit)
    finished_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    write_provenance_md(
        repo_root / "docs/zhuzi_punctuation_provenance.md",
        stats,
        args.model,
        args.revision,
        pun.device,
        started_at,
        finished_at,
    )

    added = stats["total_chars_out"] - stats["total_chars_in"]
    logging.info(
        "Done. %d권, %d paragraphs, %s자 → %s자 (+%s자, +%.1f%%)",
        stats["n_juan"],
        stats["n_paragraphs"],
        f"{stats['total_chars_in']:,}",
        f"{stats['total_chars_out']:,}",
        f"{added:,}",
        added / max(stats["total_chars_in"], 1) * 100,
    )


if __name__ == "__main__":
    main()
