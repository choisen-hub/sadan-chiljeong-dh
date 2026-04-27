#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
11_crawl_itkc.py — Extract letters from local 한국문집총간 XML.

Step 11 of the Phase 0 data preprocessing pipeline for the project
"사단칠정 논쟁의 발생 원인 규명: 주자어류 텍스트 분석을 통한 理의 다의성 탐색".

Reads ZIP files of 한국문집총간 (downloaded from
공공데이터포털 https://www.data.go.kr/data/3074298/fileData.do)
and extracts letters matching a title pattern from specified 권.


CORPUS DEFINITION
=================

(A) 퇴계 측  (분석 대상: 황준연(2009) 표 1의 퇴계 발신 7편)
    Source:    퇴계선생문집 권16-17 「答奇明彦」 series
    Container: 한국문집총간 ITKC_MO_0144A
    Note:      권16+권17 답기명언 letters는 총 22편(38,085자).
               그 중 황준연 표 1의 12편이 사단칠정 핵심이며, 본 연구는
               그 12편 중 퇴계 발신 7편(번호 1, 3, 5, 7, 9, 10, 12)을 사용.
               황준연 7편 매핑은 책을 직접 대조하여 별도 작업으로 처리.
               이 스크립트는 22편 전체를 추출하고, mapping은 후속 단계.

(B) 율곡 측  (1572년 사단칠정 논변 9편)
    Source:    율곡선생전서 권11 「答成浩原」 9편 (1572 壬申)
    Container: 한국문집총간 ITKC_MO_0201A
    Note:      한국민족문화대백과사전 등에서 "권10 답성호원 9편"이라 한 것은
               부정확. 한국문집총간本 율곡전서 실제 편제는
                  - 권10: 답성호원 8편 (1554~1572 산재)
                  - 권11: 답성호원 9편 (1572 壬申, 사단칠정 논변 본체)
                  - 권12: 답성호원 11편 (1573 이후 후속)
               1572년 9차 서신 논변 = 권11의 9편 (21,090자).


XML SCHEMA  (한국문집총간 정본 원문)
====================================

  <아이템>
    <레벨1 id="ITKC_MO_0144A" type="서지">
      <레벨2 id="ITKC_MO_0144A_0160" type="권차">      <!-- 권16 -->
        <레벨3 id="..._010" type="문체">                <!-- 書 (서간류) -->
          <레벨4 id="..._0010" type="최종정보">         <!-- letter 한 통 -->
            <메타정보>
              <제목정보><제목>答奇明彦 <원주>大升○己未</원주></제목></제목정보>
              <저자정보 type="필자"><저자>...</저자></저자정보>
              <분류정보>...</분류정보>
            </메타정보>
            <본문정보>
              <내용>
                <단락 align="left" indent="0">
                  本文…<line/>本文…<페이지 number="..."/>本文…
                </단락>
              </내용>
            </본문정보>

  태그 처리 규칙:
    <line/>          제거 (인쇄 줄바꿈, 의미 없음)
    <페이지 .../>     제거 (페이지 마커)
    <문자효과 .../>   제거 (zero-width 형식)
    <고유명사>...</>  속 텍스트 유지 (NER 태그, 텍스트는 본문)
    <원주>...</>     제목/본문 모두 텍스트 유지 (편자 주 또는 자주)
    <단락>           paragraph boundary, raw_text에 \\n\\n 로 보존


USAGE
=====

  # 모든 target 추출 (default config)
  python3 scripts/11_crawl_itkc.py

  # 특정 config 사용
  python3 scripts/11_crawl_itkc.py --config config/letter_targets.yaml

  # 추출 결과 미리보기 (파일 안 만들고 stdout만)
  python3 scripts/11_crawl_itkc.py --dry-run


OUTPUTS
=======

  data/raw/letters.jsonl              한 줄당 한 letter
  data/processed/letter_index.csv     검색용 인덱스
  docs/letter_provenance.md           수집 출처 자동 기록


DEPENDENCIES
============

  Python ≥ 3.9 표준 라이브러리만 (xml.etree, zipfile, csv, json).
  + pyyaml (config 파싱).
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import sys
import zipfile
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional
from xml.etree import ElementTree as ET

try:
    import yaml
except ImportError:
    sys.exit("Missing dependency: pyyaml\nInstall: pip install pyyaml")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Repo root: parent of this script's directory
DEFAULT_REPO_ROOT = Path(__file__).resolve().parent.parent

# 문집 → ZIP 파일명 + XML id prefix
MUNJIP_INFO = {
    "toegye": {
        "zip_name": "144__퇴계집_退溪集_.zip",
        "xml_prefix": "ITKC_MO_0144A",
        "korean_name": "퇴계선생문집",
        "hanja_name": "退溪先生文集",
    },
    "yulgok": {
        "zip_name": "201__율곡전서_栗谷全書_.zip",
        "xml_prefix": "ITKC_MO_0201A",
        "korean_name": "율곡선생전서",
        "hanja_name": "栗谷先生全書",
    },
}

# 권 번호 → XML 파일 suffix 코드 (e.g. 16 → "0160")
def kwon_code(k: int) -> str:
    return f"{k:03d}0"


# 干支 → 서기 매핑 (16~17세기 范圍, 사단칠정 corpus 시기)
GANJI_TO_YEAR = {
    "甲寅": 1554, "乙卯": 1555, "丙辰": 1556, "丁巳": 1557, "戊午": 1558,
    "己未": 1559, "庚申": 1560, "辛酉": 1561, "壬戌": 1562, "癸亥": 1563,
    "甲子": 1564, "乙丑": 1565, "丙寅": 1566, "丁卯": 1567, "戊辰": 1568,
    "己巳": 1569, "庚午": 1570, "辛未": 1571, "壬申": 1572, "癸酉": 1573,
    "甲戌": 1574, "乙亥": 1575, "丙子": 1576, "丁丑": 1577, "戊寅": 1578,
    "己卯": 1579, "庚辰": 1580, "辛巳": 1581, "壬午": 1582, "癸未": 1583,
}

# Default targets (overrideable via --config)
DEFAULT_TARGETS = {
    "toegye_gobong_22pyeon": {
        "munjip": "toegye",
        "kwon": [16, 17],
        "title_pattern": r"明彦",
        "label": "T",
        "description": (
            "퇴계전서 권16+권17 명언 관련 letters (22편). "
            "答/與/重答 기명언 + 답우인논학서(奉寄明彦) 포함. "
            "황준연(2009) 표 1의 12편 corpus의 superset. "
            "최종 분석 corpus(퇴계 발신 7편)는 후속 단계에서 mapping."
        ),
    },
    "yulgok_ugye_9pyeon": {
        "munjip": "yulgok",
        "kwon": [11],
        "title_pattern": r"[答與]成浩原",
        "label": "Y",
        "description": (
            "율곡전서 권11 답/여 성호원 letters (9편). "
            "1572년(壬申) 사단칠정 논변 본체 corpus."
        ),
    },
}


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Letter:
    """One letter, the canonical analysis unit."""
    # Identity
    data_id: str                     # ITKC_MO_xxxA_xxxx_xxx_xxxx
    munjip: str                      # 'toegye' / 'yulgok'
    kwon: int                        # 권 number (e.g. 16)
    seq: int                         # within 권 (1-indexed)

    # Title metadata
    title: str                       # 제목 (e.g., 答奇明彦)
    annotation: Optional[str]        # 원주 (e.g., 大升○己未)
    inferred_year: Optional[int]     # 干支에서 추정한 서기 연도

    # Body
    raw_text: str                    # 표점 + 단락 구조 보존
    plain_text: str                  # 표점 제거 백문 (단락도 합침)
    char_count_raw: int              # raw_text 글자 수 (공백 제외)
    char_count_plain: int            # plain_text 글자 수
    body_first_50: str               # plain_text 앞 50자 (검색용)

    # Provenance
    sender_label: str                # T (Toegye) / Y (Yulgok) — *발신인이 아닐 수도 있음*
    target_name: str                 # config의 target 이름
    source_zip: str                  # 원본 ZIP 파일명
    source_xml_path: str             # ZIP 안의 XML path
    fetched_at: str                  # ISO timestamp


# ---------------------------------------------------------------------------
# XML parsing
# ---------------------------------------------------------------------------

# Tags whose content is NOT semantic body text — drop entirely
_VOID_TAGS = {"line", "페이지", "문자효과"}

def _walk_text(elem: ET.Element, parts: list[str]) -> None:
    """Recursively collect text from an element, stripping void markers."""
    if elem.tag in _VOID_TAGS:
        return  # skip self entirely
    if elem.text:
        parts.append(elem.text)
    for child in elem:
        _walk_text(child, parts)
        if child.tail:
            parts.append(child.tail)


def _extract_para_text(para: ET.Element) -> str:
    """Get text of one <단락>, with line breaks/page markers removed."""
    parts: list[str] = []
    _walk_text(para, parts)
    text = "".join(parts)
    # Collapse whitespace (the XML has CRLF inserted by line wrapping)
    text = re.sub(r"\s+", "", text)
    return text


def _extract_title(meta: ET.Element) -> tuple[str, Optional[str]]:
    """Extract (제목, 원주) from <메타정보>.

    제목 includes everything except <원주>...</원주>; the 원주 is returned
    separately so it can be used to infer 干支/연도.
    """
    title_elem = meta.find(".//제목")
    if title_elem is None:
        return ("", None)

    # Collect title text (excluding 원주) and 원주 text separately
    title_parts: list[str] = []
    annotation_parts: list[str] = []

    def _walk(e: ET.Element, in_annotation: bool) -> None:
        if e.tag == "원주":
            in_annotation = True
        target = annotation_parts if in_annotation else title_parts
        if e.text:
            target.append(e.text)
        for child in e:
            _walk(child, in_annotation)
            if child.tail:
                tail_target = title_parts if not in_annotation else annotation_parts
                tail_target.append(child.tail)

    # Iterate children of <제목>, deciding annotation scope per-child
    if title_elem.text:
        title_parts.append(title_elem.text)
    for child in title_elem:
        if child.tag == "원주":
            # collect annotation text
            sub: list[str] = []
            _walk_text(child, sub)
            annotation_parts.append("".join(sub))
            if child.tail:
                title_parts.append(child.tail)
        else:
            sub: list[str] = []
            _walk_text(child, sub)
            title_parts.append("".join(sub))
            if child.tail:
                title_parts.append(child.tail)

    title = re.sub(r"\s+", "", "".join(title_parts))
    annotation = re.sub(r"\s+", "", "".join(annotation_parts)) or None
    return title, annotation


def _infer_year(annotation: Optional[str]) -> Optional[int]:
    """Find the first 干支 token in the annotation and map to year."""
    if not annotation:
        return None
    for ganji, year in GANJI_TO_YEAR.items():
        if ganji in annotation:
            return year
    return None


_PUNCT_PATTERN = re.compile(
    r"[，。、；：？！「」『』《》〈〉（）()\[\]【】〔〕,.;:!?\"'’‘“”—\-·…\s]+"
)

def _strip_punct(text: str) -> str:
    """Remove all punctuation/whitespace, leaving only characters (백문)."""
    return _PUNCT_PATTERN.sub("", text)


def parse_kwon_xml(xml_bytes: bytes) -> list[dict]:
    """Parse one 권 XML and return list of letter dicts.

    Each dict has:
      - data_id, kwon, seq
      - title, annotation
      - paragraphs: list[str] (텍스트, 표점 포함)
    """
    # Parse — XML uses Korean tag names natively
    root = ET.fromstring(xml_bytes)

    # 권 number from 레벨2 id (e.g. ITKC_MO_0144A_0160 → 16)
    # Root is <아이템>, structure: <아이템> > <레벨1> > <레벨2> > <레벨3> > <레벨4>
    level2 = root.find(".//레벨2")
    if level2 is None:
        return []
    level2_id = level2.get("id", "")
    m = re.search(r"_(\d{3})0$", level2_id)
    kwon = int(m.group(1)) if m else 0

    letters = []
    for seq, l4 in enumerate(level2.iter("레벨4"), start=1):
        if l4.get("type") != "최종정보":
            continue
        data_id = l4.get("id", "")

        # Title + annotation
        meta = l4.find("메타정보")
        title, annotation = _extract_title(meta) if meta is not None else ("", None)

        # Body paragraphs
        body = l4.find(".//본문정보/내용")
        paragraphs = []
        if body is not None:
            for para in body.iter("단락"):
                paragraphs.append(_extract_para_text(para))

        letters.append({
            "data_id": data_id,
            "kwon": kwon,
            "seq": seq,
            "title": title,
            "annotation": annotation,
            "paragraphs": paragraphs,
        })
    return letters


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def iter_zip_xml(zip_path: Path, kwon_list: list[int],
                 xml_prefix: str) -> Iterable[tuple[str, bytes]]:
    """Yield (member_name, raw_bytes) for each requested 권."""
    with zipfile.ZipFile(zip_path) as z:
        names = set(z.namelist())
        for k in kwon_list:
            member = f"{xml_prefix}_{kwon_code(k)}.xml"
            if member not in names:
                logging.warning("ZIP %s 에 %s 없음 — 권%d 건너뜀",
                                zip_path.name, member, k)
                continue
            yield member, z.read(member)


def extract_target(target_name: str, target: dict, zip_dir: Path) -> list[Letter]:
    """Process one target (e.g. 'toegye_gobong_22pyeon') → list[Letter]."""
    munjip_key = target["munjip"]
    info = MUNJIP_INFO.get(munjip_key)
    if info is None:
        raise ValueError(f"Unknown munjip key: {munjip_key}")

    zip_path = zip_dir / info["zip_name"]
    if not zip_path.exists():
        raise FileNotFoundError(
            f"Munjip ZIP not found: {zip_path}\n"
            f"Download from https://www.data.go.kr/data/3074298/fileData.do "
            f"and place in data/raw/munjip/"
        )

    pattern = re.compile(target["title_pattern"])
    label = target.get("label", "")
    fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    out: list[Letter] = []
    for member_name, xml_bytes in iter_zip_xml(
        zip_path, target["kwon"], info["xml_prefix"]
    ):
        letters = parse_kwon_xml(xml_bytes)
        for L in letters:
            if not pattern.search(L["title"]):
                continue
            paragraphs = L["paragraphs"]
            raw_text = "\n\n".join(paragraphs)
            plain_text = _strip_punct(raw_text)
            out.append(Letter(
                data_id=L["data_id"],
                munjip=munjip_key,
                kwon=L["kwon"],
                seq=L["seq"],
                title=L["title"],
                annotation=L["annotation"],
                inferred_year=_infer_year(L["annotation"]),
                raw_text=raw_text,
                plain_text=plain_text,
                char_count_raw=len(raw_text.replace("\n", "")),
                char_count_plain=len(plain_text),
                body_first_50=plain_text[:50],
                sender_label=label,
                target_name=target_name,
                source_zip=info["zip_name"],
                source_xml_path=member_name,
                fetched_at=fetched_at,
            ))
            logging.info(
                "  ✓ %s — %s%s (%d자)",
                L["data_id"], L["title"],
                f" [{L['annotation']}]" if L["annotation"] else "",
                len(plain_text),
            )

    return out


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------

def write_jsonl(letters: list[Letter], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for L in letters:
            f.write(json.dumps(asdict(L), ensure_ascii=False) + "\n")
    logging.info("Wrote %d letters → %s", len(letters), path)


def write_index_csv(letters: list[Letter], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "data_id", "munjip", "kwon", "seq", "title", "annotation",
            "inferred_year", "char_count_raw", "char_count_plain",
            "sender_label", "target_name", "body_first_50",
        ])
        for L in letters:
            w.writerow([
                L.data_id, L.munjip, L.kwon, L.seq, L.title, L.annotation or "",
                L.inferred_year or "", L.char_count_raw, L.char_count_plain,
                L.sender_label, L.target_name, L.body_first_50,
            ])
    logging.info("Wrote index → %s", path)


def write_provenance_md(letters: list[Letter], path: Path,
                        config_path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    by_target: dict[str, list[Letter]] = {}
    for L in letters:
        by_target.setdefault(L.target_name, []).append(L)

    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    lines = [
        "# Letter corpus — extraction provenance",
        "",
        f"Auto-generated by `scripts/11_crawl_itkc.py` at `{now_iso}`.",
        "",
        "이 파일은 `11_crawl_itkc.py` 가 매 실행 시 덮어씁니다. "
        "수동 편집하지 마세요. 판본/출처 등 안정적인 메타데이터는 "
        "`docs/판본정보.md` 를 참조하세요.",
        "",
        "## Source",
        "",
        "- 데이터 출처: 공공데이터포털 한국문집총간",
        "  (https://www.data.go.kr/data/3074298/fileData.do)",
        "- 한국고전번역원 발행 한국문집총간 정본 원문 XML",
        f"- Config: `{config_path.relative_to(DEFAULT_REPO_ROOT) if config_path.is_absolute() else config_path}`",
        "",
        "## Extracted letters",
        "",
    ]
    for target, ll in sorted(by_target.items()):
        lines += [
            f"### {target} ({len(ll)} letters)",
            "",
            "| seq | data_id | 권 | 제목 | 원주(干支) | 추정연도 | 백문 자수 |",
            "|-----|---------|----|------|-----------|----------|----------|",
        ]
        for L in sorted(ll, key=lambda x: (x.kwon, x.seq)):
            lines.append(
                f"| {L.seq} | `{L.data_id}` | {L.kwon} | {L.title} | "
                f"{L.annotation or ''} | {L.inferred_year or ''} | "
                f"{L.char_count_plain:,} |"
            )
        total = sum(L.char_count_plain for L in ll)
        lines += ["", f"**합계: {len(ll)}편, {total:,}자 (백문 기준)**", ""]

    path.write_text("\n".join(lines), encoding="utf-8")
    logging.info("Wrote provenance log → %s", path)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _load_config(path: Path) -> dict:
    if not path.exists():
        logging.warning("Config not found: %s — using DEFAULT_TARGETS", path)
        return DEFAULT_TARGETS
    with path.open(encoding="utf-8") as f:
        loaded = yaml.safe_load(f)
    return loaded or DEFAULT_TARGETS


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--config", type=Path,
                    default=DEFAULT_REPO_ROOT / "config/letter_targets.yaml",
                    help="Target spec YAML (default: %(default)s)")
    ap.add_argument("--repo-root", type=Path, default=DEFAULT_REPO_ROOT,
                    help="Repo root (default: %(default)s)")
    ap.add_argument("--zip-dir", type=Path, default=None,
                    help="Override ZIP directory (default: <repo>/data/raw/munjip)")
    ap.add_argument("--debug", action="store_true",
                    help="Verbose logging")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print results to stdout without writing files")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    repo_root = args.repo_root.resolve()
    zip_dir = args.zip_dir or (repo_root / "data/raw/munjip")
    zip_dir = zip_dir.resolve()

    config = _load_config(args.config)
    logging.info("Loaded %d targets from config", len(config))

    all_letters: list[Letter] = []
    for tname, target in config.items():
        logging.info("=== Target: %s ===", tname)
        try:
            letters = extract_target(tname, target, zip_dir)
            all_letters.extend(letters)
            total_chars = sum(L.char_count_plain for L in letters)
            logging.info("  → %d letters, %d chars (백문)",
                         len(letters), total_chars)
        except FileNotFoundError as e:
            logging.error("  %s", e)
            continue

    if args.dry_run:
        for L in all_letters:
            print(json.dumps({
                "data_id": L.data_id, "title": L.title,
                "annotation": L.annotation, "year": L.inferred_year,
                "chars": L.char_count_plain,
            }, ensure_ascii=False))
        return

    write_jsonl(all_letters, repo_root / "data/raw/letters.jsonl")
    write_index_csv(all_letters, repo_root / "data/processed/letter_index.csv")
    write_provenance_md(all_letters,
                        repo_root / "docs/letter_provenance.md",
                        args.config)
    logging.info("Done. Total: %d letters, %d chars (백문)",
                 len(all_letters),
                 sum(L.char_count_plain for L in all_letters))


if __name__ == "__main__":
    main()
