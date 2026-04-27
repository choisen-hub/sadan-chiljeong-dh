"""
03_parse_zhuzi_xml.py

祝平次(Chu Ping-tzu) 편집본 TEI-P5 XML을 구조화된 JSONL로 변환한다.
02번 출력(칸리포)과 동일한 스키마로 맞춰 04번 정렬(align) 단계에서 직접 비교 가능.

입력:
  data/raw/zhuzi_xml/zhuziyulei_tei.xml  (로컬에만 두는 XML 파일)

출력:
  data/intermediate/zhuzi_parsed.jsonl
  한 줄에 한 권. 각 레코드 구조 (02와 동일):
    {
      "juan_num": 1,
      "juan_label": "卷一",
      "source": "zhuzi_tei",
      "headings": [
        {"level": 2, "text": "理氣上"},
        {"level": 3, "text": "太極、天地上"},
        ...
      ],
      "paragraphs": [...],            # byline 제거 본문 (분석용, 표점 포함)
      "paragraphs_raw": [...],        # byline 포함 원문 (재현용)
      "paragraphs_byline": [...],     # 각 문단의 byline 메타 (없으면 null)
      "raw_concat": "..."             # paragraphs(byline 제거본) 이어붙임
    }

처리 규칙:
  - <pb n="..."/> 페이지 구분 제거
  - <hi> 등 일반 inline 요소 내용은 유지
  - <p> 및 <list>/<item> 요소를 모두 문단으로 수집
  - 표점(「」、。，？ 등)은 그대로 유지
  - 提要 div는 4개 섹션(提要/原序/門目/姓氏)으로 분리하여 칸리포 스키마와 일치시킴
  - 권 번호 한자 파싱은 02번과 동일

화자표시(byline) 처리:
  - TEI <byline> 요소는 명시 태그로 들어 있으므로 정규식 추측 불필요
  - <p> 안에 <byline>이 있으면:
      * byline의 raw 텍스트를 paragraphs_byline에 보존
      * paragraphs(분석용)에서는 byline 텍스트를 제거
      * paragraphs_raw에는 byline 포함 원문 보존
  - 누가 기록자인지는 식별하지 않음 (utterance 경계 신호 + 본문 정제만 목적)

사용법:
  python scripts/03_parse_zhuzi_xml.py
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from lxml import etree

# ---------- 경로 ----------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
XML_PATH = PROJECT_ROOT / "data" / "raw" / "zhuzi_xml" / "zhuziyulei_tei.xml"
OUT_DIR = PROJECT_ROOT / "data" / "intermediate"
OUT_PATH = OUT_DIR / "zhuzi_parsed.jsonl"

# ---------- 상수 ----------

TEI_NS = "http://www.tei-c.org/ns/1.0"
NS = {"tei": TEI_NS}

HANZI_NUM = {
    "一": 1, "二": 2, "三": 3, "四": 4, "五": 5,
    "六": 6, "七": 7, "八": 8, "九": 9,
    "十": 10, "百": 100,
}


# ---------- 유틸 ----------

def q(tag: str) -> str:
    """TEI 네임스페이스가 붙은 태그명."""
    return f"{{{TEI_NS}}}{tag}"


def parse_juan_num(label: str) -> int:
    """02번과 동일한 한자 숫자 파싱."""
    if label in ("提要", "原序", "門目", "姓氏"):
        return 0
    m = re.match(r"卷([一二三四五六七八九十百]+)", label)
    if not m:
        return -1
    s = m.group(1)
    total = 0
    current = 0
    for ch in s:
        val = HANZI_NUM.get(ch, 0)
        if val >= 10:
            current = 1 if current == 0 else current
            total += current * val
            current = 0
        else:
            current = val
    total += current
    return total


def extract_text(elem) -> str:
    """
    요소 내 모든 텍스트를 순서대로 합침. <pb> 태그만 건너뜀.
    <hi>, <byline>, <note> 등 인라인 요소의 텍스트는 정상 포함.
    """
    parts: list[str] = []
    if elem.text:
        parts.append(elem.text)
    for child in elem:
        tag = etree.QName(child).localname
        if tag != "pb":
            parts.append(extract_text(child))
        if child.tail:
            parts.append(child.tail)
    return "".join(parts)


def extract_text_excluding(elem, exclude_tags: set[str]) -> str:
    """
    extract_text와 동일하되, exclude_tags에 든 자식 요소의 *내부* 텍스트는 건너뜀.
    (자식 요소 자체와 그 tail은 흐름 위치를 유지하기 위해 처리)
    """
    parts: list[str] = []
    if elem.text:
        parts.append(elem.text)
    for child in elem:
        tag = etree.QName(child).localname
        if tag in exclude_tags:
            pass  # 자식 내부 텍스트는 빼고
        elif tag != "pb":
            parts.append(extract_text_excluding(child, exclude_tags))
        if child.tail:
            parts.append(child.tail)
    return "".join(parts)


def extract_byline_meta(p_elem) -> dict | None:
    """<p> 안의 첫 <byline> 요소 raw 텍스트. 없으면 None."""
    bl = p_elem.find(f".//{q('byline')}")
    if bl is None:
        return None
    raw = extract_text(bl).strip()
    if not raw:
        return None
    return {"raw": raw}


# 정규식 byline 패턴 (祝平次 XML이 <byline> 태그를 안 쓸 때의 대비)
# 02와 동일 정책: 8자 이하 + 슬래시 포함
RE_TRAILING_PAREN_BYLINE = re.compile(r"\(([^()]{1,8})\)\s*$")
RE_INLINE_PAREN_BYLINE = re.compile(r"\(([^()]{1,8})\)")


def cleanup_text_byline(text: str, existing_byline: dict | None) -> tuple[str, dict | None]:
    """
    본문 텍스트에서 정규식으로 byline-like 패턴을 추가 정제.

    - existing_byline 있으면 그 정보 유지 (TEI <byline> 태그로 이미 잡힌 경우)
    - 없으면 paragraph 끝 (X/Y) 패턴을 byline으로 분리
    - paragraph 중간의 (X/Y) (8자 이하 + slash 포함)도 본문에서 제거
      → utterance 경계는 잃지만 분절/임베딩 노이즈는 제거

    祝平次 XML이 <byline> 태그를 안 쓰고 본문 텍스트로 (賀孫/) 같은 형태로
    byline을 넣은 경우 이 함수가 핵심.
    """
    end_byline = existing_byline

    # 1. 끝 byline 분리 (TEI 태그로 못 잡았을 때만)
    if end_byline is None:
        m = RE_TRAILING_PAREN_BYLINE.search(text)
        if m and "/" in m.group(1):
            end_byline = {"raw": m.group(0)}
            text = text[: m.start()].rstrip()

    # 2. 본문 중간 byline-like 패턴 제거 (slash 포함만)
    def _replace(match: re.Match) -> str:
        inner = match.group(1)
        return "" if "/" in inner else match.group(0)

    text = RE_INLINE_PAREN_BYLINE.sub(_replace, text)
    return text, end_byline


def collect_content(
    div,
    depth: int,
    headings: list[dict],
    paragraphs: list[str],
    paragraphs_raw: list[str],
    paragraphs_byline: list[dict | None],
) -> None:
    """
    div 요소를 재귀 탐색하면서 headings와 paragraphs(+ raw, byline)를 누적한다.
    depth=0은 권 수준 (head는 juan_label로 별도 처리, headings에 넣지 않음).
    depth>=1은 편/절/소절 수준 (head를 level=depth+1로 기록).

    paragraphs       : byline 텍스트 제거된 분석용 본문
    paragraphs_raw   : byline 포함 원문 (재현용)
    paragraphs_byline: 각 문단의 byline 메타 (없으면 None)
    """
    if depth >= 1:
        head = div.find(q("head"))
        if head is not None:
            head_text = extract_text(head).strip()
            if head_text:
                headings.append({"level": depth + 1, "text": head_text})

    # 직계 p 수집
    # 주의: <p rend="heading N"> 은 본문이 아니라 소제목이므로 headings에 넣음
    for p in div.findall(q("p")):
        rend = p.get("rend", "")
        if rend.startswith("heading"):
            m = re.search(r"\d+", rend)
            level = int(m.group()) if m else (depth + 1)
            head_text = extract_text(p).strip()
            if head_text:
                headings.append({"level": level, "text": head_text})
        else:
            text_raw = extract_text(p).strip()
            text_clean = extract_text_excluding(p, {"byline"}).strip()
            byline_meta = extract_byline_meta(p)
            # p 끝의 권말 표지 문구 제거 (양쪽 문자열 모두에 적용)
            cover_re = re.compile(r"朱子語類[卷巻]第?[一二三四五六七八九十百]+\s*$")
            text_raw = cover_re.sub("", text_raw).rstrip()
            text_clean = cover_re.sub("", text_clean).rstrip()
            # 정규식 byline 추가 정제 (祝平次 XML이 <byline> 태그를 안 쓰는 경우 대비)
            text_clean, byline_meta = cleanup_text_byline(text_clean, byline_meta)
            if text_clean:
                paragraphs.append(text_clean)
                paragraphs_raw.append(text_raw)
                paragraphs_byline.append(byline_meta)

    # 직계 list/item 수집 (주로 姓氏 섹션)
    for lst in div.findall(q("list")):
        for item in lst.findall(q("item")):
            text_raw = extract_text(item).strip()
            text_clean = extract_text_excluding(item, {"byline"}).strip()
            byline_meta = extract_byline_meta(item)
            text_clean, byline_meta = cleanup_text_byline(text_clean, byline_meta)
            if text_clean:
                paragraphs.append(text_clean)
                paragraphs_raw.append(text_raw)
                paragraphs_byline.append(byline_meta)

    # 재귀 탐색
    for subdiv in div.findall(q("div")):
        collect_content(
            subdiv, depth + 1, headings,
            paragraphs, paragraphs_raw, paragraphs_byline,
        )


def make_record(
    juan_label: str,
    headings: list[dict],
    paragraphs: list[str],
    paragraphs_raw: list[str],
    paragraphs_byline: list[dict | None],
) -> dict:
    """단일 레코드 dict 생성. paragraphs/raw/byline은 같은 인덱스로 정렬됨."""
    # 祝平次 XML의 일부 권(卷三十四, 卷七十五)에 동일 문단이 70여 회 반복되는
    # 데이터 파손이 있음. 5회 이상 반복되는 문단은 첫 등장만 남겨 중복 제거.
    # paragraphs(분석용 본문) 기준으로 dedupe하되, raw/byline도 동기 제거.
    from collections import Counter
    counts = Counter(paragraphs)
    seen: set[str] = set()
    cleaned: list[str] = []
    cleaned_raw: list[str] = []
    cleaned_byline: list[dict | None] = []
    removed = 0
    for p, r, b in zip(paragraphs, paragraphs_raw, paragraphs_byline):
        if counts[p] >= 5 and p in seen:
            removed += 1
            continue
        cleaned.append(p)
        cleaned_raw.append(r)
        cleaned_byline.append(b)
        seen.add(p)
    if removed > 0:
        print(f"  [DEDUPE] {juan_label}: {removed}개 중복 문단 제거 "
              f"(원본 {len(paragraphs)} → {len(cleaned)})")

    return {
        "juan_num": parse_juan_num(juan_label),
        "juan_label": juan_label,
        "source": "zhuzi_tei",
        "headings": headings,
        "paragraphs": cleaned,
        "paragraphs_raw": cleaned_raw,
        "paragraphs_byline": cleaned_byline,
        "raw_concat": "".join(cleaned),
    }


# ---------- 메인 ----------

def parse_xml() -> list[dict]:
    if not XML_PATH.exists():
        print(f"[ERROR] XML 파일 없음: {XML_PATH}", file=sys.stderr)
        print("        XML 파일을 해당 경로에 두어야 함", file=sys.stderr)
        sys.exit(1)

    print(f"[PARSE] {XML_PATH.name} 읽는 중...")
    tree = etree.parse(str(XML_PATH))
    body = tree.getroot().find(".//tei:body", NS)
    top_divs = body.findall("tei:div", NS)
    print(f"        top-level div: {len(top_divs)}개")

    records: list[dict] = []

    for top_div in top_divs:
        head_elem = top_div.find(q("head"))
        juan_label = head_elem.text.strip() if head_elem is not None and head_elem.text else "(unknown)"

        if juan_label == "提要":
            # 특수 케이스: 提要 div는 4개 섹션으로 분리
            # (1) 提要 본문 (직계 p만)
            headings: list[dict] = []
            paragraphs: list[str] = []
            paragraphs_raw: list[str] = []
            paragraphs_byline: list[dict | None] = []
            for p in top_div.findall(q("p")):
                t_raw = extract_text(p).strip()
                t_clean = extract_text_excluding(p, {"byline"}).strip()
                bl_meta = extract_byline_meta(p)
                t_clean, bl_meta = cleanup_text_byline(t_clean, bl_meta)
                if t_clean:
                    paragraphs.append(t_clean)
                    paragraphs_raw.append(t_raw)
                    paragraphs_byline.append(bl_meta)
            records.append(make_record(
                "提要", headings, paragraphs, paragraphs_raw, paragraphs_byline,
            ))

            # (2) 내부 div들 각각 레코드로 (原序, 門目, 姓氏)
            for sub in top_div.findall(q("div")):
                sub_head = sub.find(q("head"))
                sub_label_raw = extract_text(sub_head).strip() if sub_head is not None else ""
                sub_label = sub_label_raw.replace("朱子語類", "")

                headings, paragraphs = [], []
                paragraphs_raw, paragraphs_byline = [], []
                collect_content(
                    sub, 0, headings,
                    paragraphs, paragraphs_raw, paragraphs_byline,
                )
                records.append(make_record(
                    sub_label, headings,
                    paragraphs, paragraphs_raw, paragraphs_byline,
                ))
        else:
            # 일반 권
            headings, paragraphs = [], []
            paragraphs_raw, paragraphs_byline = [], []
            collect_content(
                top_div, 0, headings,
                paragraphs, paragraphs_raw, paragraphs_byline,
            )
            records.append(make_record(
                juan_label, headings,
                paragraphs, paragraphs_raw, paragraphs_byline,
            ))

    return records


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    records = parse_xml()

    # 저장
    with OUT_PATH.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    # 검증
    total_paragraphs = sum(len(r["paragraphs"]) for r in records)
    total_chars = sum(len(r["raw_concat"]) for r in records)
    total_with_byline = sum(
        1 for r in records for bl in r["paragraphs_byline"] if bl is not None
    )
    failed = [r for r in records if r["juan_num"] == -1]

    print(f"[DONE] 저장: {OUT_PATH}")
    print(f"       레코드 수: {len(records)}")
    print(f"       문단 수:   {total_paragraphs:,}")
    print(f"       총 글자:   {total_chars:,}  (표점 포함, byline 제거 후)")
    print(f"       byline 분리 문단: {total_with_byline:,} "
          f"({total_with_byline / max(1, total_paragraphs) * 100:.1f}%)")
    if failed:
        print(f"[WARN] juan_num 파싱 실패: {[r['juan_label'] for r in failed]}", file=sys.stderr)

    # 권 번호 누락 검사
    nums = sorted({r["juan_num"] for r in records if r["juan_num"] > 0})
    missing = [i for i in range(1, 141) if i not in nums]
    if missing:
        print(f"[WARN] 누락된 권: {missing}", file=sys.stderr)

    # 샘플: 卷一
    juan_one = next((r for r in records if r["juan_label"] == "卷一"), None)
    if juan_one:
        print(f"\n[SAMPLE] 卷一")
        print(f"  juan_num: {juan_one['juan_num']}")
        print(f"  headings ({len(juan_one['headings'])}개): {juan_one['headings'][:4]}")
        print(f"  문단 수: {len(juan_one['paragraphs'])}")
        print(f"  첫 문단: {juan_one['paragraphs'][0][:70]}...")


if __name__ == "__main__":
    main()
