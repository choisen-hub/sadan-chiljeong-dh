"""
02_parse_kanripo.py

칸리포 주자어류 원본(Mandoku .txt 포맷)을 구조화된 JSONL로 변환한다.

입력:
  data/raw/kanripo_KR3a0047/KR3a0047_NNN.txt  (01번에서 받은 원본)

출력:
  data/intermediate/kanripo_parsed.jsonl
  한 줄에 한 권(卷). 각 레코드 구조:
    {
      "juan_num": 1,                 # 권 번호 (0 = 提要)
      "juan_label": "卷一",           # 원본 라벨
      "file_name": "KR3a0047_001.txt",
      "headings": [
        {"level": 2, "text": "理氣上"},
        {"level": 3, "text": "太極天地上"}
      ],
      "paragraphs": [                # 문단 본문 (byline 제거됨, 분석용)
        "問太極不是未有天地之先有箇渾成之物...",
        "問昨謂未有天地之先畢竟是先有理如何曰..."
      ],
      "paragraphs_raw": [             # byline 포함 원문 (재현용)
        "問太極不是未有天地之先有箇渾成之物...(僴/)",
        "問昨謂未有天地之先畢竟是先有理如何曰...(賀孫/)"
      ],
      "paragraphs_byline": [          # 각 문단의 byline 메타 (없으면 null)
        {"raw": "(僴/)"},
        {"raw": "(賀孫/)"}
      ],
      "raw_concat": "問太極...又輕之嘗有簡"  # paragraphs(byline 제거본) 이어붙임
    }

처리 규칙:
  - 헤더 라인 (#, #+로 시작) 제거
  - <pb:...> 페이지 태그 제거
  - ¶ 행 구분자 제거
  - 표지 텍스트 제거 ("欽定四庫全書", "朱子語類卷X")
  - 들여쓰기 패턴으로 구조 판별:
      0칸: 본문 문단 시작
      1칸(전각공백 1개): 본문 이어짐 (앞 문단과 합침)
      2~4칸: 제목 (편/절/소절) → headings에 기록, 본문에서 제외

화자표시(byline) 처리:
  - 칸리포 () 안 패턴 100%가 슬래시 포함. 두 종류로 갈림:
      (a) 8자 이하 + '/' 포함 → byline 후보, 약 12,600건
      (b) 그 외                → 편집 주석, 본문에 그대로 둠
  - paragraph 끝에 붙은 (a)는 raw 그대로 paragraphs_byline에 보존하고
    paragraphs(분석용 본문)에서는 제거. 누가 기록자인지는 식별하지 않음.
    목적은 (i) utterance 경계 신호, (ii) 본문 임베딩 시 noise 제거.
  - paragraphs_raw는 손대지 않은 원문.

주의:
  - 이체자는 그대로 보존

사용법:
  python scripts/02_parse_kanripo.py
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

# ---------- 경로 ----------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "kanripo_KR3a0047"
OUT_DIR = PROJECT_ROOT / "data" / "intermediate"
OUT_PATH = OUT_DIR / "kanripo_parsed.jsonl"


# ---------- 상수/정규식 ----------

FULLWIDTH_SPACE = "\u3000"  # 전각 공백 '　'

# 한자 숫자 → 아라비아 숫자 (권 번호 파싱용)
HANZI_NUM = {
    "一": 1, "二": 2, "三": 3, "四": 4, "五": 5,
    "六": 6, "七": 7, "八": 8, "九": 9,
    "十": 10, "百": 100,
}

# 제거 대상 패턴
RE_PB = re.compile(r"<pb:[^>]+>")
RE_JUAN_PROPERTY = re.compile(r"^#\+PROPERTY:\s*JUAN\s+(.+?)\s*$")

# Byline / 괄호주석 패턴.
# 칸리포 데이터 전수 조사 결과 () 안은 100% '/' 포함.
# 길이 8자 이하 + '/' 포함 → 발화 기록자(speaker)로 분류 (~14,039건)
# 길이 9자 이상         → 편집 주석(note)으로 분류 (~2,267건)
RE_PAREN = re.compile(r"\(([^()]{1,80})\)")
BYLINE_MAX_LEN = 8  # 8자 이하 + 슬래시 포함 → byline 후보


def extract_trailing_byline(text: str) -> tuple[str, dict | None]:
    """
    문단 끝에 붙은 byline을 본문에서 떼어낸다.

    목적은 두 가지뿐:
      1. utterance 경계 신호 (이 paragraph는 byline으로 닫혔음)
      2. 본문 임베딩/매칭 시 noise가 되는 byline 텍스트 제거

    기록자가 누구인지는 식별하지 않는다. raw 보존만 한다.

    매칭 조건: 끝에 붙은 () + 안쪽이 8자 이하 + 슬래시 포함
    (章節 안내 「以下五常」 등 false positive가 일부 섞일 수 있으나,
     본문 정제 측면에서는 어차피 빼는 편이 안전하므로 무시)
    """
    m = re.search(r"\(([^()]{1,80})\)\s*$", text)
    if not m:
        return text, None
    inner = m.group(1)
    if "/" not in inner or len(inner) > BYLINE_MAX_LEN:
        return text, None
    cleaned = text[: m.start()].rstrip()
    return cleaned, {"raw": m.group(0)}

# 표지 텍스트 (문단/제목 모두 아닌 것)
COVER_PATTERNS = [
    re.compile(r"^欽定四庫全書"),
    # 卷/巻 이체자 모두, 第 유무 모두, 한자숫자 (오식 "二百十五" 포함)
    re.compile(r"^朱子語類[卷巻]第?[一二三四五六七八九十百]+$"),
    re.compile(r"^朱子語類提要$"),
    re.compile(r"^朱子語類原序$"),
    re.compile(r"^朱子語類門目$"),
    re.compile(r"^朱子語類姓氏$"),
    # 권0 첫 줄: "朱子語類　　　　　　　儒家類" 형식 (전각공백 섞임)
    re.compile(r"^朱子語類[\u3000\s]+儒家類$"),
]


# ---------- 유틸 ----------

def parse_juan_num(label: str) -> int:
    """
    '卷一' → 1, '卷十二' → 12, '卷一百四十' → 140, '提要' → 0
    """
    if "提要" in label or "門目" in label or "姓氏" in label or "原序" in label:
        return 0

    # '卷' 이후 한자 숫자 추출
    m = re.match(r"卷([一二三四五六七八九十百]+)", label)
    if not m:
        return -1  # 파싱 실패 표시

    s = m.group(1)
    # 한자 숫자 파싱: 一百四十, 十二, 二十, 一百 등
    total = 0
    current = 0
    for ch in s:
        val = HANZI_NUM.get(ch, 0)
        if val >= 10:  # 十(10), 百(100) 등
            current = 1 if current == 0 else current
            total += current * val
            current = 0
        else:
            current = val
    total += current
    return total


def count_leading_fw_spaces(line: str) -> int:
    """전각 공백 들여쓰기 개수."""
    n = 0
    for ch in line:
        if ch == FULLWIDTH_SPACE:
            n += 1
        else:
            break
    return n


def is_cover_line(text: str) -> bool:
    """표지 텍스트(四庫全書, 책 제목 등)인지."""
    return any(p.match(text) for p in COVER_PATTERNS)


# ---------- 파싱 ----------

def parse_file(path: Path) -> list[dict]:
    """
    한 파일을 파싱하여 레코드 리스트 반환.
    대부분 1개 레코드지만, 000 파일처럼 여러 JUAN을 포함하면 여러 개 반환.
    """
    lines = path.read_text(encoding="utf-8").splitlines()

    records: list[dict] = []

    # 현재 섹션 상태
    juan_label = ""
    headings: list[dict] = []
    paragraphs_raw: list[str] = []      # byline 그대로 포함된 원문
    current_para: list[str] = []

    def flush_para() -> None:
        if current_para:
            paragraphs_raw.append("".join(current_para))
            current_para.clear()

    def flush_section() -> None:
        """현재 섹션을 레코드로 확정하고 상태 초기화."""
        nonlocal headings, paragraphs_raw
        flush_para()
        if juan_label:
            # paragraphs_raw → (paragraphs 본문, paragraphs_byline) 분리
            paragraphs: list[str] = []
            paragraphs_byline: list[dict | None] = []
            for raw_para in paragraphs_raw:
                cleaned, meta = extract_trailing_byline(raw_para)
                paragraphs.append(cleaned)
                paragraphs_byline.append(meta)
            records.append({
                "juan_num": parse_juan_num(juan_label),
                "juan_label": juan_label,
                "file_name": path.name,
                "headings": headings,
                "paragraphs": paragraphs,
                "paragraphs_raw": paragraphs_raw,
                "paragraphs_byline": paragraphs_byline,
                "raw_concat": "".join(paragraphs),
            })
        headings = []
        paragraphs_raw = []

    for raw in lines:
        # 1. 헤더 라인 처리
        if raw.startswith("#"):
            m = RE_JUAN_PROPERTY.match(raw)
            if m:
                # 새 JUAN 섹션 시작 → 이전 섹션 flush
                flush_section()
                juan_label = m.group(1)
            continue

        # 2. 페이지 태그, 행 구분자 제거
        cleaned = RE_PB.sub("", raw).replace("¶", "").rstrip()

        # 3. 빈 줄 → 문단 경계
        if not cleaned:
            flush_para()
            continue

        # 4. 들여쓰기 카운트
        indent = count_leading_fw_spaces(cleaned)
        content = cleaned[indent:]

        # 5. 표지 라인 제거
        if is_cover_line(content):
            flush_para()
            continue

        # 6. 구조 판별
        if indent == 0:
            flush_para()
            current_para.append(content)
        elif indent == 1:
            current_para.append(content)
        elif 2 <= indent <= 4:
            # 提要/原序/門目/姓氏는 들여쓰기 4칸이 본문
            if juan_label in ("提要", "原序", "門目", "姓氏") and indent == 4:
                current_para.append(content)
            else:
                flush_para()
                headings.append({"level": indent, "text": content})
        else:
            # 5칸 이상: 방어적으로 본문 처리
            flush_para()
            current_para.append(content)

    # 파일 끝 - 마지막 섹션 flush
    flush_section()

    return records


# ---------- 메인 ----------

def main() -> None:
    if not RAW_DIR.exists():
        print(f"[ERROR] 원본 디렉토리 없음: {RAW_DIR}", file=sys.stderr)
        print("        먼저 'python scripts/01_fetch_kanripo.py' 실행 필요", file=sys.stderr)
        sys.exit(1)

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # 파일 목록 수집 (번호 순)
    files = sorted(RAW_DIR.glob("KR3a0047_*.txt"))
    if not files:
        print(f"[ERROR] .txt 파일 없음: {RAW_DIR}", file=sys.stderr)
        sys.exit(1)

    print(f"[PARSE] 파일 {len(files)}개 처리 중...")

    records: list[dict] = []
    total_paragraphs = 0
    total_chars = 0
    total_with_byline = 0

    for path in files:
        file_records = parse_file(path)
        for rec in file_records:
            records.append(rec)
            total_paragraphs += len(rec["paragraphs"])
            total_chars += len(rec["raw_concat"])
            for bl in rec["paragraphs_byline"]:
                if bl is not None:
                    total_with_byline += 1

    # JSONL 저장
    with OUT_PATH.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    # 검증 출력
    print(f"[DONE] 저장: {OUT_PATH}")
    print(f"       파일 수:   {len(files)}")
    print(f"       레코드 수: {len(records)}  (000 파일은 4섹션으로 분리)")
    print(f"       문단 수:   {total_paragraphs:,}")
    print(f"       총 글자:   {total_chars:,} (byline 제거 후)")
    print(f"       byline 분리 문단: {total_with_byline:,} "
          f"({total_with_byline / max(1, total_paragraphs) * 100:.1f}%)")

    # 샘플 출력
    # 卷一 레코드 찾기
    juan_one = next((r for r in records if r["juan_label"] == "卷一"), records[0])
    print(f"\n[SAMPLE] {juan_one['juan_label']} ({juan_one['file_name']})")
    print(f"  juan_num: {juan_one['juan_num']}")
    print(f"  headings: {juan_one['headings']}")
    print(f"  첫 문단: {juan_one['paragraphs'][0][:60]}...")
    print(f"  문단 수: {len(juan_one['paragraphs'])}")


if __name__ == "__main__":
    main()
