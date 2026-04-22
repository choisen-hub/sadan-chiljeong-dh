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
      "paragraphs": [                # 문단 리스트
        "問太極不是未有天地之先有箇渾成之物...",
        "問昨謂未有天地之先畢竟是先有理如何曰..."
      ],
      "raw_concat": "問太極...又輕之嘗有簡"  # 문단 전체 이어붙인 것 (비교용)
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

주의:
  - 화자 표시 (淳/) 등 괄호 주석은 그대로 보존 (祝平次와의 비교 위해)
  - 이체자도 그대로 보존

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

# 표지 텍스트 (문단/제목 모두 아닌 것)
COVER_PATTERNS = [
    re.compile(r"^欽定四庫全書"),      # 시리즈 표지
    re.compile(r"^朱子語類卷[一二三四五六七八九十百]+$"),  # 책 제목
    re.compile(r"^朱子語類提要$"),      # 제요 제목
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
    paragraphs: list[str] = []
    current_para: list[str] = []

    def flush_para() -> None:
        if current_para:
            paragraphs.append("".join(current_para))
            current_para.clear()

    def flush_section() -> None:
        """현재 섹션을 레코드로 확정하고 상태 초기화."""
        nonlocal headings, paragraphs
        flush_para()
        if juan_label:  # 라벨이 있어야 저장
            records.append({
                "juan_num": parse_juan_num(juan_label),
                "juan_label": juan_label,
                "file_name": path.name,
                "headings": headings,
                "paragraphs": paragraphs,
                "raw_concat": "".join(paragraphs),
            })
        headings = []
        paragraphs = []

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

    for path in files:
        file_records = parse_file(path)
        for rec in file_records:
            records.append(rec)
            total_paragraphs += len(rec["paragraphs"])
            total_chars += len(rec["raw_concat"])

    # JSONL 저장
    with OUT_PATH.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    # 검증 출력
    print(f"[DONE] 저장: {OUT_PATH}")
    print(f"       파일 수:   {len(files)}")
    print(f"       레코드 수: {len(records)}  (000 파일은 4섹션으로 분리)")
    print(f"       문단 수:   {total_paragraphs:,}")
    print(f"       총 글자:   {total_chars:,}")

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
