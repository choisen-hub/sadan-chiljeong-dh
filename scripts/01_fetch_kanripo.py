"""
01_fetch_kanripo.py

칸리포(Kanseki Repository)에서 주자어류(朱子語類, KR3a0047) 원본을 수집한다.

저본: 文淵閣 四庫全書 (WYG)
원본 URL: https://github.com/kanripo/KR3a0047
라이선스: CC BY-SA 4.0

동작:
  1. git clone으로 레포 전체를 받아옴 (깊이 1, 경량)
  2. .txt 파일 개수와 용량 검증
  3. 커밋 해시, 수집 시각 등 메타데이터를 JSON으로 기록

출력:
  data/raw/kanripo_KR3a0047/      원본 .txt 141개 + Readme.org
  data/raw/kanripo_metadata.json  수집 메타데이터 (재현성)

사용법:
  python scripts/01_fetch_kanripo.py
  python scripts/01_fetch_kanripo.py --refetch   # 기존 것 덮어쓰기
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

# ---------- 설정 ----------

REPO_URL = "https://github.com/kanripo/KR3a0047.git"
TEXT_ID = "KR3a0047"
TITLE = "朱子語類"
BASE_EDITION = "文淵閣 四庫全書 (WYG)"
EXPECTED_FILE_COUNT = 141  # 000(提要·原序·門目·姓氏) + 001~140

# 프로젝트 루트 기준 경로 (이 스크립트는 scripts/ 안에 있음)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
CLONE_DIR = RAW_DIR / f"kanripo_{TEXT_ID}"
METADATA_PATH = RAW_DIR / "kanripo_metadata.json"


# ---------- 유틸 ----------

def run(cmd: list[str], cwd: Path | None = None) -> str:
    """서브프로세스 실행. 실패 시 에러 메시지와 함께 종료."""
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"[ERROR] Command failed: {' '.join(cmd)}", file=sys.stderr)
        print(result.stderr, file=sys.stderr)
        sys.exit(1)
    return result.stdout.strip()


def get_commit_hash(repo_dir: Path) -> str:
    """현재 체크아웃된 커밋의 전체 해시."""
    return run(["git", "rev-parse", "HEAD"], cwd=repo_dir)


def get_dir_size_bytes(path: Path) -> int:
    """디렉토리 총 용량 (바이트)."""
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


# ---------- 메인 ----------

def fetch(refetch: bool = False) -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    # 이미 있으면 건너뛰기 (재현성: 기본적으로 한 번 받은 것 유지)
    if CLONE_DIR.exists() and not refetch:
        print(f"[SKIP] 이미 존재: {CLONE_DIR}")
        print("       강제 재수집하려면 --refetch 옵션 사용")
        return

    if CLONE_DIR.exists() and refetch:
        print(f"[INFO] 기존 디렉토리 제거: {CLONE_DIR}")
        shutil.rmtree(CLONE_DIR)

    # Clone
    print(f"[FETCH] {REPO_URL}")
    print(f"        → {CLONE_DIR}")
    run(
        ["git", "clone", "--depth", "1", REPO_URL, str(CLONE_DIR)],
    )

    # 검증: 파일 개수
    txt_files = sorted(CLONE_DIR.glob("*.txt"))
    file_count = len(txt_files)
    print(f"[CHECK] .txt 파일 개수: {file_count}")

    if file_count != EXPECTED_FILE_COUNT:
        print(
            f"[WARN] 예상 {EXPECTED_FILE_COUNT}개와 다름. "
            "업스트림 구조 변경 가능성.",
            file=sys.stderr,
        )

    # 검증: 파일명 규칙 (KR3a0047_NNN.txt)
    missing = []
    for i in range(EXPECTED_FILE_COUNT):
        expected_name = f"{TEXT_ID}_{i:03d}.txt"
        if not (CLONE_DIR / expected_name).exists():
            missing.append(expected_name)
    if missing:
        print(f"[WARN] 누락 파일 {len(missing)}개: {missing[:5]}...", file=sys.stderr)

    # 메타데이터 기록
    metadata = {
        "text_id": TEXT_ID,
        "title": TITLE,
        "base_edition": BASE_EDITION,
        "source_url": REPO_URL.removesuffix(".git"),
        "commit_hash": get_commit_hash(CLONE_DIR),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "file_count": file_count,
        "total_size_bytes": get_dir_size_bytes(CLONE_DIR),
        "license": "CC BY-SA 4.0",
    }

    METADATA_PATH.write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"[DONE] 메타데이터 저장: {METADATA_PATH}")
    print(f"       commit: {metadata['commit_hash'][:8]}")
    print(f"       size:   {metadata['total_size_bytes']:,} bytes")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--refetch",
        action="store_true",
        help="기존 디렉토리를 삭제하고 다시 받음",
    )
    args = parser.parse_args()
    fetch(refetch=args.refetch)


if __name__ == "__main__":
    main()
