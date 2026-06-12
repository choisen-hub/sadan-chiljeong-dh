"""
40_pipeline_figure.py

그림 4-1: 전체 분석 프로세스 개념도 생성.

출력:
  figures/fig_pipeline.png
"""

from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT = PROJECT_ROOT / "figures" / "fig_pipeline.png"

# 한글/한자 폰트: macOS(AppleGothic) → Noto CJK(리눅스) 폴백
for p in [
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
]:
    try:
        fm.fontManager.addfont(p)
    except Exception:
        pass
plt.rcParams["font.family"] = [
    "AppleGothic", "Apple SD Gothic Neo",
    "Noto Sans CJK KR", "Noto Sans CJK JP", "DejaVu Sans",
]
plt.rcParams["axes.unicode_minus"] = False

C_COLLECT = "#EAF2FA"; C_PREP = "#FDF3E3"; C_DATA = "#EDF7ED"
C_PHASE = "#F4EEFA"; C_RQ = "#FFFFFF"
EDGE = "#666666"


def main():
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(18, 10), dpi=100)
    ax.set_xlim(0, 18); ax.set_ylim(-0.45, 10); ax.axis("off")

    def box(x, y, w, h, title, lines, fc, title_fs=15, body_fs=12.5):
        ax.add_patch(FancyBboxPatch((x, y), w, h,
                                    boxstyle="round,pad=0.08,rounding_size=0.12",
                                    fc=fc, ec=EDGE, lw=1.4))
        ax.text(x + w/2, y + h - 0.42, title, ha="center", va="center",
                fontsize=title_fs, fontweight="bold", color="#222222")
        for i, ln in enumerate(lines):
            ax.text(x + w/2, y + h - 0.95 - i*0.42, ln, ha="center", va="center",
                    fontsize=body_fs, color="#333333")

    def arrow(x1, y1, x2, y2):
        ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>",
                                     mutation_scale=22, lw=2.0, color="#555555"))

    def stage(x, w, label):
        ax.text(x + w/2, 9.55, label, ha="center", va="center",
                fontsize=16, fontweight="bold", color="#1A4E7A")

    # ── 상단: 수집 → 전처리 → 분석 데이터 ──
    stage(0.4, 4.6, "데이터 수집 (4.1)")
    box(0.4, 7.0, 4.6, 2.1, "주자어류",
        ["칸리포(Kanripo) KR3a0047", "원문 XML 수집"], C_COLLECT)
    box(0.4, 4.5, 4.6, 2.1, "퇴계·율곡 서신",
        ["공공데이터포털", "한국고전번역원_한국문집총간 XML"], C_COLLECT, body_fs=11.8)

    stage(6.4, 4.6, "데이터 전처리 (4.2)")
    box(6.4, 5.6, 4.6, 2.6, "자동 표점·문장 분리",
        ["SikuRoBERTa-PUNC-AJD-KLC", "이체자 처리·판본 검증", "문장 단위 통일"], C_PREP)

    stage(12.6, 5.0, "분석 데이터")
    box(12.6, 5.6, 5.0, 2.6, "최종 데이터 (표 4-1)",
        ["주자어류 71,645문장 (理 8,443)", "퇴계 서신 1,305문장", "율곡 서신 776문장"], C_DATA)

    arrow(5.1, 8.0, 6.3, 7.4)
    arrow(5.1, 5.6, 6.3, 6.3)
    arrow(11.1, 6.9, 12.5, 6.9)
    arrow(15.1, 5.5, 15.1, 4.55)

    # ── 하단: 분석 파이프라인 → RQ ──
    ax.text(0.4 + 14.0/2, 3.85, "분석 파이프라인 (4.3)", ha="center", va="center",
            fontsize=16, fontweight="bold", color="#1A4E7A")
    box(0.4, 2.3, 14.0, 1.25, "", [], C_PHASE)
    ax.text(0.75, 2.92, "Phase 0–1", fontsize=14, fontweight="bold",
            color="#4A2A7A", va="center")
    ax.text(2.6, 2.92, "핵심 글자 분포 정량화 + 理 토큰 임베딩 비지도 군집화 "
                       "(SikuBERT, K-means·HDBSCAN)", fontsize=12.5, va="center")
    box(0.4, 1.15, 14.0, 0.95, "", [], C_PHASE)
    ax.text(0.75, 1.62, "Phase 2", fontsize=14, fontweight="bold",
            color="#4A2A7A", va="center")
    ax.text(2.6, 1.62, "형태론적 인용 매칭 (LCS × IDF) — 퇴계·율곡 서신 ↔ 주자어류",
            fontsize=12.5, va="center")
    box(0.4, 0.0, 14.0, 0.95, "", [], C_PHASE)
    ax.text(0.75, 0.47, "Phase 3", fontsize=14, fontweight="bold",
            color="#4A2A7A", va="center")
    ax.text(2.6, 0.47, "다중 LLM 문장별 판정 (gpt-4.1-mini · gemini-2.5-flash, "
                       "temperature=0)", fontsize=12.5, va="center")

    def rq(y, yc, label):
        box(15.4, y, 2.2, 0.95, "", [], C_RQ)
        ax.text(16.5, yc, label, ha="center", va="center",
                fontsize=15, fontweight="bold", color="#7A2A2A")

    rq(2.45, 2.92, "RQ1"); rq(1.15, 1.62, "RQ2"); rq(0.0, 0.47, "RQ3")
    arrow(14.5, 2.92, 15.3, 2.92)
    arrow(14.5, 1.62, 15.3, 1.62)
    arrow(14.5, 0.47, 15.3, 0.47)

    plt.tight_layout(pad=0.4)
    plt.savefig(OUTPUT, dpi=100, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"Saved {OUTPUT}")


if __name__ == "__main__":
    main()
