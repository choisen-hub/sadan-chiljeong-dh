"""
40_pipeline_figure.py

그림 4-1: 전체 분석 프로세스 개념도 생성.
Phase 0(데이터 수집·전처리·기초 분포 분석)을 외곽 박스로 묶고,
Phase 1·Phase 2·LLM 판정이 RQ1~RQ3에 대응하는 구조.

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


def main():
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(18, 10.6), dpi=100)
    ax.set_xlim(0, 18); ax.set_ylim(-0.45, 10.15); ax.axis("off")

    C_COLLECT = "#EAF2FA"; C_PREP = "#FDF3E3"; C_DATA = "#EDF7ED"; C_STAT = "#FBEFF2"
    C_PHASE0_BG = "#F8F7FC"; C_PHASE = "#F4EEFA"; C_RQ = "#FFFFFF"
    EDGE = "#666666"; P0_EDGE = "#9B8BB4"; PH_COLOR = "#4A2A7A"

    def box(x, y, w, h, title, lines, fc, title_fs=14.5, body_fs=11.8, ec=EDGE, lw=1.4):
        ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.07,rounding_size=0.12",
                                    fc=fc, ec=ec, lw=lw))
        if lines:
            ax.text(x + w/2, y + h - 0.40, title, ha="center", va="center",
                    fontsize=title_fs, fontweight="bold", color="#222222")
            for i, ln in enumerate(lines):
                ax.text(x + w/2, y + h - 0.90 - i*0.40, ln, ha="center", va="center",
                        fontsize=body_fs, color="#333333")
        elif title:
            ax.text(x + w/2, y + h/2, title, ha="center", va="center",
                    fontsize=title_fs, fontweight="bold", color="#222222")

    def arrow(x1, y1, x2, y2):
        ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>",
                                     mutation_scale=20, lw=2.0, color="#555555"))

    # ════ Phase 0 외곽 박스 ════
    ax.add_patch(FancyBboxPatch((0.3, 4.45), 17.4, 5.35,
                                boxstyle="round,pad=0.10,rounding_size=0.18",
                                fc=C_PHASE0_BG, ec=P0_EDGE, lw=1.8))
    ax.text(0.62, 9.42, "Phase 0", fontsize=16, fontweight="bold", color=PH_COLOR,
            ha="left", va="center")
    ax.text(2.42, 9.42, "데이터 구축과 기초 분석", fontsize=13.5, color="#555555",
            ha="left", va="center")

    def stage(xc, label):
        ax.text(xc, 8.72, label, ha="center", va="center",
                fontsize=13.5, fontweight="bold", color="#1A4E7A")

    # 내부 4단: 수집 → 전처리 → 분석 데이터 → 기초 분포 분석
    stage(2.55, "데이터 수집 (4.1)")
    box(0.75, 6.85, 3.6, 1.55, "주자어류",
        ["칸리포(Kanripo)", "KR3a0047 원문 XML"], C_COLLECT, body_fs=11.2)
    box(0.75, 4.85, 3.6, 1.55, "퇴계·율곡 서신",
        ["공공데이터포털", "한국문집총간 XML"], C_COLLECT, body_fs=11.2)

    stage(7.0, "데이터 전처리 (4.2)")
    box(5.15, 5.55, 3.7, 2.15, "자동 표점·문장 분리",
        ["SikuRoBERTa-PUNC-AJD-KLC", "이체자 처리·판본 검증", "문장 단위 통일"], C_PREP, body_fs=11.0)

    stage(11.45, "분석 데이터")
    box(9.65, 5.55, 3.6, 2.15, "최종 데이터 (표 4-1)",
        ["주자어류 71,645문장", "(理 포함 8,443)", "서신 2,081문장"], C_DATA, body_fs=11.2)

    stage(15.75, "기초 분포 분석")
    box(14.05, 5.55, 3.4, 2.15, "핵심 글자 분포 정량화",
        ["理·氣 등 출현 빈도와", "공기 패턴 집계", "(표 5-1)"], C_STAT, title_fs=13.5, body_fs=11.2)

    arrow(4.42, 7.6, 5.05, 7.1)
    arrow(4.42, 5.65, 5.05, 6.2)
    arrow(8.92, 6.6, 9.55, 6.6)
    arrow(13.32, 6.6, 13.95, 6.6)

    # Phase 0 → 본 분석
    arrow(9.0, 4.32, 9.0, 3.62)

    # ════ 본 분석 3행 → RQ ════
    rows = [
        ("Phase 1", "理 토큰 임베딩 비지도 군집화 (SikuBERT, K-means·HDBSCAN)", "RQ1"),
        ("Phase 2", "형태론적 인용 매칭 (LCS × IDF): 퇴계·율곡 서신 ↔ 주자어류", "RQ2"),
        ("LLM 판정", "다중 LLM 문장별 판정 (gpt-4.1-mini · gemini-2.5-flash, temperature=0)", "RQ3"),
    ]
    ROW_H, GAP, Y0 = 0.92, 0.22, 0.0
    for i, (ph, desc, rqlab) in enumerate(rows):
        y = Y0 + (len(rows) - 1 - i) * (ROW_H + GAP)
        yc = y + ROW_H / 2
        box(0.4, y, 14.0, ROW_H, "", [], C_PHASE)
        ax.text(0.78, yc, ph, fontsize=14, fontweight="bold", color=PH_COLOR, va="center")
        ax.text(2.75, yc, desc, fontsize=12.3, va="center")
        box(15.4, y, 2.2, ROW_H, rqlab, [], C_RQ, title_fs=15)
        ax.text(16.5, yc, "", ha="center")
        # RQ 라벨 색 보정: box()가 검정으로 그렸으니 덧칠
        ax.text(16.5, yc, rqlab, ha="center", va="center", fontsize=15,
                fontweight="bold", color="#7A2A2A",
                bbox=dict(boxstyle="square,pad=0.08", fc=C_RQ, ec="none"))
        arrow(14.55, yc, 15.32, yc)

    plt.tight_layout(pad=0.4)
    plt.savefig(OUTPUT, dpi=100, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"Saved {OUTPUT}")


if __name__ == "__main__":
    main()
