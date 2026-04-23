"""
test_05_punctuate.py

05_punctuate.py의 알고리즘을 작은 toy 예제로 검증한다.
실제 데이터 없이 단독 실행 가능.

사용법:
  python scripts/test_05_punctuate.py
"""

from __future__ import annotations
import sys
from pathlib import Path

# 같은 scripts 디렉토리의 05_punctuate 임포트
sys.path.insert(0, str(Path(__file__).parent))
# 모듈명에 숫자 시작 불가 → 재작성 없이 importlib 사용
import importlib.util
spec = importlib.util.spec_from_file_location(
    "punctuate05", Path(__file__).parent / "05_punctuate.py"
)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


def run_case(name: str, k_full: str, z_full_with_punct: str, expect_out: str,
             expect_fallback_count: int = 0) -> None:
    out, prov, k2o, fb = mod.punctuate_juan(k_full, z_full_with_punct, juan_label=name)
    ok_out = out == expect_out
    ok_fb = len(fb) == expect_fallback_count
    # provenance 길이 검증
    ok_prov = len(prov) == len(out)
    # k_to_out 단조성 + 센티널
    ok_mono = all(k2o[i] <= k2o[i+1] for i in range(len(k2o)-1))
    ok_sent = k2o[-1] == len(out)

    status = "OK" if (ok_out and ok_fb and ok_prov and ok_mono and ok_sent) else "FAIL"
    print(f"[{status}] {name}")
    print(f"        k_full = {k_full!r}")
    print(f"        z_full = {z_full_with_punct!r}")
    print(f"        got    = {out!r}")
    print(f"        prov   = {prov!r}")
    print(f"        expect = {expect_out!r}")
    print(f"        fallback={len(fb)} (expect {expect_fallback_count})")
    if fb:
        for sp in fb:
            print(f"          - {sp['type']}: k[{sp['k_start']}:{sp['k_end']}]="
                  f"{sp['k_text']!r}, z={sp['z_text']!r}")
    if not (ok_out and ok_fb and ok_prov and ok_mono and ok_sent):
        print(f"        [diagnostics] ok_out={ok_out}, ok_fb={ok_fb}, "
              f"ok_prov={ok_prov}, ok_mono={ok_mono}, ok_sent={ok_sent}")
    print()


def main() -> None:
    # --- 경우 1: equal, 순수 표점 차용 ---
    run_case(
        "case1_equal",
        k_full="學而時習之不亦說乎",
        z_full_with_punct="學而時習之，不亦說乎？",
        expect_out="學而時習之，不亦說乎？",
    )

    # --- 경우 2: 1자 이체자 (len 일치 replace) ---
    run_case(
        "case2a_variant_1char",
        k_full="傳曰民為貴",                # 칸리포: 傳
        z_full_with_punct="𫝊曰，民為貴。",  # 祝平次: 𫝊 (이체자)
        expect_out="傳曰，民為貴。",         # 칸리포 글자 + 祝平次 표점
    )

    # --- 경우 2b: 2자 연속 이체자 ---
    run_case(
        "case2b_variant_2char",
        k_full="别録載之",                    # 칸리포
        z_full_with_punct="別錄，載之。",    # 祝平次
        expect_out="别録，載之。",
    )

    # --- 경우 4: 짧은 결자 (delete len <= 3), 표점 없이 통과 ---
    # 실제 데이터 예: "中缺二十餘卷" (k) / "中缺十餘卷" (z), '二'가 칸리포 고유
    run_case(
        "case4_short_delete",
        k_full="中缺二十餘卷眀年",
        z_full_with_punct="中缺十餘卷。眀年",
        expect_out="中缺二十餘卷。眀年",
    )

    # --- 경우 5: 祝平次 고유 insert, 출력에 미반영 ---
    run_case(
        "case5_insert_ignored",
        k_full="甲乙丙丁",
        z_full_with_punct="甲乙丙注釋丁。",  # 祝平次에만 '注釋'이 있음
        expect_out="甲乙丙丁。",             # 칸리포는 그대로, 祝平次 표점은 가져옴
    )

    # --- 경우 3a: len 불일치 replace (hanja.dev 대상) ---
    # 큰 판본 차이: 칸리포 짧은 구간, 祝平次 긴 구간
    run_case(
        "case3a_replace_lendiff",
        k_full="前A後段",
        z_full_with_punct="前甲乙丙丁戊，後段。",
        expect_out="前A後段。",  # A는 표점 없이 통과, 後 뒤의 。만 祝平次에서 옴
        expect_fallback_count=1,
    )

    # --- 경우 3b: 긴 delete (hanja.dev 대상) ---
    # 칸리포에만 5자 덩어리가 있는 경우
    run_case(
        "case3b_delete_large",
        k_full="前ABCDE後段",
        z_full_with_punct="前，後段。",
        expect_out="前，ABCDE後段。",
        expect_fallback_count=1,
    )

    # --- 경우 6: 이체자 + 인접 결자 병합 ---
    # SequenceMatcher가 '𫝊甲乙丙'(4자)를 '傳'(1자)과 하나의 replace로 묶는다.
    # 결과: 전체 구간이 replace_lendiff → 경우 3(fallback)으로 분류.
    # 祝平次의 '傳' 뒤 표점 '，'은 fallback 구간 안에 묻혀 버려진다 (hanja.dev가 복원).
    # 이는 알고리즘의 의도된 동작. 독립적인 이체자 분리는 시도하지 않음.
    run_case(
        "merged_variant_and_delete",
        k_full="學而時𫝊甲乙丙之不亦說乎",
        z_full_with_punct="學而時傳，之不亦說乎？",
        expect_out="學而時𫝊甲乙丙之不亦說乎？",  # 중간 , 없음 (fallback으로 흡수)
        expect_fallback_count=1,
    )

    # --- 문단 split 테스트 ---
    k_full = "學而時習之不亦說乎有朋自遠方來不亦樂乎"
    z_full = "學而時習之，不亦說乎？有朋自遠方來，不亦樂乎？"
    k_paragraphs = ["學而時習之不亦說乎", "有朋自遠方來不亦樂乎"]
    out, prov, k2o, fb = mod.punctuate_juan(k_full, z_full, "split_test")
    paras, provs = mod.split_to_paragraphs(out, prov, k2o, k_paragraphs)
    expect_paras = ["學而時習之，不亦說乎？", "有朋自遠方來，不亦樂乎？"]
    ok = paras == expect_paras
    print(f"[{'OK' if ok else 'FAIL'}] split_to_paragraphs")
    print(f"        got    = {paras}")
    print(f"        expect = {expect_paras}")
    print(f"        prov lengths = {[len(p) for p in provs]} / text lengths = {[len(p) for p in paras]}")
    print()


if __name__ == "__main__":
    main()
