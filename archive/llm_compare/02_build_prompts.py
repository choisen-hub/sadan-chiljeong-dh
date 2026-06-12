"""
LLM 비교 실험용 프롬프트 4개 파일을 생성한다.

산출물 (archive/llm_compare/data/prompts/):
    prompts_dataX.txt              데이터 미제시 (모든 모델 × N=3 공통)
    prompts_dataO_seed42.txt       데이터 제시 (seed=42 표본 200문장 포함)
    prompts_dataO_seed123.txt      데이터 제시 (seed=123 표본 200문장 포함)
    prompts_dataO_seed456.txt      데이터 제시 (seed=456 표본 200문장 포함)

각 모델 웹 UI에서 이 4개 파일 내용을 그대로 복붙하여 실험을 진행한다.
GitHub 저장소 공개를 통해 18개 셀에 던진 프롬프트 텍스트의 완전 재현을 보장한다.
"""

import os

SAMPLE_DIR = 'archive/llm_compare/data'
OUTDIR = 'archive/llm_compare/data/prompts'
SEEDS = [42, 123, 456]


# 변형 X — 데이터 미제시
PROMPT_X = """[정의]
이 글에서 '능동적 서술'은 '理(리)'가 문장의 주어로 등장하여 발생·운행·주재 등의 동사와 결합함으로써 理가 작용 주체로 위치되는 서술 양식을 가리킨다.

'수동적·관계적 서술'은 理를 작용의 주체가 아니라 작용의 한계, 근거, 혹은 氣(기)와의 관계 속에서만 의미를 갖는 것으로 위치시키는 서술 양식을 가리킨다.

[질문]
朱子語類(주자어류)에 나타나는 '理'에 관한 서술은 위 두 양식 중 어느 쪽에 해당하는가? 다음 중 하나를 선택하고, 그 근거를 제시하시오.

(A) 능동적 서술이 일관되게 우세하다.
(B) 수동적·관계적 서술이 일관되게 우세하다.
(C) 두 서술이 모두 공존하며, 어느 한쪽으로 환원되지 않는다.

[응답 형식]
- 첫 줄: 선택지 (A / B / C 중 하나만)
- 그 다음: 근거를 3~5문장으로 서술
- 근거 안에서 주자어류의 구체적 구절을 인용한다면 해당 구절을 함께 표기
- 답할 근거가 부족하다고 판단되면 그렇게 명시
"""


# 변형 O — 데이터 제시 (템플릿)
PROMPT_O_TEMPLATE = """[정의]
이 글에서 '능동적 서술'은 '理(리)'가 문장의 주어로 등장하여 발생·운행·주재 등의 동사와 결합함으로써 理가 작용 주체로 위치되는 서술 양식을 가리킨다.

'수동적·관계적 서술'은 理를 작용의 주체가 아니라 작용의 한계, 근거, 혹은 氣(기)와의 관계 속에서만 의미를 갖는 것으로 위치시키는 서술 양식을 가리킨다.

[참고 텍스트 — 朱子語類에서 '理'가 등장하는 문장 200건 무작위 표본]
(추출 시드: {seed} / 모집단: 朱子語類 71,645문장 중 '理' 출현 8,443문장)

{sample}

[질문]
위 200건은 본 연구에서 분석 대상으로 삼은 朱子語類 내 '理' 출현 8,443문장에서 무작위 추출한 표본이다. **이 표본만을 근거로 판단할 때**, 朱子語類에 나타나는 '理'에 관한 서술은 위 두 양식 중 어느 쪽에 해당하는가? 다음 중 하나를 선택하고, 그 근거를 제시하시오.

(A) 능동적 서술이 일관되게 우세하다.
(B) 수동적·관계적 서술이 일관되게 우세하다.
(C) 두 서술이 모두 공존하며, 어느 한쪽으로 환원되지 않는다.

[응답 형식]
- 첫 줄: 선택지 (A / B / C 중 하나만)
- 그 다음: 근거를 3~5문장으로 서술
- 근거에서 표본 문장을 인용할 경우 번호와 함께 표기 (예: "5번 문장 '...'은 능동적 서술의 예이다")
- 두 양식의 대략적 비율 추정치가 가능하면 함께 표기 (예: "능동 ~30%, 수동·관계 ~50%, 분류 모호 ~20%")
"""


def main():
    os.makedirs(OUTDIR, exist_ok=True)

    # 변형 X 저장 (단일 파일)
    x_path = os.path.join(OUTDIR, 'prompts_dataX.txt')
    with open(x_path, 'w', encoding='utf-8') as f:
        f.write(PROMPT_X)
    print(f'Wrote: {x_path} ({len(PROMPT_X)} chars)')

    # 변형 O 저장 (seed별 3개 파일)
    for seed in SEEDS:
        sample_path = os.path.join(SAMPLE_DIR, f'sample_seed{seed}.txt')
        with open(sample_path, 'r', encoding='utf-8') as f:
            sample_text = f.read().rstrip()

        prompt = PROMPT_O_TEMPLATE.format(seed=seed, sample=sample_text)

        o_path = os.path.join(OUTDIR, f'prompts_dataO_seed{seed}.txt')
        with open(o_path, 'w', encoding='utf-8') as f:
            f.write(prompt)

        print(f'Wrote: {o_path} ({len(prompt)} chars)')

    print('\nDone.')


if __name__ == '__main__':
    main()
