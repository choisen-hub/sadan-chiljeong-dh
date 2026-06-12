"""
주자어류 내 '理' 출현 8,443문장에서 200건씩 무작위 표본 3개를 추출한다.
seed: 42, 123, 456
산출물:
    archive/llm_compare/data/sample_seed{seed}.txt (프롬프트 인라인 삽입용)
    archive/llm_compare/data/sample_seed{seed}.csv (sentence_id 포함, 재현용 메타데이터)

본 스크립트는 사단칠정 디지털 인문학 연구의 LLM 비교 실험을 위한
표본 추출 단계이다. 동일한 seed로 다시 실행하면 동일한 200문장이
재현된다. GitHub 저장소에 코드와 산출물을 함께 공개하여
타 연구자가 동일 표본 위에서 후속 분석을 수행할 수 있게 한다.
"""

import os
import random
import pandas as pd

# 컬럼 설정
TEXT_COL = 'text_punctuated'   # 표점 적용 버전을 LLM 프롬프트에 삽입
ID_COL = 'sentence_id'

# 경로 및 파라미터
SOURCE = 'data/final/zhuzi_sentences.xlsx'
SHEET = 'li_sentences'
OUTDIR = 'archive/llm_compare/data'
SEEDS = [42, 123, 456]
N = 200


def main():
    os.makedirs(OUTDIR, exist_ok=True)

    df = pd.read_excel(SOURCE, sheet_name=SHEET)
    print(f'Loaded: {len(df)} sentences from {SOURCE}::{SHEET}')

    # sanity check
    assert TEXT_COL in df.columns, (
        f'{TEXT_COL} not in columns: {df.columns.tolist()}'
    )

    for seed in SEEDS:
        random.seed(seed)
        indices = random.sample(range(len(df)), N)
        sample_df = df.iloc[indices].reset_index(drop=True)

        # 프롬프트 인라인 삽입용 텍스트
        txt_path = os.path.join(OUTDIR, f'sample_seed{seed}.txt')
        with open(txt_path, 'w', encoding='utf-8') as f:
            for i, row in sample_df.iterrows():
                text = str(row[TEXT_COL]).strip()
                f.write(f'{i + 1}. {text}\n')

        # 재현용 메타데이터 CSV (sentence_id 포함 전체 컬럼)
        csv_path = os.path.join(OUTDIR, f'sample_seed{seed}.csv')
        sample_df.to_csv(csv_path, index=False, encoding='utf-8')

        print(f'seed={seed}: {len(sample_df)} sentences -> {txt_path}')

    print('\nDone.')


if __name__ == '__main__':
    main()
