# data/llm_judgment/

주자어류 理 문장 8,428건에 대한 다중 LLM 문장별 판정 결과 (최종보고서 §5.4).

## 파일

| 파일 | 내용 |
|---|---|
| `labels_openai.jsonl` | ChatGPT (gpt-4.1-mini) 라벨, 8,428건 |
| `labels_gemini.jsonl` | Gemini (gemini-2.5-flash) 라벨, 8,428건 |
| `labels_deepseek.jsonl` | DeepSeek 부분 실행 잔존분 (230건, 분석 제외 — 아래 참조) |
| `merged_2models.csv` | 두 모델 라벨 병합본 (`31_llm_agreement.py` 입력) |
| `agreement_summary.txt` | 일치도 요약 (만장일치율 0.699, Fleiss κ 0.448 등) |
| `consensus.csv` | 만장일치 문장 5,889건 (강한 신호) |
| `contested_AB.csv` | 능동/수동 정면 충돌 문장 7건 (정성 분석 대상) |

## 라벨 체계

A(능동) / B(수동·무위) / C(능·수 미결정) / U(판단불가) / N(비대상·비형이상학적 용법).
정의와 프롬프트는 `scripts/30_llm_judgment.py` 참조.

## DeepSeek 제외에 관하여

당초 3개 모델(ChatGPT·Gemini·DeepSeek)을 계획하였으나, DeepSeek는 본
과제에서 응답 지연·무응답(socket hang)이 반복되어 230건 시점에서 중단하고
분석에서 제외하였다. 이는 예비 실험(`data/llm_compare/`)에서 관찰된
DeepSeek의 높은 출력 변동성과도 일관된다 (최종보고서 §5.4.1).
부분 실행 파일은 투명성을 위해 삭제하지 않고 보존한다.

## 재현

```
export OPENROUTER_API_KEY=...
python3 scripts/30_llm_judgment.py --input data/final/zhuzi_sentences.xlsx --sheet li_sentences --providers openai,gemini
python3 scripts/31_llm_agreement.py --merged data/llm_judgment/merged_2models.csv
```

호출 경로·옵션 상세는 각 스크립트의 docstring 및 `--help` 참조.
temperature=0, 동일 시스템 프롬프트, 동일 입력으로 통제.
