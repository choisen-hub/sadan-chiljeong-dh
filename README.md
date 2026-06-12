# 사단칠정 디지털 인문학 분석

서울대학교 자유전공학부 자율연구 (2026 Spring)

*[English README](README_EN.md)*

주자어류(朱子語類)의 텍스트적 다의성이 퇴계·율곡 사단칠정론 해석 분기의 구조적 조건일 수 있다는 가설을 디지털 인문학 방법(임베딩 클러스터링·인용 매칭·다중 LLM 판정)으로 검토하고, 각 방법이 이 문제에서 갖는 가능성과 한계를 진단한다. 본 저장소는 최종보고서 「사단칠정 논쟁 성립의 텍스트적 조건 탐색 — 주자어류와 퇴계·율곡 서신의 텍스트 분석을 통한 다의성 검토와 방법론적 한계」(2026-06-12)의 전체 데이터·코드·분석 산출물을 담는다.

- 연구자: 최승호 (choisen@snu.ac.kr)
- 지도교수: 김바로 (한국학중앙연구원), 양일모 (서울대)

## 연구 질문

- **RQ1**: 주자어류 내 理 용례를 비지도 임베딩 클러스터링으로 분석할 때, 의미적 분기(능동적 서술과 수동적·관계적 서술의 변별을 포함한)가 포착되는가?
- **RQ2**: 퇴계와 율곡의 주자어류 인용 양상은 어떻게 다르며, 그 차이가 RQ1의 의미 분기와 어떻게 연관되는가?
- **RQ3**: RQ1·RQ2를 종합할 때, 주자어류의 텍스트 구조가 양측의 상이한 해석을 허용하는 다의성을 갖는다고 볼 근거가 있는가?

## 파이프라인

단계 구분은 최종보고서의 장 구성을 따른다.

### Phase 0: 데이터 구축과 기초 분석 (보고서 §4.1–4.2, §5.1)

데이터 수집·전처리(01~15)와 핵심 글자 분포의 정량화(16)까지가 후속 분석의 공통 기반인 Phase 0이다.

- `common/punctuate_hanja.py` — SikuRoBERTa-PUNC-AJD-KLC 표점 부여 공통 모듈

주자어류 처리:
- `01_fetch_kanripo.py` — 칸리포 KR3a0047 수집
- `02_parse_kanripo.py` — 원본 → paragraph 구조화 (백문)
- `03_punctuate.py` — 백문 → 표점 부여
- `04_segment.py` — 표점 기준 sentence 분절
- `05_annotate.py` — 理/氣 플래그, char_count, 카테고리
- `06_export_xlsx.py` — 최종 스프레드시트

서신 처리 (퇴계 22편 + 율곡 9편):
- `11_crawl_itkc.py` — 공공데이터포털 한국문집총간 XML(ZIP)에서 letter 추출
- `12_punctuate.py` — 백문 → 표점 부여
- `13_segment_letters.py` — sentence 분절
- `14_annotate_letters.py` — 理/氣 플래그
- `15_export_letters_xlsx.py` — 최종 스프레드시트

기초 분포 분석:
- `16_letter_char_stats.py` — 서신 데이터 핵심 글자 분포 집계 (보고서 표 5-1 재현)

### Phase 1: 理 임베딩 클러스터링 (보고서 §5.2 → RQ1)

- `21_control_candidates.py` — 대조군 후보 도출 (心/性/天)
- `22_embed_li_sentences.py` — SikuBERT 理 토큰 임베딩 (layer 12, 10,474 토큰)
- `23_cluster_kmeans.py` — UMAP 50D + K-means K=2~10
- `23b_cluster_hdbscan.py` — HDBSCAN grid
- `24_visualize_clusters.py` — silhouette curve, UMAP 2D 시각화
- `28_cluster_interpretation.py` — 클러스터별 대표 문장 + bigram 추출

초기의 문장 단위 임베딩 분석(理·心·性·天 비교, 보고서 §5.2.2 전반부)은 `archive/superseded_phase1/22_embed_li_sentences_v1_sentence_emb.py.bak`로 보존되어 있다.

### Phase 2: 인용 매칭 (보고서 §5.3 → RQ2)

- `31_citation_matching.py` — LCS + IDF 가중치 매칭
- `32_export_candidates_xlsx.py` — 검토용 엑셀 변환

매칭 대상은 理 출현 문장 간 쌍이다: 서신 理 문장 510건 (퇴계 251 + 율곡 259) × 주자어류 理 문장 8,443건.

### 다중 LLM 문장별 판정 (보고서 §5.4 → RQ3 보완)

- `archive/llm_compare/` — 예비 실험 (총괄 판단 방식, 모델 간 분기 관찰; 산출물은 같은 폴더의 `data/`)
- `30_llm_judgment.py` — 理 문장 8,428건 문장별 A/B/C/U/N 판정 (OpenRouter, temperature=0)
- `31_llm_agreement.py` — 모델 간 일치도·Fleiss κ·합의/논쟁 문장 추출 (산출물 `data/llm_judgment/`, 상세는 해당 폴더 README)

## 산출 규모

### 데이터 (보고서 표 4-1)

| 항목 | 수치 |
|---|---|
| 주자어류 권 수 | 144 records (권0의 4섹션 포함) |
| 주자어류 paragraph 수 | 14,597 |
| 주자어류 sentence 수 | 71,645 (평균 21.5자) |
| 주자어류 理 포함 sentence | 8,443 (理+氣 동시: 484) |
| 서신 letter 수 | 31편 (퇴계 22 + 율곡 9) |
| 서신 sentence 수 | 2,081 (퇴계 1,305 + 율곡 776, 평균 23.3자) |

### Phase 0 기초 분석: 분포 비대칭 (보고서 표 5-1)

| 항목 | 퇴계 | 율곡 | 비율(율곡/퇴계) |
|---|---|---|---|
| 理 또는 氣 등장 문장률 | 24.1% (314/1,305) | 42.8% (332/776) | 1.78배 |
| 互 총출현 | 6 | 41 | 6.83배 |
| 情 총출현 | 186 | 73 | 0.39배 |

### Phase 1: 클러스터링 (보고서 그림 5-1~5-3)

| 항목 | 수치 |
|---|---|
| 문장 임베딩 (v1) | 理 K=2 실루엣 0.058 · 대조군 心 0.046 / 性 0.052 / 天 0.039 — 군집 거의 미형성 |
| 토큰 임베딩 K-means | K=4, 실루엣 0.85 |
| 토큰 임베딩 HDBSCAN | mcs=200, 5개 군집, noise 0.3%, 실루엣 0.79 |
| 군집 실체 | 天理 / 理會 / 之理·此理 / 道理·義理 — 능동/수동 의미축이 아닌 표면 어휘 공기 패턴 |

### Phase 2: 인용 매칭 (보고서 표 5-3)

| 항목 | 수치 |
|---|---|
| 알고리즘 | LCS + IDF 가중치 (4-gram 결합 코퍼스 IDF), 최소 LCS 4자 |
| 총 매칭 후보 | 1,504건 (퇴계 762 + 율곡 742) |
| 6자 이상 일치 | 50건 (퇴계 42 + 율곡 8) |
| 7자 이상 일치 | 29건 (전부 퇴계; 율곡 0건) |
| 최장 일치 | 퇴계 24자 / 율곡 6자 |
| 해석 | 퇴계: 全引·截引 등 축자적 장구 인용 다수. 율곡: 約引·意引 개연성이 높으나 형태론적 방법으로는 직접 확인 불가 (보고서 §5.3.2) |

### 다중 LLM 판정 (보고서 표 5-4, §5.4.2)

| 항목 | 수치 |
|---|---|
| 판정 대상 | 理 문장 8,428건 (8,443건 중 6자 미만 조각 15건 제외) |
| 모델 | ChatGPT (gpt-4.1-mini) · Gemini (gemini-2.5-flash), 2모델 |
| 전체 만장일치율 / Fleiss κ | 0.699 / 0.448 (다국어 LLM-judge 선행연구 평균 κ≈0.3 — Fu & Liu 2025) |
| 두 모델 모두 A/B 판정 | 239건, 그중 97.1%(232건) 일치 — 정면 충돌은 단 7건 |
| 한쪽 이상 A/B 판정 | 654건, 완전 일치 35.5% · 한쪽만 가시 63.5% (비대칭적 가시성) |
| C(능·수 미결정) 비중 | 모델 평균 약 40% |
| 모델 성향 | A/B 비율 ChatGPT 2.3 vs Gemini 6.0 — Gemini가 理의 능동성을 더 자주 인정 |
| DeepSeek | 응답 지연·무응답 반복으로 230건 시점 중단·분석 제외 (`data/llm_judgment/README.md`) |

## 데이터 출처

판본 정보는 `docs/editions.md` 참조.

- 주자어류 저본: Kanseki Repository (漢籍リポジトリ, Kanripo; KR3a0047)
- 주자어류 표점: hanja.dev (SikuRoBERTa-PUNC-AJD-KLC; Song et al. 2025, HERITAGE)
- 퇴계선생문집·율곡선생전서: 한국문집총간 (한국고전번역원; 공공데이터포털 XML, release 2024-08-30)
- 임베딩 모델: SIKU-BERT/sikubert
- 판정 LLM: OpenRouter 경유 `openai/gpt-4.1-mini`, `google/gemini-2.5-flash`

## 데이터 수집 시점 및 AI 사용 정보

**데이터 수집 시점**
- 칸리포 KR3a0047: 2026-04 수집
- 한국문집총간 XML: release 2024-08-30, 2026-04-27 추출 (`docs/letter_provenance.md`)
- 표점 일원화(hanja.dev): 2026-05-06
- LLM 문장별 판정 실행: 2026-06-01

**AI 사용 정보**
- 분석 도구로서의 LLM: 理 문장의 능동/수동 판정에 ChatGPT·Gemini 2개 모델을 OpenRouter API로 호출 (temperature=0, 동일 프롬프트·입력). 프롬프트 전문은 `scripts/30_llm_judgment.py`에 포함. 모델 의존성·문화적 편향 등 한계는 최종보고서 §5.4.5–5.4.6에서 논의.
- 개발 보조로서의 LLM: 분석 파이프라인 코드 작성·디버깅에 LLM(Claude)을 보조 도구로 사용하였다. 연구 설계, 코퍼스·판본 결정, 결과의 정성 판단과 검증, 보고서 집필은 연구자가 수행하였다.

## 재현 방법

의존성 설치:

```
pip install -r requirements.txt
```

단계별 실행 (각 스크립트의 옵션은 `--help` 또는 docstring 참조):

```
python3 scripts/01_fetch_kanripo.py
python3 scripts/02_parse_kanripo.py
python3 scripts/03_punctuate.py
python3 scripts/04_segment.py
python3 scripts/05_annotate.py
python3 scripts/06_export_xlsx.py
python3 scripts/11_crawl_itkc.py --config config/letter_targets.yaml
python3 scripts/12_punctuate.py
python3 scripts/13_segment_letters.py
python3 scripts/14_annotate_letters.py
python3 scripts/15_export_letters_xlsx.py
python3 scripts/16_letter_char_stats.py
python3 scripts/21_control_candidates.py
python3 scripts/22_embed_li_sentences.py
python3 scripts/23_cluster_kmeans.py
python3 scripts/23b_cluster_hdbscan.py
python3 scripts/24_visualize_clusters.py
python3 scripts/28_cluster_interpretation.py
python3 scripts/31_citation_matching.py
python3 scripts/32_export_candidates_xlsx.py
```

LLM 판정 (OpenRouter API 키 필요):

```
export OPENROUTER_API_KEY=...
python3 scripts/30_llm_judgment.py --input data/final/zhuzi_sentences.xlsx --sheet li_sentences --providers openai,gemini
python3 scripts/31_llm_agreement.py --merged data/llm_judgment/merged_2models.csv
```

## 프로젝트 구조

- `README.md`
- `requirements.txt`
- `common/punctuate_hanja.py`
- `config/letter_targets.yaml` — 서신 추출 대상 정의
- `scripts/` — 실행 스크립트 (번호 순; 활성 코드만)
- `data/`
  - `raw/`, `intermediate/`, `processed/` — git 제외
  - `final/` — 최종 xlsx (커밋)
    - `zhuzi_sentences.xlsx`, `corpus_review.xlsx`
    - `li_clustering_results.xlsx`, `citation_candidates_review.xlsx`
  - `llm_judgment/` — LLM 문장별 판정 결과 (폴더 README 참조)
- `figures/` — 보고서 그림
  - `fig_pipeline.png` (그림 4-1, 전체 분석 프로세스; 생성: `scripts/40_pipeline_figure.py`)
  - `fig_silhouette_curve.png` (그림 5-1)
  - `fig_umap_kmeans.png` (그림 5-2; K=4 패널에 군집 정체 레이블)
  - `fig_umap_hdbscan.png` (그림 5-3; 군집 정체 레이블)
- `docs/` — 판본 정보, provenance, Phase 1 판정 rubric
- `archive/` — 비활성 코드 보존 (v1 파이프라인·Phase 1/2 구버전·검증 스크립트·LLM 예비 실험; 폴더 README 참조)

## 변경 이력

- **2026-06-12**: 최종보고서 확정판 동기화. archive/를 하위 폴더 5개로 재구조화하고 `scripts/_archive/`를 통합(비활성 코드 보존 위치 일원화). Phase 0 정의를 '데이터 구축(수집·전처리)과 기초 분포 분석 전체'로 명확화(보고서 §4.3·그림 4-1과 일치). 보고서 제목 확정 반영, '코퍼스'→'데이터' 용어 통일, UMAP 그림 2종에 군집 정체 레이블 추가(`24_visualize_clusters.py` 패치 — 크기 순위 기반 매핑), 전체 프로세스 개념도(그림 4-1) 및 생성 스크립트(`40_pipeline_figure.py`) 추가, 영문 README(`README_EN.md`) 추가, 인용 매칭 표 정정 반영(T0513 → 程明道 「定性書」 공통 전거, Y0018 → 是理當如此 관용 표현)
- **2026-06-11**: 최종보고서 기준 문서 동기화. LLM 판정 결과·AI 사용 정보·데이터 수집 시점을 README에 명시, requirements.txt 정비, Phase 0 집계 스크립트(16) 추가, 퇴계 핵심 7편 식별 출처를 김세종(2024, 한국철학논집 83)으로 정정(종전 황준연(2009) 오기), 인용 매칭 6자 이상 수치를 산출물 기준으로 정정(퇴계 42·율곡 8)
- **2026-06-01**: LLM 문장별 판정 파이프라인(30·31_llm_agreement) 및 결과 추가 (OpenRouter 2모델, 8,428문장)
- **2026-05-14**: Phase 1 토큰 임베딩 전환 (문장 임베딩 → 理 토큰 임베딩), Phase 2 LCS+IDF 인용 매칭 파이프라인 추가
- **2026-05-06**: 표점 부여 hanja.dev 일원화. 주자어류·서신 동일 규칙
- **2026-04-28**: 초기 파이프라인. 祝平次 표점 + hanja.dev fallback 혼합

## 라이선스

코드는 MIT. 원문 텍스트 데이터는 각 출처의 이용 약관을 따름.
