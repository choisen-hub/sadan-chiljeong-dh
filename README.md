# 사단칠정 디지털 인문학 분석

서울대학교 자유전공학부 자율연구 (2026 Spring)
주자어류(朱子語類)의 텍스트 구조가 퇴계·율곡의 사단칠정론 해석 분기의 근거가 되는지 계산적으로 검증한다.

- 연구자: 최승호 (choisen@snu.ac.kr)
- 지도교수: 김바로 (한국학중앙연구원), 양일모 (서울대)

## 연구 질문

- **RQ1**: 주자어류에서 理의 용법이 의미적 클러스터를 형성하는가?
- **RQ2**: 퇴계·율곡의 인용이 텍스트 수준에서 체계적으로 구별되는가?
- **RQ3**: 주자어류의 텍스트 구조가 양측 해석을 모두 허용하는가? (RQ1 × RQ2)

## 파이프라인
Phase 0: 코퍼스 구축
공통
common/punctuate_hanja.py   SikuRoBERTa-PUNC-AJD-KLC 표점 부여 모듈
주자어류
01_fetch_kanripo.py         칸리포 KR3a0047 수집
02_parse_kanripo.py         원본 .txt → paragraph 구조화 (백문)
03_punctuate.py             백문 → hanja.dev 표점 부여
04_segment.py               표점 기준 sentence 분절
05_annotate.py              理/氣 플래그, char_count, 카테고리
06_export_xlsx.py           최종 스프레드시트
서신 (퇴계 22편 + 율곡 9편)
11_crawl_itkc.py            한국문집총간 ZIP에서 letter 추출
12_punctuate.py             백문 → hanja.dev 표점 부여
13_segment_letters.py       표점 기준 sentence 분절
14_annotate_letters.py      理/氣 플래그
15_export_letters_xlsx.py   최종 스프레드시트
Phase 1: 理 클러스터링 (→ RQ1)
21_control_candidates.py    대조군 후보 도출 (心/性/天)
22_embed_li_sentences.py    SikuBERT 토큰 임베딩 (layer 12, 10,474 토큰)
23_cluster_kmeans.py        UMAP 50D + K-means K=2~10
23b_cluster_hdbscan.py      HDBSCAN grid (min_cluster_size 30~200)
24_visualize_clusters.py    silhouette curve, UMAP 2D 시각화
28_cluster_interpretation.py 클러스터별 대표 문장 + bigram 추출
Phase 2: 인용 매칭 (→ RQ2)
31_citation_matching.py     LCS + IDF 가중치 매칭 (1,504 후보)
32_export_candidates_xlsx.py 검토용 엑셀 변환

## 산출 규모

### Phase 0 코퍼스

| 항목 | 수치 |
|---|---|
| 주자어류 권 수 | 144 records (권0의 4섹션 포함) |
| 주자어류 paragraph 수 | 14,597 |
| 주자어류 sentence 수 | 71,645 (평균 21.5자) |
| 주자어류 理 포함 sentence | 8,443 (理+氣 동시: 484) |
| 서신 letter 수 | 31편 (퇴계 22 + 율곡 9) |
| 서신 sentence 수 | 2,081 (평균 23.3자) |

### Phase 1 클러스터링

| 항목 | 수치 |
|---|---|
| 추출된 理 토큰 | 10,474 (8,443 문장에서) |
| SikuBERT 임베딩 차원 | 768 (layer 12) |
| K-means best | K=4, silhouette 0.85 |
| HDBSCAN best | mcs=200, 5개 클러스터, noise 0.3% |
| 클러스터 실체 | 道理 / 天理 / 之理 / 此理 / 理會 (collocation 표면 패턴) |

### Phase 2 인용 매칭

| 항목 | 수치 |
|---|---|
| 매칭 알고리즘 | LCS + IDF 가중치 (4-gram 결합 코퍼스 IDF) |
| 최소 LCS 길이 | 4자 |
| 총 매칭 후보 | 1,504건 |
| 6자 이상 매칭 | 50건 (퇴계 41 + 율곡 9) |
| 인용 양식 | 퇴계: 全引/截引 위주, 율곡: 約引/意引 위주 |

## 데이터 출처

판본 정보는 [docs/판본정보.md](docs/판본정보.md) 참조.

- 주자어류 저본: 京都大學 人文科學研究所 漢籍リポジトリ (Kanripo, KR3a0047)
- 주자어류 표점: hanja.dev (`seyoungsong/SikuRoBERTa-PUNC-AJD-KLC`) 일괄 부여
- 퇴계선생문집: 한국문집총간 29~31집 (한국고전번역원)
- 율곡선생전서: 한국문집총간 44~45집 (한국고전번역원)
- 임베딩 모델: SIKU-BERT/sikubert (SikuBERT, 사고전서 사전훈련)

## 재현 방법

```bash
# 1. 의존성
pip install -r requirements.txt

# 2. Phase 0: 코퍼스 구축
python scripts/01_fetch_kanripo.py
python scripts/02_parse_kanripo.py
python scripts/03_punctuate.py
python scripts/04_segment.py
python scripts/05_annotate.py
python scripts/06_export_xlsx.py
# 공공데이터포털에서 한국문집총간 ZIP 다운로드 후 data/raw/munjip/ 에 배치
python scripts/11_crawl_itkc.py
python scripts/12_punctuate.py
python scripts/13_segment_letters.py
python scripts/14_annotate_letters.py
python scripts/15_export_letters_xlsx.py

# 3. Phase 1: 理 클러스터링
python scripts/22_embed_li_sentences.py
python scripts/23_cluster_kmeans.py
python scripts/23b_cluster_hdbscan.py
python scripts/24_visualize_clusters.py
python scripts/28_cluster_interpretation.py

# 4. Phase 2: 인용 매칭
python scripts/31_citation_matching.py
python scripts/32_export_candidates_xlsx.py
```

## 프로젝트 구조
sadan-chiljeong-dh/
├── README.md
├── requirements.txt
├── common/
│   └── punctuate_hanja.py
├── scripts/                   # 실행 스크립트
│   └── _archive/              # 미사용/실험 잔재
├── data/
│   ├── raw/                   # 원본 (git 제외)
│   ├── intermediate/          # 중간 산출물 (git 제외)
│   ├── processed/             # 임베딩·클러스터링 결과 (git 제외)
│   └── final/                 # 최종 xlsx
│       ├── zhuzi_sentences.xlsx
│       ├── li_clustering_results.xlsx
│       └── citation_candidates_review.xlsx
├── figures/                   # 시각화 (Phase 1)
│   ├── fig_silhouette_curve.png
│   ├── fig_umap_kmeans.png
│   └── fig_umap_hdbscan.png
└── docs/

## 변경 이력

- **2026-05-14**: Phase 1 토큰 임베딩 전환 (문장 임베딩 → 理 토큰 임베딩), Phase 2 LCS+IDF 인용 매칭 파이프라인 추가.
- **2026-05-06 (v2)**: 표점 부여를 hanja.dev로 일원화. 주자어류·서신 동일 표점 규칙.
- **2026-04-28 (v1)**: 초기 파이프라인. 祝平次 표점 차용 + hanja.dev fallback 혼합.

## 라이선스

코드는 MIT. 원문 텍스트 데이터는 각 출처의 이용 약관을 따름.
