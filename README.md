# 사단칠정 디지털 인문학 분석

서울대학교 자유전공학부 자율연구 (2026 Spring)
주자어류(朱子語類)의 텍스트 구조가 퇴계·율곡의 사단칠정론 해석 분기의 근거가 되는지 계산적으로 검증한다.

- 연구자: 최승호 (choisen@snu.ac.kr)
- 지도교수: 김바로 (한국학중앙연구원), 양일모 (서울대)

## 연구 질문

- **RQ1**: 주자어류에서 理의 용법이 의미적 클러스터를 형성하는가?
- **RQ2**: 퇴계·율곡의 인용이 텍스트 수준에서 체계적으로 구별되는가?
- **RQ3**: 주자어류의 텍스트 구조가 양측 해석을 모두 허용하는가? (RQ1 × RQ2)

## 파이프라인 (v2: hanja.dev 일괄 표점)

표점 부여를 hanja.dev 모델 (`seyoungsong/SikuRoBERTa-PUNC-AJD-KLC`)로 일원화하여
주자어류·서신 두 corpus가 동일한 표점 규칙으로 처리되도록 했다. v1 (祝平次 차용)
관련 변경사항은 [archive/](archive/) 디렉토리 및 변경 이력 참조.

```
공통
  common/punctuate_hanja.py     SikuRoBERTa-PUNC-AJD-KLC 호출 모듈
                                (캐싱, 청킹, 메타데이터 기록)

주자어류 파이프라인
  01_fetch_kanripo.py           칸리포 KR3a0047 수집
  02_parse_kanripo.py           원본 .txt → paragraph 구조화 (백문)
  03_punctuate.py               백문 → hanja.dev 표점 부여
  04_segment.py                 표점 기준 sentence 분절
  05_annotate.py                理/氣 플래그, char_count, 카테고리
  06_export_xlsx.py             최종 스프레드시트 생성

서신 파이프라인 (퇴계·율곡)
  11_crawl_itkc.py              한국문집총간 ZIP에서 letter 추출
  12_punctuate.py               백문 → hanja.dev 표점 부여
  13_segment_letters.py         표점 기준 sentence 분절
  14_export_letters_xlsx.py     최종 스프레드시트 생성
```

### Phase 1~3

- Phase 1: 주자어류 비지도 클러스터링 → RQ1
- Phase 2: 퇴계/율곡 인용선호 예측 → RQ2
- Phase 3: 종합 해석 + 논문 집필 → RQ3

## 데이터 출처

판본 정보는 [docs/판본정보.md](docs/판본정보.md) 참조.

- 주자어류 저본: 京都大學 人文科學研究所 漢籍リポジトリ (Kanripo, KR3a0047)
- 주자어류 표점: hanja.dev (`seyoungsong/SikuRoBERTa-PUNC-AJD-KLC`) 일괄 부여
- 퇴계선생문집: 한국문집총간 29~31집 (한국고전번역원)
- 율곡선생전서: 한국문집총간 44~45집 (한국고전번역원)

## 산출 규모

| 항목 | 수치 |
|---|---|
| 주자어류 권 수 | 144 records (권0의 4섹션 포함) |
| 주자어류 paragraph 수 | 14,597 |
| 주자어류 sentence 수 | 71,645 (평균 21.5자) |
| 주자어류 理 포함 sentence | 8,443 (理+氣 동시: 484) |
| 서신 letter 수 | 31편 (퇴계 22 + 율곡 9) |
| 서신 sentence 수 | 2,081 (평균 23.3자) |

표점 부여 메타데이터: [docs/zhuzi_punctuation_provenance.md](docs/zhuzi_punctuation_provenance.md), [docs/letter_punctuation_provenance.md](docs/letter_punctuation_provenance.md)

## 재현 방법

```bash
# 1. 의존성 설치
pip install -r requirements.txt

# 2. 주자어류 파이프라인 (순서대로)
python scripts/01_fetch_kanripo.py
python scripts/02_parse_kanripo.py
python scripts/03_punctuate.py
python scripts/04_segment.py
python scripts/05_annotate.py
python scripts/06_export_xlsx.py

# 3. 서신 파이프라인
#    공공데이터포털에서 한국문집총간 ZIP 다운로드 후 data/raw/munjip/ 에 배치
python scripts/11_crawl_itkc.py
python scripts/12_punctuate.py
python scripts/13_segment_letters.py
python scripts/14_export_letters_xlsx.py
```

표점 부여 단계(03, 12)는 모델 첫 실행 시 HuggingFace에서 가중치 다운로드.
이후 실행은 `.cache/punctuate_hanja/` 캐시 사용.

## 프로젝트 구조

```
sadan-chiljeong-dh/
├── README.md
├── requirements.txt
├── .gitignore
├── common/
│   └── punctuate_hanja.py     # 표점 부여 공통 모듈
├── scripts/                   # 실행 스크립트 (01~06, 11~14)
├── archive/                   # v1 잔재 (祝平次 정렬, 구 표점 hybrid)
├── data/
│   ├── raw/                   # 원본 데이터 (git 제외)
│   ├── intermediate/          # 중간 산출물 jsonl (git 제외)
│   └── final/                 # 최종 xlsx (커밋 대상)
└── docs/
    ├── 판본정보.md
    ├── letter_provenance.md
    ├── letter_punctuation_provenance.md
    └── zhuzi_punctuation_provenance.md
```

## 변경 이력

- **v2 (2026-05-06)**: 표점 부여를 hanja.dev (`seyoungsong/SikuRoBERTa-PUNC-AJD-KLC`)로 일원화.
  주자어류·서신 corpus가 동일 표점 규칙으로 처리됨.
  祝平次 차용 단계 폐기. v1 코드는 `archive/`로 이전.
  부수적으로 02 의 페이지 태그 처리 버그 수정 (paragraph가 페이지 경계에서 잘못 분절되던 문제).
- **v1 (2026-04-28)**: 초기 파이프라인. 1~7권 祝平次 표점 차용 + 8권~ hanja.dev fallback 혼합.

## 라이선스

코드는 MIT 라이선스. 원문 텍스트 데이터는 각 출처의 이용 약관을 따름.
