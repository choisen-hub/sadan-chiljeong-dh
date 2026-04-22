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

### Phase 0: 데이터 정제

```
주자어류 파이프라인
  01_fetch_kanripo.py       칸리포 원본 수집
  02_parse_zhuzi_xml.py     祝平次 TEI-XML 파싱 (표점 참조용)
  03_align.py               칸리포 vs 祝平次 일치율 측정 + 불일치 목록
  04_punctuate.py           칸리포 백문에 표점 부여
  05_segment.py             표점 기준 문장 분절
  06_annotate.py            理/氣 플래그, char_count, 카테고리
  07_export_xlsx.py         최종 스프레드시트 생성

서신 파이프라인 (퇴계·율곡)
  11_crawl_itkc.py          고전종합DB 크롤링
  12_segment_letters.py     표점 기준 문장 분절
  13_export_letters_xlsx.py 최종 스프레드시트 생성
```

### Phase 1~3

- Phase 1: 주자어류 비지도 클러스터링 → RQ1
- Phase 2: 퇴계/율곡 인용선호 예측 → RQ2
- Phase 3: 종합 해석 + 논문 집필 → RQ3

## 데이터 출처

판본 정보는 [docs/판본정보.md](docs/판본정보.md) 참조.

- 주자어류 저본: 京都大學 人文科學研究所 漢籍リポジトリ (Kanripo, KR3a0047)
- 주자어류 표점 참조: 祝平次 교수 편집본 (TEI-P5 XML)
- 퇴계선생문집: 한국문집총간 29~31집 (한국고전번역원)
- 율곡선생전서: 한국문집총간 44~45집 (한국고전번역원)

## 재현 방법

```bash
# 1. 의존성 설치
pip install -r requirements.txt

# 2. 파이프라인 실행 (순서대로)
python scripts/01_fetch_kanripo.py
python scripts/02_parse_zhuzi_xml.py
python scripts/03_align.py
# ...
```

## 프로젝트 구조

```
sadan-chiljeong-dh/
├── README.md
├── requirements.txt
├── .gitignore
├── scripts/              # 실행 스크립트 (01~07, 11~13)
├── data/
│   ├── raw/              # 원본 데이터 (git 제외)
│   ├── intermediate/     # 중간 산출물 jsonl (git 제외)
│   └── final/            # 최종 xlsx / csv (커밋 대상)
└── docs/
    └── 판본정보.md
```

## 라이선스

코드는 MIT 라이선스. 원문 텍스트 데이터는 각 출처의 이용 약관을 따름.
