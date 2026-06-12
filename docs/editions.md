# 판본 정보

본 연구에서 사용한 원전 텍스트의 저본 및 디지털 판본 정보를 기록한다.

## 주자어류 (朱子語類)

### 저본 (底本)

* **Kanseki Repository (漢籍リポジトリ, 칸리포)**
* 운영: 京都大学 人文科学研究所의 Christian Wittern 교수가 구축·관리하는 한문 고전 디지털 텍스트 컬렉션
* 식별번호: KR3a0047
* URL: <https://www.kanripo.org/text/KR3a0047/>
* GitHub: <https://github.com/kanripo/KR3a0047>
* 저자: 朱熹 찬, 黎靖德 편
* 특징: 원본 백문(표점 無)

### 표점 부여 (v2부터)

* **모델**: `seyoungsong/SikuRoBERTa-PUNC-AJD-KLC` (HuggingFace)
* 아키텍처: BertForTokenClassification (NER 스타일 토큰 분류)
* 표점 체계: 한국고전번역원 (AJD/KLC) — 한국문집총간 표점 컨벤션과 일치
* 적용 범위: 1–141권 전체 (권당 평균 약 100 paragraphs, 총 14,597 paragraphs)
* 호출 메타데이터 (모델 ID·revision·시각·해시·통계): [`zhuzi_punctuation_provenance.md`](zhuzi_punctuation_provenance.md)
* 호출 모듈: [`common/punctuate_hanja.py`](../common/punctuate_hanja.py)

### 인용 방식

> 본 연구의 분석 데이터는 Kanseki Repository(漢籍リポジトリ, KR3a0047; 京都大学 人文科学研究所의 Christian Wittern 교수 구축·관리)를 底本으로 한다. 백문 본문에 대한 표점 부여는 한국고전번역원 표점 시스템(AJD/KLC)으로 학습된 SikuRoBERTa 기반 토큰 분류 모델(`seyoungsong/SikuRoBERTa-PUNC-AJD-KLC`)을 사용하여 paragraph 단위로 수행하였다.

### v1과의 차이

v1에서는 1–7권 祝平次 교수 편집본(TEI-P5 XML) 표점을 차용하고 8권 이후만 hanja.dev fallback으로 처리하는 hybrid 방식을 사용했다. 이 방식은 corpus 내부에서 표점 부여 규칙이 일관되지 않다는 문제(권1–7과 권8–141의 표점 컨벤션 차이)가 있었으며, 100번대 이후 권에서 표점 누락 사례도 발견되었다. v2에서는 1–141권 전체를 hanja.dev 모델로 일괄 부여하여 corpus 내 표점 규칙을 일원화하였다.

祝平次 편집본은 v2 파이프라인에서 더 이상 사용하지 않는다. v1 관련 코드(`03_parse_zhuzi_xml.py`, `04_align.py`, 구 `05_punctuate.py`)는 [`archive/`](../archive/)에 보존.

## 한국문집총간 자료 입수 경로 (퇴계집·율곡전서 공통)

퇴계선생문집·율곡선생전서는 모두 **한국고전번역원 발행 『한국문집총간』 정본 원문 XML**을 1차 자료로 한다.

* **출처**: 공공데이터포털 — [한국고전번역원_한국문집총간](https://www.data.go.kr/data/3074298/fileData.do) (데이터셋 ID 3074298)
* **종류**: 파일데이터(fileData)
* **사용 release**: `한국고전번역원_한국문집총간_20240830` (2024-08-30 release)
* **수집 시점**: 2026-04-28
* **수집자**: 최승호 (서울대학교 자유전공학부)
* **XML 자료생성일** (개별 문집): 2004-12-31

## 퇴계선생문집 (退溪先生文集)

* **底本**: 退溪先生文集 木版本 (도산서원 소장)
* **디지털 판본**: 한국문집총간 29-31집 (한국고전번역원, 1989)
* **한국문집총간 ID**: `ITKC_MO_0144A` (정본 한문 원문)
* **공공데이터포털 등록일**: 2020-03-26
* **분석 범위**: **권16–권17 「答奇明彦」 명언 관련 22편**
  * 권16: 5편 (`_0010`–`_0050`)
  * 권17: 17편 (`_0010`–`_0170`)
  * 자수: **38,085자** (한국문집총간 표점 포함) / **31,219자** (백문)
  * 정확한 22편 list는 [`letter_provenance.md`](letter_provenance.md) 참조
* **분석 corpus 범위**: 정량 분석(인용 매칭 등)은 위 22편 전체를 대상으로 한다. 김세종(2024, 한국철학논집 83)이 사단칠정 핵심 텍스트로 지목한 퇴계 발신 7편(번호 1, 3, 5, 7, 9, 10, 12)은 정성 검토의 참조 기준으로 활용한다.

> ※ 권41 「非理氣爲一物辯證」은 letter 형식이 아닌 변증문이라, 현 시점 corpus에는 포함하지 않는다.

## 율곡선생전서 (栗谷先生全書)

* **底本**: 栗谷全書 木版本
* **디지털 판본**: 한국문집총간 44-45집 (한국고전번역원, 1989)
* **한국문집총간 ID**: `ITKC_MO_0201A` (정본 한문 원문)
* **공공데이터포털 등록일**: 2020-03-30
* **분석 범위**: **권11 「答成浩原」 9편 (1572 壬申, 사단칠정 논변 본체)**
  * 자수: **21,090자** (한국문집총간 표점 포함) / **17,352자** (백문)
  * 권11 9편 모두가 1572년 율곡-우계 사단칠정 논변 letter들이다 (권10/권12의 답성호원은 시기·주제가 다름)
  * 정확한 9편 list는 [`letter_provenance.md`](letter_provenance.md) 참조

## 서신 표점 부여 (v2부터)

서신 corpus도 동일하게 hanja.dev 모델로 표점을 일괄 부여한다.

한국문집총간 정본 XML에는 한국고전번역원 표점위원회의 표점이 이미 부여되어 있으나(`raw_text` 필드에 보존), 분석에는 사용하지 않는다. 한국문집총간 표점은 句點(`。`) 위주의 전통식 표점 (평균 문장 길이 약 6자)으로 임베딩 분석에 부적합하며, 또한 주자어류 corpus와 다른 규칙으로 부여되어 corpus 간 일관성이 깨진다. 따라서 백문화(`plain_text`) 후 hanja.dev 모델로 재부여한다 (v2 평균 문장 길이 23.3자).

* **모델·메타데이터**: [`letter_punctuation_provenance.md`](letter_punctuation_provenance.md)
* **호출 단위**: letter 단위 (paragraph 정보 없이 letter 본문 전체 처리)

## 데이터 처리 파이프라인

```
[공공데이터포털 한국문집총간_20240830]
  ├─ 144__퇴계집_退溪集_.zip
  └─ 201__율곡전서_栗谷全書_.zip
       │
       ▼
  scripts/11_crawl_itkc.py        (XML 파싱 + letter 추출 + 백문화)
       │
       ▼
  data/raw/letters.jsonl          (31편: 퇴계 22 + 율곡 9)
       │
       ▼
  scripts/12_punctuate.py         (hanja.dev 표점 부여)
       │
       ▼
  data/processed/letters_punctuated.jsonl
       │
       ▼
  scripts/13_segment_letters.py   (표점 기준 분절)
       │
       ▼
  data/processed/sentences.jsonl  (2,081 문장, 평균 23.3자)
       │
       ▼
  scripts/14_export_letters_xlsx.py
       │
       ▼
  data/final/corpus_review.xlsx
```

```
[칸리포 KR3a0047]
       │
       ▼
  scripts/01_fetch_kanripo.py
  scripts/02_parse_kanripo.py     (백문 paragraph 14,597개)
       │
       ▼
  scripts/03_punctuate.py         (hanja.dev 표점 부여)
       │
       ▼
  data/intermediate/kanripo_punctuated.jsonl  (1.55M자 → 1.90M자)
       │
       ▼
  scripts/04_segment.py           (표점 기준 분절, 71,645 문장)
       │
       ▼
  scripts/05_annotate.py          (理/氣 플래그)
       │
       ▼
  scripts/06_export_xlsx.py
       │
       ▼
  data/final/zhuzi_sentences.xlsx
```

## 변경 이력

* **2026-06-12**: 칸리포 표기 정비 — 일본 기관명을 한국식 정체자 임의 표기(京都大學 人文科學研究所)에서 기관 공식 신자체 표기(京都大学 人文科学研究所)로 수정하고, 주 표기를 공식 영문명 Kanseki Repository로 통일. '附屬 漢字情報研究センター' 소속 단정 표현을 운영자(Christian Wittern) 중심 서술로 교체(보고서 §4.1.1과 일치).
* **2026-06-12**: 파일명을 `판본정보.md`에서 `editions.md`로 변경(저장소 파일명 영문 통일). 범위 표시의 틸드(`~`)를 en dash(`–`)로 일괄 교체 — GitHub 마크다운(GFM)이 단락 내 틸드 쌍을 취소선으로 렌더링하여 'v1과의 차이' 절이 취소선으로 표시되던 문제 수정.
* **2026-06-11**: 퇴계 핵심 7편 식별 출처를 황준연(2009)에서 김세종(2024, 한국철학논집 83)으로 정정. 실행되지 않은 "7편 narrowing 후속 filter" 계획 서술을 삭제하고 실제 분석 범위(22편 전체)로 명확화.
* **2026-05-06**: 표점 부여를 hanja.dev (`seyoungsong/SikuRoBERTa-PUNC-AJD-KLC`) 모델로 일원화. 주자어류 1–141권 전체 + 서신 31편 모두 동일 모델로 처리하여 corpus 간 표점 규칙 일관성 확보. 祝平次 편집본 사용 중단(archive 보존). 02 단계의 페이지 태그 처리 버그(paragraph가 페이지 경계에서 잘못 분절되던 문제) 수정으로 권1 paragraph 수가 90 → 65로 정정됨.
* **2026-04-28**: 한국문집총간 자료 입수 경로를 공공데이터포털 XML 다운로드(release 2024-08-30)로 확정. 율곡 corpus 권차 정정(권10 답성호원 9편 → 권11 답성호원 9편). 자수 재산정.
* **2026-04-23**: 초안 작성. 주자어류 판본 방침을 祝平次 메인에서 칸리포 메인 + 祝平次 참조로 변경.
