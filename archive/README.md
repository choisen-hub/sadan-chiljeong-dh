# archive/

활성 파이프라인(`scripts/`)에서 더 이상 사용하지 않는 코드의 보존 기록. 폐기된 접근의 흔적은 부정적 결과의 일부로서 재현성·검증 가능성에 기여하므로 삭제하지 않고 하위 폴더로 분류해 둔다.

```
archive/
├── v1_pipeline/         v1 데이터 파이프라인 (祝平次 표점 혼용 시기)
├── superseded_phase1/   Phase 1 구버전 (문장 임베딩 v1 계열)
├── superseded_phase2/   Phase 2 부속 구버전
├── validation/          개발 중 검증·확인용 일회성 스크립트
└── llm_compare/         LLM 예비 실험 (총괄 판단 방식; 본 실험은 scripts/30·31)
```

## v1_pipeline/ — v1 데이터 파이프라인

v2(2026-05-06)부터 표점 부여가 hanja.dev 일괄 적용으로 전환되면서 폐기된 코드. 자세한 전환 사유는 [`docs/editions.md`](../docs/editions.md)의 변경 이력 참조.

| 파일 | 역할 (v1) | 폐기 사유 (v2) |
|---|---|---|
| `03_parse_zhuzi_xml.py` | 祝平次 TEI-XML 파싱 | 祝平次 표점 사용 안 함 |
| `04_align.py` | 칸리포 vs 祝平次 정렬 + 일치율 측정 | 祝平次 표점 사용 안 함 |
| `05_punctuate.py` | 1–7권 祝平次 차용 + 8권 이후 hanja.dev fallback hybrid | 1–141권 일괄 hanja.dev로 통일 |
| `06_segment.py` | 주자어류 sentence 분절 (구) | 입력 변경(`paragraphs_punctuated`) 위해 04로 재작성 |
| `07_annotate.py` | 理/氣 플래그 (구) | 단계 번호 정리 위해 05로 이동 (로직 동일) |
| `08_export_xlsx.py` | 최종 xlsx (구) | 단계 번호 정리 위해 06으로 이동 (로직 동일) |
| `12_segment_letters.py` | 서신 분절 (구, 한국문집총간 표점 기반) | hanja.dev 표점 기반 13으로 재작성 |
| `13_export_xlsx.py` | 서신 xlsx (구) | 14로 이동 + 이름에 `_letters_` 명시 |

v1 검증 산출물(`data/processed/`의 `alignment_summary.csv`, `zhuzi_mismatch_*.csv` 등)은 GitHub 히스토리에 보존. 칸리포·祝平次 판본 일치율 검증을 거쳤다는 흔적은 재현성 측면에서 가치가 있다.

## superseded_phase1/ — Phase 1 구버전

토큰 임베딩 전환(2026-05-14) 전후로 폐기된 문장 임베딩 계열과 초기 토큰 임베딩 판. 보고서 §5.2.2 전반부(理·心·性·天 비교, 실루엣 0.039–0.058)가 이 코드 계열의 산출이다.

| 파일 | 역할 | 폐기 사유 |
|---|---|---|
| `22_embed_li_sentences_v1_sentence_emb.py.bak` | 문장 단위 SikuBERT 임베딩 (v1) | 군집 미형성(실루엣 0.058) → 理 토큰 임베딩으로 전환 |
| `22_embed_li_sentences_v2_lastlayer.py.bak` | 토큰 임베딩 초기판 (최종 hidden layer) | layer 12 표상을 쓰는 현행 22로 대체 |
| `23_cluster_kmeans_v1_multichar.py.bak` | 다글자(理·心·性·天) K-means | 理 단일 글자 토큰 분석으로 범위 확정 |
| `24_visualize_clusters_v1.py` | v1 시각화 | 토큰 임베딩용 현행 24로 재작성 |

## superseded_phase2/ — Phase 2 부속 구버전

| 파일 | 역할 | 폐기 사유 |
|---|---|---|
| `33_enrich_candidates.py` | 매칭 후보에 noise 판별 보조 컬럼(±3자 일치율, 인용표지 유무) 추가 | 후보 전수 수작업 검토 절차로 대체 |
| `34_cluster_mapping.py` | 고점수 인용을 Phase 1 군집에 매핑하는 시도 | 군집 축이 표면 공기 패턴으로 판명되어(보고서 §5.2.3) 후속 분석에서 제외 |

## validation/ — 개발 중 검증 스크립트

| 파일 | 용도 | 비고 |
|---|---|---|
| `sanity_check_hanja.py` | 권1/127/130 문체별 표점 품질 비교 | 김바로 교수님 미팅 검증 자료 |
| `test_hanja_local.py` | 모델 로딩 패턴 발견용 minimal example | `common/punctuate_hanja.py`의 `_load_model()` 원형 |
| `check_tokenizer.py` | SikuBERT 토크나이저의 理 단일 토큰 처리·위치 추출 검증 | Phase 1 토큰 임베딩 전환 시 검증 |

검증된 로직은 `common/` 및 `scripts/`의 정식 코드에 통합되어 있다.

## llm_compare/ — LLM 예비 실험

문장별 판정(`scripts/30_llm_judgment.py`) 이전에 수행한 총괄 판단 방식 비교 실험. 무작위 표본을 모델에 일괄 제시하고 경향에 대한 총평을 받는 방식으로, 모델 간 분기의 존재를 처음 관찰한 실험이다. 산출물(프롬프트·응답)은 [`data/llm_compare/`](../data/llm_compare/)에 보존.

| 파일 | 역할 |
|---|---|
| `01_extract_samples.py` | seed별 무작위 표본 추출 (seed 42/123/456) |
| `02_build_prompts.py` | 총괄 판단 프롬프트 생성 |

## 변경 이력

- **2026-06-12**: 평면 구조였던 archive/를 하위 폴더 5개(v1_pipeline, superseded_phase1, superseded_phase2, validation, llm_compare)로 재구조화. `scripts/_archive/`의 구버전 스크립트를 본 폴더로 통합하여 비활성 코드 보존 위치를 일원화.
