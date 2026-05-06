# archive/

v1 파이프라인 잔재. v2부터 hanja.dev 일괄적용으로 전환되면서 사용하지 않는 코드들.

## 보존된 파일

| 파일 | 역할 (v1) | 폐기 사유 (v2) |
|---|---|---|
| `03_parse_zhuzi_xml.py` | 祝平次 TEI-XML 파싱 | 祝平次 표점 사용 안 함 |
| `04_align.py` | 칸리포 vs 祝平次 정렬 + 일치율 측정 | 祝平次 표점 사용 안 함 |
| `05_punctuate.py` | 1~7권 祝平次 차용 + 8권~ hanja.dev fallback hybrid | 1~141권 일괄 hanja.dev로 통일 |
| `06_segment.py` | 주자어류 sentence 분절 (구) | 입력 변경(`paragraphs_punctuated`) 위해 04 로 재작성 |
| `07_annotate.py` | 理/氣 플래그 (구) | 단계 번호 정리 위해 05 로 이동 (로직 동일) |
| `08_export_xlsx.py` | 최종 xlsx (구) | 단계 번호 정리 위해 06 으로 이동 (로직 동일) |
| `12_segment_letters.py` | 서신 분절 (구, 한국문집총간 표점 기반) | hanja.dev 표점 기반 13 으로 재작성 |
| `13_export_xlsx.py` | 서신 xlsx (구) | 14 로 이동 + 이름에 `_letters_` 명시 |

## 산출물 보존

`data/processed/` 의 `alignment_summary.csv`, `zhuzi_mismatch_*.csv` 등 v1 검증 산출물은 GitHub 히스토리에 그대로 남겨둔다. v1 단계에서 칸리포·祝平次 판본 일치율 검증을 거쳤다는 흔적은 재현성 측면에서 가치가 있다.

자세한 변경 사유는 [`docs/판본정보.md`](../docs/판본정보.md) 의 변경 이력 참조.
