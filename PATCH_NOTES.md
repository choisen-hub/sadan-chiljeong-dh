# repo_sync_0612 패치

최종보고서 확정판(2026-06-12) 동기화 패치. 저장소 루트에서 압축을 풀면
아래 파일이 같은 경로에 덮어쓰기/추가된다.

## 변경 파일
- `README.md` — 확정 제목 반영, '코퍼스'→'데이터' 용어 통일, figures 목록·변경 이력 갱신, 영문 README 링크
- `scripts/24_visualize_clusters.py` — UMAP 두 그림에 군집 정체 레이블 추가
  (크기 내림차순 순위 → 레이블 매핑: 之理·此理 / 道理·義理 / 理會 / 天理 (+HDBSCAN 窮理)).
  로컬(iMac)에서 재실행하면 figures가 레이블 포함판으로 재생성된다.

## 신규 파일
- `README_EN.md` — 영문 README
- `scripts/40_pipeline_figure.py` — 보고서 그림 4-1(전체 분석 프로세스) 생성 스크립트
- `figures/fig_pipeline.png` — 그림 4-1
- `figures/fig_umap_kmeans.png`, `figures/fig_umap_hdbscan.png` — 군집 레이블 포함판
  (원본 PNG에 PIL 주석; 24번 재실행 시 데이터 좌표 기반으로 동일하게 재생성 가능)

## 적용
```
cd /Users/vairocana/projects/sadan-chiljeong-dh
unzip -o repo_sync_0612.zip
git add -A
git commit -m 'sync: final report 2026-06-12 (labels, pipeline figure, EN README)'
git push
```
