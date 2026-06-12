# Digital Text Analysis of the Four-Seven Debate

Independent Research Project, College of Liberal Studies, Seoul National University (2026 Spring)

*[한국어 README](README.md)*

This project examines the hypothesis that the textual polysemy of the *Zhuzi yulei* (朱子語類, Classified Conversations of Master Zhu) may constitute a structural condition for the divergent interpretations of the Four-Seven Debate by Toegye Yi Hwang and Yulgok Yi I, using digital humanities methods (embedding-based clustering, citation matching, and multi-LLM adjudication), and diagnoses the possibilities and limits of each method for this problem. This repository contains the full dataset, code, and analysis outputs of the final report, "Exploring the Textual Conditions for the Formation of the Four-Seven Debate: An Examination of Polysemy through Text Analysis of the Zhuzi yulei and the Correspondence of Toegye and Yulgok, with Its Methodological Limitations" (2026-06-12).

- Researcher: Choi Seungho (choisen@snu.ac.kr)
- Advisors: Kim Baro (Academy of Korean Studies), Yang Il-mo (Seoul National University)

## Research Questions

- **RQ1**: When the usages of *li* (理) in the *Zhuzi yulei* are analyzed by unsupervised embedding clustering, is a semantic division captured (including the distinction between active and passive/relational descriptions)?
- **RQ2**: How do Toegye's and Yulgok's citation patterns of the *Zhuzi yulei* differ, and how does that difference relate to the semantic division of RQ1?
- **RQ3**: Taking RQ1 and RQ2 together, is there evidence that the textual structure of the *Zhuzi yulei* exhibits a polysemy that permits both sides' divergent interpretations?

## Pipeline

Stage divisions follow the chapter structure of the final report.

### Phase 0: Data Construction & Baseline Analysis (Report §4.1–4.2, §5.1)

Phase 0 spans data collection and preprocessing (scripts 01–15) plus the quantitative distribution of key characters (16); together they form the common basis for all subsequent analyses.

- `common/punctuate_hanja.py` — shared module for punctuation restoration with SikuRoBERTa-PUNC-AJD-KLC

*Zhuzi yulei* processing:
- `01_fetch_kanripo.py` — fetch Kanripo KR3a0047
- `02_parse_kanripo.py` — raw source → paragraph structuring (unpunctuated text)
- `03_punctuate.py` — unpunctuated text → punctuation restoration
- `04_segment.py` — sentence segmentation by punctuation
- `05_annotate.py` — 理/氣 flags, char_count, categories
- `06_export_xlsx.py` — final spreadsheet

Correspondence processing (22 Toegye letters + 9 Yulgok letters):
- `11_crawl_itkc.py` — extract letters from the Open Government Data Portal *Munjip ch'onggan* XML (ZIP)
- `12_punctuate.py` — punctuation restoration
- `13_segment_letters.py` — sentence segmentation
- `14_annotate_letters.py` — 理/氣 flags
- `15_export_letters_xlsx.py` — final spreadsheet

Baseline distribution analysis:
- `16_letter_char_stats.py` — distribution of key characters in the correspondence data (reproduces Report Table 5-1)

### Phase 1: Embedding Clustering of 理 (Report §5.2 → RQ1)

- `21_control_candidates.py` — control-group candidates (心/性/天)
- `22_embed_li_sentences.py` — SikuBERT 理 token embeddings (layer 12, 10,474 tokens)
- `23_cluster_kmeans.py` — UMAP 50D + K-means K=2–10
- `23b_cluster_hdbscan.py` — HDBSCAN grid
- `24_visualize_clusters.py` — silhouette curve, UMAP 2D visualization (cluster identity labels included)
- `28_cluster_interpretation.py` — representative sentences and bigrams per cluster

The earlier sentence-level embedding analysis (理·心·性·天 comparison, first half of Report §5.2.2) is preserved as `scripts/_archive/22_embed_li_sentences_v1_sentence_emb.py.bak`.

### Phase 2: Citation Matching (Report §5.3 → RQ2)

- `31_citation_matching.py` — LCS + IDF-weighted matching
- `32_export_candidates_xlsx.py` — review spreadsheet export

The matching domain is restricted to 理-containing sentence pairs: 510 letter sentences containing 理 (251 Toegye + 259 Yulgok) × 8,443 *Zhuzi yulei* sentences containing 理.

### Multi-LLM Sentence-Level Adjudication (Report §5.4 → complements RQ3)

- `scripts/llm_compare/` — pilot experiments (aggregate-level judgments; outputs in `data/llm_compare/`)
- `30_llm_judgment.py` — A/B/C/U/N adjudication of 8,428 理 sentences (OpenRouter, temperature=0)
- `31_llm_agreement.py` — inter-model agreement, Fleiss κ, consensus/contested sentence extraction (outputs in `data/llm_judgment/`; see folder README)

### Figures

- `40_pipeline_figure.py` — generates the overall process diagram (Report Figure 4-1)

## Output Scale

### Data (Report Table 4-1)

| Item | Value |
|---|---|
| *Zhuzi yulei* juan records | 144 (incl. 4 sections of juan 0) |
| *Zhuzi yulei* paragraphs | 14,597 |
| *Zhuzi yulei* sentences | 71,645 (avg. 21.5 chars) |
| *Zhuzi yulei* sentences containing 理 | 8,443 (理+氣 co-occurring: 484) |
| Letters | 31 (22 Toegye + 9 Yulgok) |
| Letter sentences | 2,081 (1,305 Toegye + 776 Yulgok, avg. 23.3 chars) |

### Phase 0 Baseline: Distributional Asymmetry (Report Table 5-1)

| Item | Toegye | Yulgok | Ratio (Y/T) |
|---|---|---|---|
| Sentences with 理 or 氣 | 24.1% (314/1,305) | 42.8% (332/776) | 1.78× |
| Total occurrences of 互 | 6 | 41 | 6.83× |
| Total occurrences of 情 | 186 | 73 | 0.39× |

### Phase 1: Clustering (Report Figures 5-1 to 5-3)

| Item | Value |
|---|---|
| Sentence embeddings (v1) | 理 K=2 silhouette 0.058; controls 心 0.046 / 性 0.052 / 天 0.039 — clusters barely formed |
| Token embeddings, K-means | K=4, silhouette 0.85 |
| Token embeddings, HDBSCAN | mcs=200, 5 clusters, 0.3% noise, silhouette 0.79 |
| Cluster identities | 天理 / 理會 / 之理·此理 / 道理·義理 — surface lexical collocation patterns, not the active/passive semantic axis |

### Phase 2: Citation Matching (Report Table 5-3)

| Item | Value |
|---|---|
| Algorithm | LCS + IDF weighting (4-gram combined-corpus IDF), minimum LCS 4 chars |
| Total match candidates | 1,504 (762 Toegye + 742 Yulgok) |
| Matches ≥ 6 chars | 50 (42 Toegye + 8 Yulgok) |
| Matches ≥ 7 chars | 29 (all Toegye; 0 Yulgok) |
| Longest match | 24 chars (Toegye) / 6 chars (Yulgok) |
| Interpretation | Toegye: numerous verbatim long-passage citations (全引·改引). Yulgok: 約引·意引 plausible but not directly verifiable by morphological methods (Report §5.3.2). Manual review corrections: T0513 is a shared source (Cheng Mingdao's "Dingxing shu" 定性書), and Y0018 (是理當如此) is an idiomatic expression, not a citation. |

### Multi-LLM Adjudication (Report §5.4)

| Item | Value |
|---|---|
| Target | 8,428 理 sentences (15 fragments under 6 chars excluded from 8,443) |
| Models | ChatGPT (gpt-4.1-mini) · Gemini (gemini-2.5-flash) |
| Overall unanimity / Fleiss κ | 0.699 / 0.448 (vs. multilingual LLM-judge prior average κ≈0.3 — Fu & Liu 2025) |
| Both models A/B | 239 sentences, 97.1% (232) in agreement — only 7 head-on conflicts |
| At least one model A/B | 654 sentences; full agreement 35.5%, one-sided visibility 63.5% (asymmetric visibility) |
| Share of C (undetermined) | ~40% per model on average |
| Model disposition | A/B ratio: ChatGPT 2.3 vs. Gemini 6.0 — Gemini grants 理 agency far more often |
| DeepSeek | excluded after repeated timeouts/non-responses (stopped at 230 sentences; see `data/llm_judgment/README.md`) |

## Data Sources

See `docs/판본정보.md` for edition details.

- *Zhuzi yulei* base text: Kanseki Repository, Kyoto University (Kanripo, KR3a0047)
- *Zhuzi yulei* punctuation: hanja.dev (SikuRoBERTa-PUNC-AJD-KLC; Song et al. 2025, HERITAGE)
- *T'oegye sŏnsaeng munjip* / *Yulgok sŏnsaeng chŏnsŏ*: *Han'guk munjip ch'onggan* (Institute for the Translation of Korean Classics; Open Government Data Portal XML, release 2024-08-30; dataset "한국고전번역원_한국문집총간", https://www.data.go.kr/data/3074298/fileData.do)
- Embedding model: SIKU-BERT/sikubert
- Adjudication LLMs: `openai/gpt-4.1-mini`, `google/gemini-2.5-flash` via OpenRouter

## Data Collection Dates & AI Usage

**Data collection dates**
- Kanripo KR3a0047: collected 2026-04
- *Munjip ch'onggan* XML: release 2024-08-30, extracted 2026-04-27 (`docs/letter_provenance.md`)
- Punctuation unification (hanja.dev): 2026-05-06
- LLM sentence-level adjudication run: 2026-06-01

**AI usage**
- LLMs as analytical instruments: two models (ChatGPT, Gemini) were called via the OpenRouter API to adjudicate the active/passive reading of 理 sentences (temperature=0, identical prompt and input). The full prompt is included in `scripts/30_llm_judgment.py`. Model dependence and cultural bias are discussed in Report §5.4.5–5.4.6.
- LLMs as development aids: an LLM (Claude) was used as an auxiliary tool for writing and debugging the analysis pipeline. Research design, data and edition decisions, qualitative judgment and verification of results, and the writing of the report were performed by the researcher.

## Reproduction

Install dependencies:

```
pip install -r requirements.txt
```

Run stage by stage (see `--help` or each script's docstring for options):

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
python3 scripts/40_pipeline_figure.py
```

LLM adjudication (OpenRouter API key required):

```
export OPENROUTER_API_KEY=...
python3 scripts/30_llm_judgment.py --input data/final/zhuzi_sentences.xlsx --sheet li_sentences --providers openai,gemini
python3 scripts/31_llm_agreement.py --merged data/llm_judgment/merged_2models.csv
```

## Project Layout

- `README.md` / `README_EN.md`
- `requirements.txt`
- `common/punctuate_hanja.py`
- `config/letter_targets.yaml` — letter extraction targets
- `scripts/` — numbered pipeline scripts
  - `llm_compare/` — LLM pilot experiments
  - `_archive/` — superseded versions (sentence-embedding v1, etc.)
- `data/`
  - `raw/`, `intermediate/`, `processed/` — git-ignored
  - `final/` — final spreadsheets (committed)
    - `zhuzi_sentences.xlsx`, `corpus_review.xlsx`
    - `li_clustering_results.xlsx`, `citation_candidates_review.xlsx`
  - `llm_compare/` — pilot prompts and responses
  - `llm_judgment/` — sentence-level adjudication outputs (see folder README)
- `figures/` — report figures
  - `fig_pipeline.png` (Figure 4-1; generated by `scripts/40_pipeline_figure.py`)
  - `fig_silhouette_curve.png` (Figure 5-1)
  - `fig_umap_kmeans.png` (Figure 5-2; cluster identity labels on the K=4 panel)
  - `fig_umap_hdbscan.png` (Figure 5-3; cluster identity labels)
- `docs/` — edition notes, provenance, Phase 1 decision rubric
- `archive/` — v1 pipeline remnants (see folder README)

## Changelog

- **2026-06-12**: Synchronized with the finalized report. Phase 0 redefined explicitly as the whole of data construction (collection & preprocessing) plus the baseline distribution analysis, matching Report §4.3 and Figure 4-1. Final title reflected; terminology unified ('corpus' → 'data'); cluster identity labels added to both UMAP figures (`24_visualize_clusters.py` patched with size-rank-based mapping); overall process diagram (Figure 4-1) and its generator (`40_pipeline_figure.py`) added; English README added; citation-table corrections reflected (T0513 → shared source, Cheng Mingdao's "Dingxing shu"; Y0018 → idiomatic 是理當如此, not a citation)
- **2026-06-11**: Documentation sync against the final report. LLM adjudication results, AI usage, and data collection dates recorded in README; requirements.txt rebuilt; Phase 0 stats script (16) added; source for the 7 core Toegye letters corrected to Kim Sejong (2024, *Han'guk ch'ŏrhak nonjip* 83) (previously misattributed to Hwang Junyeon (2009)); ≥6-char citation counts corrected against the outputs (Toegye 42, Yulgok 8)
- **2026-06-01**: Sentence-level LLM adjudication pipeline (30, 31_llm_agreement) and results added (2 models via OpenRouter, 8,428 sentences)
- **2026-05-14**: Phase 1 switched to token embeddings (from sentence embeddings); Phase 2 LCS+IDF citation matching pipeline added
- **2026-05-06 (v2)**: Punctuation unified to hanja.dev for both *Zhuzi yulei* and the letters
- **2026-04-28 (v1)**: Initial pipeline (祝平次 punctuation + hanja.dev fallback)

## License

Code is MIT. Source-text data follow the terms of their respective providers.
