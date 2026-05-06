"""
22_embed_li_sentences.py
=========================

Phase 1 Step 1·3 준비: 주자어류 sentence를 SikuBERT로 임베딩.

대상:
- 理 포함 문장 (Step 1 메인, ~8,443개)
- 心/性/天 포함 문장 (Step 3 대조군)

산출:
- data/processed/phase1/embeddings/{char}_embeddings.parquet
  컬럼: sentence_id, sent_text_plain, has_li/has_qi/li_qi_category(있으면), embedding(768-dim)

설계 결정 (2026-05-06 합의):
- mean pooling (last hidden layer) — [CLS] 아님
- 길이 필터: 3자 미만 제외 (default, --min_char_count로 조정)
- max_length=64 (평균 22자 기준 충분)
- 디바이스: CUDA > MPS(Apple Silicon) > CPU 자동 선택

사용법:
    python scripts/22_embed_li_sentences.py
    python scripts/22_embed_li_sentences.py --batch_size 16 --min_char_count 5
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from tqdm.auto import tqdm
from transformers import AutoModel, AutoTokenizer

# ---------------------------------------------------------------------------
TARGET_CHARS = ["理", "心", "性", "天"]  # 메인 + 대조군
SIKUBERT_MODEL = "SIKU-BERT/sikubert"
DEFAULT_BATCH_SIZE = 32
DEFAULT_MAX_LENGTH = 64
MIN_CHAR_COUNT = 3
# ---------------------------------------------------------------------------


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


def detect_device() -> torch.device:
    """CUDA > MPS > CPU 자동 선택."""
    if torch.cuda.is_available():
        return torch.device("cuda")
    if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        return torch.device("mps")
    return torch.device("cpu")


def load_data(path: Path, sheet: str | None = None) -> pd.DataFrame:
    """multi-format auto-detection. sheet는 xlsx에만 적용."""
    suffix = path.suffix.lower()
    if suffix == ".jsonl":
        return pd.read_json(path, lines=True)
    if suffix == ".json":
        return pd.read_json(path)
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in (".xlsx", ".xls"):
        return pd.read_excel(path, sheet_name=sheet) if sheet else pd.read_excel(path)
    if suffix == ".parquet":
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported format: {suffix}")


def report_length_distribution(df: pd.DataFrame, text_col: str) -> None:
    """길이 분포 보고."""
    lens = df[text_col].dropna().str.len()
    logging.info(f"=== Length distribution ({text_col}) ===")
    logging.info(f"  count: {len(lens):,}")
    logging.info(f"  mean: {lens.mean():.1f}, median: {lens.median():.0f}")
    logging.info(f"  min: {lens.min()}, max: {lens.max()}")
    logging.info(f"  p95: {lens.quantile(0.95):.0f}, p99: {lens.quantile(0.99):.0f}")
    logging.info(f"  < 3 chars: {(lens < 3).sum():,} ({100*(lens<3).mean():.2f}%)")
    logging.info(f"  < 5 chars: {(lens < 5).sum():,} ({100*(lens<5).mean():.2f}%)")
    logging.info(f"  > 60 chars: {(lens > 60).sum():,} ({100*(lens>60).mean():.2f}%)")


def extract_target_sentences(df: pd.DataFrame, char: str, text_col: str) -> pd.DataFrame:
    """타겟 글자 포함 문장 추출."""
    return df[df[text_col].str.contains(char, na=False, regex=False)].copy()


def mean_pool(last_hidden: torch.Tensor, attention_mask: torch.Tensor) -> torch.Tensor:
    """attention mask 고려한 mean pooling.

    last_hidden: (B, L, H), attention_mask: (B, L)
    """
    mask = attention_mask.unsqueeze(-1).float()  # (B, L, 1)
    summed = (last_hidden * mask).sum(dim=1)     # (B, H)
    counts = mask.sum(dim=1).clamp(min=1e-9)     # (B, 1)
    return summed / counts


@torch.inference_mode()
def embed_batch(
    texts: list[str],
    tokenizer,
    model,
    device: torch.device,
    max_length: int,
) -> np.ndarray:
    enc = tokenizer(
        texts,
        padding=True,
        truncation=True,
        max_length=max_length,
        return_tensors="pt",
    ).to(device)
    out = model(**enc)
    pooled = mean_pool(out.last_hidden_state, enc["attention_mask"])
    return pooled.cpu().numpy()


def embed_sentences(
    df: pd.DataFrame,
    text_col: str,
    tokenizer,
    model,
    device: torch.device,
    batch_size: int,
    max_length: int,
    desc: str = "embedding",
) -> np.ndarray:
    """배치 단위 임베딩."""
    texts = df[text_col].tolist()
    out_parts = []
    for i in tqdm(range(0, len(texts), batch_size), desc=desc):
        batch = texts[i : i + batch_size]
        emb = embed_batch(batch, tokenizer, model, device, max_length)
        out_parts.append(emb)
    return np.vstack(out_parts)


def save_embeddings(df: pd.DataFrame, embeddings: np.ndarray, output_path: Path) -> None:
    """parquet 저장. embedding은 list-of-list로 저장."""
    df_out = df.copy()
    df_out["embedding"] = list(embeddings)  # 각 행이 ndarray → parquet에서 list로 직렬화
    df_out.to_parquet(output_path, index=False)
    logging.info(f"saved → {output_path} ({len(df_out):,} rows, dim={embeddings.shape[1]})")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--zhuzi_path", type=Path,
                        default=Path("data/final/zhuzi_sentences.xlsx"),
                        help="주자어류 sentence-level 파일")
    parser.add_argument("--sheet", type=str, default="all_sentences",
                        help="xlsx 시트명 (xlsx 입력 시에만 적용)")
    parser.add_argument("--output_dir", type=Path,
                        default=Path("data/processed/phase1/embeddings"),
                        help="출력 디렉토리")
    parser.add_argument("--text_col", type=str, default="sent_text_plain",
                        help="임베딩 입력 텍스트 컬럼")
    parser.add_argument("--id_col", type=str, default="sentence_id",
                        help="고유 ID 컬럼")
    parser.add_argument("--target_chars", type=str, nargs="+",
                        default=TARGET_CHARS,
                        help="대상 글자들 (기본: 理 心 性 天)")
    parser.add_argument("--batch_size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--max_length", type=int, default=DEFAULT_MAX_LENGTH)
    parser.add_argument("--min_char_count", type=int, default=MIN_CHAR_COUNT,
                        help="이 글자 수 미만 문장 제외 (기본 3)")
    parser.add_argument("--model_name", type=str, default=SIKUBERT_MODEL)
    args = parser.parse_args()

    setup_logging()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    # 1) load
    logging.info(f"load: {args.zhuzi_path}" + (f" (sheet={args.sheet})" if args.zhuzi_path.suffix.lower() in (".xlsx", ".xls") else ""))
    df = load_data(args.zhuzi_path, sheet=args.sheet)
    logging.info(f"total sentences: {len(df):,}")
    logging.info(f"columns: {df.columns.tolist()}")

    if args.text_col not in df.columns:
        raise KeyError(f"text_col '{args.text_col}' not in columns: {df.columns.tolist()}")
    if args.id_col not in df.columns:
        raise KeyError(f"id_col '{args.id_col}' not in columns: {df.columns.tolist()}")

    # 2) 길이 분포
    report_length_distribution(df, args.text_col)

    # 3) 길이 필터
    initial = len(df)
    df = df[df[args.text_col].str.len() >= args.min_char_count].copy()
    filtered = initial - len(df)
    logging.info(
        f"filtered <{args.min_char_count} chars: {filtered:,} "
        f"({100*filtered/initial:.2f}%) → remaining {len(df):,}"
    )

    # 4) device + model
    device = detect_device()
    logging.info(f"device: {device}")
    logging.info(f"loading model: {args.model_name}")
    tokenizer = AutoTokenizer.from_pretrained(args.model_name)
    model = AutoModel.from_pretrained(args.model_name).to(device)
    model.eval()

    # 5) 글자별 처리
    keep_cols_optional = ["has_li", "has_qi", "li_qi_category", "source_id"]

    for char in args.target_chars:
        logging.info(f"\n=== {char} ===")
        sub = extract_target_sentences(df, char, args.text_col)
        logging.info(f"sentences containing '{char}': {len(sub):,}")

        if len(sub) == 0:
            logging.warning(f"  no sentences for '{char}', skip")
            continue

        embeddings = embed_sentences(
            sub, args.text_col, tokenizer, model, device,
            args.batch_size, args.max_length,
            desc=f"embed {char}",
        )

        # 저장 컬럼 정리
        keep_cols = [args.id_col, args.text_col]
        for c in keep_cols_optional:
            if c in sub.columns:
                keep_cols.append(c)
        sub = sub[keep_cols].reset_index(drop=True)

        output_path = args.output_dir / f"{char}_embeddings.parquet"
        save_embeddings(sub, embeddings, output_path)

    logging.info("done.")


if __name__ == "__main__":
    main()
