"""
22_embed_li_sentences.py (v3 - MID-LAYER TOKEN EMBEDDING)

변경 사유 (v2 → v3):
  v2 (last layer hidden state) → silhouette 0.85, 클러스터 실체는 道理/天理/理會
  같은 bigram surface form. BERT의 last layer는 인접 토큰에 과도하게 편향됨.
  Mid-layer는 semantic 정보를 더 잘 담는 것으로 알려짐.

LAYER 상수만 바꿔서 다른 layer로도 재실험 가능 (6, 9, 12 비교 가능).

입력:
  data/final/zhuzi_sentences.xlsx (li_sentences 시트)

출력:
  data/processed/li_token_embeddings.npy       (10474, 768)
  data/processed/li_token_mapping.parquet
"""

from pathlib import Path
import numpy as np
import pandas as pd
import torch
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModel

PROJECT_ROOT = Path("/Users/vairocana/projects/sadan-chiljeong-dh")
INPUT_PATH = PROJECT_ROOT / "data" / "final" / "zhuzi_sentences.xlsx"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed"
OUTPUT_VECTORS = OUTPUT_DIR / "li_token_embeddings.npy"
OUTPUT_MAPPING = OUTPUT_DIR / "li_token_mapping.parquet"

MODEL_NAME = "SIKU-BERT/sikubert"
TARGET_CHAR = "理"
MAX_LENGTH = 512
CONTEXT_WINDOW = 10
DEVICE = "mps" if torch.backends.mps.is_available() else \
         ("cuda" if torch.cuda.is_available() else "cpu")
BATCH_SIZE = 16

# === LAYER SELECTION ===
# hidden_states 인덱싱:
#   0    = embedding layer (transformer pass 전)
#   1~12 = transformer layers
#   12   = last (v2 default, surface 패턴 편향)
#   6    = mid-layer (semantic info가 더 강하게 보존)
LAYER = 12

COL_SENT_ID = "sentence_id"
COL_TEXT = "text_plain"


def load_sentences():
    p = INPUT_PATH
    if p.suffix == ".xlsx":
        df = pd.read_excel(p, sheet_name="li_sentences")
    elif p.suffix == ".jsonl":
        df = pd.read_json(p, lines=True)
    else:
        df = pd.read_csv(p)
    print(f"Loaded {len(df):,} sentences from {p.name}")
    return df


def extract_li_embeddings_batch(sentences, sentence_ids, tokenizer, model, li_token_id, layer):
    enc = tokenizer(
        sentences,
        return_tensors="pt", padding=True, truncation=True,
        max_length=MAX_LENGTH, return_offsets_mapping=True,
    )
    input_ids = enc["input_ids"].to(DEVICE)
    attention_mask = enc["attention_mask"].to(DEVICE)
    offsets = enc["offset_mapping"]

    with torch.no_grad():
        out = model(input_ids=input_ids, attention_mask=attention_mask,
                    output_hidden_states=True)
    hidden = out.hidden_states[layer]   # ← layer 선택

    vectors = []
    records = []
    truncated_count = 0

    for b_idx, sent_id in enumerate(sentence_ids):
        orig_text = sentences[b_idx]
        seq_ids = input_ids[b_idx]
        seq_offsets = offsets[b_idx].tolist()

        li_positions = (seq_ids == li_token_id).nonzero(as_tuple=True)[0].tolist()
        orig_li_count = orig_text.count(TARGET_CHAR)
        if len(li_positions) < orig_li_count:
            truncated_count += 1

        for li_idx_in_sent, tok_pos in enumerate(li_positions):
            vec = hidden[b_idx, tok_pos].cpu().numpy().astype(np.float32)
            vectors.append(vec)

            start, end = seq_offsets[tok_pos]
            char_left = orig_text[max(0, start - CONTEXT_WINDOW):start]
            char_right = orig_text[end:end + CONTEXT_WINDOW]

            records.append({
                "token_id": f"{sent_id}_li{li_idx_in_sent + 1}",
                "sentence_id": sent_id,
                "li_idx_in_sent": li_idx_in_sent + 1,
                "token_pos": int(tok_pos),
                "char_pos_in_sent": int(start),
                "char_left": char_left,
                "char_right": char_right,
            })

    return vectors, records, truncated_count


def main():
    print(f"\n=== Token Embedding Extraction (LAYER {LAYER}) ===")
    print(f"Model:   {MODEL_NAME}")
    print(f"Device:  {DEVICE}")
    print(f"Layer:   {LAYER} (0=embedding, 12=last)\n")

    df = load_sentences()
    if COL_TEXT not in df.columns:
        raise KeyError(f"Column '{COL_TEXT}' not found. Available: {list(df.columns)}")

    print("Loading model...")
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    model = AutoModel.from_pretrained(MODEL_NAME).to(DEVICE)
    model.eval()

    li_token_id = tokenizer.convert_tokens_to_ids(TARGET_CHAR)
    print(f"  {TARGET_CHAR} token_id = {li_token_id}")
    print(f"  Total transformer layers: {model.config.num_hidden_layers}\n")

    all_vectors = []
    all_records = []
    total_truncated = 0

    sentences = df[COL_TEXT].tolist()
    sentence_ids = df[COL_SENT_ID].tolist()

    for start in tqdm(range(0, len(sentences), BATCH_SIZE), desc=f"Embed L{LAYER}"):
        end = min(start + BATCH_SIZE, len(sentences))
        batch_sents = sentences[start:end]
        batch_ids = sentence_ids[start:end]
        vecs, recs, trunc = extract_li_embeddings_batch(
            batch_sents, batch_ids, tokenizer, model, li_token_id, LAYER
        )
        all_vectors.extend(vecs)
        all_records.extend(recs)
        total_truncated += trunc

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    vectors_arr = np.stack(all_vectors)
    np.save(OUTPUT_VECTORS, vectors_arr)
    mapping_df = pd.DataFrame(all_records)
    mapping_df.to_parquet(OUTPUT_MAPPING, index=False)

    print("\n" + "=" * 60)
    print(f"DONE (LAYER {LAYER})")
    print(f"  Total 理 tokens: {len(all_records):,}")
    print(f"  Vector shape:    {vectors_arr.shape}")
    print(f"  Truncated:       {total_truncated}")
    print("=" * 60)


if __name__ == "__main__":
    main()
