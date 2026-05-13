"""
SikuBERT 토크나이저 점검 (3분 소요)

목적:
1. 理가 단일 토큰으로 처리되는지 확인
2. 토큰 위치 추출 로직이 정확히 동작하는지 검증
3. 다중 출현 케이스에서 모든 理 위치가 잡히는지 확인

본인 로컬에서:
  python check_tokenizer.py
"""

from transformers import AutoTokenizer

# ============================================================
# 모델 선택 — 본인이 phase1에서 쓰던 거 그대로 (확인 필요)
# ============================================================
MODEL_NAME = "SIKU-BERT/sikubert"   # 가장 일반적인 SikuBERT
# 만약 SikuRoBERTa-PUNC-AJD-KLC를 쓴다면 (표점 모델이라 임베딩용으로는 비추천):
# MODEL_NAME = "seyoungsong/SikuRoBERTa-PUNC-AJD-KLC"

print(f"=== Loading: {MODEL_NAME} ===\n")
tok = AutoTokenizer.from_pretrained(MODEL_NAME)

# ============================================================
# 테스트 1: 理 단일 토큰 여부
# ============================================================
print("--- Test 1: 理 토큰 ID ---")
li_id = tok.convert_tokens_to_ids("理")
print(f"理 -> token_id = {li_id}")
print(f"  (UNK token id = {tok.unk_token_id})")
assert li_id != tok.unk_token_id, "理가 UNK로 처리됨! 모델 교체 필요"
print("  OK: 理는 vocab에 단일 토큰으로 존재\n")

# ============================================================
# 테스트 2: 다양한 문장에서 토큰화 결과
# ============================================================
test_sentences = [
    "理之發",                            # 능동 핵심
    "太極乃理也",                        # 형이상학적
    "理會去做",                          # 동사적
    "理只是無爲",                        # 수동
    "至妙之理有生生之意焉",              # 골든 샘플 M
    "義理無窮心體有限",                  # 골든 샘플 E
    "此理常流通者惟天地與聖人耳",        # 골든 샘플 M (지시 포함)
    "理流行而理動理生理能理之發",        # 한 문장에 理 6번 (다중 위치 테스트)
]

print("--- Test 2: 문장별 토큰화 ---")
for sent in test_sentences:
    tokens = tok.tokenize(sent)
    print(f"\n원문 ({len(sent)}자): {sent}")
    print(f"토큰 ({len(tokens)}): {tokens}")

    # 理 위치 찾기 (offset 매핑까지)
    encoding = tok(sent, return_offsets_mapping=True, add_special_tokens=True)
    input_ids = encoding['input_ids']
    offsets = encoding['offset_mapping']
    decoded_tokens = tok.convert_ids_to_tokens(input_ids)

    li_positions = []
    for i, tid in enumerate(input_ids):
        if tid == li_id:
            li_positions.append((i, offsets[i]))
    print(f"理 출현 위치 (CLS 포함 인덱스): {li_positions}")
    print(f"전체 토큰 (with special): {decoded_tokens}")

# ============================================================
# 테스트 3: 한 문장 내 理 multi-occurrence 카운트 정확성
# ============================================================
print("\n--- Test 3: 다중 출현 카운트 ---")
multi_sent = "理流行而理動理生理能理之發"
char_count = multi_sent.count("理")
tokens = tok.tokenize(multi_sent)
token_count = tokens.count("理")
print(f"원문 글자 카운트: {char_count}")
print(f"토큰 카운트: {token_count}")
assert char_count == token_count, "글자 수 != 토큰 수! subword 쪼개짐 발생 가능성"
print("OK: 모든 理가 단일 토큰으로 잡힘\n")

# ============================================================
# 결과 요약
# ============================================================
print("=" * 50)
print("✓ 토크나이저 점검 통과")
print(f"  모델: {MODEL_NAME}")
print(f"  理 token_id: {li_id}")
print(f"  vocab_size: {tok.vocab_size}")
print("=" * 50)
print("\n다음 단계: 22_embed_li_sentences.py 실행")
