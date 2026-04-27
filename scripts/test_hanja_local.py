"""
HERITAGE (hanja.dev) 표점 복원 모델 로컬 실행

backend/tool/punc.py 로직을 그대로 따라감:
1. snapshot_download로 모델 repo 전체 다운로드
2. *.safetensors 파일을 재귀 검색해서 실제 가중치 위치 파악
3. label2id.json 로드 후 표점 매핑
"""

import json
from pathlib import Path

from huggingface_hub import snapshot_download
from transformers import BertTokenizerFast, BertForTokenClassification, pipeline

MODEL_TAG = "seyoungsong/SikuRoBERTa-PUNC-AJD-KLC"
LOCAL_DIR = Path.home() / ".cache" / "hanja_punc_model"


def load_model():
    print(f"모델 다운로드/캐시 확인 중... ({MODEL_TAG})")
    snapshot_download(repo_id=MODEL_TAG, local_dir=LOCAL_DIR)

    # 가중치가 있는 실제 폴더 탐색
    safetensors_files = sorted(LOCAL_DIR.rglob("*.safetensors"))
    if not safetensors_files:
        raise FileNotFoundError(f"No .safetensors found in {LOCAL_DIR}")
    hface_path = safetensors_files[0].parent
    print(f"  → 가중치 위치: {hface_path}")

    # label2id.json 찾기 (가중치 폴더 또는 부모 폴더)
    label2id_path = hface_path / "label2id.json"
    if not label2id_path.is_file():
        label2id_path = hface_path.parent / "label2id.json"
    label2id: dict = json.loads(label2id_path.read_text(encoding="utf-8"))
    print(f"  → label2id: {label2id}")

    # 토크나이저 + 모델 로드
    tokenizer = BertTokenizerFast.from_pretrained(hface_path, model_max_length=512)
    model = BertForTokenClassification.from_pretrained(hface_path)
    model.eval()

    pipe = pipeline(task="ner", model=model, tokenizer=tokenizer)
    print("로딩 완료\n")
    return pipe, label2id


def punctuate(text: str, pipe, label2id: dict) -> str:
    """백문 → 표점 찍힌 텍스트 (Comprehensive 스타일)"""
    # entity는 "B-{id}" 형태. label2id를 뒤집어 "B-{id}" → 표점 매핑
    label2punc = {f"B-{v}": k for k, v in label2id.items()}
    label2punc["O"] = ""

    predictions = pipe(text)
    chars = list(text)
    puncs = [""] * len(chars)
    for pred in predictions:
        idx = pred["end"] - 1
        puncs[idx] = label2punc.get(pred["entity"], "")

    return "".join(c + p for c, p in zip(chars, puncs))


def main():
    pipe, label2id = load_model()

    samples = [
        "天命之謂性率性之謂道修道之謂教",                                          # 中庸
        "性即理也",                                                                # 주자 명제
        "四端理之發七情氣之發",                                                    # 퇴계식 명제
        "理與氣決是二物但在物上看則二物渾淪不可分開各在一處然不害二物之各為一物也",  # 주자어류 권1
    ]

    for s in samples:
        print(f"원문 ({len(s):2d}자): {s}")
        print(f"결과       : {punctuate(s, pipe, label2id)}\n")


if __name__ == "__main__":
    main()
