"""
hanja.dev 모델 sanity check: 문체별 비교
- 권1: 어록체 (理氣 논의, 短句 위주)
- 권127: 어록체+정치사 짧은 평론 (본조 太祖朝)
- 권130: 송대 정치사 서술체 (本朝四 / 自熈寧至靖康人物, 王安石 신법)

원본: KR3a0047 (칸리포 백문). byline·페이지구분·들여쓰기 제거 후 이어붙임.
이체자(隂/陰 등) 원문 그대로.
"""

import json
import unicodedata
from pathlib import Path

from huggingface_hub import snapshot_download
from transformers import BertTokenizerFast, BertForTokenClassification, pipeline

MODEL_TAG = "seyoungsong/SikuRoBERTa-PUNC-AJD-KLC"
LOCAL_DIR = Path.home() / ".cache" / "hanja_punc_model"


def load_model():
    snapshot_download(repo_id=MODEL_TAG, local_dir=LOCAL_DIR)
    safetensors_files = sorted(LOCAL_DIR.rglob("*.safetensors"))
    hface_path = safetensors_files[0].parent
    label2id_path = hface_path / "label2id.json"
    if not label2id_path.is_file():
        label2id_path = hface_path.parent / "label2id.json"
    label2id = json.loads(label2id_path.read_text(encoding="utf-8"))
    tokenizer = BertTokenizerFast.from_pretrained(hface_path, model_max_length=512)
    model = BertForTokenClassification.from_pretrained(hface_path)
    model.eval()
    pipe = pipeline(task="ner", model=model, tokenizer=tokenizer)
    return pipe, label2id


def punctuate(text: str, pipe, label2id: dict) -> str:
    label2punc = {f"B-{v}": k for k, v in label2id.items()}
    label2punc["O"] = ""
    predictions = pipe(text)
    chars = list(text)
    puncs = [""] * len(chars)
    for pred in predictions:
        idx = pred["end"] - 1
        puncs[idx] = label2punc.get(pred["entity"], "")
    return "".join(c + p for c, p in zip(chars, puncs))


def remove_punc(s: str) -> str:
    return "".join(c for c in s if unicodedata.category(c)[0] not in "PZ")


# ============================================================
# 칸리포 발췌 (백문)
# ============================================================

SAMPLES = [
    (
        "권1 첫 條 전반 (理氣論, 어록체 短句)",
        "問太極不是未有天地之先有箇渾成之物是天地萬物之理總名否曰太極只是天地萬物之理在天地言則天地中有太極在萬物言則萬物中各有太極未有天地之先畢竟是先有此理動而生陽亦只是理静而生隂亦只是理",
    ),
    (
        "권1 첫 條 후반 (動静論, 길게 이어지는 백문)",
        "問太極解何以先動而後静先用而後體先感而後寂曰在隂陽言則用在陽而體在隂然動静無端隂陽無始不可分先後今只就起處言之畢竟動前又是静用前又是體感前又是寂陽前又是隂而寂前又是感静前又是動將何者為先後不可只道今日動便為始而昨日静更不說也如鼻息言呼吸則辭順不可道吸呼畢竟呼前又是吸吸前又是呼",
    ),
    (
        "권127 太祖朝 (短篇 정치 평론, 王介甫 비교)",
        "或言太祖受命盡除五代弊法用能易亂為治曰不然只是去其甚者其他法令條目多仍其舊大凡做事底人多是先其大綱其他節目可因則因此方是英雄手段如王介甫大綱都不曾理㑹却纎悉於細微之間所以弊也",
    ),
    (
        "권130 自熈寧至靖康人物 (송대 정치사 서술체, 인명 다수)",
        "問荆公得君之故曰神宗聰明絶人與羣臣説話往往領畧不去才與介甫説便有於吾言無所不説底意思所以君臣相得甚懽向見何萬一之少年時所著數論其間有説云本朝自李文靖公王文正公當國以來廟論主於安靜凡有建明便以生事歸之馴至後來天下弊事極多此説甚好且如仁宗朝是甚次第時節國勢却如此緩弱事多不理英宗即位已自有性氣要改作但以聖躬多病不久晏駕所以當時諡之曰英神宗繼之性氣越緊尤欲更新之便是天下事難得恰好却又撞著介甫出來承當所以作壞得如此又曰介甫變法固有以召亂後來又却不别去整理一向放倒亦無縁治安",
    ),
]


def main():
    pipe, label2id = load_model()
    print()
    for label, plain in SAMPLES:
        plain = remove_punc(plain)
        pred = punctuate(plain, pipe, label2id)
        print(f"=== {label} ({len(plain)}자) ===")
        print(f"백문 : {plain}")
        print(f"모델 : {pred}")
        print()


if __name__ == "__main__":
    main()
