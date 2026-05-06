"""
common/punctuate_hanja.py

표점 부여 공통 유틸 (SikuRoBERTa-PUNC-AJD-KLC).
주자어류 / 퇴계집 / 율곡전서 파이프라인이 공유.

모델       : seyoungsong/SikuRoBERTa-PUNC-AJD-KLC
인터페이스 : 토큰 분류 (NER 스타일). 글자마다 "뒤에 붙일 표점" 라벨 분류.
표점 체계  : 한국고전번역원 (AJD/KLC) — 한국문집총간 corpus와 매치.

핵심 설계
---------
1. 모델 1회 로드 → 다회 호출 재사용 (HanjaPunctuator 클래스)
2. 청킹: 긴 텍스트는 max_chars 단위로 분할 후 결과 재결합
3. 캐싱: SHA256(model_id + revision + text) 기반, 중복 호출 방지
4. 메타데이터: 모델 ID / revision / 시각 / 해시 모두 결과에 기록 (재현성)
5. 입출력은 jsonl

특이사항
--------
- Auto* 클래스 못 씀 (config.json에 model_type 누락).
  BertTokenizerFast + BertForTokenClassification 직접 사용.
- label2id.json이 모델 repo 내 별도 파일 → 따로 로드.
- 라벨 형식: "B-。", "B-，", "B-；", ..., "O"

사용 예
-------
    from common.punctuate_hanja import HanjaPunctuator

    pun = HanjaPunctuator()
    stats = pun.punctuate_jsonl(
        "data/zhuzi_baekmun.jsonl",
        "data/zhuzi_punctuated.jsonl",
    )

CLI
---
    python -m common.punctuate_hanja input.jsonl output.jsonl
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path

import torch
from huggingface_hub import snapshot_download
from transformers import BertForTokenClassification, BertTokenizerFast, pipeline

logger = logging.getLogger(__name__)

DEFAULT_MODEL_ID = "seyoungsong/SikuRoBERTa-PUNC-AJD-KLC"
DEFAULT_MAX_CHARS = 480  # 모델 max_length 512 안쪽 안전 마진 ([CLS]/[SEP] 고려)
DEFAULT_LOCAL_DIR = Path.home() / ".cache" / "hanja_punc_model"
CACHE_DIR = Path(".cache/punctuate_hanja")


@dataclass
class PunctuationResult:
    text_id: str
    input_text: str
    punctuated_text: str
    model_id: str
    model_revision: str | None
    n_chunks: int
    timestamp: str
    input_hash: str


class HanjaPunctuator:
    """모델 1회 로드 후 재사용. punctuate() 또는 punctuate_jsonl() 호출."""

    def __init__(
        self,
        model_id: str = DEFAULT_MODEL_ID,
        revision: str | None = None,
        max_chars: int = DEFAULT_MAX_CHARS,
        device: str | None = None,
        cache_dir: Path | str = CACHE_DIR,
        local_dir: Path | str = DEFAULT_LOCAL_DIR,
    ):
        self.model_id = model_id
        self.revision = revision
        self.max_chars = max_chars
        self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.local_dir = Path(local_dir)

        logger.info("Loading %s (revision=%s) on %s", model_id, revision, self.device)
        self._pipe, self._label2punc = self._load_model()

    # ── 모델 로드 ─────────────────────────────────────────────

    def _load_model(self):
        """snapshot_download로 가중치 받아 BertForTokenClassification + NER pipeline 구성."""
        snapshot_download(
            repo_id=self.model_id,
            revision=self.revision,
            local_dir=str(self.local_dir),
        )
        # 가중치 위치 찾기 (subfolder 가능성 대비 rglob)
        safetensors_files = sorted(self.local_dir.rglob("*.safetensors"))
        if not safetensors_files:
            raise FileNotFoundError(f"safetensors 없음: {self.local_dir}")
        hface_path = safetensors_files[0].parent

        # label2id.json은 가중치와 같은 디렉토리, 또는 부모에 있을 수 있음
        label2id_path = hface_path / "label2id.json"
        if not label2id_path.is_file():
            label2id_path = hface_path.parent / "label2id.json"
        if not label2id_path.is_file():
            raise FileNotFoundError(f"label2id.json 없음: {hface_path}")

        label2id = json.loads(label2id_path.read_text(encoding="utf-8"))
        # 모델 출력 라벨("B-。" 등) → 실제 표점 문자 매핑
        label2punc = {f"B-{v}": k for k, v in label2id.items()}
        label2punc["O"] = ""

        tokenizer = BertTokenizerFast.from_pretrained(hface_path, model_max_length=512)
        model = BertForTokenClassification.from_pretrained(hface_path)
        model.eval()

        device_id = 0 if self.device == "cuda" else -1
        pipe = pipeline(
            task="ner",
            model=model,
            tokenizer=tokenizer,
            device=device_id,
        )
        return pipe, label2punc

    # ── 내부 헬퍼 ────────────────────────────────────────────

    def _hash(self, text: str) -> str:
        h = hashlib.sha256()
        for part in (self.model_id, self.revision or "", text):
            h.update(part.encode("utf-8"))
        return h.hexdigest()[:16]

    def _cache_path(self, input_hash: str) -> Path:
        return self.cache_dir / f"{input_hash}.json"

    def _split_chunks(self, text: str) -> list[str]:
        """글자수 기준 단순 분할.
        백문은 자연 경계 없음 → 균등 분할로 충분.
        품질 이슈 생기면 문단 경계 기반으로 교체."""
        if len(text) <= self.max_chars:
            return [text]
        return [text[i : i + self.max_chars] for i in range(0, len(text), self.max_chars)]

    def _punctuate_chunk(self, chunk: str) -> str:
        """단일 청크 추론. 각 글자 뒤에 분류된 표점 삽입한 문자열 반환."""
        if not chunk:
            return ""
        predictions = self._pipe(chunk)
        chars = list(chunk)
        puncs = [""] * len(chars)
        for pred in predictions:
            idx = pred["end"] - 1
            if 0 <= idx < len(puncs):
                puncs[idx] = self._label2punc.get(pred["entity"], "")
        return "".join(c + p for c, p in zip(chars, puncs))

    # ── 공개 API ────────────────────────────────────────────

    def punctuate(
        self,
        text: str,
        text_id: str,
        use_cache: bool = True,
    ) -> PunctuationResult:
        """단일 텍스트 표점 부여."""
        input_hash = self._hash(text)
        cache_file = self._cache_path(input_hash)

        if use_cache and cache_file.exists():
            return PunctuationResult(**json.loads(cache_file.read_text(encoding="utf-8")))

        chunks = self._split_chunks(text)
        punctuated = "".join(self._punctuate_chunk(c) for c in chunks)

        result = PunctuationResult(
            text_id=text_id,
            input_text=text,
            punctuated_text=punctuated,
            model_id=self.model_id,
            model_revision=self.revision,
            n_chunks=len(chunks),
            timestamp=datetime.now(timezone.utc).isoformat(),
            input_hash=input_hash,
        )

        if use_cache:
            cache_file.write_text(
                json.dumps(asdict(result), ensure_ascii=False),
                encoding="utf-8",
            )
        return result

    def punctuate_jsonl(
        self,
        input_path: Path | str,
        output_path: Path | str,
        text_field: str = "raw_text",
        id_field: str = "id",
        use_cache: bool = True,
    ) -> dict:
        """jsonl 일괄 처리.

        입력 한 줄: {"id": "...", "raw_text": "白文...", ...기타필드}
        출력 한 줄: 입력 + PunctuationResult 필드 머지.
        """
        input_path = Path(input_path)
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        n_total = 0
        n_cached = 0

        with input_path.open(encoding="utf-8") as f_in, \
             output_path.open("w", encoding="utf-8") as f_out:
            for line in f_in:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                text = rec[text_field]
                text_id = rec[id_field]

                cache_hit = self._cache_path(self._hash(text)).exists()
                result = self.punctuate(text, text_id, use_cache=use_cache)

                # 입력 + 결과 머지. PunctuationResult.input_text는 모델 입력 사본
                # (재현성용), 입력 레코드의 다른 필드와 이름 충돌 없음.
                out_rec = {**rec, **asdict(result)}
                f_out.write(json.dumps(out_rec, ensure_ascii=False) + "\n")

                n_total += 1
                if cache_hit:
                    n_cached += 1
                if n_total % 50 == 0:
                    logger.info("processed %d (cache hit %d)", n_total, n_cached)

        return {
            "n_total": n_total,
            "n_cached": n_cached,
            "n_new": n_total - n_cached,
        }


# ── CLI ──────────────────────────────────────────────────────────

def _main():
    import argparse

    parser = argparse.ArgumentParser(description="SikuRoBERTa-PUNC-AJD-KLC 표점 부여")
    parser.add_argument("input", help="입력 jsonl (id, raw_text 필드 필요)")
    parser.add_argument("output", help="출력 jsonl 경로")
    parser.add_argument("--model", default=DEFAULT_MODEL_ID)
    parser.add_argument("--revision", default=None, help="커밋 해시 권장 (재현성)")
    parser.add_argument("--max-chars", type=int, default=DEFAULT_MAX_CHARS)
    parser.add_argument("--no-cache", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    pun = HanjaPunctuator(
        model_id=args.model,
        revision=args.revision,
        max_chars=args.max_chars,
    )
    stats = pun.punctuate_jsonl(args.input, args.output, use_cache=not args.no_cache)
    print(json.dumps(stats, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    _main()
