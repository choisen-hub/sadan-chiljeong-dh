"""
30_llm_judgment.py
朱子語類 '理' 문장별 능동/수동/공존 판정 — 다중 LLM 비교 실행기 (2차 설계)

지도교수 5/25 피드백 반영:
- (1) 문장별 출력: 200개 집계형 폐기 → 문장마다 sentence_id + label 출력
- (2) 전체 문장 우선: temp=0, API, 문장 단위 판정 (학술적 정석)
- (3) 무작위 200 폐기 → 필요시 능/수 층화추출(--stratify)
- (4) 모델 간 합의가 핵심(에반게리온 MAGI): N(--reps)회 반복으로 자기일치도까지 측정

라벨 (1차 프롬프트 A/B/C 체계 유지 + 노이즈 거르개 2종)
  A 능동      : 理가 發/生/流行/主宰 등 작용의 주체로 서술됨
  B 수동·관계 : 理가 無爲/無形/所以/근거/한계, 또는 氣에 의존·탑승(乘氣)하여 현현
  C 공존      : 한 문장에 능동·수동 측면이 함께 나타나 환원 불가
  U 판단불가  : 맥락 부족·모호로 A/B/C 판정 불가
  N 비대상    : 형이상학적 理 아님 (理會/整理/文理/條理/일상 道理 등 어휘적 용법)

사용 예 (테스트 50, Gemini):
  python 30_llm_judgment.py --input data/final/zhuzi_sentences.xlsx --sheet li_sentences \
      --providers gemini --max-n 50

사용 예 (전체 3모델 × 3반복):
  python 30_llm_judgment.py --input data/final/zhuzi_sentences.xlsx --sheet li_sentences \
      --providers openai,gemini,deepseek --reps 3

사용 예 (폴백: 능/수 층화 각 100):
  python 30_llm_judgment.py --input data/final/zhuzi_sentences.xlsx --sheet li_sentences \
      --providers openai,gemini,deepseek --reps 3 \
      --stratify 理_cluster_samples_classified.xlsx --stratify-sheet 분류 --stratify-n 100
"""

import os
import re
import sys
import json
import time
import random
import argparse
from pathlib import Path

import pandas as pd


VALID_LABELS = {"A", "B", "C", "U", "N"}

SYSTEM_PROMPT = """당신은 朝鮮 性理學과 朱子學에 정통한 연구자다. 朱子語類(주자어류)의 한문 문장을 분석한다.
아래에 '理'(리)가 포함된 문장들이 주어진다. 각 문장에서 '理'가 어떤 성격으로 쓰였는지 하나의 라벨로 판정하라.

이 분석의 초점은 '理가 스스로 작용하는 능동적 존재로 그려지는가, 아니면 작용하지 못하는 수동적 존재로 그려지는가'이다. 라벨을 다음과 같이 엄격히 구분한다.

- A (능동): 理가 문장의 주어로서 發·生·流行·主宰·動·行 등 작용을 직접 일으키는 주체로 서술됨. 예: 理生氣, 理之行於天地, 理流行發育萬物.

- B (수동·무위): 理가 스스로 작용하지 못함이 명시적으로 진술됨. 즉 理는 無爲·無情意·無造作·無計度이며, 작용하는 것은 氣이고 理는 氣에 의존·부착(掛搭·附著)하거나 氣를 타고서만(乘氣·搭氣) 드러난다는 취지. 핵심은 '理가 직접 무엇을 하지 않는다'는 점이 문장에 드러나야 한다. 예: 理無情意無造作, 氣依傍這理行, 無是氣則理亦無掛搭處.

- C (능·수 미결정/공존): 위 A에도 B에도 분명히 속하지 않는 경우. 특히 다음을 모두 C로 분류한다. (i) 理와 氣의 선후(先後)·존재(有無)·근거 관계를 진술하되 理의 작용 여부는 언급하지 않는 문장(예: 先有是理後有是氣, 有此理便有此天地, 未有天地之先畢竟先有此理). (ii) 理가 '하나임·총칭·근본·갖추어짐' 등으로 서술되나 능동/수동이 드러나지 않는 문장(예: 太極只是一箇理, 理一分殊, 理是本). (iii) 한 문장에 능동적 측면과 수동적 측면이 함께 나타나 환원되지 않는 경우. 요컨대 '理의 능동/수동을 이 문장만으로 단정할 수 없으면' C이다.

- U (판단불가): 문장이 토막나 있거나(앞뒤가 잘림) 너무 짧아 의미 자체를 파악할 수 없는 경우.

- N (비대상): 이 문장의 '理'가 형이상학적 원리로서의 理가 아니라 어휘적·관용적 용법임 (예: 理會=이해하다, 理學, 明理, 整理, 文理, 條理, 道理의 일상적 용법 등).

판정 원칙:
- 주어진 문장 안의 근거만으로 판정한다. 외부 지식으로 보충하지 말 것.
- B는 '理가 작용하지 못한다'는 점이 실제로 드러날 때만 부여한다. 단지 理가 어떤 서술의 대상·주제가 된다는 이유로 B를 남발하지 말 것. 능동도 수동도 단정할 수 없으면 C를 택한다.
- 반드시 다섯 라벨(A/B/C/U/N) 중 하나만 선택한다.
- 각 문장마다 25자 이내의 한국어 근거를 단다. 근거에는 어떤 단어/구가 판단의 핵심이었는지 밝힌다.

출력 형식: 아래 JSON 객체만 출력한다. 마크다운 코드펜스나 부연 설명을 절대 덧붙이지 말 것.
{"items":[{"idx":0,"label":"A","basis":"..."}]}
idx는 입력의 idx와 정확히 일치시킨다."""


def build_user_prompt(chunk):
    payload = [{"idx": i, "text": s["text"]} for i, s in enumerate(chunk)]
    return "다음 문장들을 판정하라.\n" + json.dumps(payload, ensure_ascii=False)


# ----------------------------- 입력 로딩 -----------------------------

def _pick_col(cols, prefer):
    lower = {str(c).lower(): c for c in cols}
    for p in prefer:
        if p in lower:
            return lower[p]
    return None


def _read_li_frame(path, sheet, id_col, text_col):
    xls = pd.ExcelFile(path)
    if sheet and sheet in xls.sheet_names:
        df = pd.read_excel(path, sheet_name=sheet)
    elif "li_sentences" in xls.sheet_names:
        df = pd.read_excel(path, sheet_name="li_sentences")
    else:
        df = pd.read_excel(path, sheet_name=0)
        has_li = _pick_col(df.columns, ["has_li", "is_li", "li"])
        if has_li is not None:
            df = df[df[has_li].astype(str).str.lower().isin(["1", "true", "yes", "y"])]
    idc = id_col or _pick_col(df.columns, ["sentence_id", "id", "sid"])
    txc = text_col or _pick_col(df.columns, ["text", "sent_text_plain", "sent_text", "sentence", "sent"])
    if idc is None or txc is None:
        raise SystemExit(f"열을 못 찾음. 발견된 열: {list(df.columns)}\n--id-col / --text-col 로 지정하세요.")
    df = df[[idc, txc]].dropna()
    df.columns = ["sentence_id", "text"]
    df["sentence_id"] = df["sentence_id"].astype(str)
    df["text"] = df["text"].astype(str).str.strip()
    df = df[df["text"].str.len() > 0].drop_duplicates("sentence_id")
    return df


def load_sentences(args):
    df = _read_li_frame(args.input, args.sheet, args.id_col, args.text_col)

    # 너무 짧은 조각(理氣。 性理。 등) 제거 — 의미 판단 불가
    mc = getattr(args, "min_chars", 0) or 0
    if mc > 0:
        before = len(df)
        df = df[df["text"].str.len() >= mc]
        print(f"[min-chars={mc}] {before} → {len(df)} (짧은 조각 {before-len(df)}개 제외)")

    if args.stratify:
        # 능/수 분류표(사람이 라벨링한 것)에서 능동/수동 sentence_id 풀을 만들어 각 N개 추출
        sdf = pd.read_excel(args.stratify, sheet_name=args.stratify_sheet or 0, dtype=str)
        sid = _pick_col(sdf.columns, ["sentence_id", "id"])
        slab = next((c for c in sdf.columns if ("label" in str(c).lower() or "분류" in str(c) or "판단" in str(c))), None)
        if sid is None or slab is None:
            raise SystemExit(f"층화표 열 인식 실패: {list(sdf.columns)}")
        sdf = sdf[[sid, slab]].dropna()
        sdf.columns = ["sentence_id", "lab"]
        sdf["sentence_id"] = sdf["sentence_id"].astype(str)
        sdf["lab"] = sdf["lab"].astype(str).str.upper().str[:1]
        active_ids = set(sdf[sdf["lab"].isin(["A", "능", "M"])]["sentence_id"])  # M: 일부 표기 호환
        passive_ids = set(sdf[sdf["lab"].isin(["B", "P", "수"])]["sentence_id"])
        rng = random.Random(args.seed)
        a_pool = [r for r in df.to_dict("records") if r["sentence_id"] in active_ids]
        p_pool = [r for r in df.to_dict("records") if r["sentence_id"] in passive_ids]
        rng.shuffle(a_pool); rng.shuffle(p_pool)
        rows = a_pool[:args.stratify_n] + p_pool[:args.stratify_n]
        rows.sort(key=lambda r: r["sentence_id"])
        print(f"[stratify] 능동 풀 {len(a_pool)} → {min(args.stratify_n,len(a_pool))} | 수동 풀 {len(p_pool)} → {min(args.stratify_n,len(p_pool))}")
        return rows

    rows = df.to_dict("records")
    if args.sample_n:
        random.Random(args.seed).shuffle(rows)
        rows = rows[:args.sample_n]
        rows.sort(key=lambda r: r["sentence_id"])
    if args.max_n:
        rows = rows[:args.max_n]
    return rows


# ----------------------------- 파싱/검증 -----------------------------

def parse_response(raw, n):
    if raw is None:
        return None
    txt = raw.strip()
    txt = re.sub(r"^```(?:json)?\s*", "", txt)
    txt = re.sub(r"\s*```$", "", txt)
    try:
        obj = json.loads(txt)
    except Exception:
        m = re.search(r"\{.*\}", txt, re.S)
        if not m:
            return None
        try:
            obj = json.loads(m.group(0))
        except Exception:
            return None
    items = obj.get("items") if isinstance(obj, dict) else (obj if isinstance(obj, list) else None)
    if not isinstance(items, list) or len(items) != n:
        return None
    out = {}
    for it in items:
        try:
            idx = int(it["idx"])
            label = str(it["label"]).strip().upper()[:1]
            basis = str(it.get("basis", "")).strip()
        except Exception:
            return None
        if label not in VALID_LABELS or idx < 0 or idx >= n:
            return None
        out[idx] = (label, basis)
    if len(out) != n:
        return None
    return out


# ----------------------------- provider 어댑터 -----------------------------

def call_openai(model, system, user, temperature):
    from openai import OpenAI
    client = OpenAI()
    r = client.chat.completions.create(
        model=model, temperature=temperature,
        response_format={"type": "json_object"},
        messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
    )
    return r.choices[0].message.content


def call_deepseek(model, system, user, temperature):
    from openai import OpenAI
    client = OpenAI(api_key=os.environ["DEEPSEEK_API_KEY"], base_url="https://api.deepseek.com")
    r = client.chat.completions.create(
        model=model, temperature=temperature,
        response_format={"type": "json_object"},
        messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
    )
    return r.choices[0].message.content


def call_gemini(model, system, user, temperature):
    from google import genai
    from google.genai import types
    key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    client = genai.Client(api_key=key)
    r = client.models.generate_content(
        model=model, contents=user,
        config=types.GenerateContentConfig(
            system_instruction=system, temperature=temperature,
            response_mime_type="application/json",
        ),
    )
    return r.text


def call_anthropic(model, system, user, temperature):
    from anthropic import Anthropic
    client = Anthropic()
    r = client.messages.create(
        model=model, max_tokens=4096, temperature=temperature,
        system=system, messages=[{"role": "user", "content": user}],
    )
    return "".join(b.text for b in r.content if getattr(b, "type", "") == "text")


def call_openrouter(model, system, user, temperature):
    """OpenRouter: 한 키로 여러 모델 호출 (OpenAI 호환). model에는 슬러그 전체를 넘긴다.
    예: 'openai/gpt-4.1-mini', 'google/gemini-2.5-flash', 'deepseek/deepseek-chat'."""
    from openai import OpenAI
    client = OpenAI(api_key=os.environ["OPENROUTER_API_KEY"],
                    base_url="https://openrouter.ai/api/v1")
    r = client.chat.completions.create(
        model=model, temperature=temperature,
        response_format={"type": "json_object"},
        messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
        extra_headers={"HTTP-Referer": "https://github.com/choisen-hub/sadan-chiljeong-dh",
                       "X-Title": "sadan-chiljeong-dh"},
    )
    return r.choices[0].message.content


def call_mock(model, system, user, temperature):
    payload = json.loads(user.split("\n", 1)[1])
    rnd = random.Random((hash(model) ^ hash(user)) & 0xFFFFFF)
    items = []
    for p in payload:
        t = p["text"]
        if any(k in t for k in ["理會", "整理", "文理", "條理"]):
            label = "N"
        elif any(k in t for k in ["發", "生", "流行", "主宰"]):
            label = "A" if rnd.random() > 0.25 else "C"
        elif any(k in t for k in ["無", "乘", "所以"]):
            label = "B" if rnd.random() > 0.25 else "C"
        else:
            label = rnd.choice(["A", "B", "C", "U"])
        items.append({"idx": p["idx"], "label": label, "basis": "mock"})
    return json.dumps({"items": items}, ensure_ascii=False)


PROVIDERS = {
    "openai": call_openai, "deepseek": call_deepseek, "gemini": call_gemini,
    "anthropic": call_anthropic, "openrouter": call_openrouter, "mock": call_mock,
}
DEFAULT_MODELS = {
    "openai": "gpt-4.1-mini", "deepseek": "deepseek-chat", "gemini": "gemini-2.5-flash",
    "anthropic": "claude-sonnet-4-5", "mock": "mock-1",
}

# OpenRouter 한 키로 3모델 비교 시, 별칭 → 슬러그 매핑 (필요시 슬러그만 바꾸면 됨)
OPENROUTER_SLUGS = {
    "openai": "openai/gpt-4.1-mini",
    "gemini": "google/gemini-2.5-flash",
    "deepseek": "deepseek/deepseek-chat",
}


def call_with_retry(fn, model, system, user, temperature, n, retries):
    for attempt in range(retries):
        try:
            raw = fn(model, system, user, temperature)
            parsed = parse_response(raw, n)
            if parsed is not None:
                return parsed
        except Exception as e:
            sys.stderr.write(f"  [retry {attempt+1}/{retries}] {type(e).__name__}: {str(e)[:160]}\n")
        time.sleep(min(2 ** attempt, 20))
    return None


# ----------------------------- 체크포인트 -----------------------------

def load_done(jsonl_path):
    """rep까지 포함한 키 (sentence_id, rep) 기준으로 완료 여부 추적."""
    done = {}
    if jsonl_path.exists():
        for line in jsonl_path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                try:
                    rec = json.loads(line)
                    done[(rec["sentence_id"], rec.get("rep", 1))] = rec
                except Exception:
                    pass
    return done


def append_jsonl(jsonl_path, recs):
    with jsonl_path.open("a", encoding="utf-8") as f:
        for r in recs:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


# ----------------------------- 실행 -----------------------------

def run_provider(name, model, sentences, outdir, chunk_size, temperature, retries, reps, adapter=None):
    fn = PROVIDERS[adapter or name]
    jsonl_path = outdir / f"labels_{name}.jsonl"
    done = load_done(jsonl_path)

    tasks = [(s, rep) for rep in range(1, reps + 1) for s in sentences
             if (s["sentence_id"], rep) not in done]
    print(f"[{name}/{model}] 문장 {len(sentences)} × reps {reps} = {len(sentences)*reps} | 완료 {len(done)} | 남음 {len(tasks)}")

    processed = 0
    for i in range(0, len(tasks), chunk_size):
        batch = tasks[i:i + chunk_size]
        chunk = [t[0] for t in batch]
        reps_of = [t[1] for t in batch]
        system, user = SYSTEM_PROMPT, build_user_prompt(chunk)
        parsed = call_with_retry(fn, model, system, user, temperature, len(chunk), retries)

        recs = []
        if parsed is None:
            for j, s in enumerate(chunk):
                one = call_with_retry(fn, model, SYSTEM_PROMPT, build_user_prompt([s]), temperature, 1, retries)
                lb, bs = ("ERR", "") if one is None else one[0]
                recs.append({"sentence_id": s["sentence_id"], "rep": reps_of[j],
                             "model": model, "label": lb, "basis": bs})
        else:
            for j, s in enumerate(chunk):
                lb, bs = parsed[j]
                recs.append({"sentence_id": s["sentence_id"], "rep": reps_of[j],
                             "model": model, "label": lb, "basis": bs})
        append_jsonl(jsonl_path, recs)
        processed += len(batch)
        if processed % 200 < chunk_size or i + chunk_size >= len(tasks):
            print(f"  ...{processed}/{len(tasks)}")
    return jsonl_path


def _majority(labels):
    from collections import Counter
    c = Counter(labels)
    top = max(c.values())
    cands = sorted([k for k, v in c.items() if v == top])
    return cands[0], top, len(labels)


def merge_outputs(outdir, provider_names, sentences, reps):
    base = pd.DataFrame(sentences)
    # 31번 분석기가 'text' 컬럼을 기대하므로, 텍스트 컬럼명을 표준화한 보조 컬럼 추가
    if "text" not in base.columns:
        for cand in ["text_punctuated", "text_plain", "sent_text_plain", "sentence"]:
            if cand in base.columns:
                base["text"] = base[cand]
                break
    selfcon_rows = []
    for name in provider_names:
        recs = list(load_done(outdir / f"labels_{name}.jsonl").values())
        if not recs:
            continue
        d = pd.DataFrame(recs)
        # reps 다수결로 모델별 단일 라벨 집계 + 자기일치도 + 대표 basis
        agg = {}
        for sid, g in d.groupby("sentence_id"):
            labs = list(g["label"])
            maj, topn, total = _majority(labs)
            # 다수결 라벨과 일치하는 행들 중 첫 basis를 대표로
            match = g[g["label"] == maj]
            basis = ""
            if "basis" in g.columns and len(match):
                bvals = [b for b in match["basis"].tolist() if isinstance(b, str) and b.strip()]
                basis = bvals[0] if bvals else ""
            agg[sid] = (maj, topn / total if total else float("nan"), basis)
            selfcon_rows.append({"model": name, "sentence_id": sid,
                                 "label": maj, "self_consistency": topn / total if total else float("nan")})
        ad = pd.DataFrame([{"sentence_id": k, f"label_{name}": v[0],
                            f"selfcon_{name}": round(v[1], 3), f"basis_{name}": v[2]}
                           for k, v in agg.items()])
        base = base.merge(ad, on="sentence_id", how="left")

    out_csv = outdir / "merged_labels.csv"
    base.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"\n병합 저장: {out_csv}  ({len(base)} 행)")

    for name in provider_names:
        col = f"label_{name}"
        if col in base.columns:
            vc = base[col].value_counts(dropna=False).to_dict()
            sc = base[f"selfcon_{name}"].astype(float).mean() if f"selfcon_{name}" in base.columns else float("nan")
            print(f"  {name:9s} 분포: {vc} | 평균 자기일치도(reps={reps}): {sc:.3f}")

    if selfcon_rows:
        pd.DataFrame(selfcon_rows).to_csv(outdir / "self_consistency_long.csv", index=False, encoding="utf-8-sig")
    return out_csv


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--sheet", default=None)
    ap.add_argument("--id-col", default=None)
    ap.add_argument("--text-col", default=None)
    ap.add_argument("--providers", default="gemini")
    ap.add_argument("--openrouter", action="store_true",
                    help="OpenRouter 한 키로 --providers의 별칭들을 모두 호출 (OPENROUTER_API_KEY 사용)")
    ap.add_argument("--openai-model", default=DEFAULT_MODELS["openai"])
    ap.add_argument("--deepseek-model", default=DEFAULT_MODELS["deepseek"])
    ap.add_argument("--gemini-model", default=DEFAULT_MODELS["gemini"])
    ap.add_argument("--anthropic-model", default=DEFAULT_MODELS["anthropic"])
    ap.add_argument("--outdir", default="data/llm_judgment")
    ap.add_argument("--chunk-size", type=int, default=10)
    ap.add_argument("--temperature", type=float, default=0.0)
    ap.add_argument("--reps", type=int, default=1, help="같은 문장 반복 횟수(자기일치도). 기본 1")
    ap.add_argument("--sample-n", type=int, default=None)
    ap.add_argument("--max-n", type=int, default=None)
    ap.add_argument("--min-chars", type=int, default=0,
                    help="이 글자수 미만 문장 제외 (예: 6). 깨진 짧은 조각 거르기. 기본 0(끄기)")
    ap.add_argument("--stratify", default=None, help="능/수 분류표 xlsx 경로 (폴백 층화추출)")
    ap.add_argument("--stratify-sheet", default=None)
    ap.add_argument("--stratify-n", type=int, default=100, help="능/수 각 N개")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--retries", type=int, default=4)
    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    sentences = load_sentences(args)
    print(f"분석 문장 수: {len(sentences)} | reps: {args.reps}")

    provider_names = [p.strip() for p in args.providers.split(",") if p.strip()]
    model_for = {"openai": args.openai_model, "deepseek": args.deepseek_model,
                 "gemini": args.gemini_model, "anthropic": args.anthropic_model, "mock": "mock-1"}

    if args.openrouter:
        # 별칭(openai/gemini/deepseek)을 OpenRouter 슬러그로 호출. 라벨/파일명은 별칭 유지.
        for name in provider_names:
            if name not in OPENROUTER_SLUGS:
                raise SystemExit(f"OpenRouter 모드에서 알 수 없는 별칭: {name} (가능: {list(OPENROUTER_SLUGS)})")
            slug = OPENROUTER_SLUGS[name]
            print(f"[openrouter] {name} → {slug}")
            run_provider(name, slug, sentences, outdir,
                         args.chunk_size, args.temperature, args.retries, args.reps,
                         adapter="openrouter")
    else:
        for name in provider_names:
            if name not in PROVIDERS:
                raise SystemExit(f"알 수 없는 provider: {name}")
            run_provider(name, model_for[name], sentences, outdir,
                         args.chunk_size, args.temperature, args.retries, args.reps)

    merge_outputs(outdir, provider_names, sentences, args.reps)


if __name__ == "__main__":
    main()
