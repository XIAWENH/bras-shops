"""
Brás 店铺调研流水线:Instagram 抓帖 → AI 识别「服装 × 布料」→ 统计每种布料/服装有多少家店在做。

用法(在本文件夹里):
    python bras_pipeline.py profile               # 先抓所有店主页(简介/链接/最近帖),约 $0.7
    python bras_pipeline.py locate                # AI 判断每家店在哪 → shops_local.csv(可手工改 regiao)
    python bras_pipeline.py all --days 7          # 抓取 + 识别 + 统计(默认 Brás + 圣保罗市)
    python bras_pipeline.py all --regiao all      # 不按地区过滤
    python bras_pipeline.py scrape --days 7       # 只抓取(会顺手把首图存成缩略图,IG 图片链接几天就过期)
    python bras_pipeline.py tag                   # 只识别还没识别过的帖子
    python bras_pipeline.py summarize             # 只出统计表
    python bras_pipeline.py scrape --days 7 --limit 5   # 先拿前 5 个店试跑

数据都在 ./data(或 --data-dir 指定的目录,比如 NAS):
    raw/posts_*.jsonl   Apify 原始结果(什么字段都在,以后想补分析不用重抓)
    posts.csv           帖子表(一帖一行,带店铺名/点赞/文案/缩略图路径)
    tags.jsonl          AI 识别结果(一帖一行,可断点续跑)
    thumbs/             帖子首图缩略图(给以后的 Dashboard 用)
    bras_resumo_YYYYMMDD.xlsx / summary/*.csv   统计表

店铺名单在 shops.csv:tipo = loja(服装店)/ tecido(布料商)/ marca / midia ...,ativo=0 的不抓。
统计默认只算 tipo=loja 的店。
"""

from __future__ import annotations

import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Literal

import pandas as pd
import requests
from dotenv import load_dotenv
from PIL import Image
from pydantic import BaseModel, Field

HERE = Path(__file__).resolve().parent
load_dotenv(HERE / ".env")

ACTOR = "apify/instagram-scraper"
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# ---------------------------------------------------------------------------
# 词表:AI 只能从这里面选,保证统计口径统一。改词表 = 改这两个列表 + 对应的中文名。
# ---------------------------------------------------------------------------
GARMENTS = {
    "vestido": "连衣裙", "saia": "半身裙", "blusa": "女式上衣", "camisa": "衬衫",
    "cropped_top": "短上衣/Top", "regata": "背心", "body": "连体衣Body",
    "conjunto": "套装", "calca": "长裤", "short": "短裤", "bermuda": "五分裤",
    "macacao": "长连体裤", "macaquinho": "短连体裤", "kimono": "和服外搭",
    "jaqueta": "夹克", "blazer": "西装外套", "colete": "马甲",
    "casaco_cardigan": "外套/开衫", "saida_praia": "沙滩罩衫", "pijama": "睡衣",
    "moda_praia": "泳装", "tecido_rolo": "面料(布卷本身)", "outro": "其他",
}
FABRICS = {
    "viscose": "人棉 Viscose", "viscolinho": "Viscolinho 人棉麻感", "viscosarja": "Viscosarja 人棉斜纹",
    "linho": "麻 Linho", "sensorial": "Sensorial(含 crepe sensorial/marrant)",
    "crepe": "Crepe 绉布", "malha": "针织 Malha", "canelado_ribana": "罗纹 Canelado",
    "trico": "毛织 Tricô", "moletom": "卫衣布 Moletom", "jeans": "牛仔 Jeans",
    "sarja": "斜纹 Sarja", "tricoline": "府绸 Tricoline", "algodao": "棉 Algodão",
    "cetim": "缎 Cetim", "chiffon_musseline": "雪纺 Chiffon/Musseline", "tule": "网纱 Tule",
    "renda": "蕾丝 Renda", "laise": "绣花洞洞 Laise", "airflow": "Airflow",
    "duna": "Duna", "alfaiataria": "西装料 Alfaiataria", "suplex": "Suplex",
    "couro_sintetico": "人造革", "poliester": "涤纶 Poliéster", "outro": "其他",
    "nao_informado": "未说明",
}

# 只有这些布料允许 AI 看图判断(肉眼不会认错);其余必须文案写明,否则算「未说明」。
# 试跑时同一款素色裤子,看图猜出了 viscose/duna/alfaiataria/linho 四种,所以不能放开。
VISUAL_FABRICS = {"jeans", "renda", "tule", "trico", "laise", "couro_sintetico", "moletom", "canelado_ribana"}

PROMPT = f"""Você analisa posts de Instagram de lojas de atacado de roupas do Brás (São Paulo).
Para o post abaixo (legenda + imagens), identifique as peças de roupa VENDIDAS e o tecido de cada peça.

Regras:
- is_product = false se o post não vende roupa (meme, aviso, evento, foto pessoal, vaga de emprego...).
- Uma linha em items para cada tipo de peça. Conjunto (peças vendidas juntas) = garment "conjunto".
- Se o post vende o tecido em si (rolos/metro), use garment "tecido_rolo".
- fabric: use o tecido ESCRITO na legenda, hashtags ou como texto dentro das imagens (fabric_source="caption").
  NÃO adivinhe tecido pela foto, exceto quando é inconfundível visualmente: {", ".join(sorted(VISUAL_FABRICS))}
  (fabric_source="image"). Caso contrário: fabric="nao_informado", fabric_source="none".
- Mapeamentos: "visco sarja", "sarja viscose", "viscosarja" = viscosarja; "visco linho", "viscolinho sarjado" = viscolinho;
  "sensorial", "crepe sensorial", "sensorial marrant" = sensorial; "moletinho" = moletom; "lese" = laise;
  "crepe de seda", "crepe seda" = crepe. Post com vários tecidos ("viscose + alfaiataria") = um fabric por item/shop_fabrics.
- Se o tecido está escrito mas não existe na lista, use "outro" (NUNCA troque por um tecido parecido) e copie em fabric_raw.
- Post que só cita tecido de passagem (dica de look, nome de lugar, comida) não conta.
- fabric_raw: copie o trecho exato da legenda que cita o tecido (ou "" se não houver).
- shop_fabrics: tecidos que a loja diz trabalhar EM GERAL, mesmo em post institucional
  (ex.: "todas as nossas peças são em viscolinho"). Vazio se não disser.
- garment válidos: {", ".join(GARMENTS)}
- fabric válidos: {", ".join(FABRICS)}
"""

FabricT = Literal[tuple(FABRICS)]  # type: ignore[valid-type]


class Item(BaseModel):
    garment: Literal[tuple(GARMENTS)]  # type: ignore[valid-type]
    fabric: FabricT
    fabric_source: Literal["caption", "image", "none"]


class PostTags(BaseModel):
    is_product: bool
    items: list[Item] = Field(default_factory=list)
    fabric_raw: str = ""
    shop_fabrics: list[FabricT] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# 路径 & 工具
# ---------------------------------------------------------------------------
class Paths:
    def __init__(self, data_dir: Path):
        self.root = data_dir
        self.raw = data_dir / "raw"
        self.thumbs = data_dir / "thumbs"
        self.summary = data_dir / "summary"
        self.posts = data_dir / "posts.csv"
        self.tags = data_dir / "tags.jsonl"
        for p in (self.root, self.raw, self.thumbs, self.summary):
            p.mkdir(parents=True, exist_ok=True)


SHOPS_LOCAL = HERE / "shops_local.csv"  # locate 步骤的结果:每家店在哪


def load_shops(only_active=True, regioes: list[str] | None = None) -> pd.DataFrame:
    df = pd.read_csv(HERE / "shops.csv", encoding="utf-8-sig", dtype={"username": str})
    df["username"] = df["username"].str.strip().str.lower()
    df = df.drop_duplicates("username")
    if SHOPS_LOCAL.exists():
        loc = pd.read_csv(SHOPS_LOCAL, encoding="utf-8-sig", dtype=str)
        df = df.merge(loc[["username", "regiao", "local", "tipo_negocio"]], on="username", how="left")
        if regioes:
            new = df["regiao"].isna().sum()
            if new:
                print(f"ℹ️ shops.csv 里有 {new} 个店还没定位,先跑 profile + locate 才会算进来")
            df = df[df["regiao"].isin(regioes)]
    return df[df["ativo"] == 1] if only_active else df


def apify_token() -> str:
    tok = os.getenv("APIFY_TOKEN")
    if not tok:
        auth = Path.home() / ".apify" / "auth.json"
        if auth.exists():
            tok = json.loads(auth.read_text(encoding="utf-8")).get("token")
    if not tok:
        raise SystemExit("找不到 APIFY_TOKEN:写到 .env 或 ~/.apify/auth.json")
    return tok


def fetch_image(url: str, max_side=768) -> Image.Image | None:
    try:
        r = requests.get(url, headers=HTTP_HEADERS, timeout=20)
        r.raise_for_status()
        img = Image.open(BytesIO(r.content)).convert("RGB")
        img.thumbnail((max_side, max_side))
        return img
    except Exception:
        return None


# ---------------------------------------------------------------------------
# 1. 抓取
# ---------------------------------------------------------------------------
def post_row(item: dict) -> dict:
    images = item.get("images") or []
    children = item.get("childPosts") or []
    image_urls = [u for u in images if u] or [c.get("displayUrl") for c in children if c.get("displayUrl")]
    if not image_urls and item.get("displayUrl"):
        image_urls = [item["displayUrl"]]
    return {
        "shortcode": item.get("shortCode"),
        "shop": (item.get("ownerUsername") or "").lower(),
        "shop_name": item.get("ownerFullName"),
        "timestamp": item.get("timestamp"),
        "type": item.get("type"),
        "url": item.get("url"),
        "likes": item.get("likesCount"),
        "comments": item.get("commentsCount"),
        "video_views": item.get("videoViewCount"),
        "caption": item.get("caption") or "",
        "hashtags": " ".join(item.get("hashtags") or []),
        "image_urls": json.dumps(image_urls[:3]),
    }


def scrape(paths: Paths, days: int, results_limit: int, max_usd: float, limit: int | None, regioes: list[str] | None):
    from apify_client import ApifyClient

    shops = load_shops(regioes=regioes)
    usernames = shops["username"].tolist()[:limit] if limit else shops["username"].tolist()
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    run_input = {
        "directUrls": [f"https://www.instagram.com/{u}/" for u in usernames],
        "resultsType": "posts",
        "onlyPostsNewerThan": since,
        "resultsLimit": results_limit,  # 每个店最多几条
        "addParentData": False,
    }
    print(f"🚀 抓取 {len(usernames)} 个店 {since} 之后的帖子(每店最多 {results_limit} 条,花费上限 ${max_usd})...")
    client = ApifyClient(apify_token())
    run = client.actor(ACTOR).call(run_input=run_input, max_total_charge_usd=max_usd, logger=None)
    if run is None:
        raise SystemExit("❌ Apify 运行没返回结果")
    print(f"📦 run {run.id} 状态 {run.status},花费 ${run.usage_total_usd or 0:.2f}")

    items = [it for it in client.dataset(run.default_dataset_id).iterate_items() if it.get("shortCode")]
    stamp = datetime.now().strftime("%Y%m%d_%H%M")
    with open(paths.raw / f"posts_{stamp}.jsonl", "w", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")

    new = pd.DataFrame([post_row(it) for it in items])
    if new.empty:
        print("⚠️ 没抓到帖子")
        return
    new["scraped_at"] = datetime.now().isoformat(timespec="seconds")
    if paths.posts.exists():
        old = pd.read_csv(paths.posts, encoding="utf-8-sig", dtype=str)
        new = pd.concat([old, new.astype(str)]).drop_duplicates("shortcode", keep="last")
    new.to_csv(paths.posts, index=False, encoding="utf-8-sig")
    print(f"💾 本次 {len(items)} 帖,累计 {len(new)} 帖 → {paths.posts}")
    save_thumbs(paths, new)


def save_thumbs(paths: Paths, posts: pd.DataFrame):
    todo = [r for r in posts.itertuples() if not (paths.thumbs / f"{r.shortcode}.jpg").exists()]

    def one(r):
        urls = json.loads(r.image_urls) if isinstance(r.image_urls, str) and r.image_urls else []
        img = fetch_image(urls[0], 512) if urls else None
        if img:
            img.save(paths.thumbs / f"{r.shortcode}.jpg", quality=85)
        return bool(img)

    with ThreadPoolExecutor(max_workers=12) as ex:
        ok = sum(ex.map(one, todo))
    print(f"🖼️ 缩略图 新存 {ok}/{len(todo)} 张 → {paths.thumbs}")


# ---------------------------------------------------------------------------
# 0. 店铺定位:抓主页(简介/链接/最近帖子)→ AI 判断店在哪
# ---------------------------------------------------------------------------
REGIOES = {
    "bras": "Brás(含 Pari、Feira da Madrugada、Brás 各 shopping)",
    "bom_retiro": "Bom Retiro(José Paulino 一带)",
    "sp_capital": "圣保罗市其他区(25 de Março、Mooca、Tatuapé…)",
    "sp_ddd11": "只知道是 11 区号(大圣保罗),没写具体地址",
    "sp_grande": "大圣保罗其他城市(Guarulhos、Osasco、ABC…)",
    "sp_interior": "圣保罗州内陆",
    "outro_estado": "其他州",
    "exterior": "国外",
    "desconhecido": "看不出来",
}
TIPOS_NEGOCIO = ["confeccao_atacado", "loja_varejo", "tecidos", "assessoria_compras", "marca", "midia_outro"]
DEFAULT_REGIOES = ["bras", "bom_retiro", "sp_capital", "sp_ddd11"]  # Brás + 圣保罗市;只看 Brás 用 --regiao bras

LOCATE_PROMPT = f"""Você identifica ONDE fica uma loja de moda a partir do perfil de Instagram.
Pistas: endereço do perfil comercial, bio, nome, links (wa.me/55DDxxxx → DDD), legendas recentes.

Referências do Brás/Pari (São Paulo capital): Feira da Madrugada, Pátio do Pari, Shopping Vautier, Shopping Total Brás,
Mega Polo Moda, Shopping Ponto Brás, Porto Brás, Brás Fashion, Galeria/Shopping Oriente, Shopping 205, Rua Oriente,
Rua Maria Marcolina, Rua Silva Teles, Rua Barão de Ladário, Rua Tiers, Rua Xavantes, Rua Miller, Rua Rodrigues dos Santos,
Rua Monsenhor Andrade, Rua Coimbra, Rua Bresser, Rua do Hipódromo, Largo da Concórdia, Rua Mendes Caldeira, Av. Vautier.
Bom Retiro: Rua José Paulino, Rua Aimorés, Rua Prof. Cesare Lombroso, Rua Ribeiro de Lima.
Cuidado com polos de OUTROS estados: José Avelino / Monsenhor Tabosa (Fortaleza-CE), Feira de Caruaru / Toritama /
Santa Cruz do Capibaribe (PE), Polo de Moda 44 / Rua 44 (Goiânia-GO), Jardim Catarina / Feirão de Malhas (São Gonçalo-RJ),
Cianorte / Maringá (PR), Brusque (SC).

DDD 11 = Grande São Paulo. Se a única pista for DDD 11, use regiao "sp_ddd11".
Se a loja tem unidade no Brás E em outro lugar, use "bras".
regiao válidas: {", ".join(REGIOES)}
tipo_negocio: confeccao_atacado (fabrica/vende roupa no atacado), loja_varejo, tecidos (vende tecido),
assessoria_compras (guia/assessoria, "não somos loja"), marca (grife/marca grande), midia_outro.
local: shopping/galeria/rua + box/loja exatamente como escrito (ou ""). evidencia: o trecho que prova a localização.
"""


class ShopLocation(BaseModel):
    regiao: Literal[tuple(REGIOES)]  # type: ignore[valid-type]
    cidade: str = ""
    uf: str = ""
    local: str = ""
    tipo_negocio: Literal[tuple(TIPOS_NEGOCIO)]  # type: ignore[valid-type]
    evidencia: str = ""
    confianca: Literal["alta", "media", "baixa"]


def profile_row(it: dict) -> dict:
    links = [l.get("url", "") for l in (it.get("externalUrls") or [])] + [it.get("externalUrl") or ""]
    addr = {k: v for k, v in it.items() if k.lower().startswith(("business", "address", "city", "zip")) and v}
    caps = [(p.get("caption") or "")[:300] for p in (it.get("latestPosts") or [])[:10]]
    return {
        "username": (it.get("username") or "").lower(),
        "full_name": it.get("fullName"),
        "biography": it.get("biography") or "",
        "links": " ".join(sorted({l for l in links if l})),
        "address": json.dumps(addr, ensure_ascii=False) if addr else "",
        "followers": it.get("followersCount"),
        "posts_count": it.get("postsCount"),
        "is_business": it.get("isBusinessAccount"),
        "category": it.get("businessCategoryName"),
        "recent_captions": json.dumps(caps, ensure_ascii=False),
    }


def profile(paths: Paths, max_usd: float, limit: int | None):
    """抓所有店(含 ativo=0)的主页资料 → data/profiles.csv"""
    from apify_client import ApifyClient

    usernames = load_shops(only_active=False)["username"].tolist()
    usernames = usernames[:limit] if limit else usernames
    print(f"👤 抓 {len(usernames)} 个店的主页资料(花费上限 ${max_usd})...")
    client = ApifyClient(apify_token())
    run = client.actor(ACTOR).call(
        run_input={"directUrls": [f"https://www.instagram.com/{u}/" for u in usernames],
                   "resultsType": "details", "resultsLimit": 1},
        max_total_charge_usd=max_usd, logger=None)
    items = list(client.dataset(run.default_dataset_id).iterate_items())
    stamp = datetime.now().strftime("%Y%m%d_%H%M")
    with open(paths.raw / f"profiles_{stamp}.jsonl", "w", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")
    new = pd.DataFrame([profile_row(it) for it in items if it.get("username")])
    out = paths.root / "profiles.csv"
    if out.exists():
        new = pd.concat([pd.read_csv(out, encoding="utf-8-sig", dtype=str), new.astype(str)]) \
            .drop_duplicates("username", keep="last")
    new.to_csv(out, index=False, encoding="utf-8-sig")
    missing = set(usernames) - set(new["username"])
    print(f"💾 {len(items)} 个主页 → {out}" + (f";没抓到 {len(missing)} 个(改名/注销/私密?)" if missing else ""))


def locate(paths: Paths, workers: int, redo: bool):
    """AI 判断每家店在哪 → shops_local.csv(和 shops.csv 放一起,可手工改)"""
    from google import genai
    from google.genai import types

    client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
    prof = pd.read_csv(paths.root / "profiles.csv", encoding="utf-8-sig", dtype=str).fillna("")
    old = pd.read_csv(SHOPS_LOCAL, encoding="utf-8-sig", dtype=str) if SHOPS_LOCAL.exists() else pd.DataFrame()
    if not redo and not old.empty:
        prof = prof[~prof["username"].isin(old["username"])]
    print(f"📍 定位 {len(prof)} 家店...")
    cfg = types.GenerateContentConfig(response_mime_type="application/json", response_schema=ShopLocation, temperature=0)

    def one(r):
        text = (f"@{r['username']} | Nome: {r['full_name']}\nBio: {r['biography']}\nLinks: {r['links']}\n"
                f"Endereço comercial: {r['address'] or '-'}\nCategoria: {r['category']}\n"
                f"Legendas recentes: {r['recent_captions'][:3000]}")
        for attempt in range(4):
            try:
                resp = client.models.generate_content(model=GEMINI_MODEL, contents=[LOCATE_PROMPT, text], config=cfg)
                return {"username": r["username"], **ShopLocation.model_validate_json(resp.text).model_dump(),
                        "followers": r["followers"], "bio": r["biography"].replace("\n", " | ")}
            except Exception as e:
                if attempt == 3:
                    print(f"  ❌ @{r['username']}: {str(e)[:100]}")
                    return None
                time.sleep(10 * (attempt + 1))

    with ThreadPoolExecutor(max_workers=workers) as ex:
        res = [x for x in ex.map(one, [r for _, r in prof.iterrows()]) if x]
    new = pd.DataFrame(res)
    if not old.empty:
        new = pd.concat([old, new.astype(str)]).drop_duplicates("username", keep="last")
    new = new.sort_values(["regiao", "username"])
    new.to_csv(SHOPS_LOCAL, index=False, encoding="utf-8-sig")
    print(f"✅ → {SHOPS_LOCAL}")
    print(new["regiao"].map(REGIOES).value_counts().to_string())


# ---------------------------------------------------------------------------
# 2. AI 识别
# ---------------------------------------------------------------------------
def tag(paths: Paths, workers: int, limit: int | None):
    from google import genai
    from google.genai import types

    key = os.getenv("GEMINI_API_KEY")
    if not key:
        raise SystemExit("找不到 GEMINI_API_KEY:写到 .env")
    client = genai.Client(api_key=key)

    posts = pd.read_csv(paths.posts, encoding="utf-8-sig", dtype=str).fillna("")
    done = set()
    if paths.tags.exists():
        done = {json.loads(l)["shortcode"] for l in paths.tags.read_text(encoding="utf-8").splitlines() if l.strip()}
    todo = posts[~posts["shortcode"].isin(done)]
    if limit:
        todo = todo.head(limit)
    print(f"🤖 待识别 {len(todo)} 帖(已识别 {len(done)},模型 {GEMINI_MODEL},并发 {workers})")

    def images_for(r) -> list[Image.Image]:
        imgs = [fetch_image(u) for u in json.loads(r["image_urls"] or "[]")]
        imgs = [i for i in imgs if i]
        thumb = paths.thumbs / f"{r['shortcode']}.jpg"
        if not imgs and thumb.exists():  # 原链接过期就用存下来的缩略图
            imgs = [Image.open(thumb).convert("RGB")]
        return imgs

    def one(r) -> dict:
        text = f"Loja: @{r['shop']}\nLegenda:\n{r['caption'][:2500]}\nHashtags: {r['hashtags']}"
        contents = [PROMPT, text, *images_for(r)]
        cfg = types.GenerateContentConfig(
            response_mime_type="application/json", response_schema=PostTags, temperature=0
        )
        for attempt in range(4):
            try:
                resp = client.models.generate_content(model=GEMINI_MODEL, contents=contents, config=cfg)
                parsed = resp.parsed if isinstance(resp.parsed, PostTags) else PostTags.model_validate_json(resp.text)
                return {"shortcode": r["shortcode"], "ok": True, **parsed.model_dump()}
            except Exception as e:  # 429/503/网络抖动:退避重试
                if attempt < 3:
                    time.sleep(10 * (attempt + 1))
                    continue
                return {"shortcode": r["shortcode"], "ok": False, "error": str(e)[:200]}

    n_ok = n_err = 0
    with ThreadPoolExecutor(max_workers=workers) as ex, open(paths.tags, "a", encoding="utf-8") as f:
        futs = [ex.submit(one, r) for _, r in todo.iterrows()]
        for i, fut in enumerate(as_completed(futs), 1):
            res = fut.result()
            if res["ok"]:
                n_ok += 1
                f.write(json.dumps(res, ensure_ascii=False) + "\n")
                f.flush()
            else:
                n_err += 1  # 失败的不写,下次 tag 会重试
            if i % 50 == 0 or i == len(futs):
                print(f"  进度 {i}/{len(futs)}  成功 {n_ok}  失败 {n_err}")
    if n_err:
        print(f"⚠️ {n_err} 帖失败,再跑一次 `python bras_pipeline.py tag` 会只重试这些")


# ---------------------------------------------------------------------------
# 3. 统计
# ---------------------------------------------------------------------------
def summarize(paths: Paths, since: str | None, all_types: bool, regioes: list[str] | None):
    posts = pd.read_csv(paths.posts, encoding="utf-8-sig", dtype=str).fillna("")
    tags = pd.read_json(paths.tags, lines=True, dtype={"shortcode": str})
    tags = tags.drop_duplicates("shortcode", keep="last")

    shops = load_shops(only_active=False, regioes=regioes).rename(columns={"username": "shop"})
    keep = ["shop", "tipo"] + [c for c in ("regiao", "local", "tipo_negocio") if c in shops]
    posts = posts.merge(shops[keep], on="shop", how="inner")  # 不在名单/不在所选地区的店不算
    if not all_types:
        posts = posts[posts["tipo"] == "loja"]
        if "tipo_negocio" in posts:  # 定位时识别出的布料商/采购助理/大品牌也不算
            posts = posts[~posts["tipo_negocio"].isin(["tecidos", "assessoria_compras", "marca", "midia_outro"])]
    if since:
        posts = posts[posts["timestamp"] >= since]

    tagged = posts.merge(tags, on="shortcode", how="inner")
    df = tagged[tagged["is_product"] == True]  # noqa: E712
    long = df.explode("items").dropna(subset=["items"])
    long = pd.concat([long.drop(columns=["items"]).reset_index(drop=True),
                      pd.json_normalize(long["items"].tolist())], axis=1)
    long = long[long["garment"] != "tecido_rolo"]
    long["服装"] = long["garment"].map(GARMENTS)
    long["布料"] = long["fabric"].map(FABRICS)
    known = long[long["fabric"] != "nao_informado"]

    # 店铺自己声明「我们都用 xx 布」(可能出现在非商品帖里)
    declared = tagged[["shop", "shop_fabrics"]].explode("shop_fabrics").dropna()
    declared = declared[declared["shop_fabrics"] != "nao_informado"]
    declared["布料"] = declared["shop_fabrics"].map(FABRICS)

    n_shops = len(set(long["shop"]) | set(declared["shop"]))
    print(f"📊 {n_shops} 家店 / {df['shortcode'].nunique()} 个商品帖 / {len(long)} 个服装×布料条目")

    # 每种布料多少家店做 = 商品帖里出现过 ∪ 店铺声明
    fab_shop = pd.concat([known[["shop", "布料", "fabric_source"]],
                          declared[["shop", "布料"]].assign(fabric_source="caption")])
    fab = (fab_shop.groupby("布料")["shop"].nunique().rename("店铺数").to_frame()
           .join(fab_shop[fab_shop["fabric_source"] == "caption"].groupby("布料")["shop"].nunique().rename("店铺数_文案写明"))
           .join(known.groupby("布料")["shortcode"].nunique().rename("帖子数"))
           .fillna(0).astype(int).sort_values("店铺数", ascending=False))
    fab["店铺占比"] = (fab["店铺数"] / n_shops).round(3)

    # 每种服装多少家店做
    gar = (long.groupby("服装").agg(店铺数=("shop", "nunique"), 帖子数=("shortcode", "nunique"))
           .sort_values("店铺数", ascending=False))
    gar["店铺占比"] = (gar["店铺数"] / n_shops).round(3)

    # 服装 × 布料
    mx_shops = known.pivot_table(index="服装", columns="布料", values="shop", aggfunc="nunique", fill_value=0)
    mx_posts = known.pivot_table(index="服装", columns="布料", values="shortcode", aggfunc="nunique", fill_value=0)
    order_r = [g for g in gar.index if g in mx_shops.index]
    order_c = [c for c in fab.index if c in mx_shops.columns]
    mx_shops, mx_posts = mx_shops.loc[order_r, order_c], mx_posts.loc[order_r, order_c]

    # 每家店做什么
    shop_tbl = (long.groupby("shop").agg(
        商品帖=("shortcode", "nunique"),
        服装=("服装", lambda s: "、".join(s.value_counts().index[:5])),
        平均点赞=("likes", lambda s: round(pd.to_numeric(s, errors="coerce").mean(), 1)),
    ).join(fab_shop.groupby("shop")["布料"].agg(lambda s: "、".join(s.value_counts().index[:5])), how="outer")
     .fillna({"商品帖": 0, "服装": "", "布料": ""}).sort_values("商品帖", ascending=False))
    if "regiao" in posts:
        shop_tbl = posts.drop_duplicates("shop").set_index("shop")[["regiao", "local"]]             .assign(regiao=lambda x: x["regiao"].map(REGIOES)).rename(columns={"regiao": "地区", "local": "地址"})             .join(shop_tbl, how="right")

    # 布料未说明的比例,判断数据可信度
    cover = pd.DataFrame({
        "指标": ["统计地区", "店铺数", "商品帖", "服装×布料条目", "布料文案写明", "布料靠图片推断", "布料未说明"],
        "数值": [", ".join(regioes) if regioes else "全部", n_shops, df["shortcode"].nunique(), len(long),
                 (long["fabric_source"] == "caption").sum(), (long["fabric_source"] == "image").sum(),
                 (long["fabric"] == "nao_informado").sum()],
    })

    # 词表里没有的布料:看原文,常出现的就加进 FABRICS
    other = (long[long["fabric"] == "outro"].assign(原文=lambda x: x["fabric_raw"].str.lower().str.strip())
             .groupby("原文").agg(店铺数=("shop", "nunique"), 帖子数=("shortcode", "nunique"))
             .sort_values("店铺数", ascending=False))

    stamp = datetime.now().strftime("%Y%m%d")
    xlsx = paths.root / f"bras_resumo_{stamp}.xlsx"
    with pd.ExcelWriter(xlsx) as w:
        cover.to_excel(w, sheet_name="概况", index=False)
        fab.to_excel(w, sheet_name="布料-店铺数")
        gar.to_excel(w, sheet_name="服装-店铺数")
        mx_shops.to_excel(w, sheet_name="服装x布料-店铺数")
        mx_posts.to_excel(w, sheet_name="服装x布料-帖子数")
        shop_tbl.to_excel(w, sheet_name="店铺明细")
        other.to_excel(w, sheet_name="其他布料原文")
    # CSV 版给以后的 Dashboard 读
    fab.to_csv(paths.summary / "fabric_shops.csv", encoding="utf-8-sig")
    gar.to_csv(paths.summary / "garment_shops.csv", encoding="utf-8-sig")
    mx_shops.to_csv(paths.summary / "garment_fabric_shops.csv", encoding="utf-8-sig")
    mx_posts.to_csv(paths.summary / "garment_fabric_posts.csv", encoding="utf-8-sig")
    shop_tbl.to_csv(paths.summary / "shops.csv", encoding="utf-8-sig")
    long[["shortcode", "shop", "timestamp", "url", "likes", "garment", "fabric", "fabric_source", "fabric_raw"]] \
        .to_csv(paths.summary / "post_items.csv", index=False, encoding="utf-8-sig")
    print(f"✅ 统计表 → {xlsx}")
    print(fab.head(10).to_string())


# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Brás 店铺 服装×布料 调研")
    ap.add_argument("step", choices=["profile", "locate", "scrape", "tag", "summarize", "all"])
    ap.add_argument("--days", type=int, default=7, help="抓最近几天的帖子")
    ap.add_argument("--per-shop", type=int, default=50, help="每个店最多抓几条")
    ap.add_argument("--max-usd", type=float, default=15.0, help="单次 Apify 花费上限(美元)")
    ap.add_argument("--limit", type=int, help="试跑:只取前 N 个店 / N 个帖子")
    ap.add_argument("--workers", type=int, default=8, help="Gemini 并发数")
    ap.add_argument("--since", help="统计只算这天之后的帖子,如 2026-10-01")
    ap.add_argument("--all-types", action="store_true", help="统计也算上布料商等非服装店")
    ap.add_argument("--regiao", default=",".join(DEFAULT_REGIOES),
                    help=f"只抓/只统计这些地区,逗号分隔;all=不过滤。可选:{','.join(REGIOES)}")
    ap.add_argument("--redo", action="store_true", help="locate:已定位过的店也重新判断")
    ap.add_argument("--data-dir", default=os.getenv("BRAS_DATA_DIR", str(HERE / "data")))
    a = ap.parse_args()

    paths = Paths(Path(a.data_dir))
    regioes = None if a.regiao == "all" else [r.strip() for r in a.regiao.split(",")]
    if a.step == "profile":
        profile(paths, min(a.max_usd, 3.0), a.limit)
    if a.step == "locate":
        locate(paths, a.workers, a.redo)
    if a.step in ("scrape", "all"):
        scrape(paths, a.days, a.per_shop, a.max_usd, a.limit, regioes)
    if a.step in ("tag", "all"):
        tag(paths, a.workers, a.limit if a.step == "tag" else None)
    if a.step in ("summarize", "all"):
        summarize(paths, a.since, a.all_types, regioes)


if __name__ == "__main__":
    main()
