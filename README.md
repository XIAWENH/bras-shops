# Brás 店铺调研

Instagram 抓 Brás / 圣保罗市批发店的帖子 → Gemini 识别「服装 × 布料」→ 统计每种布料、每种服装各有多少家店在做。

## 新电脑上安装

```bash
git clone <这个仓库地址>
cd bras_shops
pip install -r requirements.txt
copy .env.example .env      # 然后用记事本把两个 key 填进去
```

## 日常使用

```bash
python bras_pipeline.py all --days 7               # 抓最近 7 天 + 识别 + 出表(默认 Brás + 圣保罗市)
python bras_pipeline.py all --days 7 --regiao bras # 只看 Brás
python bras_pipeline.py scrape --days 7 --limit 5  # 先拿 5 个店试跑
```

结果:`data/bras_resumo_日期.xlsx`(布料-店铺数、服装-店铺数、服装×布料、店铺明细…)

## 店铺名单

- `shops.csv`:要跟踪的 Instagram 账号。`ativo=0` 不抓;`tipo=loja` 才进统计。
- `shops_local.csv`:每家店在哪(AI 根据主页简介/WhatsApp 区号/帖子判断,附证据)。判断错了直接改 `regiao`。
- 往 `shops.csv` 加新账号后,先跑一次定位,新店才会被算进来:

```bash
python bras_pipeline.py profile   # 抓主页资料,约 $0.0015/个
python bras_pipeline.py locate    # 只定位还没定位过的店
```

## 费用

Apify Instagram Scraper 约 $2.30 / 1000 条结果;242 个店抓一周约 $5–6。单次上限默认 $15(`--max-usd`)。
