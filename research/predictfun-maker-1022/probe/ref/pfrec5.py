# 东京录制 v5:加密板块全量(v4 + 币安现货/合约前 10 档盘口 + OKX/Bybit/Coinbase 买一卖一)
#   币安现货 BTC/ETH/BNB USDT 的逐笔成交 + 买一卖一 + 前 10 档(100ms)(bn);币安 U 本位永续同上(bf)
#   OKX(ok)、Bybit(by)现货与永续 BTC/ETH/BNB 买一卖一逐笔;Coinbase(cb)BTC/ETH-USD ticker
#   Predict BTC/ETH/BNB 涨跌盘(5m/15m/小时/日)的推送(pf):盘口、市场状态与结算、交易状态、页面显示的 Polymarket 概率;
#     Chainlink 价格推送(结算用的那条价格流)和平台价格推送;市场清单含开盘/收盘价与价格编号(pmeta)
#   Predict 上述市场的逐笔成交(pt):公开成交接口每 2 秒轮询一次,按内容去重(推送里没有公开成交频道)
# 每行 = 本机收到纳秒 \t 源 \t 原文;按 UTC 小时分文件 data/YYYYMMDDHH_启动时刻.log.gz;到点后写 REC4_DONE
import asyncio, gzip, json, signal, time, os, sys, urllib.request, websockets
DUR = float(sys.argv[1]); T_END = time.time() + DUR
START = time.strftime("%H%M%S", time.gmtime())
COINS = ("btc", "eth", "bnb")
os.makedirs("data", exist_ok=True)
_cur = [None, None, 0.0]
signal.signal(signal.SIGTERM, lambda *a: sys.exit(0))  # 让 finally 正常收尾 gzip,避免文件损坏
def w(src, msg):
    h = time.strftime("%Y%m%d%H", time.gmtime())
    if _cur[0] != h:
        if _cur[1]: _cur[1].close()
        # 每次启动写新文件(带启动时刻),绝不追加到别的进程写过的 gz
        _cur[0], _cur[1] = h, gzip.open(f"data/{h}_{START}.log.gz", "wt", compresslevel=3)
    _cur[1].write(f"{time.time_ns()}\t{src}\t{msg}\n")
    if time.time() - _cur[2] > 10:  # 每 10 秒同步刷新一次,正在写的文件也能读
        _cur[1].flush(); _cur[2] = time.time()

def get(url, body=None):
    req = urllib.request.Request(url, data=json.dumps(body).encode() if body else None,
                                 headers={"content-type": "application/json", "user-agent": "Mozilla/5.0"})
    return json.load(urllib.request.urlopen(req, timeout=10))
def gql(query, variables=None):
    return get("https://graphql.predict.fun/graphql", {"query": query, "variables": variables or {}})

async def binance(src, url):
    streams = "/".join(f"{c}usdt@trade/{c}usdt@bookTicker/{c}usdt@depth10@100ms" for c in COINS)
    while time.time() < T_END:
        try:
            async with websockets.connect(f"{url}?streams={streams}", max_size=None) as ws:
                async for m in ws:
                    w(src, m)
                    if time.time() > T_END: return
        except Exception as e: w("err", f"{src} {e!r}"); await asyncio.sleep(1)

MD = "marketData{marketId priceFeedId priceFeedSymbol startPrice startPricePublishTime endPrice endPricePublishTime}"
MKTS = {}   # marketId -> {"slug", "startsAt"(秒), "endsAt"(秒)}
def ts(s): return int(time.mktime(time.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")) - time.timezone)
def refresh_markets():
    out = {}
    # 1) 当前正在交易的全部加密涨跌盘(含小时、日)
    d = gql('{categories(filter:{marketVariants:[CRYPTO_UP_DOWN]},pagination:{first:50},sort:STARTS_AT_DESC){edges{node{slug startsAt endsAt markets{edges{node{id}}} ... on CryptoUpDownCategory{%s}}}}}' % MD)
    for e in d["data"]["categories"]["edges"]:
        n = e["node"]
        if n["slug"].split("-")[0] in COINS + ("bitcoin", "ethereum"):
            for me in n["markets"]["edges"]: out[me["node"]["id"]] = {"slug": n["slug"], "startsAt": ts(n["startsAt"]), "endsAt": ts(n["endsAt"]), "md": n.get("marketData")}
    # 2) 5m 的后两轮、15m 的下一轮按编号直接查,开始前就订阅好,避免每轮开头漏数据
    now = int(time.time())
    for c in COINS:
        for per, n_ahead in (("5m", 2), ("15m", 1)):
            L = 300 if per == "5m" else 900
            for k in range(0, n_ahead + 1):
                slug = f"{c}-updown-{per}-{now // L * L + k * L}"
                cat = (gql('query($id:ID!){category(id:$id){slug startsAt endsAt markets{edges{node{id}}} ... on CryptoUpDownCategory{%s}}}' % MD, {"id": slug}).get("data") or {}).get("category")
                if cat:
                    for me in cat["markets"]["edges"]: out[me["node"]["id"]] = {"slug": cat["slug"], "startsAt": ts(cat["startsAt"]), "endsAt": ts(cat["endsAt"]), "md": cat.get("marketData")}
    return out

MARKET_TOPICS = ("predictOrderbook", "predictMarketStatus", "predictTradingStatus", "polymarketChance")
async def predict_book():
    rid = 0
    while time.time() < T_END:
        try:
            async with websockets.connect("wss://ws.predict.fun/ws", max_size=None, origin="https://predict.fun") as ws:
                subs = set()
                async def resub():
                    nonlocal rid
                    while True:
                        try:
                            cur = await asyncio.to_thread(refresh_markets)
                            MKTS.clear(); MKTS.update(cur); w("pmeta", json.dumps(cur))
                            want = set()
                            for mid, v in cur.items():
                                want |= {f"{t}/{mid}" for t in MARKET_TOPICS}
                                for md in v.get("md") or []:
                                    if md.get("priceFeedId"): want |= {f"chainlinkAssetPriceUpdate/{md['priceFeedId']}", f"assetPriceUpdate/{md['priceFeedId']}"}
                            for tp in sorted(want - subs):
                                rid += 1; await ws.send(json.dumps({"requestId": rid, "method": "subscribe", "params": [tp]})); subs.add(tp)
                            if cur:  # 已结束的市场退订,订阅数不无限增长
                                for tp in sorted(subs - want):
                                    rid += 1; await ws.send(json.dumps({"requestId": rid, "method": "unsubscribe", "params": [tp]})); subs.discard(tp)
                        except Exception as e: w("err", f"pmeta {e!r}")
                        await asyncio.sleep(20)
                t = asyncio.create_task(resub())
                try:
                    async for m in ws:
                        if m.startswith('{"type":"M","topic":"heartbeat"'):
                            d = json.loads(m); await ws.send(json.dumps({"method": "heartbeat", "data": d["data"]})); continue
                        w("pf", m)
                        if time.time() > T_END: return
                finally: t.cancel()
        except Exception as e: w("err", f"pf {e!r}"); await asyncio.sleep(1)

FILLS_Q = 'query($f:MatchEventLogFilterInput,$p:ForwardPaginationInput){matchEventLog(filter:$f,pagination:$p){edges{node{transactionHash amountFilled priceExecuted quoteType timestamp outcome{name} account{address}}}}}'
async def predict_trades():
    seen = {}   # marketId -> set(去重键);市场结束后清掉
    while time.time() < T_END:
        t0 = time.time()
        live = [mid for mid, v in list(MKTS.items()) if v["startsAt"] - 5 <= t0 <= v["endsAt"] + 30]
        for mid in live:
            try:
                d = await asyncio.to_thread(gql, FILLS_Q, {"f": {"marketId": mid}, "p": {"first": 100}})
                rows = [e["node"] for e in d["data"]["matchEventLog"]["edges"]]
                s = seen.setdefault(mid, set()); new = []
                for r in rows:
                    k = (r["transactionHash"], r["account"]["address"], r["amountFilled"], r["priceExecuted"], r["outcome"]["name"], r["quoteType"])
                    if k not in s: s.add(k); new.append(r)
                if new: w("pt", json.dumps({"marketId": mid, "fills": new, "page_full": len(rows) == 100}))
            except Exception as e: w("err", f"pt {mid} {e!r}")
        for mid in [m for m in seen if m not in MKTS]: seen.pop(mid)
        await asyncio.sleep(max(0.2, 2 - (time.time() - t0)))


async def ws_feed(src, url, subs, ping=None, ping_every=20):
    # 通用:连上后发送订阅消息,收到的原文全记;断线重连
    while time.time() < T_END:
        try:
            async with websockets.connect(url, max_size=None, ping_interval=20) as ws:
                for m in subs: await ws.send(json.dumps(m))
                async def pinger():
                    while True:
                        await asyncio.sleep(ping_every)
                        if ping: await ws.send(ping)
                pt = asyncio.create_task(pinger())
                try:
                    async for m in ws:
                        if m == "pong": continue
                        w(src, m)
                        if time.time() > T_END: return
                finally: pt.cancel()
        except Exception as e: w("err", f"{src} {e!r}"); await asyncio.sleep(2)

OKX_ARGS = [{"channel": "bbo-tbt", "instId": f"{c.upper()}-USDT"} for c in COINS] + [{"channel": "bbo-tbt", "instId": f"{c.upper()}-USDT-SWAP"} for c in COINS]

async def main():
    await asyncio.gather(binance("bn", "wss://stream.binance.com:9443/stream"), binance("bf", "wss://fstream.binance.com/stream"),
                         predict_book(), predict_trades(),
                         ws_feed("ok", "wss://ws.okx.com:8443/ws/v5/public", [{"op": "subscribe", "args": OKX_ARGS}], ping="ping"),
                         ws_feed("by", "wss://stream.bybit.com/v5/public/spot", [{"op": "subscribe", "args": [f"orderbook.1.{c.upper()}USDT" for c in COINS]}], ping=json.dumps({"op": "ping"})),
                         ws_feed("byl", "wss://stream.bybit.com/v5/public/linear", [{"op": "subscribe", "args": [f"orderbook.1.{c.upper()}USDT" for c in COINS]}], ping=json.dumps({"op": "ping"})),
                         ws_feed("cb", "wss://ws-feed.exchange.coinbase.com", [{"type": "subscribe", "product_ids": ["BTC-USD", "ETH-USD"], "channels": ["ticker"]}]))  # Polymarket 用 PMData 回填,不在这录
try:
    asyncio.run(main())
finally:
    if _cur[1]: _cur[1].close()
open("REC5_DONE", "w").write(time.strftime("%F %T"))
