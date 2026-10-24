#!/usr/bin/env python3
"""回测数据准备:东京录制器的小时文件 + 同一时段链上成交 → 每轮一个事件文件(给 Rust 回测引擎 replay 用)。

    python3 backtest_data.py <录制器小时文件 .log.gz ...> --out 目录 [--rpc URL]
    python3 backtest_data.py index --out 目录          # 所有小时处理完后:写每轮结果(收盘价)与清单

每个小时文件只留 BTC 5 分钟盘口相关的:
  - pmeta 里 slug = btc-updown-5m-<开盘秒> 的市场(开盘价、收盘价)
  - 这些市场的盘口推送 pf(原文)、币安现货 btcusdt@bookTicker(原文)、平台成交记录 pt(用来把链上 token 对到 市场 / 涨跌)
  - 链上:这一小时(多拉 2 分钟)交易所的 OrderFilled,只留这些市场的挂单方成交;撮合时刻 = 区块时间 − 434ms(实测)
区块时间:BSC 固定每 450ms 一块(10-24 实测 1971/1972 段),每 200 块取一次真实时间锚定。
输出 <out>/parts/<开盘秒>.<小时>.jsonl.gz:每行 [种类, 本机毫秒, ...]
  ["pf", t, 原文] / ["bn", t, 原文] / ["fill", t, qside, 档位分, 股数, 挂单地址, 挂单 hash]
index 写 <out>/rounds.json:[{start_s, market_id, start_price, end_price, payout_up, parts}](收盘价 = pmeta 的 endPrice,
没有就用下一轮的开盘价;平局 payout_up = 0.5)
"""
import argparse
import glob
import gzip
import json
import os
import sys
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import maker_rank as MR  # noqa: E402

RPC = "https://bsc-rpc.publicnode.com"
BLOCK_MS = 450
LAG_MS = MR.DEFAULT_LAG_MS


def rpc(url, method, params, tries=6):
    err = None
    for i in range(tries):
        try:
            req = urllib.request.Request(url, data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
                                         headers={"content-type": "application/json", "user-agent": "curl/8.5.0"})
            with urllib.request.urlopen(req, timeout=60) as r:
                d = json.load(r)
            if "error" in d:
                raise RuntimeError(d["error"])
            return d["result"]
        except Exception as e:  # noqa: BLE001  节点限流 / 超时:退避重试
            err = e
            time.sleep(min(30, 2 ** i))
    raise err


def scan_hour(path):
    """一个录制器小时文件 → (meta, pf, bn, pt)。只留 BTC 5 分钟盘口相关的行"""
    meta, pf, bn, pt = {}, [], [], {}
    btc = set()
    with gzip.open(path, "rt", errors="replace") as f:
        for line in f:
            p = line.split("\t", 2)
            if len(p) < 3:
                continue
            kind = p[1]
            if kind not in ("pmeta", "pf", "bn", "pt"):
                continue
            t = int(p[0]) // 1_000_000
            raw = p[2].rstrip("\n")
            if kind == "pmeta":
                for k, v in json.loads(raw).items():
                    if str(v.get("slug", "")).startswith("btc-updown-5m-"):
                        m = int(k)
                        btc.add(m)
                        md = (v.get("md") or [{}])[0]
                        old = meta.get(m, {})
                        meta[m] = {"start_s": int(v["startsAt"]), "end_s": int(v["endsAt"]),
                                   "start_price": md.get("startPrice") if md.get("startPrice") is not None else old.get("start_price"),
                                   "end_price": md.get("endPrice") if md.get("endPrice") is not None else old.get("end_price")}
            elif kind == "bn":
                if raw.startswith('{"stream":"btcusdt@bookTicker"'):
                    bn.append((t, raw))
            elif kind == "pf":
                i = raw.find('"topic":"predictOrderbook/')
                if i < 0:
                    continue
                j = raw.find('"', i + 26)
                try:
                    m = int(raw[i + 26:j])
                except ValueError:
                    continue
                pf.append((t, m, raw))
            else:
                d = json.loads(raw)
                m = int(d.get("marketId") or 0)
                for x in d.get("fills") or []:
                    pt.setdefault(m, {})[str(x.get("transactionHash", "")).lower()] = x
    pf = [x for x in pf if x[1] in btc]
    pt = {m: v for m, v in pt.items() if m in btc}
    return meta, pf, bn, pt


def block_at(url, t_ms, cache):
    """时间 → 区块号(先按 450ms 推,再用真实区块时间修正)"""
    if "head" not in cache:
        h = int(rpc(url, "eth_blockNumber", []), 16)
        cache["head"] = (h, MR.block_ms(rpc(url, "eth_getBlockByNumber", [hex(h), False])))
    h, hm = cache["head"]
    b = h - (hm - t_ms) // BLOCK_MS
    for _ in range(4):
        bm = MR.block_ms(rpc(url, "eth_getBlockByNumber", [hex(b), False]))
        d = (t_ms - bm) // BLOCK_MS
        if d == 0:
            break
        b += d
    return b


def fetch_chain(url, t0_ms, t1_ms, step=200, pause=0.25):
    cache = {}
    b0, b1 = block_at(url, t0_ms, cache), block_at(url, t1_ms, cache)
    logs, bms, b = [], {}, b0
    while b <= b1:
        e = min(b + step - 1, b1)
        anchor = MR.block_ms(rpc(url, "eth_getBlockByNumber", [hex(b), False]))
        try:
            got = rpc(url, "eth_getLogs", [{"address": MR.EXCHANGE, "topics": [MR.ORDER_FILLED], "fromBlock": hex(b), "toBlock": hex(e)}], tries=2)
        except Exception:  # noqa: BLE001  范围太大:减半
            if step <= 25:
                raise
            step //= 2
            continue
        for lg in got:
            bn = int(lg["blockNumber"], 16)
            bms[str(bn)] = anchor + (bn - b) * BLOCK_MS
        logs += got
        b = e + 1
        time.sleep(pause)
    return {"from_block": b0, "to_block": b1, "logs": logs, "block_ms": bms}


def token_map(chain, pt):
    """平台成交记录 pt(交易哈希、吃单方地址、买卖、结果名)对上链上吃单方那一行 → token → (市场, Up/Down)"""
    tok = {}
    by_tx = {}
    for m, txs in pt.items():
        for tx, x in txs.items():
            by_tx.setdefault(tx, []).append((m, x))
    for lg in chain["logs"]:
        tp = lg["topics"]
        if len(tp) < 4 or "0x" + tp[3][-40:].lower() != MR.EXCHANGE:
            continue
        for m, x in by_tx.get(lg["transactionHash"].lower(), []):
            if ("0x" + tp[2][-40:]).lower() != str(x.get("account", {}).get("address", "")).lower():
                continue
            d = lg["data"][2:]
            w = [int(d[i:i + 64], 16) for i in range(0, len(d), 64)]
            buy = w[0] == 0
            token = str(w[1] if buy else w[0])
            name = (x.get("outcome") or {}).get("name")
            if name not in ("Up", "Down"):
                continue
            if (x.get("quoteType") == "BID") != buy:  # 吃单方方向与记录不符:不用这一条
                continue
            tok.setdefault(token, set()).add((m, name))
    return {t: next(iter(v)) for t, v in tok.items() if len(v) == 1}


def process_hour(path, out, url, chain_json=None):
    hour = os.path.basename(path).split("_")[0]
    meta, pf, bn, pt = scan_hour(path)
    if not pf:
        print("%s:没有 BTC 5 分钟盘口" % hour)
        return
    t0, t1 = min(x[0] for x in pf), max(x[0] for x in pf)
    if chain_json:
        chain = json.load(open(chain_json))  # 已下载的链上数据(maker_rank.py fetch 的格式);只覆盖它那段时间
    else:
        chain = fetch_chain(url, t0 - 60_000, t1 + 120_000)
    tok = token_map(chain, pt)
    rows = MR.maker_rows(chain, tok)
    os.makedirs(os.path.join(out, "parts"), exist_ok=True)
    nfill = 0
    for m, v in sorted(meta.items()):
        s = v["start_s"]
        lo, hi = (s - 90) * 1000, (v["end_s"] + 15) * 1000
        ev = [["pf", t, raw] for t, mm, raw in pf if mm == m and lo <= t <= hi]
        if not ev:
            continue
        ev += [["bn", t, raw] for t, raw in bn if lo <= t <= hi]
        f = [["fill", r["ms"] - LAG_MS, r["qside"], r["level"], r["shares"], r["maker"], r["hash"]] for r in rows if r["market_id"] == m and r["ms"] is not None]
        nfill += len(f)
        ev += f
        ev.sort(key=lambda e: (e[1], 0 if e[0] == "pf" else 1 if e[0] == "bn" else 2))
        with gzip.open(os.path.join(out, "parts", "%d.%s.jsonl.gz" % (s, hour)), "wt") as g:
            g.write(json.dumps({"market_id": m, **v}) + "\n")
            for e in ev:
                g.write(json.dumps(e, separators=(",", ":")) + "\n")
    print("%s:BTC 市场 %d 个,盘口推送 %d、币安 %d、链上日志 %d、BTC 挂单成交 %d(token 对上 %d 个)"
          % (hour, len(meta), len(pf), len(bn), len(chain["logs"]), nfill, len(tok)))


def build_index(out):
    heads = {}
    for p in sorted(glob.glob(os.path.join(out, "parts", "*.jsonl.gz"))):
        with gzip.open(p, "rt") as f:
            h = json.loads(f.readline())
        s = h["start_s"]
        r = heads.setdefault(s, {"start_s": s, "market_id": h["market_id"], "start_price": None, "end_price": None, "parts": []})
        r["parts"].append(os.path.relpath(p, out))
        for k in ("start_price", "end_price"):
            if h.get(k) is not None:
                r[k] = h[k]
    rounds = [heads[s] for s in sorted(heads)]
    by_start = {r["start_s"]: r for r in rounds}
    for r in rounds:
        nxt = by_start.get(r["start_s"] + 300)
        end = r["end_price"] if r["end_price"] is not None else (nxt or {}).get("start_price")
        r["end_price"] = end
        if r["start_price"] is None or end is None:
            r["payout_up"] = None
        else:
            r["payout_up"] = 1.0 if end > r["start_price"] else (0.0 if end < r["start_price"] else 0.5)
    json.dump(rounds, open(os.path.join(out, "rounds.json"), "w"), indent=1)
    ok = sum(1 for r in rounds if r["payout_up"] is not None)
    print("共 %d 轮,有结果 %d 轮 → %s/rounds.json" % (len(rounds), ok, out))
    return rounds


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("files", nargs="+")
    ap.add_argument("--out", required=True)
    ap.add_argument("--rpc", default=os.environ.get("BSC_RPC", RPC))
    ap.add_argument("--chain-json", help="用已下载的链上数据,不去节点拉")
    a = ap.parse_args(argv)
    if a.files == ["index"]:
        build_index(a.out)
        return 0
    for p in a.files:
        process_hour(p, a.out, a.rpc, a.chain_json)
    return 0


if __name__ == "__main__":
    sys.exit(main())
