#!/usr/bin/env python3
"""工具 5:我们在做市商队伍里排第几(链上成交 + 我们录的盘口,不拟合任何参数)。

    python3 maker_rank.py fetch  <日志目录或文件 ...> --out chain.json [--rpc URL]   # 拉日志时间段内交易所的 OrderFilled
    python3 maker_rank.py report <日志目录或文件 ...> --chain chain.json [--out 目录]

原理
  平台按价格、时间先后撮合(先挂先成);一笔吃单在链上按撮合顺序逐个发出挂单方的 OrderFilled。
  所以同一价位上,各张挂单在链上第一次出现的先后 = 它们在队里的先后。链上能看到每张单的挂单地址,
  于是能知道排在我们前面的是谁。
  - 同一价位的队:买 Up@p、卖 Down@(1−p) 在同一队("Up 买一侧 p");买 Down@q、卖 Up@(1−q) 在同一队。
  - 先挂先成核对:(1)同一笔吃单里我们的单后面还有别人在同价位成交,则我们的单此时必须已全部成交;
    (2)我们自己同价位的几张单,链上先后应与挂单先后一致。两项不成立的比例写在汇总里。

  抢新价位:某一价位在盘口从无到有(新价位诞生),我们在诞生后 race_ms 内决定挂到这个价 = 一次"抢位"。
  - 盘口量:我们的单被接受时,这一价位已有的量(我们前面的量,含后来撤掉没成交的)
  - 链上:这一价位上、诞生之后第一次成交、且排在我们的单前面的单 = 比我们快的单,记下地址
  - 我们没成交时:我们挂着期间这一价位已成交的单都排在我们前面(下限)
  对每个对手地址:在两边都在链上出现的抢位里,谁在前。

输出 rank_orders.tsv(我们每张链上成交过的单)、rank_races.tsv(每次抢位)、rank_rivals.tsv(对手),屏幕汇总。
"""
import argparse
import json
import os
import sys
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import probelog as P  # noqa: E402

EXCHANGE = "0x8bc070bedab741406f4b1eb65a72bee27894b689"
ORDER_FILLED = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"
RPC = "https://bsc-rpc.publicnode.com"
DEFAULT_LAG_MS = 435  # 10-24 实盘 314 张单实测:区块时间 − 平台撮合时刻 p50(p10 243,p90 632)


# ---------------- 链上 ----------------

def rpc(url, method, params):
    req = urllib.request.Request(url, data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
                                 headers={"content-type": "application/json", "user-agent": "curl/8.5.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        d = json.load(r)
    if "error" in d:
        raise RuntimeError(d["error"])
    return d["result"]


def block_ms(blk):
    """区块时间(毫秒)。BSC 区块头 mixHash 的末 8 字节放毫秒部分(Lorentz 之后);取不到就用整秒"""
    s = int(blk["timestamp"], 16) * 1000
    mix = blk.get("mixHash") or ""
    if len(mix) == 66:
        ms = int(mix[-16:], 16)
        if 0 <= ms < 1000:
            s += ms
    return s


def fetch(url, t0_ms, t1_ms, step=200):
    head = int(rpc(url, "eth_blockNumber", []), 16)
    cache = {}

    def ts(b):
        if b not in cache:
            cache[b] = block_ms(rpc(url, "eth_getBlockByNumber", [hex(b), False]))
        return cache[b]

    def find(t):
        lo, hi = max(0, head - 2_000_000), head
        while lo < hi:
            m = (lo + hi) // 2
            if ts(m) < t:
                lo = m + 1
            else:
                hi = m
        return lo

    b0, b1 = find(t0_ms), find(t1_ms)
    logs, b = [], b0
    while b <= b1:
        e = min(b + step - 1, b1)
        try:
            logs += rpc(url, "eth_getLogs", [{"address": EXCHANGE, "topics": [ORDER_FILLED], "fromBlock": hex(b), "toBlock": hex(e)}])
        except Exception:  # noqa: BLE001  节点嫌范围大:减半重试
            if step <= 10:
                raise
            step //= 2
            continue
        b = e + 1
    blocks = {}
    for bn in sorted({int(x["blockNumber"], 16) for x in logs}):
        blocks[str(bn)] = ts(bn)
    return {"from_block": b0, "to_block": b1, "logs": logs, "block_ms": blocks}


def maker_rows(chain, tokens):
    """链上挂单方成交行。tokens: token_id(字符串) → (market_id, "Up"/"Down")。只留这些市场的"""
    out = []
    bms = chain.get("block_ms", {})
    for lg in chain["logs"]:
        tp = lg["topics"]
        if len(tp) < 4 or tp[0].lower() != ORDER_FILLED:
            continue
        taker = "0x" + tp[3][-40:].lower()
        if taker == EXCHANGE:  # 吃单方自己那一行
            continue
        d = lg["data"][2:]
        w = [int(d[i:i + 64], 16) for i in range(0, len(d), 64)]
        m_asset, t_asset, m_amt, t_amt = w[0], w[1], w[2], w[3]
        if m_asset == 0:  # 挂单方付 USDT 买结果币
            buy, token, shares, usd = True, str(t_asset), t_amt / 1e18, m_amt / 1e18
        else:
            buy, token, shares, usd = False, str(m_asset), m_amt / 1e18, t_amt / 1e18
        if token not in tokens or shares <= 0:
            continue
        market, outcome = tokens[token]
        cents = int(round(usd / shares * 100))
        # 同一队:买 Up@p ≡ 卖 Down@(1−p) → "UpBid p";买 Down@q ≡ 卖 Up@(1−q) → "DownBid q"
        if (outcome == "Up") == buy:
            qside, level = "UpBid", cents if buy else 100 - cents
        else:
            qside, level = "DownBid", cents if buy else 100 - cents
        bn = int(lg["blockNumber"], 16)
        out.append({"block": bn, "tx_index": int(lg["transactionIndex"], 16), "log_index": int(lg["logIndex"], 16), "tx": lg["transactionHash"],
                    "ms": bms.get(str(bn)), "hash": tp[1].lower(), "maker": "0x" + tp[2][-40:].lower(), "market_id": market,
                    "qside": qside, "level": level, "buy": buy, "shares": shares})
    out.sort(key=lambda r: (r["block"], r["tx_index"], r["log_index"]))
    return out


# ---------------- 盘口(全档) ----------------

def load_books(paths, matched=None):
    """market_id → [(本机收到 ms, {Up 买价分: 量}, {Up 卖价分: 量})]。
    matched 给一个 dict 时顺便收集钱包"已撮合"推送:订单 hash → 平台撮合时刻 ms"""
    books = {}
    for l in P.iter_lines(paths):
        if l.get("kind") != "pf":
            continue
        try:
            d = json.loads(l.get("raw", ""))
        except ValueError:
            continue
        topic = d.get("topic") or ""
        if matched is not None and topic.startswith("predictWalletEvents"):
            x = d.get("data") or {}
            if x.get("type") == "orderTransactionSubmitted" and x.get("orderHash") and isinstance(x.get("timestamp"), (int, float)):
                matched.setdefault(x["orderHash"].lower(), x["timestamp"])
            continue
        if not topic.startswith("predictOrderbook/"):
            continue
        data = d.get("data") or {}
        m = int(data.get("marketId") or topic.split("/", 1)[1])
        bids = {int(round(float(p) * 100)): float(s) for p, s in data.get("bids") or []}
        asks = {int(round(float(p) * 100)): float(s) for p, s in data.get("asks") or []}
        books.setdefault(m, []).append((l["wall_ms"], bids, asks))
    for v in books.values():
        v.sort(key=lambda x: x[0])
    return books


def level_size(snap, qside, level):
    """某一队在这一刻的量:UpBid p = Up 买 p;DownBid q = Up 卖 (1−q)"""
    _, bids, asks = snap
    return bids.get(level, 0.0) if qside == "UpBid" else asks.get(100 - level, 0.0)


def snap_at(series, t):
    """t 时刻(含)之前最后一条"""
    lo, hi = 0, len(series)
    while lo < hi:
        m = (lo + hi) // 2
        if series[m][0] <= t:
            lo = m + 1
        else:
            hi = m
    return series[lo - 1] if lo else None


def birth_before(series, qside, level, t, lookback_ms):
    """t 之前 lookback_ms 内,这一价位从无到有的最后时刻;没有返回 None"""
    lo, hi = 0, len(series)
    while lo < hi:
        m = (lo + hi) // 2
        if series[m][0] <= t:
            lo = m + 1
        else:
            hi = m
    i = lo - 1
    while i > 0 and series[i][0] >= t - lookback_ms:
        if level_size(series[i], qside, level) > 1e-9 and level_size(series[i - 1], qside, level) <= 1e-9:
            return series[i][0]
        i -= 1
    return None


# ---------------- 分析 ----------------

def chain_lag(rows, matched):
    """链上区块时间比平台撮合晚多少(我们自己的单:第一次上链 − "已撮合"推送里的平台时刻),取中位数"""
    first = {}
    for x in rows:
        if x["hash"] in matched and x["hash"] not in first and x["ms"] is not None:
            first[x["hash"]] = x["ms"] - matched[x["hash"]]
    return P.median(list(first.values())) if first else None


def analyze(L, books, rows, race_ms=1000, lag_ms=DEFAULT_LAG_MS):
    """lag_ms:区块时间 − 撮合时刻。别人的单"在我们挂上之后还在队里" = 它最后一次成交的区块时间 − lag_ms ≥ 我们被接受的时刻"""
    ours = {r["hash"].lower(): r for r in L.orders.values() if r.get("hash") and r.get("ack_ok")}
    our_makers = {x["maker"] for x in rows if x["hash"] in ours}
    # 各队:第一次出现的先后
    first = {}  # (market, qside, level) → [hash 按第一次出现]
    seen = set()
    info = {}  # hash → {maker, first_ms, shares}
    for x in rows:
        k = (x["market_id"], x["qside"], x["level"])
        h = x["hash"]
        i = info.setdefault(h, {"maker": x["maker"], "first_ms": x["ms"], "last_ms": x["ms"], "shares": 0.0, "key": k})
        i["shares"] += x["shares"]
        i["last_ms"] = x["ms"]
        if h not in seen:
            seen.add(h)
            first.setdefault(k, []).append(h)

    # 先后核对 (1):同一笔吃单、同一队,我们后面还有人成交 → 我们此时已全部成交
    fifo_ok = fifo_bad = 0
    got = {}
    by_tx = {}
    for x in rows:
        by_tx.setdefault((x["tx"], x["market_id"], x["qside"], x["level"]), []).append(x)
    for k, xs in by_tx.items():
        for j, x in enumerate(xs):
            if x["hash"] in ours:
                got[x["hash"]] = got.get(x["hash"], 0.0) + x["shares"]
                if any(y["hash"] != x["hash"] for y in xs[j + 1:]):
                    if got[x["hash"]] >= (ours[x["hash"]]["shares"] or 0) - 1e-6:
                        fifo_ok += 1
                    else:
                        fifo_bad += 1
    # 先后核对 (2):我们自己同一队的单,链上先后 = 挂单(接受)先后
    own_ok = own_bad = 0
    for k, hs in first.items():
        mine = [h for h in hs if h in ours]
        for a in range(len(mine)):
            for b in range(a + 1, len(mine)):
                if ours[mine[a]]["ack_ms"] <= ours[mine[b]]["ack_ms"]:
                    own_ok += 1
                else:
                    own_bad += 1

    order_rows, races = [], []
    rivals = {}  # maker → {"ahead":我们前面次数, "behind":我们后面次数}
    for h, o in sorted(ours.items(), key=lambda kv: kv[1]["decide_ms"] or 0):
        k = (o["market_id"], o["qside"], int(round((o["price"] or 0) * 100)))
        hs = first.get(k, [])
        series = books.get(o["market_id"]) or []
        live_from = o["ack_ms"]
        live_to = o["cancel_ack_ms"] or o["end_ms"] or (live_from + 10 ** 9 if live_from else None)
        # 这张单挂着期间在这一队上成交过的别人的单
        others = [g for g in hs if g not in ours and live_from is not None
                  and info[g]["last_ms"] is not None and info[g]["last_ms"] - lag_ms >= live_from]
        if h in info:
            pos = hs.index(h)
            ahead = [g for g in hs[:pos] if g in others]
            behind = [g for g in hs[pos + 1:] if g in others]
        else:
            # 没成交:挂着期间别人成交了,他们都在前面(下限)
            ahead = [g for g in others if info[g]["first_ms"] is not None and info[g]["first_ms"] - lag_ms <= (live_to or 0)]
            behind = []
        birth = birth_before(series, o["qside"], k[2], o["decide_ms"], race_ms) if series and o["decide_ms"] else None
        snap = snap_at(series, o["ack_ms"]) if series and o["ack_ms"] else None
        book_ahead = level_size(snap, o["qside"], k[2]) if snap else None
        row = {"cid": o["cid"], "market_id": o["market_id"], "qside": o["qside"], "price": o["price"], "shares": o["shares"],
               "decide_ms": o["decide_ms"], "ack_ms": o["ack_ms"], "on_chain": h in info, "chain_shares": round(info[h]["shares"], 6) if h in info else 0.0,
               "queue_ahead_at_decide": o["queue_ahead"], "book_ahead_at_ack": book_ahead,
               "chain_ahead_orders": len(ahead), "chain_ahead_shares": round(sum(info[g]["shares"] for g in ahead), 6),
               "chain_ahead_makers": ",".join(sorted({info[g]["maker"][:10] for g in ahead})),
               "level_birth_ms": birth, "decide_after_birth_ms": (o["decide_ms"] - birth) if birth else None}
        if h in info or ahead:
            order_rows.append(row)
        if birth is not None:
            # 抢位:只算诞生之后才第一次成交的单(诞生之前就有成交的是上一次同价位,不算)
            a2 = [g for g in ahead if (info[g]["first_ms"] or 0) >= birth]
            b2 = [g for g in behind if (info[g]["first_ms"] or 0) >= birth]
            races.append({**row, "race_rank": (len(a2) + 1) if h in info else None, "race_rank_at_least": len(a2) + 1,
                          "race_ahead_makers": ",".join(info[g]["maker"][:10] for g in a2),
                          "race_behind_makers": ",".join(info[g]["maker"][:10] for g in b2)})
            if h in info:
                for g in a2:
                    rivals.setdefault(info[g]["maker"], {"ahead": 0, "behind": 0})["ahead"] += 1
                for g in b2:
                    rivals.setdefault(info[g]["maker"], {"ahead": 0, "behind": 0})["behind"] += 1
    # 对手整体:这段时间在这些市场挂单成交的笔数、股数
    vol = {}
    for x in rows:
        v = vol.setdefault(x["maker"], {"fills": 0, "shares": 0.0, "orders": set()})
        v["fills"] += 1
        v["shares"] += x["shares"]
        v["orders"].add(x["hash"])
    riv_rows = []
    for mk, v in sorted(vol.items(), key=lambda kv: -kv[1]["shares"]):
        r = rivals.get(mk, {"ahead": 0, "behind": 0})
        riv_rows.append({"maker": mk, "us": mk in our_makers, "fills": v["fills"], "orders": len(v["orders"]), "shares": round(v["shares"], 3),
                         "race_ahead_of_us": r["ahead"], "race_behind_us": r["behind"]})
    checks = {"fifo_ok": fifo_ok, "fifo_bad": fifo_bad, "own_order_ok": own_ok, "own_order_bad": own_bad, "our_makers": sorted(our_makers)}
    return order_rows, races, riv_rows, checks


def report(paths, chain_path, out, race_ms=1000):
    L = P.load(paths)
    tokens = {}
    for m, l in L.markets.items():
        tokens[str(l.get("up_token"))] = (m, "Up")
        tokens[str(l.get("down_token"))] = (m, "Down")
    chain = json.load(open(chain_path))
    rows = maker_rows(chain, tokens)
    matched = {}
    books = load_books(paths, matched)
    lag = chain_lag(rows, matched)
    order_rows, races, riv, checks = analyze(L, books, rows, race_ms=race_ms, lag_ms=lag if lag is not None else DEFAULT_LAG_MS)
    os.makedirs(out, exist_ok=True)
    cols = ["cid", "market_id", "qside", "price", "shares", "decide_ms", "ack_ms", "on_chain", "chain_shares", "queue_ahead_at_decide",
            "book_ahead_at_ack", "chain_ahead_orders", "chain_ahead_shares", "chain_ahead_makers", "level_birth_ms", "decide_after_birth_ms"]
    P.write_tsv(os.path.join(out, "rank_orders.tsv"), cols, order_rows)
    P.write_tsv(os.path.join(out, "rank_races.tsv"), cols + ["race_rank", "race_rank_at_least", "race_ahead_makers", "race_behind_makers"], races)
    P.write_tsv(os.path.join(out, "rank_rivals.tsv"), ["maker", "us", "fills", "orders", "shares", "race_ahead_of_us", "race_behind_us"], riv)

    n_maker = len(riv)
    print("链上:这几轮市场里挂单方成交 %d 行、%d 个挂单地址;我们的地址 %s" % (len(rows), n_maker, ",".join(a[:10] for a in checks["our_makers"]) or "(没出现)"))
    print("先挂先成核对:同一笔吃单里我们后面还有人 → 我们已全成 %d 次,不成立 %d 次;我们自己同价位的单先后一致 %d 对,不一致 %d 对"
          % (checks["fifo_ok"], checks["fifo_bad"], checks["own_order_ok"], checks["own_order_bad"]))
    print("区块时间比平台撮合晚:%s" % ("p50 %d 毫秒(按我们自己的单实测)" % lag if lag is not None else "没有'已撮合'推送,按 %d 毫秒" % DEFAULT_LAG_MS))
    us_rank = next((i + 1 for i, r in enumerate(riv) if r["us"]), None)
    if us_rank:
        u = riv[us_rank - 1]
        print("按成交股数,我们在 %d 个挂单地址里排第 %d(%.1f 股,%d 张单)" % (n_maker, us_rank, u["shares"], u["orders"]))
    filled = [r for r in order_rows if r["on_chain"]]
    if filled:
        print("我们链上成交过的单 %d 张:排在前面的别人的单(挂着期间也成交了的) p50 %s 张 / %s 股"
              % (len(filled), P.median([r["chain_ahead_orders"] for r in filled]), P.short(P.median([r["chain_ahead_shares"] for r in filled]))))
    print("抢新价位(价位诞生后 %d 毫秒内我们决定挂上去):%d 次" % (race_ms, len(races)))
    if races:
        done = [r for r in races if r["race_rank"] is not None]
        ranks = sorted(r["race_rank"] for r in done)
        if ranks:
            print("  其中我们链上成交的 %d 次:我们排第几 p50 %s,第 1 的 %d 次,第 2 的 %d 次,第 3 及以后 %d 次"
                  % (len(ranks), P.median(ranks), ranks.count(1), ranks.count(2), sum(1 for x in ranks if x >= 3)))
        print("  决定挂单时距价位诞生 p50 %s 毫秒;我们被接受时这一价位已有的量 p50 %s 股"
              % (P.short(P.median([r["decide_after_birth_ms"] for r in races])), P.short(P.median([r["book_ahead_at_ack"] for r in races if r["book_ahead_at_ack"] is not None]))))
    top = [r for r in riv if not r["us"]][:10]
    if top:
        print("成交最多的对手(抢位里排在我们前面 / 后面的次数):")
        for r in top:
            print("   %s…  成交 %8.1f 股 %4d 张单   前 %3d  后 %3d" % (r["maker"][:10], r["shares"], r["orders"], r["race_ahead_of_us"], r["race_behind_us"]))
    print("写出 %s/rank_orders.tsv、rank_races.tsv、rank_rivals.tsv" % out)
    return order_rows, races, riv, checks


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("fetch")
    f.add_argument("paths", nargs="+")
    f.add_argument("--out", required=True)
    f.add_argument("--rpc", default=os.environ.get("BSC_RPC", RPC))
    r = sub.add_parser("report")
    r.add_argument("paths", nargs="+")
    r.add_argument("--chain", required=True)
    r.add_argument("--out", default="analysis_out")
    r.add_argument("--race-ms", type=int, default=1000)
    a = ap.parse_args(argv)
    if a.cmd == "fetch":
        L = P.load(a.paths)
        t0 = min(r["start_ms"] for r in L.rounds.values() if r.get("start_ms"))
        t1 = max(r["end_ms"] for r in L.rounds.values() if r.get("end_ms")) + 60_000
        chain = fetch(a.rpc, t0, t1)
        json.dump(chain, open(a.out, "w"))
        print("区块 %d~%d,OrderFilled %d 条 → %s" % (chain["from_block"], chain["to_block"], len(chain["logs"]), a.out))
    else:
        report(a.paths, a.chain, a.out, race_ms=a.race_ms)
    return 0


if __name__ == "__main__":
    sys.exit(main())
