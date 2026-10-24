#!/usr/bin/env python3
"""探针日志 → 成交模型校准指标(只用标准库,东京机上直接跑)。

    python3 analyze_probe.py <日志目录或 .jsonl(.gz) 文件 ...> [--out 目录] [--markout-s 5]

输入:predict-mm-probe 写的 probe_*.jsonl.gz(README 第 6 节)。
输出(--out,默认当前目录):
  probe_orders.tsv   每张单一行:何时挂、挂在哪、前面排了多少、怎么结束、成交多少、成交后 N 秒走向
  probe_rounds.tsv   每轮汇总
  屏幕打印总汇总(和回放器逐笔对比用的四个指标):
    1. 成交率          成交的单 / 挂出的单;成交股数 / 挂出股数;按侧分开
    2. 排队位置        挂单时该价位已有的股数(queue_ahead)
    3. 成交后走向      成交后 N 秒 Predict 中间价 markout(每股,对我们有利为正)与币安中间价变动(bp,对我们有利为正)
    4. 撤单前被吃      发了撤单的单里,最终有成交的比例(含撤单在路上时被吃的)

口径说明:
  - 成交时间 = 探针**发现**成交的时间(钱包事件最快;REST 来源晚最多 poll 间隔),source 列写明来源。
  - UpBid = 在买一买 Up;DownBid = 在 1−卖一买 Down(等价于在卖一卖 Up)。
  - Predict markout:UpBid = mid(t+N) − 价;DownBid = (1 − mid(t+N)) − 价。mid = Up 买一卖一中点。
  - 币安:UpBid 取 BTC 涨为正;DownBid 取 BTC 跌为正。
"""
import argparse
import bisect
import collections
import glob
import gzip
import json
import os
import sys


def iter_lines(paths):
    files = []
    for p in paths:
        if os.path.isdir(p):
            files += sorted(glob.glob(os.path.join(p, "probe_*.jsonl.gz")) + glob.glob(os.path.join(p, "*.jsonl")))
        else:
            files.append(p)
    for f in files:
        op = gzip.open if f.endswith(".gz") else open
        with op(f, "rt", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except ValueError:
                    # 进程被杀时 gzip 末尾可能有半行
                    continue


class Series:
    """按时间排序的 (wall_ms, 值) 序列,支持"某时刻及之后的第一个值"和"某时刻之前最后一个值"。"""

    def __init__(self):
        self.t = []
        self.v = []

    def add(self, t, v):
        if self.t and t < self.t[-1]:
            i = bisect.bisect_right(self.t, t)
            self.t.insert(i, t)
            self.v.insert(i, v)
        else:
            self.t.append(t)
            self.v.append(v)

    def at_or_before(self, t):
        i = bisect.bisect_right(self.t, t) - 1
        return self.v[i] if i >= 0 else None

    def first_after(self, t):
        i = bisect.bisect_left(self.t, t)
        return self.v[i] if i < len(self.v) else None


def book_top(raw):
    """Predict 盘口原文 → (market_id, 买一, 卖一);不是盘口返回 None"""
    try:
        d = json.loads(raw)
    except ValueError:
        return None
    topic = d.get("topic") or ""
    if not topic.startswith("predictOrderbook/"):
        return None
    data = d.get("data") or {}
    mid = data.get("marketId") or int(topic.split("/", 1)[1])
    bids = [l[0] for l in data.get("bids") or [] if len(l) >= 2 and l[1] > 0]
    asks = [l[0] for l in data.get("asks") or [] if len(l) >= 2 and l[1] > 0]
    return int(mid), (max(bids) if bids else None), (min(asks) if asks else None)


def bn_mid(raw):
    try:
        d = json.loads(raw)
    except ValueError:
        return None
    x = d.get("data", d)
    if x.get("e", "bookTicker") != "bookTicker" or x.get("s") != "BTCUSDT":
        return None
    try:
        return (float(x["b"]) + float(x["a"])) / 2
    except (KeyError, ValueError, TypeError):
        return None


def new_order():
    return {
        "cid": None, "order_id": None, "hash": None, "market_id": None, "qside": None, "price": None, "shares": None,
        "decide_ms": None, "ack_ms": None, "ack_ok": None, "queue_ahead": None, "best_bid": None, "best_ask": None,
        "bn_mid_at_place": None, "ms_left": None,
        "cancel_reason": None, "cancel_ms": None, "cancel_ack_ms": None, "end": None, "end_ms": None,
        "filled": 0.0, "first_fill_ms": None, "first_fill_source": None, "fill_after_cancel": False,
    }


def analyze(paths, markout_s=5.0):
    orders = {}  # cid -> rec
    by_oid = {}  # order_id -> cid
    bn = Series()
    books = collections.defaultdict(Series)  # market_id -> (bid, ask)
    rounds = {}
    settles = {}
    mode = None
    last_ms = 0
    for l in iter_lines(paths):
        k = l.get("kind")
        t = l.get("wall_ms")
        if isinstance(t, (int, float)):
            last_ms = max(last_ms, t)
        if k == "bn":
            m = bn_mid(l.get("raw", ""))
            if m is not None:
                bn.add(t, m)
        elif k == "pf":
            b = book_top(l.get("raw", ""))
            if b:
                books[b[0]].add(t, (b[1], b[2]))
        elif k == "pf_rest":
            bids = [x[0] for x in l.get("bids") or []]
            asks = [x[0] for x in l.get("asks") or []]
            books[int(l["market_id"])].add(t, (max(bids) if bids else None, min(asks) if asks else None))
        elif k == "start":
            mode = l.get("mode")
        elif k == "decision" and l.get("action") == "place":
            o = l.get("order") or {}
            cid = o.get("cid")
            r = orders.setdefault(cid, new_order())
            ctx = l.get("ctx") or {}
            r.update(cid=cid, hash=o.get("hash"), market_id=o.get("market_id"), qside=o.get("qside"), price=o.get("price"), shares=o.get("shares"),
                     decide_ms=t, queue_ahead=ctx.get("queue_ahead"), best_bid=ctx.get("best_bid"), best_ask=ctx.get("best_ask"),
                     bn_mid_at_place=ctx.get("bn_mid"), ms_left=ctx.get("ms_left"))
        elif k == "ack":
            r = orders.setdefault(l.get("cid"), new_order())
            r["ack_ms"], r["ack_ok"] = t, bool(l.get("ok"))
            if l.get("order_id"):
                r["order_id"] = l["order_id"]
                by_oid[l["order_id"]] = l.get("cid")
            if not l.get("ok"):
                r["end"], r["end_ms"] = "rejected", t
        elif k == "decision" and l.get("action") == "cancel":
            cid = by_oid.get(l.get("order_id"), l.get("cid"))
            r = orders.get(cid)
            if r and r["cancel_ms"] is None:
                r["cancel_ms"], r["cancel_reason"] = t, l.get("reason")
        elif k == "decision" and l.get("action") == "cancel_all":
            for oid in l.get("known") or []:
                r = orders.get(by_oid.get(oid))
                if r and r["cancel_ms"] is None:
                    r["cancel_ms"], r["cancel_reason"] = t, "全撤: " + str(l.get("reason"))
        elif k == "cancel_ack" and l.get("ok"):
            r = orders.get(by_oid.get(l.get("order_id")))
            if r and r["cancel_ack_ms"] is None:
                r["cancel_ack_ms"] = t
        elif k == "cancel_all_done":
            for oid in l.get("ok") or []:
                r = orders.get(by_oid.get(oid))
                if r and r["cancel_ack_ms"] is None:
                    r["cancel_ack_ms"] = t
        elif k == "fill":
            oid = l.get("order_id")
            cid = by_oid.get(oid) or (l.get("order") or {}).get("cid")
            r = orders.get(cid)
            if r is None:
                continue
            r["filled"] = max(r["filled"], float(l.get("cum_shares") or 0))
            if r["first_fill_ms"] is None:
                r["first_fill_ms"], r["first_fill_source"] = t, l.get("source")
            if r["cancel_ms"] is not None:
                r["fill_after_cancel"] = True
        elif k == "round_end":
            rd = l.get("round") or {}
            rounds[rd.get("market_id")] = {"round": rd, "position": l.get("position")}
        elif k == "settle":
            settles[l.get("market_id")] = l

    ms = markout_s * 1000
    rows = []
    for cid in sorted(x for x in orders if x is not None):
        r = orders[cid]
        if r["decide_ms"] is None:
            continue
        if r["end"] is None:
            if r["shares"] and r["filled"] >= r["shares"] - 1e-9:
                r["end"], r["end_ms"] = "filled", r["first_fill_ms"]
            elif r["cancel_ack_ms"] is not None:
                r["end"], r["end_ms"] = ("cancelled_partial" if r["filled"] > 0 else "cancelled"), r["cancel_ack_ms"]
            else:
                r["end"] = "unknown"
        start = r["ack_ms"] or r["decide_ms"]
        stop = r["end_ms"] or r["cancel_ms"]
        r["rest_ms"] = (stop - start) if (stop and start) else None
        # 成交后走向
        for key in ("mid_at_fill", "mid_after", "markout", "bn_at_fill", "bn_after", "bn_move_bp"):
            r[key] = None
        if r["first_fill_ms"] is not None and r["market_id"] is not None:
            ser = books.get(r["market_id"])
            tf = r["first_fill_ms"]

            def mid(v):
                return (v[0] + v[1]) / 2 if v and v[0] is not None and v[1] is not None else None

            # 日志要覆盖到 t+N 才算(盘口这段时间没更新也有效:取 t+N 时刻的最新盘口)
            covered = last_ms >= tf + ms
            if ser:
                r["mid_at_fill"] = mid(ser.at_or_before(tf))
                if covered:
                    r["mid_after"] = mid(ser.at_or_before(tf + ms))
            if r["mid_after"] is not None and r["price"] is not None:
                r["markout"] = (r["mid_after"] - r["price"]) if r["qside"] == "UpBid" else ((1 - r["mid_after"]) - r["price"])
            b0, b1 = bn.at_or_before(tf), bn.at_or_before(tf + ms)
            if b0 and b1 and covered:
                r["bn_at_fill"], r["bn_after"] = b0, b1
                mv = (b1 - b0) / b0 * 1e4
                r["bn_move_bp"] = mv if r["qside"] == "UpBid" else -mv
        rows.append(r)
    return {"mode": mode, "orders": rows, "rounds": rounds, "settles": settles}


def _mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def _median(xs):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    n = len(xs)
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2


def summarize(rows):
    out = {}
    for side in ("UpBid", "DownBid", "ALL"):
        rs = [r for r in rows if (side == "ALL" or r["qside"] == side) and r["ack_ok"]]
        filled = [r for r in rs if r["filled"] > 0]
        cancelled = [r for r in rs if r["cancel_ms"] is not None]
        out[side] = {
            "orders": len(rs),
            "filled_orders": len(filled),
            "fill_rate": len(filled) / len(rs) if rs else None,
            "shares_posted": sum(r["shares"] or 0 for r in rs),
            "shares_filled": sum(r["filled"] for r in rs),
            "queue_ahead_median": _median([r["queue_ahead"] for r in rs]),
            "queue_ahead_mean": _mean([r["queue_ahead"] for r in rs]),
            "rest_ms_median": _median([r["rest_ms"] for r in rs]),
            "markout_mean": _mean([r["markout"] for r in filled]),
            "bn_move_bp_mean": _mean([r["bn_move_bp"] for r in filled]),
            "cancelled_orders": len(cancelled),
            "filled_despite_cancel": sum(1 for r in cancelled if r["fill_after_cancel"]),
            "filled_despite_cancel_rate": (sum(1 for r in cancelled if r["fill_after_cancel"]) / len(cancelled)) if cancelled else None,
            "fill_sources": dict(collections.Counter(r["first_fill_source"] for r in filled)),
        }
    return out


COLS = ["cid", "order_id", "market_id", "qside", "price", "shares", "decide_ms", "ack_ms", "ms_left", "best_bid", "best_ask", "queue_ahead", "bn_mid_at_place",
        "cancel_ms", "cancel_ack_ms", "cancel_reason", "end", "rest_ms", "filled", "first_fill_ms", "first_fill_source", "fill_after_cancel",
        "mid_at_fill", "mid_after", "markout", "bn_at_fill", "bn_after", "bn_move_bp", "hash"]


def fmt(v):
    if v is None:
        return ""
    if isinstance(v, float):
        return "%.6g" % v
    return str(v).replace("\t", " ").replace("\n", " ")


def write_tsv(path, cols, rows):
    with open(path, "w", encoding="utf-8") as f:
        f.write("\t".join(cols) + "\n")
        for r in rows:
            f.write("\t".join(fmt(r.get(c)) for c in cols) + "\n")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("paths", nargs="+")
    ap.add_argument("--out", default=".")
    ap.add_argument("--markout-s", type=float, default=5.0)
    a = ap.parse_args(argv)
    res = analyze(a.paths, a.markout_s)
    rows = res["orders"]
    os.makedirs(a.out, exist_ok=True)
    write_tsv(os.path.join(a.out, "probe_orders.tsv"), COLS, rows)
    rrows = []
    for mid, rd in sorted(res["rounds"].items(), key=lambda x: (x[1]["round"].get("start_ms") or 0)):
        rs = [r for r in rows if r["market_id"] == mid and r["ack_ok"]]
        pos = rd.get("position") or {}
        st = res["settles"].get(mid) or {}
        rrows.append({"market_id": mid, "start_ms": rd["round"].get("start_ms"), "orders": len(rs), "filled_orders": sum(1 for r in rs if r["filled"] > 0),
                      "up_shares": pos.get("up_shares"), "down_shares": pos.get("down_shares"), "cost_usd": pos.get("cost_usd"),
                      "payout_up": st.get("payout_up"), "payout_down": st.get("payout_down"), "pnl": st.get("pnl")})
    write_tsv(os.path.join(a.out, "probe_rounds.tsv"), ["market_id", "start_ms", "orders", "filled_orders", "up_shares", "down_shares", "cost_usd", "payout_up", "payout_down", "pnl"], rrows)
    s = summarize(rows)
    print("模式: %s  轮数: %d  已结算: %d" % (res["mode"], len(res["rounds"]), len(res["settles"])))
    for side, x in s.items():
        print("[%s] 挂单 %d  成交单 %d  成交率 %s  股数 %s/%s  排队中位 %s  挂单时长中位 %sms  %.0fs markout 均值 %s  币安 %s bp  撤单 %d 其中仍被吃 %d (%s)  来源 %s" % (
            side, x["orders"], x["filled_orders"], fmt(x["fill_rate"]), fmt(x["shares_filled"]), fmt(x["shares_posted"]), fmt(x["queue_ahead_median"]),
            fmt(x["rest_ms_median"]), a.markout_s, fmt(x["markout_mean"]), fmt(x["bn_move_bp_mean"]), x["cancelled_orders"], x["filled_despite_cancel"],
            fmt(x["filled_despite_cancel_rate"]), x["fill_sources"]))
    print("写出 %s 与 %s" % (os.path.join(a.out, "probe_orders.tsv"), os.path.join(a.out, "probe_rounds.tsv")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
