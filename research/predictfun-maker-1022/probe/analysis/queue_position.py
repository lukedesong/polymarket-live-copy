#!/usr/bin/env python3
"""工具 2:排队位置。

    python3 queue_position.py <日志目录或文件 ...> [--out 目录] [--edges a,b] [--horizons 1,5,30]

挂单那一刻,我们这个价位前面已有多少股(探针决策日志 ctx.queue_ahead = 挂单时盘口该价位的显示量;
我们总是排在已有队伍后面,所以这就是我们前面的股数)。
分三组:队首 / 队中 / 队尾 = 前面股数最少 / 中间 / 最多的三分之一(按本批订单的三分位数切);
  --edges a,b 可改成固定股数切分:≤a 队首,(a,b] 队中,>b 队尾。
每组统计:挂单数、成交单数、成交率、成交股数比、首笔成交用时中位数、成交后 1/5/30 秒与结算的每股盈亏(两种参考价,按成交股数加权)。

输出 queue_orders.tsv(每单一行)、queue_groups.tsv(分组),屏幕打印分组表。
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import markouts as M  # noqa: E402
import probelog as P  # noqa: E402

BUCKETS = ("队首", "队中", "队尾", "未知")


def quantile(xs, q):
    xs = sorted(xs)
    if not xs:
        return None
    pos = (len(xs) - 1) * q
    lo = int(pos)
    hi = min(lo + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (pos - lo)


def edges_for(orders, edges=None):
    if edges:
        return edges
    xs = [o["queue_ahead"] for o in orders if o["queue_ahead"] is not None]
    return (quantile(xs, 1 / 3.0), quantile(xs, 2 / 3.0)) if xs else (None, None)


def bucket(ahead, edges):
    a, b = edges
    if ahead is None or a is None:
        return "未知"
    return "队首" if ahead <= a else ("队中" if ahead <= b else "队尾")


def order_rows(L, horizons=(1, 5, 30), edges=None):
    orders = P.placed_orders(L)
    e = edges_for(orders, edges)
    fm = {f["fill_no"]: f for f in M.fill_markouts(L, horizons)}
    rows = []
    for o in orders:
        r = {k: o[k] for k in ("cid", "order_id", "market_id", "qside", "price", "shares", "queue_ahead", "decide_ms", "ack_ms", "end", "filled", "first_fill_ms", "first_fill_source", "fill_after_cancel")}
        r["side"] = P.SIDE_LABEL.get(o["qside"], o["qside"])
        r["bucket"] = bucket(o["queue_ahead"], e)
        r["ahead_in_orders"] = (o["queue_ahead"] / o["shares"]) if (o["queue_ahead"] is not None and o["shares"]) else None
        r["filled_any"] = o["filled"] > 0
        r["fill_ratio"] = (o["filled"] / o["shares"]) if o["shares"] else None
        start = o["ack_ms"] or o["decide_ms"]
        r["ms_to_first_fill"] = (o["first_fill_ms"] - start) if o["first_fill_ms"] else None
        fs = [fm[f["fill_no"]] for f in o["fills"] if f["fill_no"] in fm]
        for c in M.horizon_cols(horizons):
            if c.startswith("pnl_"):
                r[c] = P.wmean([(f[c], f["shares"]) for f in fs])
        rows.append(r)
    return rows, e


def group(rows, horizons):
    out = []
    for side in ("全部", "买Up", "卖Up(买Down)"):
        for b in BUCKETS + ("全部",):
            rs = [r for r in rows if (side == "全部" or r["side"] == side) and (b == "全部" or r["bucket"] == b)]
            if not rs:
                continue
            filled = [r for r in rs if r["filled_any"]]
            g = {"side": side, "bucket": b, "orders": len(rs), "filled_orders": len(filled), "fill_rate": len(filled) / len(rs),
                 "shares_posted": sum(r["shares"] or 0 for r in rs), "shares_filled": sum(r["filled"] for r in rs),
                 "ahead_min": min((r["queue_ahead"] for r in rs if r["queue_ahead"] is not None), default=None),
                 "ahead_median": P.median([r["queue_ahead"] for r in rs]),
                 "ahead_max": max((r["queue_ahead"] for r in rs if r["queue_ahead"] is not None), default=None),
                 "ms_to_first_fill_median": P.median([r["ms_to_first_fill"] for r in filled])}
            g["share_fill_rate"] = g["shares_filled"] / g["shares_posted"] if g["shares_posted"] else None
            for c in M.horizon_cols(horizons):
                if c.startswith("pnl_"):
                    g[c] = P.wmean([(r[c], r["filled"]) for r in filled])
            out.append(g)
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("paths", nargs="+")
    ap.add_argument("--out", default=".")
    ap.add_argument("--edges", default="", help="固定切分点 a,b(股)")
    ap.add_argument("--horizons", default="1,5,30")
    a = ap.parse_args(argv)
    hs = tuple(float(x) for x in a.horizons.split(","))
    edges = tuple(float(x) for x in a.edges.split(",")) if a.edges else None
    L = P.load(a.paths)
    rows, e = order_rows(L, hs, edges)
    pnl_cols = [c for c in M.horizon_cols(hs) if c.startswith("pnl_")]
    P.write_tsv(os.path.join(a.out, "queue_orders.tsv"), ["cid", "order_id", "market_id", "side", "qside", "price", "shares", "queue_ahead", "ahead_in_orders", "bucket",
                                                          "end", "filled", "fill_ratio", "filled_any", "ms_to_first_fill", "first_fill_source", "fill_after_cancel"] + pnl_cols, rows)
    g = group(rows, hs)
    gcols = ["side", "bucket", "orders", "filled_orders", "fill_rate", "shares_posted", "shares_filled", "share_fill_rate", "ahead_min", "ahead_median", "ahead_max", "ms_to_first_fill_median"] + pnl_cols
    P.write_tsv(os.path.join(a.out, "queue_groups.tsv"), gcols, g)
    print("模式 %s;挂单 %d 张;切分点(前面股数) 队首 ≤ %s < 队中 ≤ %s < 队尾%s" % (L.mode, len(rows), P.short(e[0]), P.short(e[1]), "(固定)" if edges else "(三分位)"))
    print("%-14s %-4s %5s %5s %7s %7s %8s  %s" % ("方向", "分组", "挂单", "成交", "成交率", "前面中位", "用时ms", "  ".join(pnl_cols)))
    for x in g:
        print("%-14s %-4s %5d %5d %7s %7s %8s  %s" % (x["side"], x["bucket"], x["orders"], x["filled_orders"], P.short(x["fill_rate"]), P.short(x["ahead_median"]),
                                                    P.short(x["ms_to_first_fill_median"]), "  ".join(P.short(x[c]) for c in pnl_cols)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
