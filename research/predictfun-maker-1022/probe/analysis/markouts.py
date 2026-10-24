#!/usr/bin/env python3
"""工具 1:成交后价格走向(markout)。

    python3 markouts.py <日志目录或文件 ...> [--out 目录] [--horizons 1,5,30] [--sources wallet,rest_list,...]

每笔成交(按成交增量,一次部分成交算一笔)在成交后 1、5、30 秒和结算时,我们的盈亏各是多少:
  每股盈亏 = 我们那一边在 t+h 的价值 − 成交价;总盈亏 = 每股盈亏 × 股数。
  参考价两种都算:predict = Predict 盘口中间价;fair = Φ(ln(S/K)/(σ√τ))(见 probelog.py)。
  t+h 过了收盘:predict 用结算兑付;fair 用币安收盘价对 K。结算列用平台结算结果(赢 1 / 输 0 / 平局 0.5)。
成交时间 = 探针发现成交的时间;钱包事件最准,REST 来源晚最多一个查单间隔(source 列)。

输出:
  markout_fills.tsv    每笔成交一行(各时点、两种参考价的每股盈亏)
  markout_groups.tsv   按 方向 × 剩余时间 分组:笔数、股数、每股盈亏(按股数加权)、每笔均值与 t 值(按笔数,不加权)、总盈亏
  屏幕:分组表
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import probelog as P  # noqa: E402

REFS = ("predict", "fair")


def horizon_cols(horizons):
    cols = []
    for ref in REFS:
        cols.append("%s_at_fill" % ref)
        for h in horizons:
            cols.append("pnl_%s_%gs" % (ref, h))
    cols.append("pnl_settle")
    return cols


def fill_markouts(L, horizons=(1, 5, 30), sources=None):
    rows = []
    for f in L.fills:
        if sources and f["source"] not in sources:
            continue
        m, q, t, p = f["market_id"], f["qside"], f["wall_ms"], f["price"]
        rd = L.round_of(m)
        r = dict(f)
        r["side"] = P.SIDE_LABEL.get(q, q)
        r["ms_left"] = (rd["end_ms"] - t) if rd else None
        r["ttl"] = P.ttl_bucket(r["ms_left"])
        for ref in REFS:
            v0 = L.token_value(ref, m, t, q)
            r["%s_at_fill" % ref] = v0
            for h in horizons:
                v = L.token_value(ref, m, t + int(h * 1000), q)
                r["pnl_%s_%gs" % (ref, h)] = (v - p) if v is not None else None
        sv = L.settle_value(m, q)
        r["pnl_settle"] = (sv - p) if sv is not None else None
        rows.append(r)
    return rows


def group(rows, horizons, keys=("side", "ttl")):
    """按 keys 分组;每个 (分组, 参考价, 时点) 一行"""
    out = []
    buckets = {}
    for r in rows:
        buckets.setdefault(tuple(r[k] for k in keys), []).append(r)
        buckets.setdefault(tuple("全部" for _ in keys), []).append(r)
        if len(keys) > 1:
            buckets.setdefault((r[keys[0]],) + tuple("全部" for _ in keys[1:]), []).append(r)
    ttl_rank = {b: i for i, b in enumerate(P.TTL_ORDER + ["全部"])}

    def sort_key(k):
        return tuple(ttl_rank.get(x, -1) if i > 0 else (1 if x == "全部" else 0, x) for i, x in enumerate(k))

    for k in sorted(buckets, key=sort_key):
        rs = buckets[k]
        cols = [("%s_%gs" % (ref, h), "pnl_%s_%gs" % (ref, h)) for ref in REFS for h in horizons] + [("settle", "pnl_settle")]
        for name, c in cols:
            vals = [(r[c], r["shares"]) for r in rs if r[c] is not None]
            if not vals:
                continue
            g = dict(zip(keys, k))
            g.update({"horizon": name, "fills": len(vals), "shares": sum(w for _, w in vals), "pnl_per_share": P.wmean(vals),
                      "pnl_per_fill": P.mean([v for v, _ in vals]), "t": P.tstat([v for v, _ in vals]), "pnl_total": sum(v * w for v, w in vals),
                      "fills_in_group": len(rs)})
            out.append(g)
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("paths", nargs="+")
    ap.add_argument("--out", default=".")
    ap.add_argument("--horizons", default="1,5,30")
    ap.add_argument("--sources", default="", help="只用这些来源的成交,逗号分隔(默认全部)")
    a = ap.parse_args(argv)
    hs = tuple(float(x) for x in a.horizons.split(","))
    L = P.load(a.paths)
    rows = fill_markouts(L, hs, set(a.sources.split(",")) if a.sources else None)
    cols = ["fill_no", "cid", "order_id", "market_id", "side", "qside", "price", "shares", "wall_ms", "ms_left", "ttl", "source", "after_cancel"] + horizon_cols(hs)
    P.write_tsv(os.path.join(a.out, "markout_fills.tsv"), cols, rows)
    g = group(rows, hs)
    P.write_tsv(os.path.join(a.out, "markout_groups.tsv"), ["side", "ttl", "horizon", "fills", "shares", "pnl_per_share", "pnl_per_fill", "t", "pnl_total", "fills_in_group"], g)
    print("模式 %s;成交 %d 笔;已结算轮 %d" % (L.mode, len(rows), len(L.settles)))
    print("每股盈亏 = 按股数加权;每笔均值与 t 值按笔数(不加权)")
    print("%-14s %-9s %-12s %5s %7s %10s %10s %7s %9s" % ("方向", "剩余", "参考/时点", "笔数", "股数", "每股盈亏", "每笔均值", "t", "总盈亏"))
    for x in g:
        print("%-14s %-9s %-12s %5d %7s %10s %10s %7s %9s" % (x["side"], x["ttl"], x["horizon"], x["fills"], P.short(x["shares"]), P.short(x["pnl_per_share"]),
                                                          P.short(x["pnl_per_fill"]), P.short(x["t"]), P.short(x["pnl_total"])))
    print("写出 %s、%s" % (os.path.join(a.out, "markout_fills.tsv"), os.path.join(a.out, "markout_groups.tsv")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
