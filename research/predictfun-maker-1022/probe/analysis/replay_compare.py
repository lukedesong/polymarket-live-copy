#!/usr/bin/env python3
"""工具 3:实盘与回放对比。对同一批轮次、按我们实际的挂撤时间重放,逐笔对比成交率、成交时刻、成交价。

三步(可分开跑):
  python3 replay_compare.py export  <日志 ...> --out live_orders.tsv
      从探针日志导出每张单的实际时间线(回放器的输入)。
  python3 replay_compare.py replay  live_orders.tsv --model-dir <.../model> --out replay_fills.tsv [适配器参数]
      用回放器的成交规则对这些单重放,写出 replay_fills.tsv。
      规则来自 research/predictfun-maker-1022/model/mm_grid.py,适配器在 mm_grid_adapter.py。
      **mm_grid.py 目前不在仓库里,适配器尚未实现**(见 README)。也可以在别处跑回放器,按下面的格式写出 replay_fills.tsv。
  python3 replay_compare.py compare live_orders.tsv replay_fills.tsv --out 目录
      逐笔对比,输出偏差大小与方向。

live_orders.tsv 列:
  order_id  market_id  round_start_ms  round_end_ms  qside(UpBid/DownBid)  outcome(Up/Down)  price(我们的买价)
  up_level(在 Up 盘口上的价位:UpBid = price,DownBid = 1 − price)  book_side(bid/ask:UpBid 排在 Up 买盘,DownBid 排在 Up 卖盘)
  shares  queue_ahead  post_ms(下单回报 = 开始挂着)  cancel_sent_ms  remove_ms(实际不再挂着:撤单确认 / 全部成交 / 收盘)
  live_filled  live_first_fill_ms  live_fill_price  live_fill_source  end
replay_fills.tsv 列(每张单一行,没成交的可以不写):
  order_id  rp_filled_shares  rp_first_fill_ms  rp_fill_price

compare 的口径:
  成交率 = 有成交的单 / 全部单;偏差倍数 = 回放成交率 / 实盘成交率(< 1 = 回放低估成交)。
  成交时刻差 = 回放首笔 − 实盘首笔(两边都成交的单;> 0 = 回放成交得晚)。实盘时刻是探针"发现"成交的时刻,钱包事件最准。
  成交价差 = 回放 − 实盘(我们是买方,> 0 = 回放的价对我们更差)。
  按 方向、排队分组(三分位)、实盘成交来源 分开给。
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import probelog as P  # noqa: E402
import queue_position as Q  # noqa: E402

LIVE_COLS = ["order_id", "cid", "market_id", "round_start_ms", "round_end_ms", "qside", "outcome", "price", "up_level", "book_side", "shares", "queue_ahead",
             "post_ms", "cancel_sent_ms", "remove_ms", "live_filled", "live_first_fill_ms", "live_fill_price", "live_fill_source", "end"]
RP_COLS = ["order_id", "rp_filled_shares", "rp_first_fill_ms", "rp_fill_price"]


def export_rows(L):
    rows = []
    for o in P.placed_orders(L):
        rd = L.round_of(o["market_id"]) or {}
        end_ms = rd.get("end_ms")
        if o["end"] == "filled":
            remove = o["end_ms"]
        elif o["cancel_ack_ms"] is not None:
            remove = o["cancel_ack_ms"]
        else:
            remove = end_ms
        up = o["qside"] == "UpBid"
        rows.append({"order_id": o["order_id"], "cid": o["cid"], "market_id": o["market_id"], "round_start_ms": rd.get("start_ms"), "round_end_ms": end_ms,
                     "qside": o["qside"], "outcome": "Up" if up else "Down", "price": o["price"],
                     "up_level": o["price"] if up else (round(1 - o["price"], 2) if o["price"] is not None else None), "book_side": "bid" if up else "ask",
                     "shares": o["shares"], "queue_ahead": o["queue_ahead"], "post_ms": o["ack_ms"], "cancel_sent_ms": o["cancel_ms"], "remove_ms": remove,
                     "live_filled": o["filled"], "live_first_fill_ms": o["first_fill_ms"], "live_fill_price": o["price"] if o["filled"] > 0 else None,
                     "live_fill_source": o["first_fill_source"], "end": o["end"]})
    return rows


def _f(x):
    return float(x) if x not in (None, "") else None


def compare_rows(live, replay):
    rp = {r["order_id"]: r for r in replay}
    out = []
    for o in live:
        r = rp.get(o["order_id"], {})
        lf, rf = _f(o["live_filled"]) or 0.0, _f(r.get("rp_filled_shares")) or 0.0
        lt, rt = _f(o["live_first_fill_ms"]), _f(r.get("rp_first_fill_ms"))
        lp, rpp = _f(o["live_fill_price"]), _f(r.get("rp_fill_price"))
        c = dict(o)
        c.update({"rp_filled_shares": rf, "rp_first_fill_ms": rt, "rp_fill_price": rpp, "live_any": lf > 0, "rp_any": rf > 0})
        c["case"] = "both" if (lf > 0 and rf > 0) else ("live_only" if lf > 0 else ("replay_only" if rf > 0 else "neither"))
        c["time_diff_ms"] = (rt - lt) if (c["case"] == "both" and rt is not None and lt is not None) else None
        c["price_diff"] = (rpp - lp) if (c["case"] == "both" and rpp is not None and lp is not None) else None
        out.append(c)
    return out


def summarize(rows):
    def one(rs):
        n = len(rs)
        lv = sum(1 for r in rs if r["live_any"])
        rv = sum(1 for r in rs if r["rp_any"])
        ls = sum(_f(r["live_filled"]) or 0 for r in rs)
        rsh = sum(r["rp_filled_shares"] or 0 for r in rs)
        td = [r["time_diff_ms"] for r in rs if r["time_diff_ms"] is not None]
        pdiff = [r["price_diff"] for r in rs if r["price_diff"] is not None]
        return {"orders": n, "live_filled_orders": lv, "replay_filled_orders": rv, "live_fill_rate": lv / n if n else None, "replay_fill_rate": rv / n if n else None,
                "fill_rate_ratio": (rv / lv) if lv else None, "live_shares": ls, "replay_shares": rsh, "share_ratio": (rsh / ls) if ls else None,
                "both": sum(1 for r in rs if r["case"] == "both"), "live_only": sum(1 for r in rs if r["case"] == "live_only"),
                "replay_only": sum(1 for r in rs if r["case"] == "replay_only"), "neither": sum(1 for r in rs if r["case"] == "neither"),
                "time_diff_ms_median": P.median(td), "time_diff_ms_mean": P.mean(td), "replay_later": sum(1 for x in td if x > 0), "replay_earlier": sum(1 for x in td if x < 0),
                "price_diff_mean": P.mean(pdiff)}

    # 排队分组用实盘订单的三分位
    aheads = [_f(r["queue_ahead"]) for r in rows if _f(r["queue_ahead"]) is not None]
    edges = (Q.quantile(aheads, 1 / 3.0), Q.quantile(aheads, 2 / 3.0)) if aheads else (None, None)
    out = []
    groups = [("全部", "全部", lambda r: True)]
    for q, lab in (("UpBid", "买Up"), ("DownBid", "卖Up(买Down)")):
        groups.append(("方向", lab, lambda r, q=q: r["qside"] == q))
    for b in Q.BUCKETS:
        groups.append(("排队", b, lambda r, b=b: Q.bucket(_f(r["queue_ahead"]), edges) == b))
    for src in sorted({r["live_fill_source"] for r in rows if r["live_fill_source"]}):
        groups.append(("实盘成交来源", src, lambda r, s=src: r["live_fill_source"] == s))
    for dim, lab, pred in groups:
        rs = [r for r in rows if pred(r)]
        if rs:
            g = {"dim": dim, "group": lab}
            g.update(one(rs))
            out.append(g)
    return out, edges


def verdict(s):
    """一句话:偏差多大、偏向哪边"""
    parts = []
    if s["fill_rate_ratio"] is not None:
        r = s["fill_rate_ratio"]
        parts.append("回放成交率 %.1f%% vs 实盘 %.1f%%(回放 = 实盘 × %.2f,%s)" % (100 * s["replay_fill_rate"], 100 * s["live_fill_rate"], r,
                                                                              "回放低估成交" if r < 0.95 else ("回放高估成交" if r > 1.05 else "基本一致")))
    elif s["replay_filled_orders"]:
        parts.append("实盘无成交,回放有 %d 单成交(回放高估)" % s["replay_filled_orders"])
    if s["share_ratio"] is not None:
        parts.append("成交股数 回放/实盘 = %.2f" % s["share_ratio"])
    parts.append("逐单:两边都成交 %d、只有实盘 %d、只有回放 %d、都没成交 %d" % (s["both"], s["live_only"], s["replay_only"], s["neither"]))
    if s["time_diff_ms_median"] is not None:
        m = s["time_diff_ms_median"]
        parts.append("共同成交的单回放成交时刻中位 %s %.0fms(晚 %d / 早 %d)" % ("晚" if m > 0 else "早", abs(m), s["replay_later"], s["replay_earlier"]))
    if s["price_diff_mean"] is not None and abs(s["price_diff_mean"]) > 1e-9:
        parts.append("成交价 回放 − 实盘 平均 %+.4f(%s)" % (s["price_diff_mean"], "回放对我们更差" if s["price_diff_mean"] > 0 else "回放对我们更好"))
    return ";".join(parts)


def cmd_export(a):
    L = P.load(a.paths)
    rows = export_rows(L)
    P.write_tsv(a.out, LIVE_COLS, rows)
    print("导出 %d 张单 → %s" % (len(rows), a.out))


def cmd_replay(a):
    import mm_grid_adapter
    live = P.read_tsv(a.live)
    fills = mm_grid_adapter.replay_orders(live, a.model_dir, a.adapter_args)
    P.write_tsv(a.out, RP_COLS, fills)
    print("回放 %d 张单,成交 %d → %s" % (len(live), sum(1 for f in fills if _f(f.get("rp_filled_shares"))), a.out))


def cmd_compare(a):
    live, rp = P.read_tsv(a.live), P.read_tsv(a.replay)
    rows = compare_rows(live, rp)
    s, edges = summarize(rows)
    os.makedirs(a.out, exist_ok=True)
    P.write_tsv(os.path.join(a.out, "compare_orders.tsv"), LIVE_COLS + ["rp_filled_shares", "rp_first_fill_ms", "rp_fill_price", "case", "time_diff_ms", "price_diff"], rows)
    cols = ["dim", "group", "orders", "live_filled_orders", "replay_filled_orders", "live_fill_rate", "replay_fill_rate", "fill_rate_ratio", "live_shares", "replay_shares",
            "share_ratio", "both", "live_only", "replay_only", "neither", "time_diff_ms_median", "time_diff_ms_mean", "replay_later", "replay_earlier", "price_diff_mean"]
    P.write_tsv(os.path.join(a.out, "compare_summary.tsv"), cols, s)
    print("排队三分位切分点(前面股数):%s / %s" % (P.fmt(edges[0]), P.fmt(edges[1])))
    for g in s:
        print("[%s/%s] %s" % (g["dim"], g["group"], verdict(g)))


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("export")
    e.add_argument("paths", nargs="+")
    e.add_argument("--out", default="live_orders.tsv")
    r = sub.add_parser("replay")
    r.add_argument("live")
    r.add_argument("--model-dir", required=True)
    r.add_argument("--out", default="replay_fills.tsv")
    r.add_argument("--adapter-arg", dest="adapter_args", action="append", default=[], help="传给适配器的参数,可重复")
    c = sub.add_parser("compare")
    c.add_argument("live")
    c.add_argument("replay")
    c.add_argument("--out", default=".")
    a = ap.parse_args(argv)
    {"export": cmd_export, "replay": cmd_replay, "compare": cmd_compare}[a.cmd](a)
    return 0


if __name__ == "__main__":
    sys.exit(main())
