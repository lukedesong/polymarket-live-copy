#!/usr/bin/env python3
"""工具 4:速度与排队(直接测量,不拟合任何参数)。

    python3 speed_queue.py <日志目录或文件 ...> [--out 目录]

速度
  - 下单 / 撤单往返:REST 请求发出到回包(resp.rtt_ms)
  - 盘口推送延迟:本机收到时刻 − 推送里的服务器时刻 updateTimestampMs(含两台机器的时钟差,只看分布形状与相对变化)
  - 改价一圈:发撤单 → 撤单确认 → 发新单 → 新单确认(同一侧、同一轮,撤后紧接着的那张新单)
  - 我们的单出现在盘口:下单发出后,第一条在该价位显示量 ≥ 挂单时前面的量 + 我们的股数 的推送
排队
  - 挂单时前面多少股(queue_ahead)
  - 首笔成交前最后一条推送里该价位的显示量 − 我们剩余的股数 = 成交时"看起来还排在我们前面"的量;
    与挂单时前面的量相比:前面的量基本没少就成交了 → 成交不是严格"排到我们才轮到"
  - 撤单在路上时被成交(撤单发出后才发现成交)的比例
输出 speed_orders.tsv(每单一行)与屏幕汇总。
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import probelog as P  # noqa: E402


def pct(xs, q):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    return xs[min(len(xs) - 1, int(q * (len(xs) - 1) + 0.5))]


def describe(xs):
    xs = [x for x in xs if x is not None]
    return {"n": len(xs), "p10": pct(xs, 0.1), "p50": pct(xs, 0.5), "p90": pct(xs, 0.9), "p99": pct(xs, 0.99)}


def load(paths):
    """逐行读一遍,收集速度与排队需要的东西"""
    place_rtt, cancel_rtt, push_lag = [], [], []
    levels = {}  # market_id -> [(wall_ms, {价: 量} 买, {价: 量} 卖)]
    dec = {}  # cid -> 决策行
    ack = {}  # cid -> (wall_ms, ok, order_id)
    req_place = []  # (sent_wall_ms, rtt_ms)
    cancels = {}  # order_id -> (决策 wall_ms, 原因)
    cancel_ack = {}  # order_id -> wall_ms
    wallet = []  # (本机收到 wall_ms, 钱包事件 data)
    for l in P.iter_lines(paths):
        k = l.get("kind")
        t = l.get("wall_ms")
        if k == "resp" and l.get("tag") in ("place", "cancel") and l.get("rtt_ms") is not None and l.get("status"):
            (place_rtt if l["tag"] == "place" else cancel_rtt).append(l["rtt_ms"])
            if l["tag"] == "place":
                req_place.append((l.get("sent_wall_ms"), l["rtt_ms"]))
        elif k == "pf":
            try:
                d = json.loads(l.get("raw", ""))
            except ValueError:
                continue
            topic = d.get("topic") or ""
            if topic.startswith("predictWalletEvents/"):
                wallet.append((t, d.get("data") or {}))
                continue
            if not topic.startswith("predictOrderbook/"):
                continue
            data = d.get("data") or {}
            m = int(data.get("marketId") or topic.split("/", 1)[1])
            if data.get("updateTimestampMs"):
                push_lag.append(t - data["updateTimestampMs"])
            b = {round(x[0], 2): x[1] for x in data.get("bids") or [] if len(x) >= 2}
            a = {round(x[0], 2): x[1] for x in data.get("asks") or [] if len(x) >= 2}
            levels.setdefault(m, []).append((t, b, a))
        elif k == "decision" and l.get("action") == "place":
            dec[(l.get("order") or {}).get("cid")] = l
        elif k == "decision" and l.get("action") == "cancel":
            cancels.setdefault(l.get("order_id"), (t, l.get("reason")))
        elif k == "ack":
            ack[l.get("cid")] = (t, bool(l.get("ok")), l.get("order_id"))
        elif k == "cancel_ack" and l.get("ok"):
            cancel_ack.setdefault(l.get("order_id"), t)
    return place_rtt, cancel_rtt, push_lag, levels, dec, ack, cancels, cancel_ack, wallet


def level_size(snap, qside, up_level):
    _, b, a = snap
    return (b if qside == "UpBid" else a).get(round(up_level, 2), 0.0)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("paths", nargs="+")
    ap.add_argument("--out", default=".")
    a = ap.parse_args(argv)
    L = P.load(a.paths)
    place_rtt, cancel_rtt, push_lag, levels, dec, ack, cancels, cancel_ack, wallet = load(a.paths)
    import bisect

    snaps_t = {m: [s[0] for s in v] for m, v in levels.items()}
    rows = []
    cycles = []
    by_side = {}
    for o in P.placed_orders(L):
        m, q, price, sh = o["market_id"], o["qside"], o["price"], o["shares"]
        up_level = price if q == "UpBid" else round(1 - price, 2)
        r = {"cid": o["cid"], "order_id": o["order_id"], "market_id": m, "qside": q, "price": price, "shares": sh, "queue_ahead": o["queue_ahead"],
             "decide_ms": o["decide_ms"], "ack_ms": o["ack_ms"]}
        snaps, ts = levels.get(m, []), snaps_t.get(m, [])
        # 我们的单出现在盘口
        r["visible_ms"] = None
        if o["queue_ahead"] is not None and ts:
            i = bisect.bisect_left(ts, o["decide_ms"])
            stop = o["end_ms"] or (o["decide_ms"] + 10_000)
            while i < len(ts) and ts[i] <= stop:
                if level_size(snaps[i], q, up_level) >= o["queue_ahead"] + sh - 1e-6:
                    r["visible_ms"] = ts[i] - o["decide_ms"]
                    break
                i += 1
        # 成交前最后一条推送:看起来还排在我们前面的量
        r["first_fill_ms"] = o["first_fill_ms"]
        r["ms_to_fill"] = (o["first_fill_ms"] - (o["ack_ms"] or o["decide_ms"])) if o["first_fill_ms"] else None
        r["shown_ahead_before_fill"] = None
        if o["first_fill_ms"] and ts:
            i = bisect.bisect_left(ts, o["first_fill_ms"]) - 1
            if i >= 0 and ts[i] >= (o["ack_ms"] or o["decide_ms"]):
                r["shown_ahead_before_fill"] = max(0.0, level_size(snaps[i], q, up_level) - sh)
        r["ahead_left_ratio"] = (r["shown_ahead_before_fill"] / o["queue_ahead"]) if (r["shown_ahead_before_fill"] is not None and o["queue_ahead"]) else None
        c = cancels.get(o["order_id"])
        r["cancel_reason"] = c[1] if c else None
        r["filled_after_cancel_sent"] = bool(c and o["first_fill_ms"] and o["first_fill_ms"] >= c[0])
        rows.append(r)
        by_side.setdefault((m, q), []).append(o)
    # 改价一圈:同一轮同一侧,撤单决策 → 撤单确认 → 下一张单决策 → 下一张单确认
    for (m, q), os_ in by_side.items():
        os_.sort(key=lambda o: o["decide_ms"])
        for prev, nxt in zip(os_, os_[1:]):
            c = cancels.get(prev["order_id"])
            ca = cancel_ack.get(prev["order_id"])
            if not c or not ca or not nxt["ack_ms"] or "改价" not in (c[1] or ""):
                continue
            if nxt["decide_ms"] < ca or nxt["decide_ms"] - ca > 2000:
                continue
            cycles.append({"cancel_rtt": ca - c[0], "gap": nxt["decide_ms"] - ca, "place_rtt": nxt["ack_ms"] - nxt["decide_ms"], "total": nxt["ack_ms"] - c[0]})
    P.write_tsv(os.path.join(a.out, "speed_orders.tsv"), ["cid", "order_id", "market_id", "qside", "price", "shares", "queue_ahead", "decide_ms", "ack_ms", "visible_ms",
                                                          "first_fill_ms", "ms_to_fill", "shown_ahead_before_fill", "ahead_left_ratio", "cancel_reason", "filled_after_cancel_sent"], rows)

    def show(name, d, unit="ms"):
        print("  %-34s n=%-5d p10 %-8s p50 %-8s p90 %-8s p99 %s %s" % (name, d["n"], P.short(d["p10"]), P.short(d["p50"]), P.short(d["p90"]), P.short(d["p99"]), unit))

    print("模式 %s;挂单 %d 张" % (L.mode, len(rows)))
    print("【速度】")
    show("下单往返(发出→回包)", describe(place_rtt))
    show("撤单往返(发出→回包)", describe(cancel_rtt))
    show("盘口推送延迟(收到−服务器时刻,含钟差)", describe(push_lag))
    show("改价一圈(发撤单→新单确认)", describe([c["total"] for c in cycles]))
    show("  其中撤单确认→发新单", describe([c["gap"] for c in cycles]))
    show("我们的单出现在盘口(决策→推送里看到)", describe([r["visible_ms"] for r in rows]))
    print("  看到自己单子出现在盘口推送里的比例 %s" % P.short(sum(1 for r in rows if r["visible_ms"] is not None) / len(rows) if rows else None))
    print("【排队】")
    show("挂单时前面多少股", describe([r["queue_ahead"] for r in rows]), "股")
    filled = [r for r in rows if r["first_fill_ms"]]
    show("挂上→首笔成交", describe([r["ms_to_fill"] for r in filled]))
    show("成交前推送里仍显示在我们前面的量", describe([r["shown_ahead_before_fill"] for r in filled]), "股")
    show("  ÷ 挂单时前面的量", describe([r["ahead_left_ratio"] for r in filled]), "")
    lr = [r["ahead_left_ratio"] for r in filled if r["ahead_left_ratio"] is not None]
    if lr:
        print("  成交时前面的量还剩一半以上的比例 %s(前面的量没怎么少就成交了 → 不是严格排到我们才成交)" % P.short(sum(1 for x in lr if x >= 0.5) / len(lr)))
    canc = [r for r in rows if r["cancel_reason"]]
    print("  发了撤单的单 %d 张,其中撤单发出后才成交的 %d 张(%s)" % (len(canc), sum(1 for r in canc if r["filled_after_cancel_sent"]),
                                                       P.short(sum(1 for r in canc if r["filled_after_cancel_sent"]) / len(canc) if canc else None)))
    platform_section(L, wallet, cancels, show)
    return 0


def platform_section(L, wallet, cancels, show):
    """钱包事件里的平台时间戳(timestamp,毫秒):接受 / 撮合(orderTransactionSubmitted)/ 上链成功或失败 / 撤销 / 拒绝"""
    if not wallet:
        print("【平台时间戳】日志里没有钱包事件(dry-run 或旧版日志)")
        return
    ev = {}  # order_id -> {type: [(平台 ts, 本机收到 wall, data)]}
    for recv, d in wallet:
        oid = str(d.get("orderId") or "")
        if oid:
            ev.setdefault(oid, {}).setdefault(d.get("type"), []).append((d.get("timestamp"), recv, d))
    orders = {o["order_id"]: o for o in L.orders.values() if o["order_id"]}
    accept_lag, push_lag, to_match, settle_lag, cxl_eff, in_flight, before_cxl = [], [], [], [], [], 0, 0
    offs = []
    for oid, e in ev.items():
        o = orders.get(oid)
        acc = (e.get("orderAccepted") or [None])[0]
        if o and acc and acc[0] and o["decide_ms"] and o["ack_ms"]:
            offs.append(acc[0] - (o["decide_ms"] + o["ack_ms"]) / 2.0)  # 平台钟 − 本机钟(假设往返对称)
        for typ in ("orderAccepted", "orderTransactionSubmitted", "orderCancelled"):
            for ts, recv, _ in e.get(typ, []):
                if ts:
                    push_lag.append(recv - ts)
    off = P.median(offs) or 0.0
    for oid, e in ev.items():
        o = orders.get(oid)
        acc = (e.get("orderAccepted") or [None])[0]
        if o and acc and acc[0]:
            accept_lag.append(acc[0] - off - o["decide_ms"])
        subs = sorted(ts for ts, _, _ in e.get("orderTransactionSubmitted", []) if ts)
        if acc and acc[0] and subs:
            to_match.append(subs[0] - acc[0])
        for (ts, _, d) in e.get("orderTransactionSuccess", []):
            if subs and ts:
                settle_lag.append(ts - subs[0])
        cx = (e.get("orderCancelled") or [None])[0]
        c = cancels.get(oid)
        if cx and cx[0] and c:
            sent = c[0] + off  # 撤单决策时刻换算成平台钟
            cxl_eff.append(cx[0] - sent)
            for m in subs:
                if sent <= m < cx[0]:
                    in_flight += 1
                elif m < sent:
                    before_cxl += 1
    n = {k: sum(len(v.get(k, [])) for v in ev.values()) for k in ("orderAccepted", "orderNotAccepted", "orderTransactionSubmitted", "orderTransactionSuccess", "orderTransactionFailed", "orderCancelled")}
    reasons = {}
    for v in ev.values():
        for _, _, d in v.get("orderNotAccepted", []):
            reasons[d.get("reason")] = reasons.get(d.get("reason"), 0) + 1
    print("【平台时间戳】(钱包事件 timestamp;平台钟 − 本机钟 估计 %+.0f ms,按往返对称估)" % off)
    print("  事件数:接受 %(orderAccepted)d、拒绝 %(orderNotAccepted)d、撮合 %(orderTransactionSubmitted)d、上链成功 %(orderTransactionSuccess)d、上链失败 %(orderTransactionFailed)d、撤销 %(orderCancelled)d" % n)
    print("  拒绝原因:%s" % reasons)
    show("下单决策 → 平台接受(换算到本机钟)", describe(accept_lag))
    show("平台事件 → 我们收到推送(含钟差)", describe(push_lag))
    show("平台接受 → 第一次撮合(同一平台钟)", describe(to_match))
    show("撮合 → 上链成功", describe(settle_lag))
    show("撤单决策 → 平台撤销生效", describe(cxl_eff))
    tot = in_flight + before_cxl
    print("  后来撤掉的单里,撮合发生在撤单发出之前 %d 次、撤单在路上时 %d 次(%s)" % (before_cxl, in_flight, P.short(in_flight / tot if tot else None)))


if __name__ == "__main__":
    sys.exit(main())
