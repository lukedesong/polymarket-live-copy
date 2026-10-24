#!/usr/bin/env python3
"""工具 6:回放成交模型(盘口 + 链上挂单成交),并用实盘真挂过的单校准。

    python3 fill_model.py validate <日志目录或文件 ...> --chain chain.json [--out 目录]

模型(一张虚拟挂单,价位 L,股数 S,挂上时刻 t0,撤掉时刻 t1):
  - 挂上时前面排着的量 = t0 时盘口 L 档的量
  - 之后链上 L 这一队(买 Up@p ≡ 卖 Down@1−p,见 maker_rank.py)每有一笔挂单方成交(撮合时刻 = 区块时间 − 实测延迟),
    先吃掉我们前面的量,吃穿了才轮到我们(平台先挂先成,maker_rank 已核对)
  - 盘口 L 档变多 = 有人排到我们后面;变少且不是成交造成的 = 有人撤单。撤的是前面还是后面看不到,三种假设都算:
      pess  全算在后面(前面的量只因成交减少)
      mixed 先算在后面,后面撤完了剩下的一半算在前面(旧回放器 mm_grid 的 mixed 口径)
      opt   全算在前面
校准:对实盘每张真挂过的单,把它自己的链上成交拿掉(回放里没有我们),按上面的规则预测它会不会成交、何时成交,
再和链上真实结果逐单比对。盘口里有我们自己这张单的量,按它真实的剩余股数扣掉。
输出 fill_model_orders.tsv(逐单)与屏幕汇总。
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import maker_rank as MR  # noqa: E402
import probelog as P  # noqa: E402

MODES = ("pess", "mixed", "opt")
CANCEL_EFFECT_MS = 15   # 撤单决定到生效(10-24 实盘:撤单发出到平台生效约 11ms,往返 29ms)
VISIBLE_MS = 70         # 我们的单出现在 / 消失于盘口推送的延迟(实测推送延迟约 66ms)


def simulate(shares, t0, t1, ahead0, events, mode):
    """events:按时间排好的 [(t, "fill", q) | (t, "book", 该档量(已扣掉被测单自己))]。返回 [(t, q)] 我们的成交"""
    ahead, behind, rem = ahead0, 0.0, float(shares)
    prev, fsince, out = None, 0.0, []
    for t, kind, x in events:
        if t < t0 or t > t1 or rem <= 1e-9:
            if kind == "book" and t < t0:
                prev = x
            continue
        if kind == "fill":
            fsince += x
            take = min(x, ahead)
            ahead -= take
            rest = x - take
            if rest > 1e-9:
                q = min(rest, rem)
                rem -= q
                out.append((t, q))
            continue
        if prev is not None:
            d = x - prev
            if d > 0:
                behind += d
            elif d < 0:
                c = max(0.0, -d - fsince)  # 撤单量 = 减少量 − 这段时间的成交
                if mode == "pess":
                    behind = max(0.0, behind - c)
                elif mode == "mixed":
                    cb = min(behind, c)
                    behind -= cb
                    ahead = max(0.0, ahead - 0.5 * (c - cb))
                else:
                    ca = min(ahead, c)
                    ahead -= ca
                    behind = max(0.0, behind - (c - ca))
            ahead = min(ahead, max(0.0, x))  # 前面的量不会超过盘口上这一档的量
        prev, fsince = x, 0.0
    return out


def pending_rows(paths, markets=None):
    """盘口推送里 settlementsPending 的新增 → 成交事件(和 maker_rows 同样的字段,没有挂单 hash)。
    asks 侧 Up 价 p 增加 = 卖一侧被吃 = 队 DownBid(1−p);bids 侧 = 队 UpBid p。股数 = 金额 ÷ Up 价。
    同一次推送同价两侧等量 = 同一笔记了两次,只留一条(旧回放器 pending_fills.py 的口径)"""
    out = []
    prev = {}
    for l in P.iter_lines(paths):
        if l.get("kind") != "pf":
            continue
        try:
            d = MR.json.loads(l.get("raw", ""))
        except ValueError:
            continue
        topic = d.get("topic") or ""
        if not topic.startswith("predictOrderbook/"):
            continue
        data = d.get("data") or {}
        m = int(data.get("marketId") or topic.split("/", 1)[1])
        if markets is not None and m not in markets:
            continue
        sp = data.get("settlementsPending") or {}
        cur = {}
        for sd in ("asks", "bids"):
            for x in sp.get(sd) or []:
                k = (sd, int(round(float(x[0]) * 100)))
                cur[k] = cur.get(k, 0.0) + float(x[1])
        seen = set()
        for (sd, c), v in cur.items():
            inc = v - prev.get(m, {}).get((sd, c), 0.0)
            if inc <= 1e-6 or c <= 0:
                continue
            key = (c, round(inc, 4))
            if key in seen:
                continue
            seen.add(key)
            qside, level = ("DownBid", 100 - c) if sd == "asks" else ("UpBid", c)
            out.append({"market_id": m, "qside": qside, "level": level, "shares": inc / (c / 100.0), "t": l["wall_ms"], "hash": None, "maker": None})
        prev[m] = cur
    out.sort(key=lambda r: r["t"])
    return out


def order_window(o, by_side):
    """虚拟单的存活区间:挂上(回报)→ 撤单生效;整单成交没撤的,存活到这一侧下一次换价(策略本来会在那时撤)"""
    t0 = o["ack_ms"]
    if o["cancel_ms"] is not None:
        return t0, o["cancel_ms"] + CANCEL_EFFECT_MS
    nxt = [x["decide_ms"] for x in by_side.get((o["market_id"], o["qside"]), []) if x["decide_ms"] > o["decide_ms"] and x["price"] != o["price"]]
    return t0, (min(nxt) if nxt else (o.get("round_end") or t0 + 300_000))


def validate(paths, chain_path, out, lag_ms=None, source="chain"):
    L = P.load(paths)
    tokens = {}
    for m, l in L.markets.items():
        tokens[str(l.get("up_token"))] = (m, "Up")
        tokens[str(l.get("down_token"))] = (m, "Down")
    import json
    rows = MR.maker_rows(json.load(open(chain_path)), tokens)
    matched = {}
    books = MR.load_books(paths, matched)
    lag = lag_ms if lag_ms is not None else (MR.chain_lag(rows, matched) or MR.DEFAULT_LAG_MS)
    for x in rows:
        x["t"] = (x["ms"] or 0) - lag
    pend = pending_rows(paths, set(L.markets)) if source == "pending" else None
    orders = [o for o in L.orders.values() if o["ack_ok"] and o["hash"] and o["decide_ms"] is not None and o["ack_ms"] is not None]
    for o in orders:
        rd = L.rounds.get(o["market_id"]) or {}
        o["round_end"] = rd.get("end_ms")
    by_side = {}
    for o in orders:
        by_side.setdefault((o["market_id"], o["qside"]), []).append(o)
    by_level = {}
    for x in rows:
        by_level.setdefault((x["market_id"], x["qside"], x["level"]), []).append(x)
    real = {}
    for x in rows:
        real.setdefault(x["hash"], []).append((x["t"], x["shares"]))
    out_rows = []
    for o in sorted(orders, key=lambda o: o["decide_ms"]):
        h = o["hash"].lower()
        lv = int(round(o["price"] * 100))
        t0, t1 = order_window(o, by_side)
        series = books.get(o["market_id"]) or []
        rf = sorted(real.get(h, []))
        # 被测单自己在盘口里的量:按真实剩余扣掉(出现 / 消失都晚 VISIBLE_MS)
        vis0 = t0 + VISIBLE_MS
        vis1 = (o["cancel_ms"] + CANCEL_EFFECT_MS + VISIBLE_MS) if o["cancel_ms"] is not None else 10 ** 15

        def own_rem(t):
            if t < vis0 or t >= vis1:
                return 0.0
            return max(0.0, o["shares"] - sum(q for (tt, q) in rf if tt + VISIBLE_MS <= t))
        if pend is None:
            ev = [(x["t"], "fill", x["shares"]) for x in by_level.get((o["market_id"], o["qside"], lv), []) if x["hash"] != h]
        else:
            # 推送里的成交量含被测单自己:按它每笔真实成交,从之后 1.5 秒内最近的一条同价位成交里扣掉
            pl = [[x["t"], x["shares"]] for x in pend if x["market_id"] == o["market_id"] and x["qside"] == o["qside"] and x["level"] == lv]
            for tt, q in rf:
                for e in sorted((e for e in pl if tt - 500 <= e[0] <= tt + 1500 and e[1] > 1e-9), key=lambda e: abs(e[0] - tt)):
                    k = min(q, e[1])
                    e[1] -= k
                    q -= k
                    if q <= 1e-9:
                        break
            ev = [(t, "fill", q) for t, q in pl if q > 1e-9]
        ev += [(s[0], "book", max(0.0, MR.level_size(s, o["qside"], lv) - own_rem(s[0]))) for s in series if t0 - 5000 <= s[0] <= t1]
        ev.sort(key=lambda e: (e[0], 0 if e[1] == "book" else 1))
        snap = MR.snap_at(series, t0)
        ahead0 = MR.level_size(snap, o["qside"], lv) if snap else 0.0
        r = {"cid": o["cid"], "market_id": o["market_id"], "qside": o["qside"], "price": o["price"], "shares": o["shares"],
             "t0": t0, "t1": t1, "life_ms": t1 - t0, "ahead0": round(ahead0, 4),
             "real_filled": round(sum(q for t, q in rf if t <= t1 + 2000), 6), "real_first": rf[0][0] if rf else None}
        for m in MODES:
            pf = simulate(o["shares"], t0, t1, ahead0, ev, m)
            r[m + "_filled"] = round(sum(q for _, q in pf), 6)
            r[m + "_first"] = pf[0][0] if pf else None
        out_rows.append(r)
    os.makedirs(out, exist_ok=True)
    cols = list(out_rows[0].keys()) if out_rows else []
    P.write_tsv(os.path.join(out, "fill_model_orders.tsv"), cols, out_rows)
    summary = summarize(out_rows)
    print("校准(成交来源 %s):实盘真挂过的单 %d 张(把它自己的成交拿掉再预测);区块时间 − 撮合 = %d 毫秒" % (source, len(out_rows), lag))
    print("实盘:有成交 %d 张(%.1f%%),成交 %.1f 股 / 挂 %d 股" % (summary["real_n"], 100 * summary["real_rate"], summary["real_sh"], summary["total_sh"]))
    for m in MODES:
        s = summary[m]
        print("%-5s 预测有成交 %d 张(%.1f%%,倍数 %.2f),股数倍数 %.2f;逐单:都成交 %d、只有实盘 %d、只有预测 %d、都没 %d;都成交的首笔时刻差 p50 %s ms"
              % (m, s["n"], 100 * s["rate"], s["ratio"], s["sh_ratio"], s["both"], s["real_only"], s["pred_only"], s["neither"], P.short(s["dt_p50"])))
    print("写出 %s/fill_model_orders.tsv" % out)
    return out_rows, summary


def summarize(rows):
    n = len(rows) or 1
    real_n = sum(1 for r in rows if r["real_filled"] > 1e-9)
    real_sh = sum(r["real_filled"] for r in rows)
    out = {"real_n": real_n, "real_rate": real_n / n, "real_sh": real_sh, "total_sh": sum(r["shares"] for r in rows)}
    for m in MODES:
        pn = sum(1 for r in rows if r[m + "_filled"] > 1e-9)
        psh = sum(r[m + "_filled"] for r in rows)
        both = [r for r in rows if r["real_filled"] > 1e-9 and r[m + "_filled"] > 1e-9]
        out[m] = {"n": pn, "rate": pn / n, "ratio": pn / real_n if real_n else None, "sh_ratio": psh / real_sh if real_sh else None,
                  "both": len(both), "real_only": real_n - len(both), "pred_only": pn - len(both),
                  "neither": len(rows) - real_n - pn + len(both),
                  "dt_p50": P.median([r[m + "_first"] - r["real_first"] for r in both])}
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    v = sub.add_parser("validate")
    v.add_argument("paths", nargs="+")
    v.add_argument("--chain", required=True)
    v.add_argument("--out", default="analysis_out")
    v.add_argument("--lag-ms", type=int, default=None)
    v.add_argument("--source", choices=["chain", "pending"], default="chain", help="成交来源:链上挂单成交,或盘口推送里的待结算增量")
    a = ap.parse_args(argv)
    validate(a.paths, a.chain, a.out, a.lag_ms, a.source)
    return 0


if __name__ == "__main__":
    sys.exit(main())
