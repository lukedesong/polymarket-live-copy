# 策略族网格回放(设计 v3.1 回放器 + 参数化):一次加载数据,多进程跑 (策略 × 轮)
# 策略参数:off(挂在最优价外几档) adv(币安不利撤单阈值 bp) behind(落后几档才改价) cap(净库存上限股) stop(剩余秒数 < stop 不挂)
#          flat(剩余 flat 秒时把净库存按当时对手价 + 吃单费平掉;0 = 不平) size(每档股数)
#          skew(库存偏移:每 10 股净库存,加仓那一侧往后退几档) edge(护栏:买价不高于 中间价−edge 档、卖价不低于 中间价+edge 档;0 = 不用,spork30 式)
#          tmax(剩余秒数 > tmax 时不挂,只做后段)。返佣按 0/20/25/40% 各报一次(审核:实测按钱包 19~40%)
# 用法:python3 mm_grid.py <bookx> <mk 目录> <fills_pending.parquet> <起ms> <止ms> <grid.json> [撤单 ms=25] [排队 mixed]
import sys, glob, json, itertools, numpy as np, pandas as pd
import multiprocessing as mp                     # macOS 默认 spawn 会在子进程重跑整个脚本;用 fork 共享已加载的数据
sys.path.insert(0, "/Users/luke/Documents/polymarket/research/lib")
from sanity import Run
BX, R, PF, W0, W1, GRID = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]), int(sys.argv[5]), sys.argv[6]
CXL = int(sys.argv[7]) if len(sys.argv) > 7 else 25; QMODE = sys.argv[8] if len(sys.argv) > 8 else "mixed"
SEE, NEW, TICK, MIN_REST, RESERVE = 6, 45, 0.01, 1000, 20
EXCH = "0x8bc070bedab741406f4b1eb65a72bee27894b689"
run = Run(__file__, f"策略族网格(撤单 +{CXL}ms,排队 {QMODE})", data_cutoff=f"{pd.to_datetime(W0, unit='ms')} ~ {pd.to_datetime(W1, unit='ms')} UTC")
B = pd.concat([pd.read_parquet(f) for f in sorted(glob.glob(f"{BX}/*.parquet")) if not f.endswith(".meta.parquet")], ignore_index=True)
B = B.drop_duplicates(["mk", "srv", "side", "lvl"]); B["px"] = B.px.round(2)
run.coverage("盘口", B.srv.to_numpy(), need=(W0, W1), max_gap_ms=120_000)
P = pd.read_parquet(PF); P["px"] = P.px.round(2)
run.coverage("pending 成交", P.srv.to_numpy(), need=(W0 + 600_000, W1 - 600_000), max_gap_ms=300_000)
bn = pd.read_parquet(f"{R}/bn_btc.parquet"); bt = bn.t_ms.to_numpy() - 6; bx = np.log(((bn.b + bn.a) / 2).to_numpy()) * 1e4
run.coverage("币安逐笔", bt, need=(W0, W1), max_gap_ms=30_000)
toks = json.load(open(f"{R}/chain/tokens.json"))
run.unique(pd.DataFrame(toks), ["token"], "tokens.json")      # 重复行会让 WON/LOST 计两次 → 被当平局/作废(10-23 东京文件出过)
nwon = {}
for t in toks:
    if t.get("status") == "WON": nwon[t["marketId"]] = nwon.get(t["marketId"], 0) + 1
nst = {}
for t in toks: nst.setdefault(t["marketId"], []).append(t.get("status"))
win = {t["marketId"]: (0.5 if nwon.get(t["marketId"], 0) >= 2 else (1.0 if t["status"] == "WON" else 0.0))          # 两边都 WON = 平局,各 0.5
       for t in toks if t["outcome"] == "Up" and t.get("status") in ("WON", "LOST") and nst[t["marketId"]].count("LOST") < 2}   # 两边都 LOST(作废)剔除
cols = ["block", "li", "tx", "oh", "maker", "taker", "mA", "tA", "mAmt", "tAmt", "fee"]
F = pd.read_csv(f"{R}/chain/fills.tsv.gz", sep="\t", names=cols, dtype={"mA": str, "tA": str, "mAmt": str, "tAmt": str})
tk = {t["token"]: (t["marketId"], t["outcome"]) for t in toks}
F["asset"] = np.where(F.mA == "0", F.tA, F.mA); F = F[F.asset.isin(tk) & (F.taker.str.lower() != EXCH)].copy()
F["mk"] = F.asset.map(lambda a: tk[a][0]); up = F.asset.map(lambda a: tk[a][1]) == "Up"; buy = F.mA == "0"
F["q"] = np.where(buy, F.tAmt.astype(float), F.mAmt.astype(float)) / 1e18
pxx = np.where(buy, F.mAmt.astype(float) / F.tAmt.astype(float), F.tAmt.astype(float) / F.mAmt.astype(float))
F["px"] = np.round(np.where(up, pxx, 1 - pxx), 2)
CAP = F.groupby(["mk", "px"]).q.sum().to_dict()
gi = np.where(np.diff(bt) > 2000)[0]; gaps = list(zip(bt[gi], bt[gi + 1]))
rounds = B.groupby("mk").start.first()
rounds = rounds[(rounds * 1000 >= W0) & ((rounds + 300) * 1000 <= W1) & rounds.index.isin(win.keys())]
rounds = rounds[[not any(a < (s + 300) * 1000 and b > (s - 5) * 1000 for a, b in gaps) for s in rounds]]
NMAX = int(sys.argv[9]) if len(sys.argv) > 9 else 0                   # 留出集:只取时间顺序前 NMAX 个干净轮(预注册的中途/最终检验点)
if NMAX:
    rounds = rounds.sort_values().iloc[:NMAX]
    run.check("干净轮数够", len(rounds) == NMAX, f"{len(rounds)} / {NMAX} 轮")
run.check("可回放轮数", len(rounds) >= 60, f"{len(rounds)} 轮")
xb_at = lambda t: bx[np.clip(np.searchsorted(bt, t, side="right") - 1, 0, None)]
DATA = {}
for mk, s in rounds.items():
    b = B[(B.mk == mk) & (B.srv >= s * 1000) & (B.srv <= (s + 300) * 1000)].sort_values(["srv", "side", "lvl"])
    snaps = [(t, {0: dict(zip(g[g.side == 0].px, g[g.side == 0].sz)), 1: dict(zip(g[g.side == 1].px, g[g.side == 1].sz))}) for t, g in b.groupby("srv", sort=True)]
    st = np.array([t for t, _ in snaps]); used = {}; fev = []
    for r in P[P.mk == mk].sort_values("srv").itertuples():
        c = CAP.get((mk, r.px), 0.0); u = used.get(r.px, 0.0); q = min(r.shares, max(0.0, c - u))
        if q <= 1e-9: continue
        used[r.px] = u + q; k = np.searchsorted(st, r.srv, side="left") - 1
        if k < 0: continue
        bk = snaps[k][1]; bb = max(bk[0]) if bk[0] else None; ba = min(bk[1]) if bk[1] else None
        side = 1 if (ba is not None and r.px >= ba - 1e-9) else (0 if (bb is not None and r.px <= bb + 1e-9) else None)
        if side is not None and s * 1000 <= r.srv <= (s + 300) * 1000: fev.append((r.srv, side, r.px, q))
    DATA[(mk, s)] = (snaps, fev)
def replay(args):
    (mk, s), p = args; snaps, fev = DATA[(mk, s)]
    if len(snaps) < 100: return []
    i0, i1 = np.searchsorted(bt, s * 1000), np.searchsorted(bt, (s + 300) * 1000)
    E = [(t, 1, k) for k, (t, _) in enumerate(snaps)] + [(f[0], 0, j) for j, f in enumerate(fev)] + [(bt[i], 2, i) for i in range(i0, i1)]
    E.sort(key=lambda e: (e[0], e[1]))
    orders, live, inv, book, last_adv, req, fl = [], {0: None, 1: None}, 0.0, snaps[0][1], -1e18, [], []
    flat_done = False; fsince = {}                 # 上次快照以来各(侧,价)的成交量(审核 2a:快照减少量要先扣掉成交)
    def active(t):                                 # 仍在盘口上的我们的单:有剩余、且撤单未生效(审核 3:撤单在途也要更新排队)
        return [o for o in orders if o["rem"] > 0 and o["ahead"] is not None and (o["t_cxl_eff"] is None or t < o["t_cxl_eff"])]
    def place(side, t, px):
        o = dict(side=side, px=px, t_live=t + NEW, ahead=None, behind=0.0, rem=p["size"], t_cxl_dec=None, t_cxl_eff=None, x_place=xb_at(t))
        orders.append(o); live[side] = o; req.append(t)
    def budget(t):                                 # 限额 rl 次/分钟(Predict 240/分钟/key);留 RESERVE 次给不利撤单和平仓;rl=0 不限
        if not p.get("rl", 240): return True
        n = 0
        for x in reversed(req):
            if x <= t - 60_000: break
            n += 1
        lim = p.get("rl", 240) - RESERVE
        tau_ = s + 300 - t / 1000
        if tau_ < 60 or tau_ > 240: lim //= 2              # 审核 c3:轮交界(上一轮最后 60 秒 + 下一轮最前 60 秒)各用一半额度,跨轮滑动 60 秒也不超限
        return n < lim
    def cancel(o, t, eff):
        o["t_cxl_dec"] = t; o["t_cxl_eff"] = eff; req.append(t)
    lv = lambda bk, side, px: bk[side].get(px, 0.0)
    for t, kind, j in E:
        tau = s + 300 - t / 1000
        if kind == 1:
            nb = snaps[j][1]
            for o in orders:
                if o["ahead"] is None and o["rem"] > 0 and t >= o["t_live"] and (o["t_cxl_eff"] is None or t < o["t_cxl_eff"]): o["ahead"] = lv(book, o["side"], o["px"])
            for o in active(t):
                side = o["side"]; d = lv(nb, side, o["px"]) - lv(book, side, o["px"])
                if d > 0: o["behind"] += d
                elif d < 0:
                    c = max(0.0, -d - fsince.get((side, o["px"]), 0.0))          # 撤单 = 减少量 − 本段内成交
                    cb = min(o["behind"], c); o["behind"] -= cb                     # 撤单先算在我们身后(悲观)
                    f = {"half": 0.5, "full": 1.0, "none": 0.0}.get(QMODE, 1.0 if t - last_adv <= 300 else 0.5)
                    o["ahead"] = max(0.0, o["ahead"] - f * (c - cb))
            for side in (0, 1):                                                      # 审核 2b:整单成交完 = 这一侧没有单,可以重挂
                if live[side] is not None and live[side]["rem"] <= 1e-9: live[side] = None
            fsince = {}; book = nb
            bb = max(book[0]) if book[0] else None; ba = min(book[1]) if book[1] else None
            if bb is None or ba is None: continue
            if p["flat"] and tau <= p["flat"] and not flat_done:          # 平库存:按当时对手价吃掉,付吃单费
                flat_done = True
                for side in (0, 1):
                    o = live[side]
                    if o is not None and o["t_cxl_dec"] is None: cancel(o, t, t + CXL); live[side] = None
                if abs(inv) > 1e-9:
                    px = ba if inv < 0 else bb; q = abs(inv)
                    fl.append(dict(t=t, side=0 if inv < 0 else 1, px=px, q=q, kind="平仓吃单", fee=0.02 * min(px, 1 - px) * q, adv=0.0)); inv = 0.0; req.append(t)
                continue
            if tau < p["stop"] or flat_done or tau > p.get("tmax", 300):
                for side in (0, 1):
                    o = live[side]
                    if o is not None and o["t_cxl_dec"] is None: cancel(o, t, t + CXL); live[side] = None
                continue
            mid = (bb + ba) / 2
            for side, best in ((0, bb), (1, ba)):
                o = live[side]
                ok_side = not ((side == 0 and inv >= p["cap"]) or (side == 1 and inv <= -p["cap"]))
                back = p["off"] + int(p.get("skew", 0) * max(0.0, inv if side == 0 else -inv) / 10)       # 加仓一侧按库存往后退
                want = (round(best - back * TICK, 2) if side == 0 else round(best + back * TICK, 2)) if ok_side else None
                if want is not None and p.get("edge", 0):
                    want = min(want, np.floor((mid - p["edge"] * TICK) * 100 + 1e-6) / 100) if side == 0 else max(want, np.ceil((mid + p["edge"] * TICK) * 100 - 1e-6) / 100)
                if want is not None and not (0.01 <= want <= 0.99): want = None
                if o is not None and o["t_cxl_dec"] is None:
                    gap = ((want - o["px"]) if side == 0 else (o["px"] - want)) if want is not None else 0.0   # 审核 c:按离"本该挂的价"几档判,不按离最优价(否则 off>behind 每秒撤了重挂;edge/skew 对已挂单不生效)
                    if want is not None and gap >= -1e-9 and (gap <= p["behind"] * TICK - 1e-9 + TICK * 0.5 or t - o["t_live"] < MIN_REST): continue
                    if want is not None and gap >= -1e-9 and not budget(t): continue      # 限额不够:落后的单先不改(比目标更激进的照撤)
                    cancel(o, t, t + CXL); live[side] = None
                if want is not None and live[side] is None and budget(t): place(side, t, want)
        elif kind == 2:
            if abs(bx[j] - xb_at(t - 300)) >= 0.2: last_adv = t                    # 审核 2d:币安急动与我们是否挂单无关
            for side in (0, 1):
                o = live[side]
                if o is None or o["t_cxl_dec"] is not None: continue
                mv = bx[j] - o["x_place"]
                if (side == 1 and mv >= p["adv"]) or (side == 0 and mv <= -p["adv"]):
                    cancel(o, t + SEE, t + SEE + CXL); live[side] = None
        else:
            ft, fside, fpx, fq = fev[j]
            fsince[(fside, fpx)] = fsince.get((fside, fpx), 0.0) + fq
            for o in sorted(orders, key=lambda o: o["t_live"]):                     # 审核 3:同一笔成交在我们多张单之间按挂上先后共享,不重复
                if fq <= 1e-9: break
                if o["side"] != fside or abs(o["px"] - fpx) > 1e-9 or o["rem"] <= 0 or ft < o["t_live"]: continue
                if o["t_cxl_eff"] is not None and ft >= o["t_cxl_eff"]: continue
                if o["ahead"] is None: o["ahead"] = lv(book, fside, fpx)
                take = min(fq, o["ahead"]); o["ahead"] -= take; rest = fq - take
                if rest > 0:
                    q = min(rest, o["rem"]); o["rem"] -= q; fq -= q          # 复审:只扣我们自己吃掉的量(各单的前方量独立跟踪)
                    fl.append(dict(t=ft, side=fside, px=fpx, q=q, kind="B" if (o["t_cxl_dec"] is not None and ft >= o["t_cxl_dec"]) else "正常",
                                   fee=-0.25 * 0.02 * min(fpx, 1 - fpx) * q, rb=0.02 * min(fpx, 1 - fpx) * q, adv=xb_at(ft) - o["x_place"]))
                    inv += q if fside == 0 else -q
    w = win[mk]
    for f in fl:
        f["start"] = s; f["pnl"] = ((w - f["px"]) if f["side"] == 0 else (f["px"] - w)) * f["q"]; f["net"] = f["pnl"] - f["fee"]
    return [dict(start=s, n_req=len(req), req=req, fills=fl, n_orders=len(orders))]
grid = json.load(open(GRID))
if isinstance(grid, list):                                             # 预注册的固定候选列表(不做笛卡尔积)
    keys = list(grid[0].keys()); run.check("候选参数键一致", all(set(g) == set(keys) for g in grid), "每个候选必须写全同一组参数(缺省值会被当 0)")
    combos = [dict(g) for g in grid]
else:
    keys = list(grid.keys()); combos = [dict(zip(keys, v)) for v in itertools.product(*[grid[k] for k in keys])]
run.number("策略组合数", len(combos), rounds=len(rounds))
jobs = [(rk, p) for p in combos for rk in DATA]
with mp.get_context("fork").Pool(10) as pool: outs = pool.map(replay, jobs, chunksize=8)
rows = []; ridx = list(DATA)
for ci, p in enumerate(combos):
    res = [x for lst in outs[ci * len(ridx):(ci + 1) * len(ridx)] for x in lst]
    fills = pd.DataFrame([f for r in res for f in r["fills"]])
    if fills.empty: continue
    fills["qm"] = np.where(fills.kind == "平仓吃单", 0.0, fills.q)                  # 每股分母只用挂单成交股数(审核 4①)
    fills["rb"] = fills["rb"].fillna(0.0) if "rb" in fills else 0.0; fills["tkfee"] = np.where(fills.kind == "平仓吃单", fills.fee, 0.0)
    per = fills.groupby("start").agg(net=("net", "sum"), pnl=("pnl", "sum"), q=("qm", "sum"), rb=("rb", "sum"), tkfee=("tkfee", "sum"))
    allr = pd.Series(0.0, index=[s for (_, s) in ridx]); per = per.reindex(allr.index).fillna(0)
    m = per.net.sum() / per.q.sum(); se = np.sqrt(((per.net - m * per.q) ** 2).sum()) / per.q.sum()
    mg = per.pnl.sum() / per.q.sum()
    rq = pd.Series([t // 60000 for r in res for t in r["req"]]).value_counts()
    h = per.index < np.median(per.index)
    reb = {f"net_c_reb{int(r * 100)}": (per.pnl.sum() - per.tkfee.sum() + r * per.rb.sum()) / per.q.sum() * 100 for r in (0.0, 0.2, 0.4)}   # 返佣率敏感性
    cum = per.net.sort_index().cumsum(); maxdd = (cum.cummax().clip(lower=0) - cum).max()
    low = min(0.0, cum.min())                                                  # Luke 口径:账户最多跌到本金以下多少(先赚后亏不算)
    rng = np.random.default_rng(7); v = per.net.to_numpy()
    lows = [min(0.0, np.cumsum(rng.permutation(v)).min()) for _ in range(2000)]   # 打乱轮次顺序 2000 次:最低点分布
    low_p50, low_p05 = np.percentile(lows, 50), np.percentile(lows, 5)
    per.to_csv(f"{run.dir}/per_round_{ci}.csv")                               # 每轮盈亏存档,供之后多日回撤模拟
    rows.append(dict(**p, net_c=m * 100, t=m / se, gross_c=mg * 100, total=per.net.sum(), worst5=per.net.nsmallest(5).sum(), worst1=per.net.min(), maxdd=maxdd, low=low, low_p50=low_p50, low_p05=low_p05, **reb,
                     pos_rounds=(per.net > 0).mean(), q=per.q.sum(), first=per.net[h].sum(), second=per.net[~h].sum(),
                     req_p99=rq.quantile(.99) if len(rq) else 0, over240=(rq > 240).mean() if len(rq) else 0,
                     b_share=fills.q[fills.kind == "B"].sum() / fills.qm.sum()))
G = pd.DataFrame(rows).sort_values("t", ascending=False)
G.to_csv(f"{run.dir}/grid.csv", index=False)
run.sample(G.head(20), "网格前 20(按净利 t)")
best = G.iloc[0]
run.number("网格最佳(按 t,调试段,已看)", best.net_c, unit="分/股 净", t=best.t, total=best.total, worst5=best.worst5, params=str({k: best[k] for k in keys}))
run.finish()
