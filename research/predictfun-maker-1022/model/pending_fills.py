# 回放器成交源:settlementsPending 的新增(Up 价、金额)→ 成交事件(服务器时间、价位、股数 = 金额 ÷ 价)
# 核对:与链上同轮挂单成交金额(Up 价 × 股数)按轮比较;输出 fills_pending.parquet 供回放器用
import sys, glob, json, numpy as np, pandas as pd
sys.path.insert(0, "/Users/luke/Documents/polymarket/research/lib")
from sanity import Run
BX, R = sys.argv[1], sys.argv[2]; EXCH = "0x8bc070bedab741406f4b1eb65a72bee27894b689"
W0, W1 = (int(sys.argv[3]), int(sys.argv[4])) if len(sys.argv) > 4 else (1792651200_000, 1792677300_000)   # 默认 10-22 06:40~13:55 UTC
run = Run(__file__, "成交源:settlementsPending 新增 vs 链上(调试段)", data_cutoff=f"{pd.to_datetime(W0, unit='ms')} ~ {pd.to_datetime(W1, unit='ms')} UTC")
U = pd.concat([pd.read_parquet(f) for f in sorted(glob.glob(f"{BX}/*.meta.parquet"))], ignore_index=True).sort_values(["mk", "srv"]).drop_duplicates(["mk", "srv", "version"])
run.coverage("盘口推送", U.srv.to_numpy(), need=(W0, W1), max_gap_ms=120_000)
ev = []
for (mk, st), g in U.groupby(["mk", "start"]):
    prev = {}
    for r in g.itertuples():
        sp = r.settle_pending; cur = {}
        if isinstance(sp, dict):
            for sd in ("asks", "bids"):
                arr = sp.get(sd)
                if arr is None: continue
                for x in arr: cur[(sd, round(float(x[0]), 3))] = cur.get((sd, round(float(x[0]), 3)), 0) + float(x[1])
        for k, v in cur.items():
            d = v - prev.get(k, 0)
            if d > 1e-6: ev.append((mk, st, r.srv, k[0], k[1], d, d / k[1] if k[1] > 0 else np.nan))
        prev = cur
P = pd.DataFrame(ev, columns=["mk", "start", "srv", "pside", "px", "usd", "shares"])
P = P[(P.srv >= W0) & (P.srv <= W1)]
# 同一次推送、同价位,买卖两侧增加量相等 = 同一笔成交记了两次 → 只留一条
P["usd_r"] = P.usd.round(4)
dup = P.duplicated(["mk", "srv", "px", "usd_r"], keep="first")
run.number("两侧重复记的条目占比", dup.mean() * 100, unit="%", usd_share=P.usd[dup].sum() / P.usd.sum() * 100)
P = P[~dup].drop(columns="usd_r")
cols = ["block", "li", "tx", "oh", "maker", "taker", "mA", "tA", "mAmt", "tAmt", "fee"]
F = pd.read_csv(f"{R}/chain/fills.tsv.gz", sep="\t", names=cols, dtype={"mA": str, "tA": str, "mAmt": str, "tAmt": str})
Bt = pd.read_csv(f"{R}/chain/blockts.tsv", sep="\t", names=["b", "ts"]); F["t_ms"] = (np.interp(F.block, Bt.b, Bt.ts) * 1000).astype(np.int64)
run.unique(F, ["block", "li"], "OrderFilled")
tok = {r["token"]: (r["marketId"], int(r["slug"].rsplit("-", 1)[1]), r["outcome"]) for r in json.load(open(f"{R}/chain/tokens.json"))}
F["asset"] = np.where(F.mA == "0", F.tA, F.mA); F = F[F.asset.isin(tok) & (F.taker.str.lower() != EXCH)].copy()
F["start"] = F.asset.map(lambda a: tok[a][1]); up = F.asset.map(lambda a: tok[a][2]) == "Up"
buy = F.mA == "0"; q = np.where(buy, F.tAmt.astype(float), F.mAmt.astype(float)) / 1e18
px = np.where(buy, F.mAmt.astype(float) / F.tAmt.astype(float), F.tAmt.astype(float) / F.mAmt.astype(float))
F["usd_up"] = q * np.where(up, px, 1 - px)
rounds = sorted(set(P.start) & set(F.start)); rounds = [s for s in rounds if W0 <= s * 1000 and (s + 300) * 1000 <= W1]
a = P[P.start.isin(rounds)].groupby("start").usd.sum(); b = F[F.start.isin(rounds)].groupby("start").usd_up.sum()
r = (a / b).dropna()
run.number("每轮 pending 成交金额 / 链上挂单成交金额", r.median(), p10=r.quantile(.1), p90=r.quantile(.9), rounds=len(r), corr=np.corrcoef(a.reindex(r.index), b.reindex(r.index))[0, 1])
run.number("合计 pending/链上", a.sum() / b.sum(), pending_usd=a.sum(), chain_usd=b.sum())
run.sample(P, "pending 成交事件")
run.check("pending 覆盖链上成交 ≥80%(金额)", a.sum() / b.sum() >= 0.8, f"{a.sum() / b.sum():.1%}")
P.to_parquet(f"{run.dir}/fills_pending.parquet")
run.finish()
