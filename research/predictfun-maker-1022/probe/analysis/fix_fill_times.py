# 10-25 回测数据的链上成交撮合时刻修正(Opus 红队:区块时间 − 434ms 误差约 ±400ms,大于 150ms 吃单延迟,撤单效果判不出)
# 做法(同老师规则书 §0):盘口推送里 settlementsPending 每个价位的累计金额增加 = 那一刻有撮合。
#   同一队:链上 UpBid c 分 ↔ pending bids 的 Up 价 c;DownBid q 分 ↔ pending asks 的 Up 价 100−q。
#   每队按时间顺序贪心配对:链上成交(按区块顺序)配给 [估计时刻 −2500ms, +1500ms] 内还有剩余量的最早一次增加;
#   修正后的撮合时刻 = 那次推送时刻 − 66ms(实测推送延迟);配不上的保留原估计。
# 用法:python3 fix_fill_times.py <bt_data/out> <输出目录> [near|cap]   输出目录结构与输入相同(parts/、rounds.json)
import sys, os, json, gzip, shutil, bisect
import numpy as np, pandas as pd
sys.path.insert(0, "/Users/luke/Documents/polymarket/research/lib")
from sanity import Run

src, dst = sys.argv[1], sys.argv[2]
PUSH_MS, W0, W1 = 66, 2500, 1500
MODE = sys.argv[3] if len(sys.argv) > 3 else "near"   # near | cap
DMAP = sys.argv[4] if len(sys.argv) > 4 else "asks"   # asks | both:Down 买单对应 pending 哪一侧
ONLY = int(sys.argv[5]) if len(sys.argv) > 5 else 0    # >0:只处理前 N 个分段文件(诊断用)
run = Run(__file__, "回测链上成交撮合时刻修正(按待结算推送对齐)", data_cutoff="录制器 10-22 10:40~10-24 17:00 UTC")
os.makedirs(os.path.join(dst, "parts"), exist_ok=True)
shutil.copy(os.path.join(src, "rounds.json"), os.path.join(dst, "rounds.json"))
stats = []
for p in sorted(os.listdir(os.path.join(src, "parts")))[: (ONLY or None)]:
    with gzip.open(os.path.join(src, "parts", p), "rt") as f:
        head = f.readline()
        ev = [json.loads(l) for l in f]
    # 待结算增量:(队, 档位) → [[t, 剩余股数], ...]
    inc, prev = {}, {}
    for x in ev:
        if x[0] != "pf":
            continue
        d = json.loads(x[2]) if isinstance(x[2], str) else x[2]
        sp = (d.get("data") or {}).get("settlementsPending") or {}
        cur = {}
        for sd in ("asks", "bids"):
            for pr, usd in sp.get(sd) or []:
                c = int(round(float(pr) * 100))
                cur[(sd, c)] = cur.get((sd, c), 0.0) + float(usd)
        for (sd, c), v in cur.items():
            dv = v - prev.get((sd, c), 0.0)
            if dv > 1e-6 and 0 < c < 100:
                if sd == "bids":
                    inc.setdefault(("UpBid", c), []).append([x[1], dv / (c / 100.0)])
                    if DMAP == "both":   # 诊断:Down 买单的撮合也可能记在 bids 侧(互补撮合)
                        inc.setdefault(("DownBid", 100 - c), []).append([x[1], dv / (c / 100.0)])
                else:
                    inc.setdefault(("DownBid", 100 - c), []).append([x[1], dv / (c / 100.0)])
        prev = cur
    ptr = {k: 0 for k in inc}
    tsk = {}
    out = []
    for x in ev:
        if x[0] == "fill":
            k = (x[2], int(x[3]))
            t_est, need = x[1], float(x[4])
            lst, i = inc.get(k, []), ptr.get(k, 0)
            t_new = None
            if MODE == "cap":
                while i < len(lst) and (lst[i][0] < t_est - W0 or lst[i][1] <= 1e-9):
                    i += 1
                ptr[k] = i
                if i < len(lst) and lst[i][0] <= t_est + W1:
                    t_new = lst[i][0] - PUSH_MS
                    lst[i][1] -= need
            else:  # near:窗口内时间上最近的一次同队增加(不扣量;老师规则书的兜底口径)
                ts = tsk.setdefault(k, [e[0] for e in lst])
                j = bisect.bisect_left(ts, t_est)
                cand = [ts[jj] for jj in (j - 1, j) if 0 <= jj < len(ts) and t_est - W0 <= ts[jj] <= t_est + W1]
                if cand:
                    t_new = min(cand, key=lambda u: abs(u - t_est)) - PUSH_MS
            stats.append((p, t_est, t_new, need, x[2], int(x[3])))
            if t_new is not None:
                x = [x[0], t_new] + x[2:]
        out.append(x)
    out.sort(key=lambda e: e[1])  # 修正后按时间重排(引擎按时间喂事件)
    with gzip.open(os.path.join(dst, "parts", p), "wt") as f:
        f.write(head)
        for x in out:
            f.write(json.dumps(x, ensure_ascii=False) + "\n")
S = pd.DataFrame(stats, columns=["part", "t_est", "t_new", "shares", "qside", "level"])
S["matched"] = S.t_new.notna()
S["dt"] = S.t_new - S.t_est
run.coverage("链上成交", S.t_est.to_numpy(), need=(int(S.t_est.min()), int(S.t_est.max())))
run.sample(S, "逐笔修正", n=10)
run.number("链上成交笔数", len(S), matched_frac=S.matched.mean(), matched_share_frac=S.shares[S.matched].sum() / S.shares.sum())
q = S.dt.dropna().quantile([.1, .25, .5, .75, .9])
run.number("修正量(新 − 原估计)", q.loc[.5], unit="ms 中位", p10=q.loc[.1], p25=q.loc[.25], p75=q.loc[.75], p90=q.loc[.9])
for k, g in S.groupby("qside"):
    run.number(f"配上比例 {k}", g.matched.mean(), n=len(g))
S["lb"] = pd.cut(S.level, [0, 10, 30, 50, 70, 90, 100])
for k, g in S.groupby("lb", observed=True):
    run.number(f"配上比例 档位{k}", g.matched.mean(), n=len(g))
run.finish()
