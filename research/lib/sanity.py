"""报数门(10-23 Luke 定):所有研究分析的数字必须经这里出。

为什么:10-22~23 报给 Luke 又撤回的数字 ≥6 次(输入数据被截断、没和平凡基准比、守恒不成立、口径转述错)。
这里把"报数前必须做的检查"变成代码:任何一项不过直接抛 SanityError 终止,不出数。

用法(脚本放 research/<目录>/ 下,开头):
    import sys; sys.path.insert(0, "/Users/luke/Documents/polymarket/research/lib")   # 远端:脚本旁边放一份 sanity.py
    from sanity import Run
    run = Run(__file__, "挂单方盈亏-留出集", data_cutoff="2026-10-17 23:59 UTC")
    run.coverage("链上成交", t_ms, need=(t0_ms, t1_ms), max_gap_ms=600_000)   # 输入必须覆盖分析区间
    run.unique(df, ["tx", "li"], "成交记录")                                   # 主键不重复
    run.conserve("每轮挂单+吃单盈亏和", per_round_sum, tol=1e-6)               # 守恒
    run.baseline("盘口 0.5s 预测", y, {"模型": p}, {"纯持久": m0, "常数0.5": 0.5}) # 必须对平凡基准
    run.sample(df, "挂单成交")                                                  # 抽 10 行写入结果,供人工核对
    run.number("H1 每股(分)", 0.31, n=2590, t=2.4)                             # 唯一出数口
    run.finish()                                                                 # 写 results/<脚本>_<时间>/report.json

finish() 要求:至少一次 coverage、至少一次 sample;number 至少一个。输出的每个数字都带"未审"标记,
过了独立 agent 审核后由人把 report.json 里 audited 改为审核 agentId(只有这时才可报给 Luke)。
"""
import hashlib, json, os, socket, sys, time

import numpy as np


class SanityError(RuntimeError):
    pass


def _f(x):
    try:
        return float(x)
    except Exception:
        return str(x)


class Run:
    def __init__(self, script, label, data_cutoff, results_root=None):
        self.script = os.path.abspath(script)
        self.label, self.cutoff = label, data_cutoff
        src = open(self.script, "rb").read()
        self.sha = hashlib.sha256(src).hexdigest()[:16]
        base = results_root or os.path.join(os.path.dirname(self.script), "results")
        stamp = time.strftime("%Y%m%d_%H%M%S")
        self.dir = os.path.join(base, f"{os.path.splitext(os.path.basename(self.script))[0]}_{stamp}")
        os.makedirs(self.dir, exist_ok=True)
        self.r = {"label": label, "script": self.script, "sha256_16": self.sha, "argv": sys.argv, "host": socket.gethostname(),
                  "started": time.strftime("%Y-%m-%d %H:%M:%S %z"), "data_cutoff": data_cutoff,
                  "checks": [], "numbers": [], "samples": [], "audited": None}
        print(f"[sanity] {label} | 脚本 {os.path.basename(self.script)} sha {self.sha} | 数据截止 {data_cutoff} | 结果 {self.dir}")

    def _fail(self, msg):
        self.r["checks"].append({"ok": False, "msg": msg}); self._dump()
        raise SanityError(msg)

    def _ok(self, kind, msg, **kw):
        self.r["checks"].append({"ok": True, "kind": kind, "msg": msg, **{k: _f(v) for k, v in kw.items()}})
        print(f"[sanity ✓] {msg}")

    def coverage(self, name, t_ms, need, max_gap_ms=None, tol_ms=0):
        """输入时间戳(毫秒)必须覆盖 need=(起, 止);可选:区间内最大空档不超过 max_gap_ms。"""
        t = np.sort(np.asarray(t_ms, dtype=np.float64)); t = t[~np.isnan(t)]
        if len(t) == 0: self._fail(f"覆盖检查[{name}]:没有数据")
        a, b = need
        if t[0] > a + tol_ms or t[-1] < b - tol_ms:
            self._fail(f"覆盖检查[{name}]:数据 {_ts(t[0])}~{_ts(t[-1])} 没覆盖分析区间 {_ts(a)}~{_ts(b)}")
        gap = None
        if max_gap_ms is not None:
            w = t[(t >= a) & (t <= b)]; gap = float(np.max(np.diff(w))) if len(w) > 1 else float(b - a)
            if gap > max_gap_ms: self._fail(f"覆盖检查[{name}]:区间内最大空档 {gap/1000:.1f}s > 允许 {max_gap_ms/1000:.1f}s")
        self._ok("coverage", f"覆盖[{name}] {_ts(t[0])}~{_ts(t[-1])} ⊇ {_ts(a)}~{_ts(b)}" + (f",最大空档 {gap/1000:.1f}s" if gap is not None else ""), n=len(t))

    def unique(self, df, cols, name):
        d = int(df.duplicated(cols).sum())
        if d: self._fail(f"去重检查[{name}]:按 {cols} 有 {d} 行重复")
        self._ok("unique", f"去重[{name}] 按 {cols} 无重复({len(df):,} 行)")

    def conserve(self, name, values, tol):
        v = np.abs(np.asarray(values, dtype=np.float64)); m = float(np.nanmax(v)) if len(v) else 0.0
        if not m <= tol: self._fail(f"守恒检查[{name}]:最大偏差 {m:.6g} > {tol}")
        self._ok("conserve", f"守恒[{name}] 最大偏差 {m:.3g} ≤ {tol}", n=len(v))

    def check(self, name, cond, detail=""):
        """通用断言(例如 '特征时间 < 决策时间')。"""
        if not bool(cond): self._fail(f"断言[{name}] 不成立 {detail}")
        self._ok("assert", f"断言[{name}] {detail}")

    def baseline(self, name, y, models, baselines, mask=None):
        """模型必须和平凡基准一起报;均方根/中位绝对误差都记;模型不如最好基准时标红但不终止。"""
        y = np.asarray(y, dtype=np.float64); m = np.ones(len(y), bool) if mask is None else np.asarray(mask, bool)
        def met(p):
            p = np.broadcast_to(np.asarray(p, dtype=np.float64), y.shape); e = np.abs(p[m] - y[m])
            return {"rmse": float(np.sqrt(np.nanmean(e ** 2))), "median": float(np.nanmedian(e)), "n": int(np.isfinite(e).sum())}
        res = {k: met(v) for k, v in {**models, **baselines}.items()}
        best_b = min((res[k]["rmse"], k) for k in baselines)
        flag = {k: res[k]["rmse"] < best_b[0] for k in models}
        self.r["checks"].append({"ok": True, "kind": "baseline", "name": name, "metrics": res, "beats_best_baseline": flag})
        for k, v in res.items():
            tag = "" if k in baselines else ("  ✓胜过最好基准" if flag[k] else f"  ⚠不如基准[{best_b[1]}]")
            print(f"[sanity 基准] {name} | {k:<14} 均方根 {v['rmse']:.4f} 中位 {v['median']:.4f} n={v['n']:,}{tag}")
        return res

    def sample(self, df, name, n=10, seed=1):
        s = df.sample(min(n, len(df)), random_state=seed)
        p = os.path.join(self.dir, f"sample_{len(self.r['samples'])}.csv"); s.to_csv(p, index=False)
        self.r["samples"].append({"name": name, "file": p, "rows": len(s)})
        print(f"[sanity 抽样] {name}:{len(s)} 行 → {p}(需人工核对)")
        print(s.to_string(max_colwidth=24))

    def number(self, key, value, **ctx):
        self.r["numbers"].append({"key": key, "value": _f(value), **{k: _f(v) for k, v in ctx.items()}})
        extra = " ".join(f"{k}={_f(v) if not isinstance(_f(v), float) else round(_f(v), 4)}" for k, v in ctx.items())
        print(f"[数字·未审] {key} = {round(_f(value), 4) if isinstance(_f(value), float) else value}  {extra}")

    def finish(self):
        kinds = [c.get("kind") for c in self.r["checks"] if c.get("ok")]
        miss = [k for k in ("coverage",) if k not in kinds]
        if not self.r["samples"]: miss.append("sample")
        if not self.r["numbers"]: miss.append("number")
        if miss: self._fail(f"finish:缺少必做项 {miss}")
        self.r["finished"] = time.strftime("%Y-%m-%d %H:%M:%S %z"); self._dump()
        print(f"[sanity] 完成。结果 {self.dir}/report.json —— 数字均为'未审',独立审核通过前不得报给 Luke。")

    def _dump(self):
        json.dump(self.r, open(os.path.join(self.dir, "report.json"), "w"), ensure_ascii=False, indent=1, default=str)


def _ts(ms):
    return time.strftime("%m-%d %H:%M:%S", time.gmtime(float(ms) / 1000)) + "Z"
