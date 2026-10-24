"""读探针日志(predict-mm-probe 写的 probe_*.jsonl.gz,按 UTC 小时滚动),重建订单、成交、盘口、币安价格与结算结果。
只用 Python 标准库(3.8+)。日志格式见 ../predict-mm-probe/README.md 第 6 节。

两种参考价(给 Up 的价格,0~1):
  predict_mid(m, t) = Predict 盘口 (买一 + 卖一) / 2(t 时刻及之前最后一条盘口)
  fair_up(m, t)     = Φ(ln(S/K) / (σ√τ))
      S = t 时刻币安 BTCUSDT 中间价,K = 本轮开盘时的币安中间价,
      σ = 近 5 分钟实际波动(1 秒采样的对数收益,方差 / 秒),τ = 距收盘秒数;
      τ ≤ 0 时取收盘价与 K 比较:高 1、低 0、相等 0.5(平局两边各 0.5)。
我们持有的那一边的价值:Up = ref,Down = 1 − ref。
"""
import bisect
import glob
import gzip
import json
import math
import os

SIDE_LABEL = {"UpBid": "买Up", "DownBid": "卖Up(买Down)"}


def iter_lines(paths):
    files = []
    for p in paths:
        if os.path.isdir(p):
            files += sorted(glob.glob(os.path.join(p, "probe_*.jsonl.gz")) + glob.glob(os.path.join(p, "probe_*.jsonl")))
        else:
            files.append(p)
    for f in files:
        op = gzip.open if f.endswith(".gz") else open
        try:
            with op(f, "rt", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        yield json.loads(line)
                    except ValueError:
                        continue  # 进程被杀时末尾可能有半行
        except (EOFError, OSError):
            continue  # 正在写的 gz 读到末尾会报 EOF,已读到的行照用


class Series:
    """(wall_ms, 值) 时间序列"""

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

    def at_or_before(self, t, max_age_ms=None):
        i = bisect.bisect_right(self.t, t) - 1
        if i < 0:
            return None
        if max_age_ms is not None and t - self.t[i] > max_age_ms:
            return None
        return self.v[i]

    def first_after(self, t, max_wait_ms=None):
        i = bisect.bisect_left(self.t, t)
        if i >= len(self.t):
            return None
        if max_wait_ms is not None and self.t[i] - t > max_wait_ms:
            return None
        return self.v[i]

    def __len__(self):
        return len(self.t)


def norm_cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _book_top(data):
    bids = [(l[0], l[1]) for l in data.get("bids") or [] if len(l) >= 2 and l[1] > 0]
    asks = [(l[0], l[1]) for l in data.get("asks") or [] if len(l) >= 2 and l[1] > 0]
    bb = max(bids) if bids else None
    ba = min(asks) if asks else None
    return (bb[0] if bb else None, ba[0] if ba else None, bb[1] if bb else None, ba[1] if ba else None)


def _new_order(cid):
    return {"cid": cid, "order_id": None, "hash": None, "market_id": None, "qside": None, "price": None, "shares": None,
            "decide_ms": None, "ack_ms": None, "ack_ok": None, "queue_ahead": None, "best_bid": None, "best_ask": None, "ms_left": None,
            "cancel_reason": None, "cancel_ms": None, "cancel_ack_ms": None, "end": None, "end_ms": None,
            "filled": 0.0, "first_fill_ms": None, "first_fill_source": None, "fill_after_cancel": False, "fills": []}


class ProbeLog:
    def __init__(self):
        self.mode = None
        self.first_ms = None
        self.last_ms = 0
        self.orders = {}  # cid -> dict
        self.by_oid = {}  # order_id -> cid
        self.fills = []  # 每次成交增量一行
        self.bn = Series()  # BTCUSDT 中间价
        self.books = {}  # market_id -> Series((bid, ask, bid_size, ask_size))
        self.rounds = {}  # market_id -> {"start_ms", "end_ms", "position"}
        self.settles = {}  # market_id -> (payout_up, payout_down)
        self.markets = {}  # market_id -> market 日志行

    # ---------- 参考价 ----------
    def predict_mid(self, m, t):
        s = self.books.get(m)
        if not s:
            return None
        v = s.at_or_before(t)
        if not v or v[0] is None or v[1] is None:
            return None
        return (v[0] + v[1]) / 2.0

    def round_of(self, m):
        return self.rounds.get(m)

    def strike(self, m):
        """K = 本轮开盘时的币安中间价(开盘前后 2 秒内要有币安数据,否则 None)"""
        r = self.rounds.get(m)
        if not r or not len(self.bn):
            return None
        s = r["start_ms"]
        v = self.bn.at_or_before(s, max_age_ms=2000)
        return v if v is not None else self.bn.first_after(s, max_wait_ms=2000)

    def sigma(self, t, window_s=300, min_s=60):
        """近 window_s 秒的实际波动(每 √秒);数据不足 min_s 秒返回 None"""
        if not len(self.bn):
            return None
        start = max(t - window_s * 1000, self.bn.t[0])
        n = int((t - start) // 1000)
        if n < min_s:
            return None
        prev = self.bn.at_or_before(start)
        ss = 0.0
        for k in range(1, n + 1):
            cur = self.bn.at_or_before(start + k * 1000)
            if prev and cur:
                r = math.log(cur / prev)
                ss += r * r
            prev = cur
        return math.sqrt(ss / n)

    def fair_up(self, m, t, window_s=300):
        r = self.rounds.get(m)
        k = self.strike(m)
        if not r or k is None:
            return None
        end = r["end_ms"]
        if t >= end:
            s_end = self.bn.at_or_before(end, max_age_ms=2000)
            if s_end is None or self.last_ms < end:
                return None
            return 1.0 if s_end > k else (0.0 if s_end < k else 0.5)
        s = self.bn.at_or_before(t, max_age_ms=2000)
        sig = self.sigma(t, window_s)
        if s is None or sig is None:
            return None
        tau = (end - t) / 1000.0
        if sig <= 0:
            return 1.0 if s > k else (0.0 if s < k else 0.5)
        return norm_cdf(math.log(s / k) / (sig * math.sqrt(tau)))

    def ref_up(self, kind, m, t):
        """kind = 'predict' 或 'fair';过了收盘:predict 用结算兑付,fair 用币安收盘价对 K"""
        r = self.rounds.get(m)
        if self.last_ms < t:
            return None  # 日志没覆盖到 t
        if kind == "fair":
            return self.fair_up(m, t)
        if r and t >= r["end_ms"]:
            st = self.settles.get(m)
            return st[0] if st else None
        return self.predict_mid(m, t)

    def token_value(self, kind, m, t, qside):
        u = self.ref_up(kind, m, t)
        if u is None:
            return None
        return u if qside == "UpBid" else 1.0 - u

    def settle_value(self, m, qside):
        st = self.settles.get(m)
        if not st:
            return None
        return st[0] if qside == "UpBid" else st[1]


def load(paths):
    L = ProbeLog()
    for l in iter_lines(paths):
        k = l.get("kind")
        t = l.get("wall_ms")
        if isinstance(t, (int, float)):
            L.last_ms = max(L.last_ms, t)
            L.first_ms = t if L.first_ms is None else min(L.first_ms, t)
        if k == "bn":
            try:
                d = json.loads(l.get("raw", ""))
                x = d.get("data", d)
                if x.get("e", "bookTicker") == "bookTicker" and x.get("s") == "BTCUSDT":
                    L.bn.add(t, (float(x["b"]) + float(x["a"])) / 2.0)
            except (ValueError, KeyError, TypeError, AttributeError):
                pass
        elif k == "pf":
            try:
                d = json.loads(l.get("raw", ""))
            except ValueError:
                continue
            topic = d.get("topic") or ""
            if topic.startswith("predictOrderbook/"):
                data = d.get("data") or {}
                m = int(data.get("marketId") or topic.split("/", 1)[1])
                L.books.setdefault(m, Series()).add(t, _book_top(data))
        elif k == "pf_rest":
            m = int(l["market_id"])
            L.books.setdefault(m, Series()).add(t, _book_top(l))
        elif k == "start":
            L.mode = l.get("mode")
        elif k == "market":
            L.markets[l.get("market_id")] = l
        elif k == "round_start":
            rd = l.get("round") or {}
            L.rounds.setdefault(rd.get("market_id"), {"start_ms": rd.get("start_ms"), "end_ms": rd.get("end_ms"), "position": None})
        elif k == "round_end":
            rd = l.get("round") or {}
            r = L.rounds.setdefault(rd.get("market_id"), {"start_ms": rd.get("start_ms"), "end_ms": rd.get("end_ms"), "position": None})
            r["position"] = l.get("position")
        elif k == "settle":
            L.settles[l.get("market_id")] = (l.get("payout_up"), l.get("payout_down"))
        elif k == "decision" and l.get("action") == "place":
            o = l.get("order") or {}
            cid = o.get("cid")
            r = L.orders.setdefault(cid, _new_order(cid))
            ctx = l.get("ctx") or {}
            r.update(hash=o.get("hash"), market_id=o.get("market_id"), qside=o.get("qside"), price=o.get("price"), shares=o.get("shares"), decide_ms=t,
                     queue_ahead=ctx.get("queue_ahead"), best_bid=ctx.get("best_bid"), best_ask=ctx.get("best_ask"), ms_left=ctx.get("ms_left"))
        elif k == "ack":
            cid = l.get("cid")
            r = L.orders.setdefault(cid, _new_order(cid))
            r["ack_ms"], r["ack_ok"] = t, bool(l.get("ok"))
            if l.get("order_id"):
                r["order_id"] = l["order_id"]
                L.by_oid[l["order_id"]] = cid
            if not l.get("ok"):
                r["end"], r["end_ms"] = "rejected", t
        elif k == "decision" and l.get("action") == "cancel":
            r = L.orders.get(L.by_oid.get(l.get("order_id"), l.get("cid")))
            if r and r["cancel_ms"] is None:
                r["cancel_ms"], r["cancel_reason"] = t, l.get("reason")
        elif k == "decision" and l.get("action") == "cancel_all":
            for oid in l.get("known") or []:
                r = L.orders.get(L.by_oid.get(oid))
                if r and r["cancel_ms"] is None:
                    r["cancel_ms"], r["cancel_reason"] = t, "全撤: " + str(l.get("reason"))
        elif k == "cancel_ack" and l.get("ok"):
            r = L.orders.get(L.by_oid.get(l.get("order_id")))
            if r and r["cancel_ack_ms"] is None:
                r["cancel_ack_ms"] = t
        elif k == "cancel_all_done":
            for oid in l.get("ok") or []:
                r = L.orders.get(L.by_oid.get(oid))
                if r and r["cancel_ack_ms"] is None:
                    r["cancel_ack_ms"] = t
        elif k == "fill_revert":
            # 撮合后上链失败:扣回这张单最近记的成交
            o = l.get("order") or {}
            r = L.orders.get(L.by_oid.get(l.get("order_id"), o.get("cid")))
            left = -float(l.get("delta_shares") or 0)
            if r is None or left <= 0:
                continue
            r["filled"] = float(l.get("cum_shares") or 0)
            for f in reversed(r["fills"]):
                take = min(left, f["shares"])
                f["shares"] -= take
                left -= take
                if left <= 1e-12:
                    break
            r["fills"] = [f for f in r["fills"] if f["shares"] > 1e-12]
            L.fills = [f for f in L.fills if f["shares"] > 1e-12]
            r["first_fill_ms"] = r["fills"][0]["wall_ms"] if r["fills"] else None
        elif k == "fill":
            o = l.get("order") or {}
            cid = L.by_oid.get(l.get("order_id"), o.get("cid"))
            r = L.orders.get(cid)
            delta = float(l.get("delta_shares") or 0)
            if r is None or delta <= 0:
                continue
            f = {"fill_no": len(L.fills) + 1, "cid": cid, "order_id": l.get("order_id"), "market_id": r["market_id"] or o.get("market_id"),
                 "qside": r["qside"] or o.get("qside"), "price": r["price"] if r["price"] is not None else (o.get("price_cents") or 0) / 100.0,
                 "shares": delta, "cum_shares": float(l.get("cum_shares") or 0), "wall_ms": t, "source": l.get("source"),
                 "after_cancel": r["cancel_ms"] is not None and t >= r["cancel_ms"]}
            L.fills.append(f)
            r["fills"].append(f)
            r["filled"] = max(r["filled"], f["cum_shares"])
            if r["first_fill_ms"] is None:
                r["first_fill_ms"], r["first_fill_source"] = t, f["source"]
            if f["after_cancel"]:
                r["fill_after_cancel"] = True
    # 订单结局
    for r in L.orders.values():
        if r["end"] is None:
            if r["shares"] and r["filled"] >= r["shares"] - 1e-9:
                r["end"], r["end_ms"] = "filled", r["fills"][-1]["wall_ms"] if r["fills"] else None
            elif r["cancel_ack_ms"] is not None:
                r["end"], r["end_ms"] = ("cancelled_partial" if r["filled"] > 0 else "cancelled"), r["cancel_ack_ms"]
            else:
                r["end"] = "unknown"
    return L


def placed_orders(L):
    """真正挂上了的单(回报成功),按挂单时间排序"""
    return sorted((r for r in L.orders.values() if r["ack_ok"] and r["decide_ms"] is not None), key=lambda r: r["decide_ms"])


def ttl_bucket(ms_left):
    """剩余时间分组(秒)"""
    if ms_left is None:
        return "未知"
    s = ms_left / 1000.0
    for lo, hi in ((240, 300), (180, 240), (120, 180), (60, 120), (30, 60)):
        if s > lo:
            return "%d-%ds" % (lo, hi)
    return "0-30s"


TTL_ORDER = ["240-300s", "180-240s", "120-180s", "60-120s", "30-60s", "0-30s", "未知"]


def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def wmean(pairs):
    """[(值, 权重)] 加权平均"""
    pairs = [(v, w) for v, w in pairs if v is not None and w]
    tw = sum(w for _, w in pairs)
    return sum(v * w for v, w in pairs) / tw if tw else None


def tstat(xs):
    xs = [x for x in xs if x is not None]
    n = len(xs)
    if n < 2:
        return None
    m = sum(xs) / n
    var = sum((x - m) ** 2 for x in xs) / (n - 1)
    return m / math.sqrt(var / n) if var > 0 else None


def median(xs):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    n = len(xs)
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2.0


def fmt(v):
    if v is None:
        return ""
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, float):
        # 毫秒时间戳有 13 位:整数值原样写,其余 12 位有效数字
        return str(int(v)) if v.is_integer() and abs(v) < 1e15 else "%.12g" % v
    return str(v).replace("\t", " ").replace("\n", " ")


def short(v):
    """屏幕显示用:4 位有效数字(TSV 里保留完整精度)"""
    if isinstance(v, float) and not v.is_integer():
        return "%.4g" % v
    return fmt(v)


def write_tsv(path, cols, rows):
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("\t".join(cols) + "\n")
        for r in rows:
            f.write("\t".join(fmt(r.get(c)) for c in cols) + "\n")


def read_tsv(path):
    with open(path, encoding="utf-8") as f:
        head = f.readline().rstrip("\n").split("\t")
        return [dict(zip(head, line.rstrip("\n").split("\t"))) for line in f if line.strip()]
