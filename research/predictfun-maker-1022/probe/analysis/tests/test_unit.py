"""手工构造、能手算结果的小日志:逐项核对数值。  cd probe/analysis && python3 -m unittest discover -s tests -v"""
import gzip
import io
import json
import math
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stdout

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
import markouts as M  # noqa: E402
import probelog as P  # noqa: E402
import queue_position as Q  # noqa: E402
import replay_compare as R  # noqa: E402

MKT = 3698227
K = 80000.0
A = 1e-4  # 币安每秒交替 ±A 的对数收益 → σ 恰好 = A


def pf(t, bid, ask, bsz=10.0, asz=10.0):
    raw = json.dumps({"type": "M", "topic": "predictOrderbook/%d" % MKT, "data": {"bids": [[bid, bsz]], "asks": [[ask, asz]], "marketId": MKT}})
    return {"wall_ms": t, "kind": "pf", "raw": raw}


def bn(t, mid):
    raw = json.dumps({"stream": "btcusdt@bookTicker", "data": {"u": 1, "s": "BTCUSDT", "b": repr(mid - 0.005), "B": "1", "a": repr(mid + 0.005), "A": "1"}})
    return {"wall_ms": t, "kind": "bn", "raw": raw}


def place(t, cid, qside, price, shares, queue):
    return {"wall_ms": t, "kind": "decision", "action": "place", "ctx": {"queue_ahead": queue, "best_bid": 50, "best_ask": 55, "bn_mid": K, "ms_left": 300000 - t},
            "order": {"cid": cid, "qside": qside, "market_id": MKT, "price": price, "shares": shares, "hash": "0x%064x" % cid}}


def ack(t, cid, oid, ok=True):
    return {"wall_ms": t, "kind": "ack", "cid": cid, "ok": ok, "order_id": oid if ok else None}


def fill(t, oid, cid, cum, delta, src="wallet"):
    return {"wall_ms": t, "kind": "fill", "order_id": oid, "source": src, "cum_shares": cum, "delta_shares": delta, "order": {"cid": cid}}


def lines():
    L = [{"wall_ms": 0, "kind": "start", "mode": "send"}, {"wall_ms": 0, "kind": "round_start", "round": {"market_id": MKT, "start_ms": 0, "end_ms": 300000}}]
    # 币安:0..400 秒每秒一条,偶数秒 K、奇数秒 K·e^A
    L += [bn(s * 1000, K * (math.exp(A) if s % 2 else 1.0)) for s in range(0, 401)]
    L += [pf(0, 0.50, 0.55)]
    # 单 1:Up 买 0.50 × 2,t=101s 成交(钱包);5s 后盘口 0.40/0.45
    L += [place(100500, 1, "UpBid", 0.50, 2, 80.0), ack(100535, 1, "A1"), fill(101000, "A1", 1, 2.0, 2.0)]
    L += [pf(103000, 0.40, 0.45), pf(140000, 0.30, 0.34)]
    # 单 2:Down 买 0.45 × 3,改价撤单;撤单在路上时被吃 1 股(撤单后查单发现)
    L += [place(100600, 2, "DownBid", 0.45, 3, 30.0), ack(100636, 2, "B2")]
    L += [{"wall_ms": 102000, "kind": "decision", "action": "cancel", "cid": 2, "order_id": "B2", "reason": "改价 45→46"},
          {"wall_ms": 102018, "kind": "cancel_ack", "order_id": "B2", "ok": True}, fill(102030, "B2", 2, 1.0, 1.0, "rest_after_cancel")]
    # 单 3:被拒
    L += [place(100700, 3, "UpBid", 0.50, 2, 80.0), ack(100740, 3, None, ok=False)]
    # 单 4:前面 5 股,全撤撤掉,没成交
    L += [place(100800, 4, "UpBid", 0.50, 2, 5.0), ack(100835, 4, "D4"),
          {"wall_ms": 150000, "kind": "decision", "action": "cancel_all", "reason": "冷却", "known": ["D4"]}, {"wall_ms": 150020, "kind": "cancel_all_done", "ok": ["D4"], "failed": []}]
    # 单 5:前面 200 股,分两次成交(1 + 1)
    L += [place(100900, 5, "UpBid", 0.50, 2, 200.0), ack(100935, 5, "E5"), fill(120000, "E5", 5, 1.0, 1.0, "rest_list"), fill(121000, "E5", 5, 2.0, 1.0, "rest_detail")]
    L += [{"wall_ms": 300000, "kind": "round_end", "round": {"market_id": MKT, "start_ms": 0, "end_ms": 300000}, "position": {"up_shares": 4.0, "down_shares": 1.0, "cost_usd": 2.45}}]
    L += [{"wall_ms": 400000, "kind": "settle", "market_id": MKT, "payout_up": 1.0, "payout_down": 0.0, "pnl": 1.55}]
    return L


class Base(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.d = tempfile.mkdtemp()
        with gzip.open(os.path.join(cls.d, "probe_2026102405_050000.jsonl.gz"), "wt") as f:
            for l in lines():
                f.write(json.dumps(l, ensure_ascii=False) + "\n")
            f.write('{"half line')  # 进程被杀时的半行
        cls.L = P.load([cls.d])


class TestLoad(Base):
    def test_orders(self):
        o = self.L.orders
        self.assertEqual(o[1]["end"], "filled")
        self.assertEqual(o[2]["end"], "cancelled_partial")
        self.assertTrue(o[2]["fill_after_cancel"])
        self.assertEqual(o[3]["end"], "rejected")
        self.assertEqual(o[4]["end"], "cancelled")
        self.assertTrue(o[4]["cancel_reason"].startswith("全撤"))
        self.assertEqual(o[5]["filled"], 2.0)
        self.assertEqual(len(o[5]["fills"]), 2)
        self.assertEqual([r["cid"] for r in P.placed_orders(self.L)], [1, 2, 4, 5], "被拒的单不算挂出")

    def test_reference_prices(self):
        L = self.L
        self.assertAlmostEqual(L.predict_mid(MKT, 101000), 0.525)
        self.assertAlmostEqual(L.predict_mid(MKT, 106000), 0.425)
        self.assertEqual(L.strike(MKT), K)
        # σ:交替 ±A → 恰好 A
        self.assertAlmostEqual(L.sigma(101000), A, places=12)
        self.assertAlmostEqual(L.sigma(350000), A, places=12)
        self.assertIsNone(L.sigma(30000), "不足 60 秒数据")
        # t=101s:S = K·e^A,τ = 199s → Φ(A / (A·√199)) = Φ(1/√199)
        self.assertAlmostEqual(L.fair_up(MKT, 101000), P.norm_cdf(1 / math.sqrt(199)), places=9)
        # 收盘时 S(300s,偶数秒)= K → 平局 0.5
        self.assertEqual(L.fair_up(MKT, 300000), 0.5)
        # 过了收盘:predict 用结算兑付
        self.assertEqual(L.ref_up("predict", MKT, 300500), 1.0)
        # 日志没覆盖到的时刻不给
        self.assertIsNone(L.ref_up("predict", MKT, 500000))


class TestMarkouts(Base):
    def test_fill_rows(self):
        rows = {(r["cid"], r["fill_no"]): r for r in M.fill_markouts(self.L, (1, 5, 30))}
        r = [v for k, v in rows.items() if k[0] == 1][0]
        self.assertEqual(r["side"], "买Up")
        self.assertEqual(r["ttl"], "180-240s")
        self.assertAlmostEqual(r["predict_at_fill"], 0.525)
        self.assertAlmostEqual(r["pnl_predict_1s"], 0.025)
        self.assertAlmostEqual(r["pnl_predict_5s"], 0.425 - 0.5)
        self.assertAlmostEqual(r["pnl_predict_30s"], 0.425 - 0.5)
        self.assertAlmostEqual(r["pnl_settle"], 0.5)
        # fair:t+5 = 106s(偶数秒,S = K)→ 0.5 − 0.5 = 0
        self.assertAlmostEqual(r["pnl_fair_5s"], 0.0, places=9)
        self.assertAlmostEqual(r["pnl_fair_1s"], P.norm_cdf(0) - 0.5, places=9)
        # Down:我们的价值 = 1 − Up 参考价;结算 Up 赢 → Down 0 − 0.45
        d = [v for k, v in rows.items() if k[0] == 2][0]
        self.assertEqual(d["side"], "卖Up(买Down)")
        self.assertAlmostEqual(d["pnl_predict_1s"], (1 - 0.425) - 0.45)
        self.assertAlmostEqual(d["pnl_settle"], -0.45)
        self.assertTrue(d["after_cancel"])

    def test_groups_and_sources(self):
        rows = M.fill_markouts(self.L, (1, 5, 30))
        g = M.group(rows, (1, 5, 30))
        allrow = [x for x in g if x["side"] == "全部" and x["ttl"] == "全部" and x["horizon"] == "settle"][0]
        # 结算:单1 +0.5×2,单2 −0.45×1,单5 +0.5×(1+1) → 总 1.55;股数 5
        self.assertEqual(allrow["fills"], 4)
        self.assertAlmostEqual(allrow["shares"], 5.0)
        self.assertAlmostEqual(allrow["pnl_total"], 1.55)
        self.assertAlmostEqual(allrow["pnl_per_share"], 1.55 / 5)
        self.assertAlmostEqual(allrow["pnl_per_fill"], (0.5 - 0.45 + 0.5 + 0.5) / 4)
        only_wallet = M.fill_markouts(self.L, (1,), {"wallet"})
        self.assertEqual([r["cid"] for r in only_wallet], [1])

    def test_cli(self):
        out = os.path.join(self.d, "m")
        with redirect_stdout(io.StringIO()):
            M.main([self.d, "--out", out])
        self.assertEqual(len(P.read_tsv(os.path.join(out, "markout_fills.tsv"))), 4)
        self.assertTrue(P.read_tsv(os.path.join(out, "markout_groups.tsv")))


class TestQueue(Base):
    def test_fixed_edges(self):
        rows, e = Q.order_rows(self.L, (1, 5, 30), edges=(10.0, 100.0))
        b = {r["cid"]: r["bucket"] for r in rows}
        self.assertEqual(b, {1: "队中", 2: "队中", 4: "队首", 5: "队尾"})
        g = {(x["side"], x["bucket"]): x for x in Q.group(rows, (1, 5, 30))}
        self.assertEqual(g[("全部", "队首")]["fill_rate"], 0.0)
        self.assertEqual(g[("全部", "队中")]["fill_rate"], 1.0)
        self.assertEqual(g[("全部", "队尾")]["orders"], 1)
        self.assertAlmostEqual(g[("全部", "全部")]["share_fill_rate"], 5 / 9)
        self.assertAlmostEqual(g[("全部", "队中")]["pnl_settle"], (0.5 * 2 - 0.45 * 1) / 3)
        self.assertAlmostEqual(rows[0]["ahead_in_orders"], 40.0)

    def test_tercile_edges(self):
        rows, e = Q.order_rows(self.L, (1,))
        self.assertEqual(sum(1 for r in rows if r["bucket"] != "未知"), 4)
        self.assertLessEqual(e[0], e[1])


class TestReplayCompare(Base):
    def live(self):
        out = os.path.join(self.d, "live.tsv")
        with redirect_stdout(io.StringIO()):
            R.main(["export", self.d, "--out", out])
        return out, P.read_tsv(out)

    def test_export(self):
        _, rows = self.live()
        x = {r["order_id"]: r for r in rows}
        self.assertEqual(set(x), {"A1", "B2", "D4", "E5"})
        self.assertEqual(x["B2"]["book_side"], "ask")
        self.assertEqual(x["B2"]["up_level"], "0.55")
        self.assertEqual(x["B2"]["remove_ms"], "102018")
        self.assertEqual(x["A1"]["remove_ms"], "101000")
        self.assertEqual(x["D4"]["remove_ms"], "150020")
        self.assertEqual(x["A1"]["post_ms"], "100535")

    def test_compare_biased_replay(self):
        path, rows = self.live()
        # 回放:只成交 A1(晚 300ms),E5 只成交 1 股
        rp = os.path.join(self.d, "rp.tsv")
        P.write_tsv(rp, R.RP_COLS, [{"order_id": "A1", "rp_filled_shares": 2, "rp_first_fill_ms": 101300, "rp_fill_price": 0.5},
                                    {"order_id": "E5", "rp_filled_shares": 1, "rp_first_fill_ms": 120000, "rp_fill_price": 0.5}])
        out = os.path.join(self.d, "cmp")
        buf = io.StringIO()
        with redirect_stdout(buf):
            R.main(["compare", path, rp, "--out", out])
        s = {(r["dim"], r["group"]): r for r in P.read_tsv(os.path.join(out, "compare_summary.tsv"))}
        a = s[("全部", "全部")]
        self.assertEqual((a["live_filled_orders"], a["replay_filled_orders"]), ("3", "2"))
        self.assertAlmostEqual(float(a["fill_rate_ratio"]), 2 / 3, places=9)
        self.assertAlmostEqual(float(a["share_ratio"]), 3 / 5)
        self.assertEqual((a["both"], a["live_only"], a["replay_only"], a["neither"]), ("2", "1", "0", "1"))
        self.assertAlmostEqual(float(a["time_diff_ms_median"]), 150.0)
        self.assertIn("回放低估成交", buf.getvalue())
        self.assertIn(("方向", "卖Up(买Down)"), s)

    def test_compare_identical_replay(self):
        path, rows = self.live()
        rp = os.path.join(self.d, "same.tsv")
        P.write_tsv(rp, R.RP_COLS, [{"order_id": r["order_id"], "rp_filled_shares": r["live_filled"], "rp_first_fill_ms": r["live_first_fill_ms"], "rp_fill_price": r["live_fill_price"]}
                                    for r in rows if float(r["live_filled"]) > 0])
        rows2 = R.compare_rows(P.read_tsv(path), P.read_tsv(rp))
        s, _ = R.summarize(rows2)
        a = s[0]
        self.assertEqual(a["fill_rate_ratio"], 1.0)
        self.assertEqual(a["time_diff_ms_median"], 0.0)
        self.assertIn("基本一致", R.verdict(a))

    def test_replay_without_mm_grid_fails_clearly(self):
        path, _ = self.live()
        with self.assertRaises(SystemExit) as cm:
            R.main(["replay", path, "--model-dir", os.path.join(self.d, "no_such_model"), "--out", os.path.join(self.d, "x.tsv")])
        self.assertIn("mm_grid.py", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
