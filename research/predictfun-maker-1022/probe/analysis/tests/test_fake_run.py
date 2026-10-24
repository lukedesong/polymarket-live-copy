"""用端到端模拟器生成的假日志(tests/data/fake_run/)测三个工具,对照模拟器写下的真值 mock_truth.json。
生成方法见 ../README.md「测试」。"""
import collections
import io
import json
import os
import statistics
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

D = os.path.join(HERE, "data", "fake_run")


class FakeRun(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.L = P.load([D])
        with open(os.path.join(D, "mock_truth.json"), encoding="utf-8") as f:
            cls.T = json.load(f)
        cls.kind = {}
        for f in cls.T["fills"]:
            cls.kind.setdefault(f["order_id"], f["kind"])
        cls.rows = M.fill_markouts(cls.L, (1, 5, 30))
        cls.out = tempfile.mkdtemp()

    def test_data_shape(self):
        self.assertEqual(self.L.mode, "send")
        self.assertGreater(len(P.placed_orders(self.L)), 300)
        self.assertGreater(len(self.L.fills), 100)
        self.assertGreaterEqual(len(self.L.settles), 1, "应跨过一次结算")
        self.assertTrue(any(self.L.strike(m) is not None for m in self.L.rounds), "至少一轮有 K(日志覆盖开盘)")

    def test_fills_match_truth(self):
        truth = collections.defaultdict(float)
        for f in self.T["fills"]:
            truth[f["order_id"]] += f["shares"]
        live = {o["order_id"]: o["filled"] for o in self.L.orders.values() if o["order_id"]}
        for oid, sh in truth.items():
            self.assertIn(oid, live)
            self.assertAlmostEqual(live[oid], sh, places=6, msg=oid)
        self.assertEqual({k for k, v in live.items() if v > 0}, set(truth), "日志里不应多出成交")

    def test_strike_sigma_fair_match_truth(self):
        L = self.L
        for m, k in self.T["k"].items():
            kt = L.strike(int(m))
            if kt is not None:
                self.assertLess(abs(kt / k - 1), 1e-4, "K %s" % m)
        sig = L.sigma(L.last_ms - 1000)
        self.assertLess(abs(sig / self.T["sigma_per_sqrt_s"] - 1), 0.35, "σ 估计 %s vs 真值 %s" % (sig, self.T["sigma_per_sqrt_s"]))
        d = [abs(L.fair_up(f["market_id"], f["wall_ms"]) - f["up_fair"]) for f in self.T["fills"] if L.fair_up(f["market_id"], f["wall_ms"]) is not None]
        self.assertGreater(len(d), 30)
        self.assertLess(statistics.mean(d), 0.04)
        self.assertLess(max(d), 0.12)

    def test_settle_pnl(self):
        res = self.T["resolutions"]
        n = 0
        for r in self.rows:
            tr = res.get(str(r["market_id"]))
            if not tr:
                continue
            pay = tr["payout_up"] if r["qside"] == "UpBid" else tr["payout_down"]
            self.assertAlmostEqual(r["pnl_settle"], pay - r["price"], places=9)
            n += 1
        self.assertGreater(n, 10)

    def test_adverse_fills_lose_on_fair(self):
        def m(kind, col):
            return statistics.mean([r[col] for r in self.rows if r[col] is not None and self.kind.get(r["order_id"]) == kind])

        # 模拟器的公允价严格夹在买一卖一之间:被市场穿过的成交(逆向)成交价在公允价不利一侧,
        # 随机吃到的在有利一侧。工具用币安价自己算的 fair 应该看出这个差别
        self.assertLess(m("through", "pnl_fair_1s"), 0)
        self.assertGreater(m("random", "pnl_fair_1s"), 0)
        self.assertLess(m("through", "pnl_fair_5s"), m("random", "pnl_fair_5s"))

    def test_markout_groups_consistent(self):
        g = M.group(self.rows, (1, 5, 30))
        tot = [x for x in g if x["side"] == "全部" and x["ttl"] == "全部" and x["horizon"] == "predict_1s"][0]
        parts = [x for x in g if x["side"] != "全部" and x["ttl"] != "全部" and x["horizon"] == "predict_1s"]
        self.assertEqual(tot["fills"], sum(x["fills"] for x in parts))
        self.assertAlmostEqual(tot["pnl_total"], sum(x["pnl_total"] for x in parts), places=9)
        self.assertTrue({x["ttl"] for x in parts} <= set(P.TTL_ORDER))

    def test_queue_matches_truth_and_front_fills_more_randomly(self):
        ta = {o["order_id"]: o["ahead"] for o in self.T["orders"]}
        rows, e = Q.order_rows(self.L, (1, 5, 30))
        same = sum(1 for r in rows if ta.get(r["order_id"]) == r["queue_ahead"])
        # 探针看到的盘口比模拟器晚一拍,少数单对不上(那一刻最优价刚变)
        self.assertGreater(same / len(rows), 0.7)
        self.assertEqual(sum(1 for r in rows if r["bucket"] in ("队首", "队中", "队尾")), len(rows))

        def rand_rate(b):
            rs = [r for r in rows if r["bucket"] == b]
            return sum(1 for r in rs if self.kind.get(r["order_id"]) == "random") / len(rs)

        # 随机对手单要先吃掉前面的排队:排得越靠前越容易被随机吃到
        self.assertGreater(rand_rate("队首"), rand_rate("队尾"))

    def test_cli_outputs(self):
        with redirect_stdout(io.StringIO()):
            M.main([D, "--out", self.out])
            Q.main([D, "--out", self.out])
            R.main(["export", D, "--out", os.path.join(self.out, "live_orders.tsv")])
        for f in ("markout_fills.tsv", "markout_groups.tsv", "queue_orders.tsv", "queue_groups.tsv", "live_orders.tsv"):
            self.assertTrue(P.read_tsv(os.path.join(self.out, f)), f)

    def test_replay_compare_detects_pessimistic_replay(self):
        live_path = os.path.join(self.out, "live_orders2.tsv")
        with redirect_stdout(io.StringIO()):
            R.main(["export", D, "--out", live_path])
        live = P.read_tsv(live_path)
        # 假回放:只成交被市场穿过的单(相当于"永远排队尾"),成交时刻晚 300ms
        rp = [{"order_id": r["order_id"], "rp_filled_shares": r["live_filled"], "rp_first_fill_ms": float(r["live_first_fill_ms"]) + 300, "rp_fill_price": r["live_fill_price"]}
              for r in live if float(r["live_filled"]) > 0 and self.kind.get(r["order_id"]) == "through"]
        rp_path = os.path.join(self.out, "rp.tsv")
        P.write_tsv(rp_path, R.RP_COLS, rp)
        buf = io.StringIO()
        with redirect_stdout(buf):
            R.main(["compare", live_path, rp_path, "--out", self.out])
        s = {(x["dim"], x["group"]): x for x in P.read_tsv(os.path.join(self.out, "compare_summary.tsv"))}
        a = s[("全部", "全部")]
        self.assertLess(float(a["fill_rate_ratio"]), 1.0)
        self.assertEqual(a["replay_only"], "0")
        self.assertGreater(int(a["live_only"]), 0)
        self.assertAlmostEqual(float(a["time_diff_ms_median"]), 300.0)
        self.assertIn("回放低估成交", buf.getvalue())
        self.assertIn("晚 300ms", buf.getvalue())


if __name__ == "__main__":
    unittest.main()
