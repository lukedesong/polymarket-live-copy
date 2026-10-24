"""analyze_probe.py 的单测(标准库 unittest):python3 -m unittest test_analyze_probe -v"""
import gzip
import json
import os
import tempfile
import unittest

import analyze_probe as A

MKT = 3698227


def pf(t, bid, ask):
    raw = json.dumps({"type": "M", "topic": "predictOrderbook/%d" % MKT, "data": {"bids": [[bid, 10.0]], "asks": [[ask, 10.0]], "marketId": MKT}})
    return {"wall_ms": t, "kind": "pf", "raw": raw}


def bn(t, mid):
    raw = json.dumps({"stream": "btcusdt@bookTicker", "data": {"u": 1, "s": "BTCUSDT", "b": str(mid - 0.005), "B": "1", "a": str(mid + 0.005), "A": "1"}})
    return {"wall_ms": t, "kind": "bn", "raw": raw}


def place(t, cid, qside, price, shares, queue):
    return {"wall_ms": t, "kind": "decision", "action": "place", "ctx": {"queue_ahead": queue, "best_bid": 50, "best_ask": 55, "bn_mid": 80000.0, "ms_left": 200000},
            "order": {"cid": cid, "qside": qside, "market_id": MKT, "price": price, "shares": shares, "hash": "0x%064x" % cid}}


def lines():
    L = [{"wall_ms": 0, "kind": "start", "mode": "send"}, pf(0, 0.50, 0.55), bn(0, 80000.0)]
    # 单 1:Up 买 0.50,1s 后成交;5s 后买一卖一 0.40/0.45 → markout = 0.425 − 0.5 = −0.075;BTC 跌 10bp → 对 UpBid 不利 −10
    L += [place(100, 1, "UpBid", 0.50, 2, 80.0), {"wall_ms": 135, "kind": "ack", "cid": 1, "ok": True, "order_id": "A1"}]
    L += [{"wall_ms": 1100, "kind": "fill", "order_id": "A1", "source": "wallet", "cum_shares": 2.0, "delta_shares": 2.0, "order": {"cid": 1}}]
    L += [pf(3000, 0.40, 0.45), bn(3000, 80000.0 * (1 - 10e-4))]
    # 单 2:Down 买 0.45,改价撤单;撤单在路上时被吃 1 股(撤单后 REST 查到)
    L += [place(200, 2, "DownBid", 0.45, 3, 30.0), {"wall_ms": 236, "kind": "ack", "cid": 2, "ok": True, "order_id": "B2"}]
    L += [{"wall_ms": 2000, "kind": "decision", "action": "cancel", "cid": 2, "order_id": "B2", "reason": "改价 45→46"}]
    L += [{"wall_ms": 2018, "kind": "cancel_ack", "order_id": "B2", "ok": True}]
    L += [{"wall_ms": 2030, "kind": "fill", "order_id": "B2", "source": "rest_after_cancel", "cum_shares": 1.0, "delta_shares": 1.0, "order": {"cid": 2}}]
    # 单 3:Up 买,被拒
    L += [place(300, 3, "UpBid", 0.50, 2, 80.0), {"wall_ms": 340, "kind": "ack", "cid": 3, "ok": False, "order_id": None}]
    # 单 4:全撤撤掉,无成交
    L += [place(400, 4, "UpBid", 0.50, 2, 82.0), {"wall_ms": 435, "kind": "ack", "cid": 4, "ok": True, "order_id": "D4"}]
    L += [{"wall_ms": 5000, "kind": "decision", "action": "cancel_all", "reason": "冷却", "known": ["D4"]}, {"wall_ms": 5020, "kind": "cancel_all_done", "ok": ["D4"], "failed": []}]
    L += [{"wall_ms": 300000, "kind": "round_end", "round": {"market_id": MKT, "start_ms": 0, "end_ms": 300000}, "position": {"up_shares": 2.0, "down_shares": 1.0, "cost_usd": 1.45}}]
    L += [{"wall_ms": 400000, "kind": "settle", "market_id": MKT, "payout_up": 1.0, "payout_down": 0.0, "pnl": 0.55}]
    return L


class T(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        with gzip.open(os.path.join(self.d, "probe_2026102405_050000.jsonl.gz"), "wt") as f:
            for l in lines():
                f.write(json.dumps(l, ensure_ascii=False) + "\n")
            f.write('{"half line')  # 被杀时的半行

    def test_orders(self):
        res = A.analyze([self.d], 5)
        o = {r["cid"]: r for r in res["orders"]}
        self.assertEqual(o[1]["end"], "filled")
        self.assertAlmostEqual(o[1]["mid_at_fill"], 0.525)
        self.assertAlmostEqual(o[1]["mid_after"], 0.425)
        self.assertAlmostEqual(o[1]["markout"], -0.075)
        self.assertAlmostEqual(o[1]["bn_move_bp"], -10.0, places=6)
        self.assertEqual(o[1]["rest_ms"], 1100 - 135)
        self.assertEqual(o[2]["end"], "cancelled_partial")
        self.assertTrue(o[2]["fill_after_cancel"])
        self.assertEqual(o[2]["filled"], 1.0)
        self.assertEqual(o[3]["end"], "rejected")
        self.assertEqual(o[4]["end"], "cancelled")
        self.assertTrue(o[4]["cancel_reason"].startswith("全撤"))
        self.assertIsNone(o[4]["markout"])

    def test_summary(self):
        s = A.summarize(A.analyze([self.d], 5)["orders"])
        a = s["ALL"]
        self.assertEqual(a["orders"], 3, "被拒的单不算挂出")
        self.assertEqual(a["filled_orders"], 2)
        self.assertEqual(a["cancelled_orders"], 2)
        self.assertEqual(a["filled_despite_cancel"], 1)
        self.assertAlmostEqual(a["filled_despite_cancel_rate"], 0.5)
        self.assertEqual(s["UpBid"]["queue_ahead_median"], 81.0)
        self.assertEqual(a["fill_sources"], {"wallet": 1, "rest_after_cancel": 1})

    def test_markout_needs_log_coverage(self):
        # 日志在成交后 5 秒内就结束:不给 markout
        res = A.analyze([self.d], 1000)
        o = {r["cid"]: r for r in res["orders"]}
        self.assertIsNone(o[1]["mid_after"])

    def test_cli_writes_tsv(self):
        out = os.path.join(self.d, "out")
        A.main([self.d, "--out", out])
        rows = open(os.path.join(out, "probe_orders.tsv")).read().splitlines()
        self.assertEqual(len(rows), 5)
        rounds = open(os.path.join(out, "probe_rounds.tsv")).read().splitlines()
        self.assertIn("0.55", rounds[1])


if __name__ == "__main__":
    unittest.main()
