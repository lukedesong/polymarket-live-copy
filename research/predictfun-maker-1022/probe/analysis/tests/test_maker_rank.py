"""maker_rank:手工构造链上成交 + 盘口,能手算排名。  cd probe/analysis && python3 -m unittest discover -s tests -v"""
import gzip
import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stdout

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
import maker_rank as MR  # noqa: E402

MKT = 3705433
UP, DOWN, OTHER = "111", "222", "999"
US, R1, R2, R3 = "0x" + "aa" * 20, "0x" + "b1" * 20, "0x" + "b2" * 20, "0x" + "b3" * 20
W = 10 ** 18


def pf(t, bids, asks):
    raw = json.dumps({"type": "M", "topic": "predictOrderbook/%d" % MKT, "data": {"bids": bids, "asks": asks, "marketId": MKT}})
    return {"wall_ms": t, "kind": "pf", "raw": raw}


def place(t, cid, qside, price, shares):
    return {"wall_ms": t, "kind": "decision", "action": "place", "ctx": {"queue_ahead": 0.0},
            "order": {"cid": cid, "qside": qside, "market_id": MKT, "price": price, "shares": shares, "hash": "0x%064x" % cid}}


def ack(t, cid):
    return {"wall_ms": t, "kind": "ack", "cid": cid, "ok": True, "order_id": "O%d" % cid}


def log(block, txi, li, order_hash, maker, buy, token, shares, price, taker=None):
    """一行 OrderFilled。buy = 挂单方付 USDT 买 token"""
    usd = int(round(shares * price * W))
    sh = int(round(shares * W))
    words = [0, int(token), usd, sh, 0] if buy else [int(token), 0, sh, usd, 0]
    pad = lambda a: "0x" + "0" * 24 + a[2:]  # noqa: E731
    return {"blockNumber": hex(block), "transactionIndex": hex(txi), "logIndex": hex(li), "transactionHash": "0xt%d_%d" % (block, txi),
            "topics": [MR.ORDER_FILLED, order_hash, pad(maker), pad(taker or "0x" + "cd" * 20)], "data": "0x" + "".join("%064x" % w for w in words)}


def h(cid):
    return "0x%064x" % cid


def scenario():
    L = [{"wall_ms": 0, "kind": "start", "mode": "send"},
         {"wall_ms": 0, "kind": "market", "market_id": MKT, "up_token": UP, "down_token": DOWN},
         {"wall_ms": 0, "kind": "round_start", "round": {"market_id": MKT, "start_ms": 0, "end_ms": 300000}}]
    # Up 买 0.51 这一价位:1000 时没有,2000 诞生(R1 10 股),2050 R2 又挂 5 股;Down 买 0.40 ≡ Up 卖 0.60 早就有
    L += [pf(1000, [[0.50, 30.0]], [[0.60, 50.0]]), pf(2000, [[0.51, 10.0], [0.50, 30.0]], [[0.60, 50.0]]),
          pf(2050, [[0.51, 15.0], [0.50, 30.0]], [[0.60, 50.0]]), pf(2150, [[0.51, 17.0], [0.50, 30.0]], [[0.60, 53.0]])]
    # 单 1:诞生后 100ms 决定挂 Up 买 0.51 × 2,2140 被接受 → 抢位
    L += [place(2100, 1, "UpBid", 0.51, 2), ack(2140, 1)]
    # 单 2:Down 买 0.40 × 3(老价位,不是抢位),一直没成交
    L += [place(2110, 2, "DownBid", 0.40, 3), ack(2145, 2)]
    # 单 3:诞生 1.5 秒后又挂一张 Up 买 0.51 × 1(不算抢位)
    L += [place(3500, 3, "UpBid", 0.51, 1), ack(3540, 3)]
    L += [{"wall_ms": 300000, "kind": "round_end", "round": {"market_id": MKT, "start_ms": 0, "end_ms": 300000}, "position": {}}]
    chain = {"logs": [
        # 一笔吃单(区块 100):R1 买 Up 0.51 10 股 → R2 卖 Down 0.49 5 股(同一队)→ 我们 2 股(全成)→ R3 1 股(部分)
        log(100, 0, 0, "0x" + "01" * 32, R1, True, UP, 10, 0.51),
        log(100, 0, 1, "0x" + "02" * 32, R2, False, DOWN, 5, 0.49),
        log(100, 0, 2, h(1), US, True, UP, 2, 0.51),
        log(100, 0, 3, "0x" + "03" * 32, R3, True, UP, 1, 0.51),
        log(100, 0, 4, "0x" + "ff" * 32, "0x" + "ee" * 20, False, UP, 18, 0.51, taker=MR.EXCHANGE),  # 吃单方那一行:不算
        log(100, 1, 5, "0x" + "04" * 32, R2, True, OTHER, 7, 0.3),  # 别的市场:不算
        # 区块 101:R1 买 Down 0.40 4 股(单 2 挂着期间,在单 2 前面);R3 剩下的 → 我们的单 3
        log(101, 0, 0, "0x" + "05" * 32, R1, True, DOWN, 4, 0.40),
        log(101, 1, 1, "0x" + "03" * 32, R3, True, UP, 2, 0.51),
        log(101, 1, 2, h(3), US, True, UP, 1, 0.51),
    ], "block_ms": {"100": 3000, "101": 4000}}
    return L, chain


class TestMakerRank(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.d = tempfile.mkdtemp()
        L, chain = scenario()
        with gzip.open(os.path.join(cls.d, "probe_2026102413_133355.jsonl.gz"), "wt") as f:
            for l in L:
                f.write(json.dumps(l) + "\n")
        cls.chain = os.path.join(cls.d, "chain.json")
        json.dump(chain, open(cls.chain, "w"))
        with redirect_stdout(io.StringIO()) as cls.out:
            cls.orders, cls.races, cls.riv, cls.checks = MR.report([cls.d], cls.chain, os.path.join(cls.d, "out"))

    def test_rows_and_same_queue(self):
        rows = MR.maker_rows(json.load(open(self.chain)), {UP: (MKT, "Up"), DOWN: (MKT, "Down")})
        self.assertEqual(len(rows), 7, "去掉吃单方一行和别的市场一行")
        r2 = [r for r in rows if r["maker"] == R2][0]
        self.assertEqual((r2["qside"], r2["level"], r2["buy"]), ("UpBid", 51, False), "卖 Down 0.49 与买 Up 0.51 排同一队")
        r1d = [r for r in rows if r["hash"] == "0x" + "05" * 32][0]
        self.assertEqual((r1d["qside"], r1d["level"]), ("DownBid", 40))
        self.assertEqual(rows[0]["ms"], 3000)

    def test_fifo_checks(self):
        self.assertEqual((self.checks["fifo_ok"], self.checks["fifo_bad"]), (1, 0), "我们后面还有 R3:我们此时已全成")
        self.assertEqual((self.checks["own_order_ok"], self.checks["own_order_bad"]), (1, 0))
        self.assertEqual(self.checks["our_makers"], [US])

    def test_race(self):
        self.assertEqual(len(self.races), 1, "只有单 1 是抢新价位")
        r = self.races[0]
        self.assertEqual((r["cid"], r["race_rank"], r["level_birth_ms"], r["decide_after_birth_ms"]), (1, 3, 2000, 100))
        self.assertEqual(r["race_ahead_makers"], R1[:10] + "," + R2[:10])
        self.assertEqual(r["race_behind_makers"], R3[:10])
        self.assertEqual(r["book_ahead_at_ack"], 15.0, "接受时(2140)这一价位已有 15 股")

    def test_unfilled_order_lower_bound(self):
        o2 = [r for r in self.orders if r["cid"] == 2][0]
        self.assertFalse(o2["on_chain"])
        self.assertEqual((o2["chain_ahead_orders"], o2["chain_ahead_shares"]), (1, 4.0), "挂着期间 R1 在这一队成交了 4 股 → 都在我们前面")
        o3 = [r for r in self.orders if r["cid"] == 3][0]
        self.assertEqual(o3["chain_ahead_makers"], R3[:10], "单 3 前面只有 R3:R1、R2 的成交(区块 3000 − 435)在它被接受(3540)之前")

    def test_chain_lag(self):
        rows = [{"hash": "a", "ms": 3000}, {"hash": "a", "ms": 5000}, {"hash": "b", "ms": 4000}, {"hash": "c", "ms": 4000}]
        self.assertEqual(MR.chain_lag(rows, {"a": 2600, "b": 3500}), 450, "各单第一次上链 − 撮合:400、500 → 中位 450")
        self.assertIsNone(MR.chain_lag(rows, {}))

    def test_rivals(self):
        riv = {r["maker"]: r for r in self.riv}
        self.assertEqual((riv[R1]["race_ahead_of_us"], riv[R2]["race_ahead_of_us"], riv[R3]["race_behind_us"]), (1, 1, 1))
        self.assertTrue(riv[US]["us"])
        self.assertAlmostEqual(riv[US]["shares"], 3.0)
        self.assertIn("第 1 的 0 次", self.out.getvalue())


if __name__ == "__main__":
    unittest.main()
