"""fill_model:排队推进的三种撤单口径(手算),以及用手工构造的链上成交 + 盘口做整套校准。
    cd probe/analysis && python3 -m unittest discover -s tests -v"""
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
sys.path.insert(0, HERE)
import fill_model as FM  # noqa: E402
import test_maker_rank as TM  # noqa: E402


def sim(events, mode, shares=5, ahead0=10.0, t0=0, t1=10_000):
    return FM.simulate(shares, t0, t1, ahead0, sorted(events, key=lambda e: (e[0], 0 if e[1] == "book" else 1)), mode)


class TestSimulate(unittest.TestCase):
    def test_fills_eat_queue_ahead_first(self):
        ev = [(100, "fill", 4.0), (200, "fill", 4.0), (300, "fill", 5.0), (400, "fill", 9.0)]
        for m in FM.MODES:
            self.assertEqual(sim(ev, m), [(300, 3.0), (400, 2.0)], m)

    def test_cancel_attribution(self):
        # 盘口 20 → 15,期间没成交:撤单 5 股。前面 10 股,后面没人
        ev = [(0, "book", 20.0), (100, "book", 15.0), (200, "fill", 8.0)]
        self.assertEqual(sim(ev, "pess"), [], "悲观:前面仍 10 股,成交 8 吃不到")
        self.assertEqual(sim(ev, "opt"), [(200, 3.0)], "乐观:前面剩 5,成交 8 → 我们 3")
        self.assertEqual(sim(ev, "mixed"), [(200, 0.5)], "一半:前面剩 7.5,成交 8 → 我们 0.5")

    def test_behind_absorbs_cancels_first_except_opt(self):
        # 10 → 14(后面来了 4)→ 11(撤 3):pess / mixed 撤的算后面;opt 算前面
        ev = [(0, "book", 10.0), (100, "book", 14.0), (200, "book", 11.0), (300, "fill", 10.0)]
        self.assertEqual(sim(ev, "pess", shares=2), [])
        self.assertEqual(sim(ev, "mixed", shares=2), [])
        self.assertEqual(sim(ev, "opt", shares=2), [(300, 2.0)], "前面 7,成交 10 → 我们 2(满)")

    def test_book_decrease_explained_by_fills_is_not_cancel(self):
        ev = [(0, "book", 20.0), (50, "fill", 5.0), (100, "book", 15.0), (200, "fill", 6.0)]
        self.assertEqual(sim(ev, "opt"), [(200, 1.0)], "减少 5 是成交,不是撤单:前面 5 → 成交 6 给我们 1")

    def test_ahead_capped_by_level_size(self):
        ev = [(0, "book", 10.0), (100, "book", 2.0), (200, "fill", 3.0)]
        self.assertEqual(sim(ev, "pess"), [(200, 1.0)], "盘口只剩 2 股,前面不可能还有 10")

    def test_outside_life_ignored(self):
        ev = [(50, "fill", 100.0), (20_000, "fill", 100.0)]
        self.assertEqual(sim(ev, "opt", t0=100, t1=10_000), [])


class TestValidate(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.d = tempfile.mkdtemp()
        L, chain = TM.scenario()
        with gzip.open(os.path.join(cls.d, "probe_2026102413_133355.jsonl.gz"), "wt") as f:
            for l in L:
                f.write(json.dumps(l) + "\n")
        cls.chain = os.path.join(cls.d, "chain.json")
        json.dump(chain, open(cls.chain, "w"))
        with redirect_stdout(io.StringIO()):
            cls.rows, cls.summary = FM.validate([cls.d], cls.chain, os.path.join(cls.d, "out"))
        cls.by = {r["cid"]: r for r in cls.rows}

    def test_order1_queue_and_fill(self):
        r = self.by[1]
        self.assertEqual(r["ahead0"], 15.0, "被接受(2140)时 Up 买 0.51 有 15 股")
        self.assertEqual(r["real_filled"], 2.0)
        # 去掉单 1 自己的链上成交:R1 10 + R2 5 吃光前面 15,R3 1 → 1 股,区块 101 的 R3 2 → 再 1 股
        for m in FM.MODES:
            self.assertEqual(r[m + "_filled"], 2.0, m)
        self.assertEqual(r["pess_first"], r["real_first"], "和真实成交在同一笔吃单里")

    def test_order2_not_filled(self):
        r = self.by[2]
        self.assertEqual((r["real_filled"], r["pess_filled"], r["opt_filled"]), (0.0, 0.0, 0.0), "前面 50 股,只成交 4")

    def test_order3_stale_book(self):
        # 单 3 被接受(3540)时最后一条盘口还是 2150 的 17 股(那笔大吃单之后没有新推送),模型只能认为前面 17 股 → 预测不成交
        r = self.by[3]
        self.assertEqual((r["ahead0"], r["real_filled"], r["opt_filled"]), (17.0, 1.0, 0.0))

    def test_summary(self):
        self.assertEqual(self.summary["real_n"], 2)
        s = self.summary["opt"]
        self.assertEqual((s["both"], s["real_only"], s["pred_only"], s["neither"]), (1, 1, 0, 1))


if __name__ == "__main__":
    unittest.main()
