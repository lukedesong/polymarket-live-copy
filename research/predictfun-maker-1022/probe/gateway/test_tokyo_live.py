"""tokyo_live.sh 单测:假 RPC(返回链上余额)、假探针(记下收到的参数),核对自动定的参数与轮数、重复启动。
    python3 -m unittest test_tokyo_live -v"""
import http.server
import json
import os
import subprocess
import tempfile
import threading
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(os.path.dirname(HERE), "tokyo_live.sh")


class RPC(http.server.BaseHTTPRequestHandler):
    balance = 391.91

    def log_message(self, *a):
        pass

    def do_POST(self):
        self.rfile.read(int(self.headers.get("content-length") or 0))
        b = json.dumps({"jsonrpc": "2.0", "id": 1, "result": hex(int(RPC.balance * 1e18))}).encode()
        self.send_response(200)
        self.send_header("content-length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)


class T(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.srv = http.server.HTTPServer(("127.0.0.1", 0), RPC)
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()

    def setUp(self):
        self.h = tempfile.mkdtemp()
        os.makedirs(os.path.join(self.h, "probe"))
        os.makedirs(os.path.join(self.h, "pflat"))
        open(os.path.join(self.h, "pflat", ".env"), "w").write("PREDICT_API_KEY=k\nPREDICT_PRIVATE_KEY=0xabc\nPREDICT_ACCOUNT_ADDRESS=0x00000000000000000000000000000000000000Ab\n")
        self.bin = os.path.join(self.h, "probe", "predict-mm-probe.v4")
        # 假探针:记下参数和 PREDICT_* 变量个数,然后以自己的路径为 argv0 睡着(和真探针一样能被 pgrep 找到)
        open(self.bin, "w").write('#!/bin/bash\n[ "$1" = --version ] && { echo "predict-mm-probe 0.4.0"; exit 0; }\necho "$@" > "%s/args"\nenv | grep -c ^PREDICT_ > "%s/nenv"\nexec -a "$0" sleep 120\n' % (self.h, self.h))
        os.chmod(self.bin, 0o755)
        os.symlink(self.bin, os.path.join(self.h, "probe", "predict-mm-probe.current"))
        self.state = os.path.join(self.h, "probe", "state_live.json")

    def tearDown(self):
        subprocess.run(["pkill", "-f", "^" + os.path.join(self.h, "probe", "predict-mm-probe")])

    def run_live(self, **env):
        e = dict(os.environ, PROBE_HOME_DIR=self.h, PROBE_RPC="http://127.0.0.1:%d" % self.srv.server_address[1], **env)
        for k in list(e):
            if k.startswith("PREDICT_"):
                del e[k]
        return subprocess.run(["bash", SCRIPT], env=e, capture_output=True, text=True, timeout=60)

    def args(self):
        for _ in range(50):
            p = os.path.join(self.h, "args")
            if os.path.exists(p) and open(p).read().strip():
                a = open(p).read().split()
                return {a[i]: a[i + 1] for i in range(len(a) - 1) if a[i].startswith("--") and not a[i + 1].startswith("--")}
            time.sleep(0.1)
        self.fail("假探针没启动")

    def test_full_balance_three_rounds(self):
        RPC.balance = 391.91
        r = self.run_live(PROBE_HOURS="1", PROBE_LOSS="30", PROBE_ROUNDS="3")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        a = self.args()
        # 不压缩:按真实余额 → 未结算上限 min(391.9−5, 500) = 386,每轮两边合计 min(150, 386/2) = 150,不匀速
        self.assertEqual((a["--capital-cap-usd"], a["--gross-cap-usd"], a["--gross-per-min-usd"]), ("386", "150", "0"))
        self.assertEqual((a["--max-daily-loss-usd"], a["--max-total-loss-usd"]), ("30", "30"))
        self.assertEqual(a["--duration-s"], "3600")
        into = int(time.time()) % 300
        self.assertIn(a["--max-rounds"], ("3", "4"))
        if into > 15:
            self.assertEqual(a["--max-rounds"], "4", "当前轮已开始:这半轮另算一轮")
        self.assertEqual(open(os.path.join(self.h, "nenv")).read().strip(), "3", "实盘要带上 .env 的三个变量")
        live = os.path.realpath(os.path.join(self.h, "probe_live3"))
        self.assertIn("启动:存款地址 0x00000000000000000000000000000000000000ab 链上余额 $391.91,按 $391 计", open(os.path.join(live, "supervisor.log")).read())
        # 已在跑:拒绝,且不动 probe_live3 软链
        time.sleep(61)  # 目录名精确到分钟:换一分钟才会是新目录
        r2 = self.run_live(PROBE_ROUNDS="3")
        self.assertNotEqual(r2.returncode, 0)
        self.assertEqual(os.path.realpath(os.path.join(self.h, "probe_live3")), live, "被拒的启动不能改软链")

    def test_cumulative_ledger(self):
        RPC.balance = 391.91
        r = self.run_live(PROBE_LOSS="100", PROBE_ROUNDS="3")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        a = self.args()
        self.assertEqual(a["--state-file"], self.state, "账本固定一个文件,跨次累计")
        self.assertIn("--clear-halt", open(os.path.join(self.h, "args")).read())
        d = json.load(open(self.state))
        self.assertAlmostEqual(d["realized_total"], -37.01, msg="第一次:记入之前已亏的 37.01")
        live = os.path.realpath(os.path.join(self.h, "probe_live3"))
        self.assertTrue(os.path.exists(os.path.join(live, "state_at_start.json")))
        log = open(os.path.join(live, "supervisor.log")).read()
        self.assertIn("记入之前已亏 $37.01", log)
        self.assertIn("predict-mm-probe.v4 sha256", log)
        # 第二次:账本已在,不重置
        subprocess.run(["pkill", "-f", "^" + os.path.join(self.h, "probe", "predict-mm-probe")])
        time.sleep(0.5)
        d["realized_total"] = -50.0
        json.dump(d, open(self.state, "w"))
        os.remove(os.path.join(self.h, "args"))
        r = self.run_live(PROBE_LOSS="100", PROBE_PRIOR_LOSS="1")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.args()
        self.assertAlmostEqual(json.load(open(self.state))["realized_total"], -50.0)
        self.assertIn("累计已实现 -50.00", r.stdout)

    def test_bad_account_address(self):
        open(os.path.join(self.h, "pflat", ".env"), "w").write("PREDICT_API_KEY=k\nPREDICT_PRIVATE_KEY=0xabc\nPREDICT_ACCOUNT_ADDRESS=0xdef\n")
        r = self.run_live()
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("PREDICT_ACCOUNT_ADDRESS", r.stdout)
        self.assertNotIn("0xabc", r.stdout + r.stderr, "私钥不能出现在输出里")

    def test_missing_bin_or_bad_prior(self):
        self.assertNotEqual(self.run_live(PROBE_PRIOR_LOSS="-3").returncode, 0)
        os.remove(os.path.join(self.h, "probe", "predict-mm-probe.current"))
        r = self.run_live()
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("找不到探针程序", r.stdout)

    def test_balance_cap_option(self):
        RPC.balance = 391.91
        r = self.run_live(PROBE_BALANCE_CAP="200")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        a = self.args()
        self.assertEqual((a["--capital-cap-usd"], a["--gross-cap-usd"], a["--max-rounds"]), ("195", "97", "0"))

    def test_bad_params_and_low_balance(self):
        self.assertNotEqual(self.run_live(PROBE_ROUNDS="three").returncode, 0)
        RPC.balance = 8.0
        r = self.run_live()
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("太少", r.stdout)


if __name__ == "__main__":
    unittest.main()
