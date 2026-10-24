"""东京网关单测:本机起服务,假 HOME / 假探针 / 假录制器 / 假 tokyo_live.sh。  python3 -m unittest test_gateway -v"""
import io
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
from contextlib import redirect_stdout

HERE = os.path.dirname(os.path.abspath(__file__))
TMP = tempfile.mkdtemp()
HOME = os.path.join(TMP, "home")
os.environ["GW_HOME"] = HOME
os.environ["GW_PORT"] = "0"
sys.path.insert(0, HERE)
import tokyo_client as C  # noqa: E402
import tokyo_gateway as G  # noqa: E402

TOKEN = "t" * 64


def setup_home():
    os.makedirs(os.path.join(HOME, ".tokyo_gw"))
    open(os.path.join(HOME, ".tokyo_gw", "token"), "w").write(TOKEN + "\n")
    os.makedirs(os.path.join(HOME, "pflat"))
    open(os.path.join(HOME, "pflat", ".env"), "w").write("PREDICT_PRIVATE_KEY=0xSECRET\n")
    run = os.path.join(HOME, "probe_live3_1024_2000")
    os.makedirs(run)
    os.symlink(run, os.path.join(HOME, "probe_live3"))
    open(os.path.join(run, "probe_2026102420_200000.jsonl.gz"), "wb").write(bytes(range(256)) * 400)
    open(os.path.join(run, "supervisor.log"), "w").write("[x] 启动\n")
    open(os.path.join(run, "notes.txt"), "w").write("不在白名单\n")
    os.symlink(os.path.join(HOME, "pflat", ".env"), os.path.join(run, "state.json"))  # 软链指向凭证:必须拒绝
    os.makedirs(os.path.join(HOME, "probe"))
    fake = os.path.join(HOME, "probe", "predict-mm-probe.v3")
    # 假探针:argv0 = 自己的路径(与真探针一样能被 pgrep -f '^…/probe/predict-mm-probe' 找到);把环境里的 PREDICT_* 记下来
    open(fake, "w").write('#!/bin/bash\nenv | grep -c ^PREDICT_ > "%s/probe_env_count" || true\nexec -a "$0" sleep 300\n' % HOME)
    os.chmod(fake, 0o755)
    open(os.path.join(HOME, "probe", "tokyo_live.sh"), "w").write(
        '#!/bin/bash\necho "hours=$PROBE_HOURS loss=$PROBE_LOSS rounds=$PROBE_ROUNDS"\n( exec setsid nohup %s --send >/dev/null 2>&1 < /dev/null ) &\nsleep 1\n' % fake)
    rec = os.path.join(TMP, "rec")
    os.makedirs(os.path.join(rec, "data"))
    open(os.path.join(rec, "data", "2026102420_064500.log.gz"), "wb").write(b"rec")
    open(os.path.join(rec, "data", "other.txt"), "w").write("x")
    return rec


class T(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        rec = setup_home()
        os.environ["PREDICT_PRIVATE_KEY"] = "0xSHOULD_NOT_LEAK"  # 网关进程环境里有,也不能传给空跑 / 实盘脚本
        cls.recorder = subprocess.Popen(["bash", "-c", "exec -a pfrec5_fake sleep 300"], cwd=rec)
        cls.srv = G.Server(("127.0.0.1", 0), G.H)
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()
        cls.url = "http://127.0.0.1:%d" % cls.srv.server_address[1]
        os.environ["TOKYO_GW_URL"], os.environ["TOKYO_GW_TOKEN"] = cls.url, TOKEN

    @classmethod
    def tearDownClass(cls):
        subprocess.run(["pkill", "-f", G.PROBE_PATTERN])
        cls.recorder.kill()
        cls.srv.shutdown()

    def req(self, method, path, body=None, tok=TOKEN):
        h = {"content-type": "application/json"}
        if tok is not None:
            h["Authorization"] = "Bearer " + tok
        r = urllib.request.Request(self.url + path, data=json.dumps(body).encode() if body is not None else None, method=method, headers=h)
        try:
            with urllib.request.urlopen(r, timeout=60) as x:
                return x.status, x.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()

    def test_1_auth(self):
        self.assertEqual(self.req("GET", "/status", tok=None)[0], 401)
        self.assertEqual(self.req("GET", "/status", tok="wrong")[0], 401)
        self.assertEqual(self.req("POST", "/stop", {}, tok="t" * 63)[0], 401)
        st, b = self.req("GET", "/status")
        self.assertEqual(st, 200)
        s = json.loads(b)
        self.assertIn("pfrec5_fake", s["recorder_processes"])
        self.assertNotIn("pgrep", s["recorder_processes"], "不列出查询命令自己")
        log = open(os.path.join(HOME, ".tokyo_gw", "access.log")).read()
        self.assertNotIn(TOKEN, log)

    def test_2_files_whitelist(self):
        st, b = self.req("GET", "/files?root=probe")
        names = sorted(os.path.basename(x["path"]) for x in json.loads(b))
        self.assertEqual(names, ["probe_2026102420_200000.jsonl.gz", "supervisor.log"], "state.json 是指向凭证的软链,不能列出")
        st, b = self.req("GET", "/files?root=recorder")
        self.assertEqual([os.path.basename(x["path"]) for x in json.loads(b)], ["2026102420_064500.log.gz"])
        for bad in ("pflat/.env", "probe_live3/../pflat/.env", "probe_live3/state.json", "probe_live3/notes.txt", "/etc/passwd", "../../etc/passwd", ""):
            st, b = self.req("GET", "/file?path=" + urllib.request.quote(bad))
            self.assertEqual(st, 403, bad)
            self.assertNotIn(b"SECRET", b)

    def test_3_download_chunks(self):
        rel = "probe_live3/probe_2026102420_200000.jsonl.gz"
        st, b = self.req("GET", "/file?path=%s&offset=100&length=50" % rel)
        self.assertEqual((st, b), (200, (bytes(range(256)) * 400)[100:150]))
        out = os.path.join(TMP, "dl")
        C.CHUNK = 7777
        dst, n = C.get_file(rel, out)
        self.assertEqual(open(dst, "rb").read(), bytes(range(256)) * 400)
        dst, n2 = C.get_file(rel, out)  # 再取一次:已完整,不重复
        self.assertEqual(n2, n)
        self.assertEqual(os.path.getsize(dst), 256 * 400)

    def test_4_param_validation(self):
        for body in ({"hours": 24, "loss": 101}, {"hours": 0, "loss": 30}, {"hours": "24", "loss": 30}, {"hours": 49}, {"rounds": 301}, {"rounds": -1}, {"loss": True}):
            self.assertEqual(self.req("POST", "/start", body)[0], 400, body)
        self.assertEqual(self.req("POST", "/dryrun", {"secs": 5})[0], 400)
        self.assertEqual(self.req("POST", "/nope", {})[0], 404)

    def test_5_start_stop_dryrun(self):
        st, b = self.req("POST", "/start", {"hours": 2, "loss": 85, "rounds": 3})
        r = json.loads(b)
        self.assertEqual(st, 200, r)
        self.assertIn("hours=2 loss=85 rounds=3", r["output"])
        self.assertTrue(G.probe_pids())
        self.assertEqual(self.req("POST", "/start", {"hours": 2, "loss": 20})[0], 409, "已在跑不能再启动")
        self.assertEqual(self.req("POST", "/dryrun", {"secs": 60})[0], 409)
        st, b = self.req("POST", "/stop", {})
        self.assertTrue(json.loads(b)["stopped"])
        self.assertFalse(G.probe_pids())
        st, b = self.req("POST", "/dryrun", {"secs": 60})
        r = json.loads(b)
        self.assertEqual(st, 200, r)
        self.assertTrue(r["log_dir"].startswith("probe_gw_dry_"))
        self.assertTrue(G.probe_pids())
        self.assertEqual(open(os.path.join(HOME, "probe_env_count")).read().strip(), "0", "空跑不能带任何 PREDICT_* 变量")
        self.req("POST", "/stop", {})
        self.assertFalse(G.probe_pids())

    def test_6_client_cli(self):
        buf = io.StringIO()
        with redirect_stdout(buf):
            self.assertEqual(C.main(["status"]), 0)
            self.assertEqual(C.main(["ls", "recorder"]), 0)
        self.assertIn("2026102420_064500.log.gz", buf.getvalue())
        self.assertNotIn(TOKEN, buf.getvalue())


if __name__ == "__main__":
    unittest.main()
