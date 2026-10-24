#!/usr/bin/env python3
"""东京机 HTTPS 网关(只认白名单操作,不提供任意命令执行)。只监听 127.0.0.1,由 Cloudflare 隧道转成 https 地址。

每个请求都要带请求头 `Authorization: Bearer <口令>`(口令在 ~/.tokyo_gw/token,chmod 600)。
  GET  /status                         探针 / 录制器进程、本次运行 supervisor.log 末尾、成交数、最近 halt/exit/balance、磁盘、负载
  POST /start   {"hours":24,"loss":30,"rounds":0}   调 ~/probe/tokyo_live.sh 启动实盘(hours 1~48,loss 1~100,rounds 0~300,0 = 不限);
                                       已有探针在跑就拒绝。loss > 30 需要探针程序的硬上限也已放开(否则探针自己拒绝启动)
  POST /stop                           pkill -INT 探针(探针先全撤再退出)
  POST /dryrun  {"secs":600}             空跑(不带任何凭证,--dry-run);已有探针在跑就拒绝
  GET  /files?root=probe|recorder      列出可下载的文件(探针日志目录 / 录制器 data 目录)
  GET  /file?path=<相对 HOME 的路径>[&offset=&length=]   下载;只允许上面两类目录里、文件名匹配白名单的文件
凭证文件(~/pflat/.env 等)、录制器进程本身一律不碰。访问记录写 ~/.tokyo_gw/access.log(不含口令)。
"""
import glob
import hmac
import http.server
import json
import os
import re
import shutil
import socketserver
import subprocess
import time
import urllib.parse

HOME = os.environ.get("GW_HOME") or os.path.expanduser("~")
GW_DIR = os.path.join(HOME, ".tokyo_gw")
PORT = int(os.environ.get("GW_PORT", "8787"))
PROBE_BIN = os.path.join(HOME, "probe", "predict-mm-probe.v3")
PROBE_PATTERN = "^" + os.path.join(HOME, "probe", "predict-mm-probe")
LIVE_SCRIPT = os.path.join(HOME, "probe", "tokyo_live.sh")
PROBE_ROOT_GLOBS = ["probe_live*", "probe_logs*", "probe_gw_dry_*"]
PROBE_FILE_RE = re.compile(r"^(probe_\d{10}_\d{6}\.jsonl\.gz|state(-dry)?\.json|supervisor\.log|probe\.out|run\.out|summary\.txt)$")
REC_FILE_RE = re.compile(r"^\d{10}_\d{6}\.log\.gz$")
MAX_CHUNK = 64 * 1024 * 1024


def token():
    with open(os.path.join(GW_DIR, "token")) as f:
        return f.read().strip()


def sh(cmd, timeout=20):
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return (r.stdout + r.stderr).strip()
    except subprocess.TimeoutExpired:
        return "(超时)"


def probe_pids():
    out = subprocess.run(["pgrep", "-f", PROBE_PATTERN], capture_output=True, text=True).stdout.split()
    return [p for p in out if p.isdigit()]


def recorder_data_dir():
    pids = subprocess.run(["pgrep", "-f", "pfrec5"], capture_output=True, text=True).stdout.split()
    for p in pids:
        try:
            d = os.path.join(os.readlink("/proc/%s/cwd" % p), "data")
            if os.path.isdir(d):
                return os.path.realpath(d)
        except OSError:
            continue
    d = os.environ.get("GW_RECORDER_DATA")
    return os.path.realpath(d) if d and os.path.isdir(d) else None


def probe_roots():
    roots = set()
    for g in PROBE_ROOT_GLOBS:
        for d in glob.glob(os.path.join(HOME, g)):
            if os.path.isdir(d):
                roots.add(os.path.realpath(d))
    return roots


def allowed(rel):
    """相对 HOME 的路径 → 真实路径;不在白名单里返回 None"""
    if not rel or "\x00" in rel:
        return None
    p = os.path.realpath(os.path.join(HOME, rel))
    if not os.path.isfile(p):
        return None
    parent, name = os.path.dirname(p), os.path.basename(p)
    if parent in probe_roots() and PROBE_FILE_RE.match(name):
        return p
    rd = recorder_data_dir()
    if rd and parent == rd and REC_FILE_RE.match(name):
        return p
    return None


def list_files(root):
    rows = []
    if root == "probe":
        dirs = sorted(probe_roots())
        rx = PROBE_FILE_RE
    elif root == "recorder":
        rd = recorder_data_dir()
        dirs = [rd] if rd else []
        rx = REC_FILE_RE
    else:
        return None
    for d in dirs:
        for n in sorted(os.listdir(d)):
            p = os.path.join(d, n)
            rel = os.path.relpath(p, HOME)
            # 与下载同一套白名单检查(软链指向白名单外的文件不列出)
            if rx.match(n) and allowed(rel):
                st = os.stat(p)
                rows.append({"path": rel, "size": st.st_size, "mtime": int(st.st_mtime)})
    return rows


def status():
    live = os.path.join(HOME, "probe_live3")
    parts = {
        "time_utc": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
        "probe_processes": sh("pgrep -af %s | cut -c1-160" % json.dumps(PROBE_PATTERN)),
        "recorder_processes": sh("pgrep -af pfrec5 | grep -v pgrep | cut -c1-120"),
        "live_dir": os.path.realpath(live) if os.path.exists(live) else None,
        "supervisor_tail": sh("tail -8 %s/supervisor.log" % live) if os.path.exists(live) else None,
        "fills": sh("zcat %s/probe_*.jsonl.gz 2>/dev/null | grep -c '\"kind\":\"fill\"'" % live, 60) if os.path.exists(live) else None,
        "last_events": sh("zcat %s/probe_*.jsonl.gz 2>/dev/null | grep -E '\"kind\":\"(halt|exit|balance|fatal)\"' | tail -3 | cut -c1-300" % live, 60) if os.path.exists(live) else None,
        "disk_free_gb": round(shutil.disk_usage(HOME).free / 1e9, 1),
        "load": os.getloadavg(),
    }
    return parts


def start_live(body):
    hours, loss, rounds = body.get("hours", 24), body.get("loss", 30), body.get("rounds", 0)
    ok = all(isinstance(v, int) and not isinstance(v, bool) for v in (hours, loss, rounds))
    if not (ok and 1 <= hours <= 48 and 1 <= loss <= 100 and 0 <= rounds <= 300):
        return 400, {"error": "hours 1~48、loss 1~100、rounds 0~300,都必须是整数"}
    if probe_pids():
        return 409, {"error": "已有探针在跑,不启动", "processes": status()["probe_processes"]}
    if not os.path.exists(LIVE_SCRIPT):
        return 500, {"error": "找不到 %s" % LIVE_SCRIPT}
    env = {k: v for k, v in os.environ.items() if not k.startswith("PREDICT_")}
    env.update(PROBE_HOURS=str(hours), PROBE_LOSS=str(loss), PROBE_ROUNDS=str(rounds))
    log = os.path.join(GW_DIR, "start_%s.log" % time.strftime("%m%d_%H%M%S"))
    with open(log, "w") as f:
        r = subprocess.run(["bash", LIVE_SCRIPT], env=env, stdout=f, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, timeout=120)
    with open(log) as f:
        out = f.read()[-4000:]
    return (200 if r.returncode == 0 else 500), {"returncode": r.returncode, "output": out, "processes": status()["probe_processes"]}


def stop_probe():
    before = probe_pids()
    if not before:
        return 200, {"msg": "没有探针在跑"}
    subprocess.run(["pkill", "-INT", "-f", PROBE_PATTERN])
    for _ in range(40):
        time.sleep(0.5)
        if not probe_pids():
            break
    left = probe_pids()
    return 200, {"stopped": not left, "pids_before": before, "pids_left": left}


def dryrun(body):
    secs = body.get("secs", 600)
    if not (isinstance(secs, int) and 10 <= secs <= 4 * 3600):
        return 400, {"error": "secs 必须是 10~14400 的整数"}
    if probe_pids():
        return 409, {"error": "已有探针在跑,不启动空跑"}
    out = os.path.join(HOME, "probe_gw_dry_%s" % time.strftime("%m%d_%H%M%S", time.gmtime()))
    os.makedirs(out)
    env = {k: v for k, v in os.environ.items() if not k.startswith("PREDICT_")}  # 空跑不带任何凭证
    with open(os.path.join(out, "run.out"), "w") as f:
        subprocess.Popen([PROBE_BIN, "--dry-run", "--quiet", "--log-dir", out, "--duration-s", str(secs)], env=env, stdout=f, stderr=subprocess.STDOUT,
                         stdin=subprocess.DEVNULL, start_new_session=True)
    time.sleep(3)
    return 200, {"log_dir": os.path.relpath(out, HOME), "processes": status()["probe_processes"]}


class H(http.server.BaseHTTPRequestHandler):
    server_version = "tokyo-gw/1"

    def log_message(self, fmt, *args):  # 默认日志会打到 stderr,改为写文件(不含口令)
        pass

    def _access(self, code):
        try:
            with open(os.path.join(GW_DIR, "access.log"), "a") as f:
                f.write("%s %s %s %s %d\n" % (time.strftime("%F %T"), self.headers.get("cf-connecting-ip", self.client_address[0]), self.command,
                                                self.path.split("?")[0], code))
        except OSError:
            pass

    def _json(self, code, obj):
        b = json.dumps(obj, ensure_ascii=False, indent=1).encode()
        self.send_response(code)
        self.send_header("content-type", "application/json; charset=utf-8")
        self.send_header("content-length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)
        self._access(code)

    def _authed(self):
        got = self.headers.get("authorization", "")
        ok = got.startswith("Bearer ") and hmac.compare_digest(got[7:].strip().encode(), token().encode())
        if not ok:
            time.sleep(0.5)
            self._json(401, {"error": "unauthorized"})
        return ok

    def _body(self):
        n = int(self.headers.get("content-length") or 0)
        if n > 10000:
            return None
        try:
            return json.loads(self.rfile.read(n) or b"{}")
        except ValueError:
            return None

    def do_GET(self):
        if not self._authed():
            return
        u = urllib.parse.urlparse(self.path)
        q = dict(urllib.parse.parse_qsl(u.query))
        if u.path == "/status":
            return self._json(200, status())
        if u.path == "/files":
            rows = list_files(q.get("root", "probe"))
            return self._json(200, rows) if rows is not None else self._json(400, {"error": "root 只能是 probe 或 recorder"})
        if u.path == "/file":
            p = allowed(q.get("path", ""))
            if not p:
                return self._json(403, {"error": "不在白名单"})
            size = os.path.getsize(p)
            try:
                off = max(0, int(q.get("offset", 0)))
                ln = min(int(q.get("length", MAX_CHUNK)), MAX_CHUNK, max(0, size - off))
            except ValueError:
                return self._json(400, {"error": "offset/length 必须是整数"})
            self.send_response(200)
            self.send_header("content-type", "application/octet-stream")
            self.send_header("content-length", str(ln))
            self.send_header("x-file-size", str(size))
            self.end_headers()
            with open(p, "rb") as f:
                f.seek(off)
                left = ln
                while left > 0:
                    b = f.read(min(1 << 20, left))
                    if not b:
                        break
                    self.wfile.write(b)
                    left -= len(b)
            return self._access(200)
        return self._json(404, {"error": "未知路径"})

    def do_POST(self):
        if not self._authed():
            return
        body = self._body()
        if body is None or not isinstance(body, dict):
            return self._json(400, {"error": "请求体必须是 JSON 对象"})
        path = urllib.parse.urlparse(self.path).path
        if path == "/start":
            return self._json(*start_live(body))
        if path == "/stop":
            return self._json(*stop_probe())
        if path == "/dryrun":
            return self._json(*dryrun(body))
        return self._json(404, {"error": "未知路径"})


class Server(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def main():
    os.makedirs(GW_DIR, exist_ok=True)
    token()  # 没有口令文件直接退出
    Server(("127.0.0.1", PORT), H).serve_forever()


if __name__ == "__main__":
    main()
