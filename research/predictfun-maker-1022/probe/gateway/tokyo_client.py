#!/usr/bin/env python3
"""云端调用东京网关的客户端(标准库)。地址与口令从环境变量 TOKYO_GW_URL / TOKYO_GW_TOKEN 读,
没有则读 ~/.tokyo_gw/url 与 ~/.tokyo_gw/token。口令不打印。

    python3 tokyo_client.py status
    python3 tokyo_client.py start --hours 1 --loss 30 --rounds 3   # 启动实盘:必须先得到 Luke 在对话里的明确同意
    python3 tokyo_client.py stop
    python3 tokyo_client.py dryrun --secs 600
    python3 tokyo_client.py ls [probe|recorder]
    python3 tokyo_client.py get <相对 HOME 的路径 ...> --out 目录      # 分块下载,已下载完整的跳过
    python3 tokyo_client.py deploy <本地程序文件> --name v4            # 上传(断点续传)+ 安装;sha256 必须已登记在仓库 probe/bin/SHA256SUMS
"""
import argparse
import json
import os
import sys
import urllib.parse
import urllib.request

CHUNK = 32 * 1024 * 1024


def conf():
    def read(name, env):
        v = os.environ.get(env)
        if v:
            return v.strip()
        p = os.path.expanduser("~/.tokyo_gw/%s" % name)
        return open(p).read().strip() if os.path.exists(p) else None

    url, tok = read("url", "TOKYO_GW_URL"), read("token", "TOKYO_GW_TOKEN")
    if not url or not tok:
        sys.exit("缺网关地址或口令(TOKYO_GW_URL / TOKYO_GW_TOKEN 或 ~/.tokyo_gw/url、token)")
    return url.rstrip("/"), tok


def call(method, path, body=None, timeout=180, raw=False):
    url, tok = conf()
    data = json.dumps(body).encode() if body is not None else (b"{}" if method == "POST" else None)
    req = urllib.request.Request(url + path, data=data, method=method, headers={"Authorization": "Bearer " + tok, "content-type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            b = r.read()
            return (r.status, b, dict(r.headers)) if raw else (r.status, json.loads(b or b"null"))
    except urllib.error.HTTPError as e:
        b = e.read()
        try:
            return (e.code, json.loads(b)) if not raw else (e.code, b, dict(e.headers))
        except ValueError:
            return (e.code, b.decode(errors="replace")) if not raw else (e.code, b, {})


def get_file(rel, out_dir):
    dst = os.path.join(out_dir, rel)
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    have = os.path.getsize(dst) if os.path.exists(dst) else 0
    off, size = have, None
    with open(dst, "ab") as f:
        while size is None or off < size:
            q = urllib.parse.urlencode({"path": rel, "offset": off, "length": CHUNK})
            st, b, h = call("GET", "/file?" + q, raw=True, timeout=600)
            if st != 200:
                raise SystemExit("下载 %s 失败:%s %s" % (rel, st, b[:200]))
            size = int(h.get("x-file-size") or h.get("X-File-Size"))
            if off > size:  # 远端文件变短(被重写):重新下载
                f.truncate(0)
                off = 0
                continue
            f.write(b)
            off += len(b)
            if not b:
                break
    return dst, off


def deploy(path, name, chunk=8 * 1024 * 1024):
    size = os.path.getsize(path)
    off = 0
    with open(path, "rb") as f:
        while off < size:
            f.seek(off)
            b = f.read(chunk)
            url, tok = conf()
            q = urllib.parse.urlencode({"name": name, "offset": off})
            req = urllib.request.Request(url + "/deploy/upload?" + q, data=b, method="POST",
                                         headers={"Authorization": "Bearer " + tok, "content-type": "application/octet-stream"})
            try:
                with urllib.request.urlopen(req, timeout=600) as r:
                    off = json.loads(r.read())["have"]
            except urllib.error.HTTPError as e:
                d = json.loads(e.read() or b"{}")
                if e.code == 409 and "have" in d:  # 续传:按网关已收到的字节数接着传
                    off = d["have"] if d["have"] <= size else 0
                    continue
                raise SystemExit("上传失败:%s %s" % (e.code, d))
            print("已上传 %d / %d 字节" % (off, size))
    return call("POST", "/deploy/install", {"name": name})


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status")
    s = sub.add_parser("start")
    s.add_argument("--hours", type=int, default=24)
    s.add_argument("--loss", type=int, default=30)
    s.add_argument("--rounds", type=int, default=0)
    sub.add_parser("stop")
    d = sub.add_parser("dryrun")
    d.add_argument("--secs", type=int, default=600)
    ls = sub.add_parser("ls")
    ls.add_argument("root", nargs="?", default="probe")
    g = sub.add_parser("get")
    g.add_argument("paths", nargs="+")
    g.add_argument("--out", default="tokyo_files")
    dp = sub.add_parser("deploy")
    dp.add_argument("file")
    dp.add_argument("--name", required=True)
    a = ap.parse_args(argv)
    if a.cmd == "status":
        r = call("GET", "/status")
    elif a.cmd == "start":
        r = call("POST", "/start", {"hours": a.hours, "loss": a.loss, "rounds": a.rounds})
    elif a.cmd == "stop":
        r = call("POST", "/stop")
    elif a.cmd == "dryrun":
        r = call("POST", "/dryrun", {"secs": a.secs})
    elif a.cmd == "deploy":
        r = deploy(a.file, a.name)
    elif a.cmd == "ls":
        r = call("GET", "/files?root=" + urllib.parse.quote(a.root))
    else:
        for p in a.paths:
            dst, n = get_file(p, a.out)
            print("%s  %d 字节" % (dst, n))
        return 0
    print(json.dumps(r[1], ensure_ascii=False, indent=1))
    return 0 if r[0] == 200 else 1


if __name__ == "__main__":
    sys.exit(main())
