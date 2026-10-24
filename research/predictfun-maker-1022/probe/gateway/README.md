# 东京 HTTPS 网关

云端会话出站只能走 HTTPS 代理,连不了 SSH(原始 TCP)和 WebSocket。所以在东京上跑一个只认白名单操作的小服务 `tokyo_gateway.py`,
只监听 127.0.0.1:8787,由 Cloudflare 免费隧道(trycloudflare.com)转成 https 地址;东京只往外连,防火墙不开新端口。

| 操作 | 说明 |
|---|---|
| `GET /status` | 探针 / 录制器进程、本次运行 supervisor.log 末尾、成交数、最近 halt/exit/balance、磁盘、负载 |
| `POST /start {"hours":24,"loss":100,"rounds":0}` | 调 `~/probe/tokyo_live.sh` 启动实盘(hours 1~48,loss 1~100 = **跨次累计**的亏损上限,rounds 0~300);已有探针在跑就拒绝。**必须先有 Luke 在对话里的明确同意** |
| `POST /stop` | `pkill -INT` 探针(先全撤再退出) |
| `POST /dryrun {"secs":600}` | 空跑,不带任何凭证;用 `~/probe/predict-mm-probe.current` |
| `POST /deploy/upload?name=v4&offset=N`、`POST /deploy/install {"name":"v4"}` | 上传探针程序(分段续传)并安装。sha256 必须登记在仓库 predict-probe 分支的 `probe/bin/SHA256SUMS`(见 `../bin/README.md`);装好后 `predict-mm-probe.current` 指向它。正在跑的探针不受影响 |
| `GET /files?root=probe\|recorder`、`GET /file?path=…&offset=&length=` | 列出 / 分块下载探针日志(probe_*.jsonl.gz、state.json、supervisor.log、probe.out 等)和录制器 data/*.log.gz;其他文件一律 403(软链也按真实路径判) |

- 每个请求带 `Authorization: Bearer <口令>`。口令在东京 `~/.tokyo_gw/token`、云端 `~/.tokyo_gw/token`(或环境变量 `TOKYO_GW_TOKEN`),都是 chmod 600,不进 git、不打印。
- 不提供任意命令执行;不碰录制器 pfrec5、不读 `~/pflat/.env`。访问记录在东京 `~/.tokyo_gw/access.log`(不含口令)。
- systemd 单元 `tokyo-gw`(KillMode=process:网关重启不会杀它启动的探针)和 `tokyo-gw-tunnel`。

## 安装 / 更新(在本机执行一条命令)

```bash
ssh -i ~/.ssh/lightsail_eu_north_1.pem ubuntu@54.95.203.70 "curl -fsSL https://raw.githubusercontent.com/lukedesong/polymarket-live-copy/<提交号>/research/predictfun-maker-1022/probe/gateway/setup.sh | GW_REF=<提交号> GW_TOKEN=<口令> bash"
```

最后一行打印网关地址 `https://….trycloudflare.com`。**快速隧道的地址在隧道重启(包括东京重启)后会变**:重新执行上面的命令(可以不带 GW_TOKEN)会打印当前地址。

## 云端用法

```bash
export TOKYO_GW_URL=https://….trycloudflare.com      # 或写进 ~/.tokyo_gw/url
python3 tokyo_client.py status
python3 tokyo_client.py ls probe
python3 tokyo_client.py get probe_live3/probe_2026102420_200000.jsonl.gz --out ~/tokyo_files
python3 tokyo_client.py stop
python3 tokyo_client.py deploy <程序文件> --name v4      # 校验码先登记进 ../bin/SHA256SUMS 并推送
python3 tokyo_client.py start --hours 24 --loss 100  # 先问 Luke
```

测试:`python3 -m unittest test_gateway test_tokyo_live -v`(本机起服务,假 HOME / 假探针 / 假录制器:口令、白名单与软链、分块下载、参数校验、启停、空跑不带凭证、
上传安装与校验码不符 / 不覆盖;启动脚本的自动定参数、跨次账本)。
