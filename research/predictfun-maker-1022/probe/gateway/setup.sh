#!/usr/bin/env bash
# 在东京机上安装 / 更新 HTTPS 网关(tokyo_gateway.py + Cloudflare 免费隧道)。可重复执行;重复执行只更新网关代码,
# 隧道不重启(地址不变),最后打印当前网关地址。
#
# 由本机一条命令调用(GW_REF = 固定的提交号,GW_TOKEN = 口令;只在首次或换口令时需要给 GW_TOKEN):
#   ssh -i <东京私钥> ubuntu@54.95.203.70 "curl -fsSL https://raw.githubusercontent.com/lukedesong/polymarket-live-copy/<提交号>/research/predictfun-maker-1022/probe/gateway/setup.sh | GW_REF=<提交号> GW_TOKEN=<口令> bash"
#
# 不碰录制器 pfrec5、不读 ~/pflat/.env。卸载:sudo systemctl disable --now tokyo-gw tokyo-gw-tunnel
set -euo pipefail
REF="${GW_REF:?需要 GW_REF(提交号)}"
REPO=lukedesong/polymarket-live-copy
D="$HOME/.tokyo_gw"
mkdir -p "$D" && chmod 700 "$D"
if [ -n "${GW_TOKEN:-}" ]; then
  ( umask 077; printf '%s\n' "$GW_TOKEN" > "$D/token" )
fi
[ -s "$D/token" ] || { echo "缺口令:首次安装要给 GW_TOKEN"; exit 1; }
chmod 600 "$D/token"

echo "下载网关代码(提交 $REF)…"
curl -fsSL "https://raw.githubusercontent.com/$REPO/$REF/research/predictfun-maker-1022/probe/gateway/tokyo_gateway.py" -o "$D/tokyo_gateway.py.new"
python3 -m py_compile "$D/tokyo_gateway.py.new"
mv "$D/tokyo_gateway.py.new" "$D/tokyo_gateway.py"
echo "$REF" > "$D/ref"

# 同一提交的 tokyo_live.sh(启动脚本);有探针在跑时不替换,旧版留 .prev
NEW_LIVE="$(mktemp)"
curl -fsSL "https://raw.githubusercontent.com/$REPO/$REF/research/predictfun-maker-1022/probe/tokyo_live.sh" -o "$NEW_LIVE"
bash -n "$NEW_LIVE"
if pgrep -f "^$HOME/probe/predict-mm-probe" >/dev/null; then
  echo "有探针在跑:tokyo_live.sh 这次不替换"
elif ! cmp -s "$NEW_LIVE" "$HOME/probe/tokyo_live.sh"; then
  [ -f "$HOME/probe/tokyo_live.sh" ] && cp "$HOME/probe/tokyo_live.sh" "$HOME/probe/tokyo_live.sh.prev"
  install -m 755 "$NEW_LIVE" "$HOME/probe/tokyo_live.sh" && echo "已更新 ~/probe/tokyo_live.sh(旧版在 tokyo_live.sh.prev)"
fi
rm -f "$NEW_LIVE"

if ! command -v cloudflared >/dev/null 2>&1; then
  echo "安装 cloudflared…"
  ARCH="$(dpkg --print-architecture 2>/dev/null || echo amd64)"
  curl -fsSL -o /tmp/cloudflared "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-$ARCH"
  sudo install -m 755 /tmp/cloudflared /usr/local/bin/cloudflared && rm -f /tmp/cloudflared
fi

# 网关:只监听 127.0.0.1:8787;KillMode=process —— 网关重启时不杀它启动的探针
sudo tee /etc/systemd/system/tokyo-gw.service >/dev/null <<UNIT
[Unit]
Description=Tokyo HTTPS gateway (whitelisted ops only)
After=network-online.target

[Service]
User=$USER
WorkingDirectory=$HOME
ExecStart=/usr/bin/python3 $D/tokyo_gateway.py
Restart=always
RestartSec=3
KillMode=process
Nice=5

[Install]
WantedBy=multi-user.target
UNIT

sudo tee /etc/systemd/system/tokyo-gw-tunnel.service >/dev/null <<UNIT
[Unit]
Description=Cloudflare quick tunnel for tokyo-gw
After=network-online.target tokyo-gw.service

[Service]
User=$USER
ExecStart=/usr/local/bin/cloudflared tunnel --no-autoupdate --url http://127.0.0.1:8787
Restart=always
RestartSec=5
Nice=5

[Install]
WantedBy=multi-user.target
UNIT

sudo systemctl daemon-reload
sudo systemctl enable tokyo-gw tokyo-gw-tunnel >/dev/null 2>&1
sudo systemctl restart tokyo-gw
sudo systemctl start tokyo-gw-tunnel   # 已在跑就不动(地址不变)

URL=""
for _ in $(seq 1 40); do
  URL="$(sudo journalctl -u tokyo-gw-tunnel -b -o cat 2>/dev/null | grep -o 'https://[a-z0-9-]*\.trycloudflare\.com' | tail -1 || true)"
  [ -n "$URL" ] && break
  sleep 2
done
sleep 2
if curl -fsS -m 10 -H "Authorization: Bearer $(cat "$D/token")" http://127.0.0.1:8787/status >/dev/null; then echo "网关本机自检:正常"; else echo "网关本机自检:失败,看 sudo journalctl -u tokyo-gw"; fi
echo "======================================"
echo "网关地址: ${URL:-(没取到,看 sudo journalctl -u tokyo-gw-tunnel)}"
echo "======================================"
